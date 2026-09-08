"""Telegram 存储后端（参考 otterhub-server 的 tg-adapter.ts / tg-tools.ts / tg-pool.ts）。

职责：把"字节"存进 Telegram 频道/群组，把"file_id 索引"交回 SQLite。

核心交互（与 otterhub 完全一致）：
  1. 上传：POST https://api.telegram.org/bot{TOKEN}/sendDocument
            multipart: chat_id + document(二进制)
            返回 result.document.file_id（file_id 与 bot 绑定，下载须用同一 bot token）
  2. 下载：GET  https://api.telegram.org/bot{TOKEN}/getFile?file_id=xxx
            -> result.file_path
            GET  https://api.telegram.org/file/bot{TOKEN}/{file_path}   （支持 Range）
  3. 分片：> 20MB 按 CHUNK_SIZE 切，每片独立 file_id，下载时按分片拼接（每片各自带 Range）
  4. 限流：每聊天 ~1 msg/s；429 带 retry_after，自动换 bot 槽位重试
  5. 空文件：Telegram 拒绝 0 字节上传 -> 用 1 字节占位并在元数据打标（空文件短路返回）

多 bot 池：file_id 与 bot 绑定，故上传时记录所用 slot，下载时按 slot 取 token。
TG_API_BASE：可指向自建代理（国内/被墙场景），所有请求加该前缀。
"""
import base64
import collections
import datetime
import http.client
import json
import re
import socket
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class TGError(Exception):
    pass


# 业务型 4xx（换代理无意义，直接抛出交给上层）：429 单独处理（限流，换 bot 重试）
_BUSINESS_4XX = ("400", "401", "403", "404", "409", "413")


def _http_code_of(msg):
    """从错误消息里提取 HTTP 状态码（如 'HTTP 429' / 'getFile: 403'）。"""
    m = re.search(r"(?:^|\s)(\d{3})\b", msg or "")
    return m.group(1) if m else None


def _log(msg):
    """统一的后台日志：带本地时间戳 + [tg] 模块前缀，便于定位。

    所有 Telegram 交互（上传 / 下载 / 代理连接 / 重试）都经过这里打印，
    连接失败、API 报错、文件头异常等都能在日志里看到具体原因。
    """
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}][tg] {msg}", flush=True)


def _fmt_speed(n, dt):
    """字节数/秒 → 人类可读速率字符串（与 webdav._fmt_speed 同款）。"""
    if not dt or dt <= 0:
        return "∞"
    bps = n / dt
    if bps >= 1024 * 1024 * 1024:
        return f"{bps / 1024 / 1024 / 1024:.2f} GB/s"
    if bps >= 1024 * 1024:
        return f"{bps / 1024 / 1024:.2f} MB/s"
    if bps >= 1024:
        return f"{bps / 1024:.2f} KB/s"
    return f"{bps:.0f} B/s"


def _mp_boundary():
    return "----tgwebdav" + uuid.uuid4().hex


def _safe_filename(name):
    """把任意路径清洗成可安全放进 multipart ``filename`` 的字节。

    - 取 basename（去掉父目录）；
    - 去掉会破坏 Content-Disposition 头/多部分边界的字符（引号、回车、换行、NUL）；
    - 超长截断到 200 字符（Telegram 对文件名长度没有硬限制，但过长是无意义的负担）；
    - 兜底为 ``part.bin``，保证永远有名字。
    """
    if not name:
        return "part.bin"
    base = name.replace("\\", "/").rsplit("/", 1)[-1]
    cleaned = "".join(ch for ch in base if ch not in '"\r\n\x00').strip()
    if not cleaned:
        return "part.bin"
    if len(cleaned) > 200:
        cleaned = cleaned[:200]
    return cleaned


def _build_multipart(boundary, fields, files):
    """构造 multipart/form-data 请求体。fields=[(name,value)], files=[(name,fname,ctype,data)]。"""
    if isinstance(boundary, str):
        boundary = boundary.encode()
    crlf = b"\r\n"
    body = b""
    for name, value in fields:
        body += b"--" + boundary + crlf
        body += b'Content-Disposition: form-data; name="' + name.encode() + b'"' + crlf
        body += crlf + str(value).encode() + crlf
    for name, fname, ctype, data in files:
        body += b"--" + boundary + crlf
        body += (
            b'Content-Disposition: form-data; name="' + name.encode()
            + b'"; filename="' + fname.encode() + b'"' + crlf
        )
        body += b"Content-Type: " + ctype.encode() + crlf
        body += crlf + data + crlf
    body += b"--" + boundary + b"--" + crlf
    return body


class TelegramBackend:
    def __init__(self, slots, api_base="https://api.telegram.org", rate_limit=1.0,
                 proxy_token=None, rotate=True, proxy_pools=None):
        self.slots = slots  # [{"token","chat_id","api_base"(可数组),"proxy_token"(可数组)}]
        self.api_base = api_base.rstrip("/")  # 全局默认 API 基址（槽位级可覆盖）
        self.rate_limit = float(rate_limit)
        # 自建 TG API 代理需要的访问令牌（官方 API 场景留空）——全局默认，槽位级可覆盖
        self.proxy_token = (proxy_token or "").strip() or None
        # 多 bot 池时是否轮转分摊（关掉则固定优先用第一个槽位 = 主备模式）
        self.rotate = bool(rotate)
        # 全局代理候选池（所有 bot 共享）：[(api_base, proxy_token), ...]
        self.proxy_pools = [tuple(p) for p in (proxy_pools or [])]
        self._lock = threading.Lock()
        self._last = {}  # slot idx -> 上次发送时间
        self._path_cache = {}  # file_id -> (file_path, expire_ts)
        self._path_cache_ttl = 50 * 60  # TG file_path 有效期 1h，缓存 50min
        # 连接池：按 (scheme,host,port) 复用 keep-alive 连接，避免视频播放时每个
        # Range 请求都重做 TLS 握手（播放器会高频发 Range，复用连接大幅降低首字节延迟）。
        self._conn_pool = {}  # key -> collections.deque([conn, ...])
        self._conn_lock = threading.Lock()
        self._slot_lock = threading.Lock()
        self._slot_cursor = 0
        self._proxy_lock = threading.Lock()
        self._proxy_cursor = {}  # slot idx -> 当前代理候选游标（负载均衡轮询）
        # 每个槽位的代理候选列表：自带 apiBase 在前，全局 proxy_pools 在后；
        # 请求级轮询分摊 + 失败自动切换（多个 TG 代理的负载均衡与容灾）
        self._candidates = {}
        for i, s in enumerate(self.slots):
            self._candidates[i] = self._build_candidates(s)
        _log(f"TelegramBackend 初始化: slots={len(self.slots)} "
             f"rate_limit={self.rate_limit}s rotate={self.rotate} "
             f"global_api_base={self.api_base} global_proxy_auth={'on' if self.proxy_token else 'off'} "
             f"global_proxy_pools={len(self.proxy_pools)}")
        for i, s in enumerate(self.slots):
            cands = self._candidates[i]
            _log(f"  槽位 {i}: chat_id={s['chat_id']} 代理候选数={len(cands)} "
                 f"首候选={cands[0][0] if cands else self.api_base}")

    # ---------- 代理候选列表（多个 TG 代理） ----------
    def _build_candidates(self, slot):
        """构建某 bot 的代理候选列表 [(api_base, proxy_token), ...]。

        顺序：① 槽位自带的 apiBase（可数组，配对 proxyToken 数组或全局默认）；
        ② 全局 proxy_pools；③ 兜底全局 api_base + proxy_token。
        """
        cands = []
        bases = slot.get("api_base")
        if bases is None:
            bases = []
        elif isinstance(bases, str):
            bases = [bases]
        toks = slot.get("proxy_token")
        if toks is None:
            toks = []
        elif isinstance(toks, str):
            toks = [toks]
        for i, b in enumerate(bases):
            tk = toks[i] if i < len(toks) else self.proxy_token
            cands.append((b.rstrip("/"), tk))
        for (gb, gt) in self.proxy_pools:
            cands.append((gb.rstrip("/"), gt))
        if not cands:
            cands.append((self.api_base, self.proxy_token))
        return cands

    def _next_slot(self):
        """轮转取下一个起始槽位（线程安全）。"""
        with self._slot_lock:
            idx = self._slot_cursor
            self._slot_cursor = (idx + 1) % len(self.slots)
            return idx

    # ---------- 每槽位的 api_base / proxy_token 解析（单值回退，兼容旧调用） ----------
    def _slot_api_base(self, slot):
        """取该槽位实际使用的 API 基址：优先槽位自带，回退全局默认。"""
        ab = slot.get("api_base")
        if isinstance(ab, list):
            ab = ab[0] if ab else None
        return (ab or self.api_base).rstrip("/")

    def _slot_proxy_token(self, slot):
        """取该槽位实际使用的代理令牌：优先槽位自带，回退全局默认。"""
        pt = slot.get("proxy_token")
        if isinstance(pt, list):
            pt = pt[0] if pt else None
        return pt or self.proxy_token

    # ---------- URL / 认证 ----------
    def _api_url(self, token, method, api_base=None):
        base = (api_base or self.api_base).rstrip("/")
        return f"{base}/bot{token}/{method}"

    def _file_url(self, token, file_path, api_base=None):
        base = (api_base or self.api_base).rstrip("/")
        return f"{base}/file/bot{token}/{file_path}"

    def _prepare(self, req, proxy_token=None):
        """统一装配请求：代理鉴权 + 自定义 UA。

        urllib 默认的 ``Python-urllib/x.y`` 会被 Cloudflare 等 WAF 直接 403
        （error code 1010），所以这里换成固定的 UA。``proxy_token`` 缺省时
        回退到全局/槽位默认；调用方传入槽位级令牌即可实现「不同 bot 走不同代理」。
        """
        req.add_header("User-Agent", "TelegramWebDAV/1.0 (+python-urllib)")
        tk = proxy_token if proxy_token is not None else self.proxy_token
        if tk:
            req.add_header("Authorization", f"Bearer {tk}")
        return req

    # ---------- 连接池（keep-alive 复用，针对视频高频 Range 优化） ----------
    def _conn_key(self, api_base):
        p = urllib.parse.urlparse(api_base or self.api_base)
        scheme = (p.scheme or "https").lower()
        port = p.port or (443 if scheme == "https" else 80)
        return (scheme, p.hostname, port)

    def _open_conn(self, api_base):
        p = urllib.parse.urlparse(api_base or self.api_base)
        scheme = (p.scheme or "https").lower()
        host = p.hostname
        port = p.port or (443 if scheme == "https" else 80)
        if scheme == "https":
            return http.client.HTTPSConnection(host, port, timeout=180)
        return http.client.HTTPConnection(host, port, timeout=180)

    def _acquire_conn(self, api_base):
        key = self._conn_key(api_base)
        with self._conn_lock:
            dq = self._conn_pool.get(key)
            if dq:
                return dq.popleft()
        return None

    def _release_conn(self, api_base, conn, alive):
        if conn is None:
            return
        if not alive:
            try:
                conn.close()
            except Exception:
                pass
            return
        key = self._conn_key(api_base)
        with self._conn_lock:
            self._conn_pool.setdefault(key, collections.deque()).append(conn)

    def _do_get(self, api_base, path, proxy_token=None, rng=None, timeout=180):
        """发一次 GET（getFile / 文件字节通用），返回 (conn, resp)。

        连接优先从连接池取（keep-alive 复用）；建连或发送失败自动换一条新连接重试一次。
        调用方读完整响应体后须用 _release_conn 归还（alive=not resp.will_close）。
        """
        last = None
        for _ in range(2):
            conn = self._acquire_conn(api_base)
            if conn is None:
                conn = self._open_conn(api_base)
            headers = {"User-Agent": "TelegramWebDAV/1.0 (+python-urllib)"}
            if proxy_token:
                headers["Authorization"] = f"Bearer {proxy_token}"
            if rng:
                headers["Range"] = rng
            try:
                conn.request("GET", path, headers=headers)
                resp = conn.getresponse()
                return conn, resp
            except (http.client.HTTPException, OSError, socket.timeout) as e:
                last = e
                try:
                    conn.close()
                except Exception:
                    pass
        raise last or TGError("连接失败")

    # ---------- 限流 ----------
    def _rate_wait(self, idx):
        with self._lock:
            last = self._last.get(idx, 0.0)
            wait = self.rate_limit - (time.time() - last)
            if wait > 0:
                time.sleep(wait)
            self._last[idx] = time.time()

    # ---------- 上传 ----------
    def upload_chunk(self, data, prefer_slot=None, file_name=None):
        """上传一块二进制到 Telegram，返回 (file_id, slot_index, message_id)。失败时抛 TGError。

        ``file_name`` 是「原始文件名」（webdav 层传进来的 basename，多分片时带 .partNN
        后缀）。它会作为 sendDocument 的 ``filename`` 写进频道消息——这样在 Telegram
        里浏览频道时看到的就是原文件名，而不是千篇一律的 ``part.bin``（参考 otterhub-server
        的 dav_gateway 把 ``file_name`` 一路透传到后端）。
        """
        if not self.slots:
            _log("upload_chunk 失败: Telegram 未配置（slots 为空）")
            raise TGError("Telegram 未配置（设置 TG_BOT_TOKEN/TG_CHAT_ID 或 TG_BOT_POOLS）")
        n = len(self.slots)
        # 轮转分摊：未指定槽位时从游标处开始，让分片均匀落到各个 bot/频道。
        # 若关闭轮转（主备模式）则固定从 0 开始——只有失败/429 才会切到下一个。
        start = (prefer_slot % n) if prefer_slot is not None else (
            self._next_slot() if self.rotate else 0
        )
        _log(f"upload_chunk 启动: file={file_name!r} size={len(data)}B "
             f"rotate={self.rotate} start_slot={start} total_slots={n}")
        tried = set()
        idx = start
        last_err = None
        fname = _safe_filename(file_name)
        # 在槽位间轮转（参考 tg-pool：429 换槽不消耗重试次数）
        for _ in range(n):
            tried.add(idx)
            slot = self.slots[idx]
            self._rate_wait(idx)
            try:
                fid, mid = self._send_document(slot, data, fname)
                _log(f"upload_chunk 成功: slot={idx} file_id={fid} message_id={mid}")
                return fid, idx, mid
            except TGError as e:
                last_err = e
                msg = str(e)
                _log(f"upload_chunk 槽位 {idx} 失败: {msg}")
                # 429 / 限流：换下一个未试过的槽位
                if "429" in msg or "Too Many Requests" in msg or "flood" in msg.lower():
                    idx = (idx + 1) % n
                    if idx in tried:
                        _log("upload_chunk 所有槽位均已尝试，限流重试结束")
                        break
                    _log(f"upload_chunk 因限流切换到槽位 {idx}")
                    continue
                # 其他错误：也试下一个槽位（最多一轮）
                idx = (idx + 1) % n
                if idx in tried:
                    _log("upload_chunk 所有槽位均已尝试，结束")
                    break
                _log(f"upload_chunk 切换到槽位 {idx}")
                continue
        _log(f"upload_chunk 最终失败: {last_err}")
        raise TGError("分片上传失败: " + (str(last_err) if last_err else "未知错误"))

    def _send_document(self, slot, data, file_name="part.bin", retries=3):
        """上传一个分片到 Telegram，返回 (file_id, message_id)。失败时抛 TGError。

        外层遍历该 bot 的全部代理候选（多个 TG 代理）：某个代理网络/5xx 失败时自动
        切换到下一个代理；429 限流（bot 级）直接抛出，由 upload_chunk 走换 bot 逻辑；
        4xx 业务错误（换代理无用）也直接抛出。
        """
        idx = self.slots.index(slot)
        cands = self._candidates.get(idx) or [(self.api_base, self.proxy_token)]
        last = None
        for ci, (api_base, proxy_token) in enumerate(cands):
            try:
                return self._try_send(api_base, proxy_token, slot, data, file_name, retries)
            except TGError as e:
                last = e
                msg = str(e)
                code = _http_code_of(msg)
                if code == "429":
                    _log(f"sendDocument 代理候选 {ci} 触发限流(同 bot)，不再换代理（交给上层换 bot）")
                    raise
                if code in _BUSINESS_4XX:
                    _log(f"sendDocument 代理候选 {ci} 业务错误({code})，换代理无意义: {msg}")
                    raise
                _log(f"sendDocument 代理候选 {ci} 失败({msg})，切换下一代理: api={api_base}")
                continue
        raise last or TGError("所有代理候选均失败")

    def _try_send(self, api_base, proxy_token, slot, data, file_name, retries):
        """用指定 api_base / proxy_token 尝试一次 sendDocument（含内部退避重试）。"""
        boundary = _mp_boundary()
        body = _build_multipart(
            boundary,
            [("chat_id", slot["chat_id"]), ("disable_content_type_detection", "true")],
            [("document", file_name, "application/octet-stream", data)],
        )
        url = self._api_url(slot["token"], "sendDocument", api_base)
        req = urllib.request.Request(url, data=body, method="POST")
        req.add_header("Content-Type", f"multipart/form-data; boundary={boundary}")
        self._prepare(req, proxy_token)
        _log(f"sendDocument 开始: file={file_name!r} size={len(data)}B "
             f"chat_id={slot['chat_id']} api={api_base} "
             f"proxy_auth={'on' if proxy_token else 'off'}")
        last = None
        for attempt in range(retries):
            try:
                with urllib.request.urlopen(req, timeout=240) as resp:
                    raw = resp.read().decode("utf-8", "replace")
                try:
                    js = json.loads(raw)
                except json.JSONDecodeError as e:
                    # 文件头异常 / 非 JSON 响应（如代理返回 HTML 错误页）
                    _log(f"sendDocument 响应解析失败（非 JSON）: attempt={attempt + 1} "
                         f"err={e} body_head={raw[:160]!r}")
                    last = f"响应非 JSON: {raw[:160]}"
                    if attempt < retries - 1:
                        time.sleep(min(30, 2 ** attempt))
                        continue
                    raise TGError(last)
                if not js.get("ok"):
                    _log(f"sendDocument 业务失败: code={js.get('error_code')} "
                         f"desc={js.get('description')} attempt={attempt + 1}")
                    raise TGError(f"HTTP {js.get('error_code')} {js.get('description')}")
                result = js["result"]
                doc = (
                    result.get("document")
                    or result.get("video")
                    or result.get("audio")
                    or {}
                )
                fid = doc.get("file_id")
                if not fid:
                    _log(f"sendDocument 响应缺少 file_id: attempt={attempt + 1} "
                         f"result={json.dumps(result)[:200]}")
                    raise TGError("响应中缺少 file_id: " + json.dumps(result)[:200])
                _log(f"sendDocument 成功: file={file_name!r} file_id={fid} "
                     f"message_id={result.get('message_id')}")
                return fid, result.get("message_id")
            except urllib.error.HTTPError as e:
                text = e.read().decode("utf-8", "replace")
                last = f"HTTP {e.code} {text[:160]}"
                _log(f"sendDocument HTTP 错误: code={e.code} attempt={attempt + 1}/{retries} "
                     f"url={url} body={text[:200]!r}")
                if e.code == 429:
                    ra = 1
                    try:
                        d = json.loads(text)
                        ra = int(d.get("parameters", {}).get("retry_after", 1) or 1)
                    except Exception:
                        pass
                    _log(f"sendDocument 触发限流(429)，等待 {ra}s 后重试")
                    time.sleep(max(ra, 1))
                    continue
                if attempt < retries - 1:
                    time.sleep(min(30, 2 ** attempt))
                    continue
                raise TGError(last)
            except TGError:
                raise
            except Exception as e:  # 网络错误：退避重试
                last = f"network: {e}"
                _log(f"sendDocument 网络错误: {type(e).__name__}: {e} attempt={attempt + 1}/{retries} "
                     f"api={api_base}")
                if attempt < retries - 1:
                    time.sleep(min(30, 2 ** attempt))
                    continue
                raise TGError(last)
        raise TGError(last or "重试耗尽")

    # ---------- 下载 ----------
    def _get_file_path(self, file_id, token, api_base=None, proxy_token=None):
        now = time.time()
        cached = self._path_cache.get(file_id)
        if cached and cached[1] > now:
            _log(f"getFile 命中缓存: file_id={file_id} path={cached[0]}")
            return cached[0]
        base = (api_base or self.api_base).rstrip("/")
        bp = urllib.parse.urlparse(base).path or ""
        path = f"{bp}/bot{token}/getFile?file_id=" + urllib.parse.quote(file_id)
        _log(f"getFile 请求: file_id={file_id} api={base} "
             f"proxy_auth={'on' if proxy_token else 'off'}")
        for attempt in range(3):
            try:
                conn, resp = self._do_get(base, path, proxy_token, timeout=30)
                alive = not getattr(resp, "will_close", False)
                if resp.status != 200:
                    body = resp.read()
                    self._release_conn(base, conn, False)
                    _log(f"getFile HTTP 错误: code={resp.status} attempt={attempt + 1}/3 "
                         f"file_id={file_id} body={body[:200]!r}")
                    if resp.status == 429:
                        time.sleep(2)
                        continue
                    raise TGError(f"getFile HTTP {resp.status}: {body[:160]}")
                raw = resp.read().decode("utf-8", "replace")
                self._release_conn(base, conn, alive)
                try:
                    js = json.loads(raw)
                except json.JSONDecodeError as e:
                    _log(f"getFile 响应解析失败（非 JSON）: attempt={attempt + 1} "
                         f"err={e} body_head={raw[:160]!r}")
                    if attempt < 2:
                        time.sleep(2)
                        continue
                    raise TGError(f"getFile 响应非 JSON: {raw[:160]}")
                if not js.get("ok"):
                    _log(f"getFile 业务失败: code={js.get('error_code')} "
                         f"desc={js.get('description')} attempt={attempt + 1}")
                    raise TGError(f"getFile: {js.get('error_code')} {js.get('description')}")
                fp = js["result"]["file_path"]
                self._path_cache[file_id] = (fp, now + self._path_cache_ttl)
                _log(f"getFile 成功: file_id={file_id} path={fp}")
                return fp
            except (http.client.HTTPException, OSError, socket.timeout) as e:
                _log(f"getFile 网络/其他错误: {type(e).__name__}: {e} attempt={attempt + 1}/3 "
                     f"file_id={file_id} api={base}")
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise TGError(f"getFile: {e}")
        raise TGError("getFile 失败")

    def iter_chunk(self, file_id, slot, start=None, end=None, blk=None):
        """生成器：按 Range 取回单块字节。start/end 为相对该块的字节区间（含端点）。

        ``blk`` 是每次 ``resp.read()`` 的块大小，直接决定「读到多少字节就 yield 一次」，
        也就是流式下发时客户端每隔多久收到一批数据：块越小越平滑、首字节越快，
        块越大系统调用越少。由 webdav 层按 ``TG_STREAM_BLOCK_KB`` 传入（缺省 1MB）。

        遍历该 bot 的全部代理候选：某代理网络/5xx 失败自动切换下一个；4xx/429 直接抛出。
        连接走 keep-alive 连接池（_do_get/_release_conn），视频高频 Range 下省去重复握手。
        """
        blk = int(blk or 1024 * 1024)
        n = len(self.slots)
        sd = self.slots[slot % n]
        token = sd["token"]
        idx = slot % n
        cands = self._candidates.get(idx) or [(self.api_base, self.proxy_token)]
        last = None
        t0 = time.time()  # 本分片下载总计时起点（发起 getFile 之前）
        for ci, (api_base, proxy_token) in enumerate(cands):
            try:
                fp = self._get_file_path(file_id, token, api_base, proxy_token)
                base = (api_base or self.api_base).rstrip("/")
                bp = urllib.parse.urlparse(base).path or ""
                path = f"{bp}/file/bot{token}/{fp}"
                rng = None
                if start is not None:
                    rng = f"bytes={start}-{end}" if end is not None else f"bytes={start}-"
                _log(f"iter_chunk 下载: file_id={file_id} slot={slot} range={rng} path={path} "
                     f"proxy_auth={'on' if proxy_token else 'off'}")
                conn, resp = self._do_get(base, path, proxy_token, rng, timeout=180)
                alive = not getattr(resp, "will_close", False)
                if resp.status >= 400:
                    body = resp.read()
                    self._release_conn(base, conn, False)
                    msg = f"iter_chunk HTTP {resp.status}: {body[:160]!r}"
                    code = str(resp.status)
                    _log(f"iter_chunk 代理候选 {ci} HTTP {code}: {msg}")
                    if code == "429":
                        raise TGError(msg)
                    if code in _BUSINESS_4XX:
                        raise TGError(msg)
                    last = TGError(msg)
                    _log(f"iter_chunk 代理候选 {ci} 失败({msg})，切换下一代理: api={api_base}")
                    continue
                total_yield = 0
                t_first = None  # 首字节到达时刻（TTFB 依据）
                try:
                    while True:
                        b = resp.read(blk)
                        if not b:
                            break
                        if t_first is None:
                            t_first = time.time()
                        total_yield += len(b)
                        yield b
                finally:
                    # 读完整响应体后才归还连接，保证 keep-alive 连接可安全复用
                    self._release_conn(base, conn, alive)
                dt_total = time.time() - t0
                dt_first = (t_first - t0) if t_first is not None else dt_total
                _log(f"iter_chunk 下载完成: file_id={file_id} bytes={total_yield} "
                     f"总耗时={dt_total:.3f}s 首字节={dt_first:.3f}s "
                     f"吞吐={_fmt_speed(total_yield, dt_total)} "
                     f"proxy={api_base}")
                return
            except TGError:
                raise
            except (http.client.HTTPException, OSError, socket.timeout) as e:
                last = e
                _log(f"iter_chunk 代理候选 {ci} 网络/其他错误: {type(e).__name__}: {e} "
                     f"api={api_base}")
                continue
            except Exception as e:
                last = e
                _log(f"iter_chunk 代理候选 {ci} 网络/其他错误: {type(e).__name__}: {e} "
                     f"api={api_base}")
                continue
        raise last or TGError("下载所有代理候选均失败")

    # ---------- webhook 入站消息解析（参考 getTelegramFileFromMessage） ----------
    @staticmethod
    def extract_media(message):
        """从 Telegram 入站消息抽取一个媒体文件描述，返回 dict 或 None。"""
        if not message:
            return None

        def ext_of(name, mime):
            if name and "." in name:
                return name.rsplit(".", 1)[-1].lower()
            return {
                "image/jpeg": "jpg", "image/png": "png", "image/webp": "webp",
                "image/gif": "gif", "video/mp4": "mp4", "video/webm": "webm",
                "audio/mpeg": "mp3", "audio/ogg": "ogg", "application/pdf": "pdf",
                "application/zip": "zip",
            }.get((mime or "").split(";")[0].strip().lower(), "bin")

        if isinstance(message.get("photo"), list) and message["photo"]:
            variants = sorted(
                message["photo"], key=lambda v: v.get("file_size", 0)
            )
            orig = variants[-1]
            fid = orig.get("file_id")
            if not fid:
                return None
            return {
                "file_id": fid,
                "file_name": f"photo_{message.get('message_id', int(time.time()))}.jpg",
                "file_size": orig.get("file_size", 0),
                "content_type": "image/jpeg",
            }

        for key, fallback_mime in [
            ("document", "application/octet-stream"),
            ("video", "video/mp4"),
            ("audio", "audio/mpeg"),
            ("voice", "audio/ogg"),
            ("animation", "video/mp4"),
            ("video_note", "video/mp4"),
            ("sticker", "image/webp"),
        ]:
            data = message.get(key)
            if not data or not data.get("file_id"):
                continue
            raw = data.get("file_name", "")
            mime = data.get("mime_type") or fallback_mime
            ext = ext_of(raw, mime)
            name = raw or f"{key}_{message.get('message_id', int(time.time()))}.{ext}"
            return {
                "file_id": data["file_id"],
                "file_name": name,
                "file_size": int(data.get("file_size", 0) or 0),
                "content_type": mime,
            }
        return None
