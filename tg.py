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
import concurrent.futures
import datetime
import http.client
import json
import os
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


class _ClientGoneEarly(Exception):
    """生成器被外部提前终止（客户端断开或流式写入失败）→ 直接停止，不要 yield/不要换候选。"""


# 业务型 4xx（换代理无意义，直接抛出交给上层）：429 单独处理（限流，换 bot 重试）
_BUSINESS_4XX = ("400", "401", "403", "404", "409", "413")

# 单分片下载的瞬断重试次数：代理偶发 ResponseNotReady/连接重置时自动重试同一下载，
# 避免「大文件里某一个分片短暂抖动就整个下载前功尽弃」。仅在尚未向客户端写出任何字节时
# 才重试（已写出则不可重试，否则会重复字节污染数据，直接换候选/终止）。
_CHUNK_RETRY = 3

# P1 修复：连接池条目淘汰上限。代理想 keep-alive 但服务端/中间盒会定时清连接
# （nginx 默认 60s、Cloudflare ~100s），用 30s 作为安全阈值：超过 30s 没用的连接
# 一律视为可疑，取出时直接关掉、走新建连接。30s < 常见代理 idle timeout，
# 既不浪费复用收益，也不捡到「服务端已 FIN」的僵尸连接。
# P1 修复：连接池条目淘汰上限。代理想 keep-alive 但服务端/中间盒会定时清连接
# （nginx 默认 60s、Cloudflare ~100s）。
# seek 优化：idle 上限从 30s 提到默认 120s 并可通过 TG_CONN_IDLE_SEC 调整——播放器
# 暂停/拖动进度条往往间隔几十秒到几分钟，30s 一过就重新 TLS 握手（实测 0.33~0.38s）
# 白付一次。放长后由两道保险兜底：① 取出前 MSG_PEEK 探活；② 请求失败即清空该 host 的
# 池（见 _do_get），不会连续拿到僵尸连接。
_CONN_IDLE_MAX = float(os.environ.get("TG_CONN_IDLE_SEC", "120") or 120)

# P1 修复：单 host 在池里最多保留多少条 keep-alive 连接。超过就 LRU 关掉最旧那条。
# 6 条对视频高频 Range 来说已远超实际并发需求（单线程串行下载），更多只会养僵尸。
_CONN_POOL_MAX = 6

# ---------- seek/起播优化：把「一大段 Range」切成多段，让连接能回池复用 ----------
# 背景（实测，20MB 分片 / 自建 CF Worker 代理）：
#   冷连接（每次新建 TCP+TLS）: 首字节 0.49~0.76s（其中 TLS 握手就占 0.33~0.38s）
#   热连接（复用 keep-alive）  : 首字节 0.16~0.39s
#   而且大 Range 的整体吞吐明显偏低（片内 10MB Range 只有 3.9MB/s），
#   切成 2MB+8MB 两段复用连接后同样的数据能跑到 8.96MB/s。
# 原因：客户端（播放器）起播后往往读几 MB 就断开/重 seek，未读完 body 的连接只能丢弃，
#   于是每次 seek 都要重新握手。切成小段后每段都能「读完 → 回池」，后续段与下次 seek
#   都能复用热连接，首字节和吞吐同时改善。
#
# 默认**关闭**（实测在自建 CF Worker 代理下有害，见下）。
#
# 实测（20MB 分片，otterhub 代理，两轮一致）：
#   冷连接 + 整片 20MB : 首字节 0.70/0.78s  总 2.23/2.46s  9.4/8.5 MB/s
#   热连接 + 整片 20MB : 首字节 0.50/0.49s  总 2.16/2.40s  9.7/8.7 MB/s
#   冷连接 + 片内 10MB : 首字节 0.72/0.74s  总 2.06/2.14s  5.1/4.9 MB/s
#   热连接 + 片内 10MB : 首字节 0.36/0.35s  总 1.84/1.68s  5.7/6.2 MB/s
# 结论：热连接确实省 0.2~0.4s，但**每多切一段就要多付一次首字节成本**（小段尤其亏：
# 2MB 段的 0.35s 首字节比它 0.2s 的传输还久）。端到端实测分段后 60MB 全量从
# 20MB/s 掉到 7.7MB/s——固定 RTT 摊不薄，反而被放大。
#
# 所以默认不切分（一次性大 Range 最划算）；保留开关是为了换环境（比如代理 RTT 极低、
# 但长连接会被限速）时能一键验证。
#   TG_SPAN_MB=0        → 关闭分段（默认）
#   TG_FAST_START_KB=0  → 首段也用 TG_SPAN_MB 的大小（不单独加速起播）
_SPAN_BYTES = max(0, int(os.environ.get("TG_SPAN_MB", "0") or 0)) * 1024 * 1024
_FAST_START_BYTES = max(0, int(os.environ.get("TG_FAST_START_KB", "2048") or 2048)) * 1024

# 连接预热（保活）：seek 后客户端常只读一小段就断开，body 没读完 → 这条连接只能丢弃，
# 于是每次 seek 都要重新做 TLS 握手（实测 0.33~0.38s，占 seek TTFB 的三到五成）。
# 预热线程让池里**常驻一条只做过握手的热连接**，下次请求直接拿来发请求，省掉握手。
#   TG_CONN_WARM=0      → 关闭预热
#   TG_CONN_WARM_IDLE   → 预热连接空闲多少秒后重建（默认 25s，小于常见代理 idle 超时）
_CONN_WARM = os.environ.get("TG_CONN_WARM", "1") not in ("0", "off", "false", "")
_CONN_WARM_IDLE = float(os.environ.get("TG_CONN_WARM_IDLE", "25") or 25)
_CONN_WARM_INTERVAL = float(os.environ.get("TG_CONN_WARM_INTERVAL", "2") or 2)

# P3 修复：_do_get 默认总超时（含建连+响应头+响应体）。原本硬编码 180s 等于黑洞场景
# 必挂死，现在按调用方语义取：getFile 走 30s（接口小响应），分片下载走 30s（默认），
# 黑洞代理最多阻塞 30s × 重试次数，而不是 18 分钟。
_DEFAULT_HTTP_TIMEOUT = 30.0


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
                 proxy_token=None, rotate=True, proxy_pools=None,
                 api_base_explicit=True):
        self.slots = slots  # [{"token","chat_id","api_base"(可数组),"proxy_token"(可数组)}]
        self.api_base = api_base.rstrip("/")  # 全局默认 API 基址（槽位级可覆盖）
        self.rate_limit = float(rate_limit)
        # 自建 TG API 代理需要的访问令牌（官方 API 场景留空）——全局默认，槽位级可覆盖
        self.proxy_token = (proxy_token or "").strip() or None
        # 多 bot 池时是否轮转分摊（关掉则固定优先用第一个槽位 = 主备模式）
        self.rotate = bool(rotate)
        # 全局代理候选池（所有 bot 共享）：[(api_base, proxy_token), ...]
        self.proxy_pools = [tuple(p) for p in (proxy_pools or [])]
        # api_base 是否用户显式配的。False 时只把它当「一个地址都没有」时的最后退路，
        # 不会凭空塞进候选列表（详见 _build_candidates 注释）。
        self.api_base_explicit = bool(api_base_explicit)
        self._lock = threading.Lock()
        self._last = {}  # slot idx -> 上次发送时间
        self._path_cache = {}  # file_id -> (file_path, expire_ts)
        self._path_cache_ttl = 50 * 60  # TG file_path 有效期 1h，缓存 50min
        # 连接池：按 (scheme,host,port) 复用 keep-alive 连接，避免视频播放时每个
        # Range 请求都重做 TLS 握手（播放器会高频发 Range，复用连接大幅降低首字节延迟）。
        #
        # P1 修复要点：
        #   - 条目改为 (conn, last_used_ts)，超 _CONN_IDLE_MAX 秒（默认 30）的连接
        #     在取出时被丢弃，避免 http.client 内部因"上一响应 body 未读完"或
        #     "服务端已主动 FIN 但 will_close 仍为 False"导致的 ResponseNotReady /
        #     RemoteDisconnected 连锁自伤；
        #   - 每 key 容量上限 _CONN_POOL_MAX（默认 6），防止恶意代理不关闭连接把
        #     池撑爆；满则直接关闭新入队的旧连接；
        #   - _release_conn 接受显式 alive 标志，仍由调用方在「明确知道 body 已读完
        #     且无异常」时置 True；其他全部归 False（关闭后丢弃）。
        self._conn_pool = {}  # key -> collections.deque([(conn, last_used_ts), ...])
        self._conn_lock = threading.Lock()
        self._slot_lock = threading.Lock()
        self._slot_cursor = 0
        self._proxy_lock = threading.Lock()
        # 每个槽位的代理候选列表：自带 apiBase 在前，全局 proxy_pools 在后，
        # 兜底全局 api_base。语义是「按序主备 + 失败自动切换」：正常永远走候选 0，
        # 只有它网络/5xx 失败才退到候选 1…（不是轮询分摊，别被早期注释误导）。
        self._candidates = {}
        for i, s in enumerate(self.slots):
            self._candidates[i] = self._build_candidates(s)
        _log(f"TelegramBackend 初始化: slots={len(self.slots)} "
             f"rate_limit={self.rate_limit}s rotate={self.rotate} "
             f"global_api_base={self.api_base} global_proxy_auth={'on' if self.proxy_token else 'off'} "
             f"global_proxy_pools={len(self.proxy_pools)} "
             f"api_base_explicit={'on' if self.api_base_explicit else 'off'}")
        for i, s in enumerate(self.slots):
            cands = self._candidates[i]
            _log(f"  槽位 {i}: chat_id={s['chat_id']} 代理候选数={len(cands)} "
                 f"首候选={cands[0][0] if cands else self.api_base}")
        # 预热（保活）目标 host：所有槽位候选去重后的 (scheme,host,port) 对应的 api_base
        self._warm_hosts = []
        seen = set()
        for cands in self._candidates.values():
            for (b, _t) in cands:
                if b not in seen:
                    seen.add(b)
                    self._warm_hosts.append(b)
        self._last_activity = time.time()
        self._warm_stop = False
        if _CONN_WARM and self._warm_hosts:
            threading.Thread(target=self._warm_loop, daemon=True).start()
            _log(f"连接预热已开启: host数={len(self._warm_hosts)} "
                 f"保活间隔={_CONN_WARM_INTERVAL}s 连接空闲上限={_CONN_WARM_IDLE}s")

    # ---------- 连接预热（保活） ----------
    def _warm_loop(self):
        """后台巡检：保证每个代理 host 的池里常有一条「只做过 TLS 握手」的热连接。

        为什么有用：seek 之后客户端往往只读几 MB 就断开，未读完 body 的连接只能丢弃。
        没有预热的话，下一次 seek 必须重新握手（实测 0.33~0.38s）。提前把握手做掉，
        请求一来就能直接发，省掉的正是这部分固定延迟。

        空闲超过 _CONN_WARM_IDLE 的连接会被重建（代理/CF 会静默关闭长空闲连接）；
        5 分钟没有任何请求时暂停预热，避免空转骚扰代理。
        """
        while not self._warm_stop:
            try:
                time.sleep(_CONN_WARM_INTERVAL)
                if time.time() - self._last_activity > 300:
                    continue
                for host in list(self._warm_hosts):
                    try:
                        self._warm_one(host)
                    except Exception:
                        pass
            except Exception:
                time.sleep(_CONN_WARM_INTERVAL)

    def _warm_one(self, api_base):
        """保证 api_base 池里有一条（不超过保活上限年龄的）已握手连接。"""
        key = self._conn_key(api_base)
        now = time.time()
        need = True
        with self._conn_lock:
            dq = self._conn_pool.get(key)
            if dq:
                # 池里最旧一条若还在保活年龄内，就不必重建
                oldest = min((ts for (_c, ts) in dq), default=0)
                need = (now - oldest) > _CONN_WARM_IDLE
                if need:
                    for conn, _ in dq:
                        try:
                            conn.close()
                        except Exception:
                            pass
                    dq.clear()
        if not need:
            return
        conn = self._open_conn(api_base, timeout=_DEFAULT_HTTP_TIMEOUT)
        try:
            conn.connect()  # 只做 TCP + TLS 握手，不发任何请求
        except Exception as e:
            try:
                conn.close()
            except Exception:
                pass
            _log(f"连接预热失败(忽略): host={key[1]} {type(e).__name__}: {e}")
            return
        self._release_conn(api_base, conn, True)

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
        # 兜底全局 api_base：显式配过就**始终**追加（去重后）。
        # 旧写法是 `if not cands: cands.append(api_base)` —— 一旦配了 TG_PROXY_POOLS，
        # TG_API_BASE 里配的主代理就被整个丢掉：想「多一个备用」结果变成「换掉主用」。
        #
        # 但「始终追加」也会引入新问题：完全不配 TG_API_BASE 时它默认是
        # api.telegram.org，凭空成为最后一个候选——国内网络下是黑洞，
        # 连接超时 180s，代理全挂时会把「几秒报错」拖成「卡十几分钟」。
        # 所以没显式配过、且池里已经有候选时，就不追加这个默认值。
        fb = (self.api_base.rstrip("/"), self.proxy_token)
        if self.api_base_explicit or not cands:
            if fb not in cands:
                cands.append(fb)
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

    def _open_conn(self, api_base, timeout=None):
        """新建一条（不复用池里任何一条）keep-alive 连接。

        P3 修复：原版硬编码 timeout=180s，导致黑洞代理 + 重试叠加出 18min 挂起。
        ``timeout`` 形参真正生效；调用方按场景传入（小响应 30、下载分片 30/60、大文件 60/120）。
        """
        p = urllib.parse.urlparse(api_base or self.api_base)
        scheme = (p.scheme or "https").lower()
        host = p.hostname
        port = p.port or (443 if scheme == "https" else 80)
        # None 表示用 http.client 默认值；显式传入 >0 才覆盖
        t = timeout if timeout else _DEFAULT_HTTP_TIMEOUT
        if scheme == "https":
            return http.client.HTTPSConnection(host, port, timeout=t)
        return http.client.HTTPConnection(host, port, timeout=t)

    def _acquire_conn(self, api_base):
        """从池里取一条还活着的连接，自动淘汰超时/超容。

        P1 修复要点：
          - 取出时立刻丢弃空闲 > _CONN_IDLE_MAX 秒的条目（防僵尸）；
          - 容量上限 _CONN_POOL_MAX，超过的最旧一条直接关掉；
          - 取出时先做一次轻量探活（MSG_PEEK|b"" 即代表已 FIN），死的关掉换下一条。
        """
        key = self._conn_key(api_base)
        now = time.time()
        with self._conn_lock:
            dq = self._conn_pool.get(key)
            if not dq:
                return None
            evicted = 0
            while dq:
                conn, last_used = dq[0]
                # 1) 太老了就不要（最坏情况：所有都老，全部清空）
                if now - last_used > _CONN_IDLE_MAX:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    dq.popleft()
                    evicted += 1
                    continue
                # 2) 拆出最旧的一条（LRU 头），看容量
                if len(dq) > _CONN_POOL_MAX:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    dq.popleft()
                    evicted += 1
                    continue
                # 3) 探活：MSG_PEEK 读 1 字节，EOF = 已关闭。
                try:
                    sock = getattr(conn, "sock", None)
                    if sock is not None:
                        import select as _select
                        rd, _, _ = _select.select([sock], [], [], 0)
                        if rd:
                            buf = sock.recv(1, _select.MSG_PEEK)
                            if not buf:
                                # 服务端已 FIN，丢弃
                                try:
                                    conn.close()
                                except Exception:
                                    pass
                                dq.popleft()
                                evicted += 1
                                continue
                except Exception:
                    # 探活本身失败 = 连接坏，丢弃
                    try:
                        conn.close()
                    except Exception:
                        pass
                    dq.popleft()
                    evicted += 1
                    continue
                # 通过校验才用
                dq.popleft()
                return conn
            # 队列空了
            if evicted:
                pass  # 之前已经清理过
        return None

    def _release_conn(self, api_base, conn, alive):
        """归还一条连接。alive=False 直接关；alive=True 才入池（带 last_used 时间戳）。"""
        if conn is None:
            return
        if not alive:
            try:
                conn.close()
            except Exception:
                pass
            return
        key = self._conn_key(api_base)
        now = time.time()
        with self._conn_lock:
            dq = self._conn_pool.setdefault(key, collections.deque())
            # 入队前再做一次容量控制：满了就关掉这条（FIFO / LRU 二选一都可，
            # 这里用 FIFO 简单稳定）。
            if len(dq) >= _CONN_POOL_MAX:
                try:
                    conn.close()
                except Exception:
                    pass
                return
            dq.append((conn, now))

    def _do_get(self, api_base, path, proxy_token=None, rng=None, timeout=None,
                force_new=False):
        """发一次 GET（getFile / 文件字节通用），返回 (conn, resp)。

        P3 修复：
          - ``timeout`` 形参真正生效（之前是死参数，硬编码 180s）；
          - ``force_new=True`` 时第一次失败后强制走 ``_open_conn`` 而非池，
            给「同代理瞬断重试」一条真正能拿到新连接的路径。
        旧行为兼容：``force_new`` 默认 False，行为几乎与原版一致
        （仍给池一次机会，区别只在 timeout 不再是 180s）。
        """
        last = None
        # 预热线程用：有请求活动就刷新时间戳（长时间无请求时暂停预热）
        try:
            self._last_activity = time.time()
        except Exception:
            pass
        # 第 1 次：force_new 时直接 _open_conn，否则池优先
        # 第 2 次：池/新建依 force_new 切换，失败即退出
        tried = 0
        while True:
            tried += 1
            if force_new and tried == 1:
                conn = self._open_conn(api_base, timeout=timeout)
            elif tried >= 2 and force_new:
                # force_new 路径失败一次后，强制 _open_conn 而不是池（防止僵尸连接）
                conn = self._open_conn(api_base, timeout=timeout)
            else:
                conn = self._acquire_conn(api_base)
                if conn is None:
                    conn = self._open_conn(api_base, timeout=timeout)
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
                # seek 优化：一次请求失败，说明这个 host 的 keep-alive 连接大概率已被
                # 中间盒静默关掉（本地看连接还在，服务端早 FIN 了）。此时**清空该 host 的
                # 整池**，否则下一次取出的还是同一批僵尸连接，白白再付一次超时。
                self._drop_pool(api_base, reason=f"{type(e).__name__}: {e}")
                if tried >= 2:
                    break
                # 第 1 次失败，再试一次（无论是 force_new 还是 not，都会换连接尝试）
        raise last or TGError("连接失败")

    def _drop_pool(self, api_base, reason=""):
        """关闭并清空某个 host 的全部池中连接（请求失败后调用，防僵尸连接连续污染）。"""
        key = self._conn_key(api_base)
        with self._conn_lock:
            dq = self._conn_pool.pop(key, None)
        n = 0
        if dq:
            for conn, _ in dq:
                try:
                    conn.close()
                except Exception:
                    pass
                n += 1
        if n:
            _log(f"连接池已清空: host={key[1]} 关闭={n}条 原因={reason}")

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
            conn = None
            resp = None
            try:
                conn, resp = self._do_get(base, path, proxy_token, timeout=30)
                if resp.status != 200:
                    body = resp.read()
                    self._release_conn(base, conn, False)
                    conn = None
                    _log(f"getFile HTTP 错误: code={resp.status} attempt={attempt + 1}/3 "
                         f"file_id={file_id} body={body[:200]!r}")
                    if resp.status == 429:
                        time.sleep(2)
                        continue
                    raise TGError(f"getFile HTTP {resp.status}: {body[:160]}")
                raw = resp.read().decode("utf-8", "replace")
                # P1 修复：读完 body 后再算 alive，并校验 body 已读完
                # （resp.isclosed() 返回 True 表示 fp 为 None，body 完整读取）。
                # will_close 反映响应头的声明、isclosed 反映 http.client 状态；
                # 二者一致才归还池子，否则一律关掉，杜绝"上一响应 body 未读完"
                # 或"服务端已主动 FIN 但 will_close 仍 False"导致的僵尸连接入库。
                alive = (not getattr(resp, "will_close", False)) and bool(resp.isclosed())
                self._release_conn(base, conn, alive)
                conn = None
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
                # P1 修复：异常路径显式关连接（即使 resp.read() 已内部关闭了，保险再 close 一次）
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
                    conn = None
                if attempt < 2:
                    time.sleep(2)
                    continue
                raise TGError(f"getFile: {e}")
            finally:
                # 兜底：任何漏网路径都关掉连接，绝不让 fd 泄漏到 GC
                if conn is not None:
                    try:
                        conn.close()
                    except Exception:
                        pass
        raise TGError("getFile 失败")

    def _get_file_path_direct(self, file_id, token, api_base=None, proxy_token=None):
        """不走连接池的 getFile（专供预取用，避免与下载线程争用 keep-alive 连接）。

        与 _get_file_path 的唯一区别是用一次性 urllib 请求，不从 _conn_pool 取连接，
        这样预取线程和下载线程互不影响。命中缓存时直接返回（零开销）。
        """
        now = time.time()
        cached = self._path_cache.get(file_id)
        if cached and cached[1] > now:
            return cached[0]
        base = (api_base or self.api_base).rstrip("/")
        url = f"{base}/bot{token}/getFile?file_id=" + urllib.parse.quote(file_id)
        req = urllib.request.Request(url)
        req.add_header("User-Agent", "TelegramWebDAV/1.0 (+python-urllib)")
        if proxy_token:
            req.add_header("Authorization", f"Bearer {proxy_token}")
        t0 = time.time()
        with urllib.request.urlopen(req, timeout=60) as resp:
            raw = resp.read().decode("utf-8", "replace")
        js = json.loads(raw)
        if not js.get("ok"):
            raise TGError(f"预取 getFile: {js.get('error_code')} {js.get('description')}")
        fp = js["result"]["file_path"]
        self._path_cache[file_id] = (fp, now + self._path_cache_ttl)
        _log(f"getFile 预取成功: file_id={file_id} path={fp} 耗时={time.time() - t0:.3f}s")
        return fp

    def prefetch_paths(self, items):
        """并发预取多个分片的 file_path，消除后续分片的 getFile 串行等待。

        ``items``: [(file_id, slot_idx), ...]

        真实环境里一次 getFile 可能要 2~4s（自建代理 RTT），多分片文件串行下载时
        每个分片都要干等一次 getFile，首次播放的等待会被成倍放大。这里在**首片下载
        的同时**后台并发把其余分片的 file_path 取回，等轮到它们时缓存已热，
        getFile 的 RTT 被完全隐藏在首片下载时间里。

        刻意不使用连接池（走 _get_file_path_direct），避免与下载线程争用 keep-alive 连接。
        任何一片预取失败都不影响主流程（下载时会自动回退到常规 getFile）。
        """
        if not items:
            return
        n_slots = len(self.slots) or 1

        def _one(it):
            fid, slot = it
            idx = slot % n_slots
            try:
                sd = self.slots[idx]
                cands = self._candidates.get(idx) or [(self.api_base, self.proxy_token)]
                api_base, ptok = cands[0]
                self._get_file_path_direct(fid, sd["token"], api_base, ptok)
            except Exception as e:
                _log(f"预取 file_path 失败(忽略,下载时会自动重试): "
                     f"file_id={fid} err={type(e).__name__}: {e}")

        t0 = time.time()
        _log(f"file_path 预取开始: 分片数={len(items)}")
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max(1, min(8, len(items)))
        ) as ex:
            list(ex.map(_one, items))
        _log(f"file_path 预取完成: 分片数={len(items)} 耗时={time.time() - t0:.3f}s")

    def _plan_spans(self, start, end):
        """把一段 Range 切成若干「小段」，返回 [(s, e), ...]（含端点）。

        目的不是省流量，而是让**连接能被复用**：播放器起播后常常读几 MB 就断开/重新
        seek，未读完 body 的连接只能丢弃。切成小段后每段都能「读完 → 回池」，
        后续段和下一次 seek 都能走热连接（实测首字节 0.7~0.9s → 0.16~0.39s）。

        切分规则（总长 <= 单段大小时不切，行为与旧版完全一致）：
          第 1 段 = TG_FAST_START_KB（默认 2MB，尽快出首字节）
          第 2 段起 = TG_SPAN_MB（默认 8MB）
        TG_SPAN_MB=0 表示关闭分段（回退到「一次请求拿完整段」的旧行为）。
        """
        if _SPAN_BYTES <= 0 or start is None or end is None:
            return [(start, end)]
        total = end - start + 1
        if total <= _SPAN_BYTES:
            return [(start, end)]
        first = min(_FAST_START_BYTES if _FAST_START_BYTES > 0 else _SPAN_BYTES, total)
        spans = []
        cur = start
        while cur <= end:
            size = first if cur == start else _SPAN_BYTES
            e = min(cur + size - 1, end)
            spans.append((cur, e))
            cur = e + 1
        return spans

    def iter_chunk(self, file_id, slot, start=None, end=None, blk=None, ctx=None):
        """生成器：按 Range 取回单块字节。start/end 为相对该块的字节区间（含端点）。

        对外签名与语义**完全不变**：调用方（webdav 流式下发）仍旧「yield 到多少就
        写多少」。内部在总长超过 TG_SPAN_MB 时会拆成多段顺序下载，每段读完即归还
        keep-alive 连接，从而让后续段与后续请求复用热连接（详见 _plan_spans 注释）。
        """
        spans = self._plan_spans(start, end)
        if len(spans) <= 1:
            yield from self._iter_chunk_range(file_id, slot, start, end, blk, ctx)
            return
        _log(f"iter_chunk 分段下载: file_id={file_id} range={start}-{end} "
             f"段数={len(spans)} 段大小={[e - s + 1 for (s, e) in spans]}")
        for i, (s, e) in enumerate(spans):
            # 第 2 段起：前面已经向客户端写出过字节，本段任何失败都不可重试/换候选
            # （P0：重复下发会污染客户端数据），直接抛 TGError 让上层中断连接。
            yield from self._iter_chunk_range(file_id, slot, s, e, blk, ctx,
                                              no_switch=(i > 0))

    def _iter_chunk_range(self, file_id, slot, start=None, end=None, blk=None,
                          ctx=None, no_switch=False):
        """下载 [start, end] 这一段（iter_chunk 的单段实现）。

        ``no_switch=True`` 表示外层已写出过字节：本段失败不可重试/换候选，直接抛错。

        ``blk`` 是每次 ``resp.read()`` 的块大小，直接决定「读到多少字节就 yield 一次」，
        也就是流式下发时客户端每隔多久收到一批数据：块越小越平滑、首字节越快，
        块越大系统调用越少。由 webdav 层按 ``TG_STREAM_BLOCK_KB`` 传入（缺省 1MB）。

        ``ctx`` 是 webdav 侧传下来的请求观测对象（可空）：每次瞬断重试都会
        ``ctx.bump_down()``，这样请求超时/结束时日志能打印「下载分片重试了几次」，
        否则分片重试散落在各个下载线程里，外部完全看不到。

        遍历该 bot 的全部代理候选：某代理网络/5xx 失败自动切换下一个；4xx/429 直接抛出。
        连接走 keep-alive 连接池（_do_get/_release_conn），视频高频 Range 下省去重复握手。

        P0/P1/P2 修复要点：
          - 一旦已 yield 过字节（``total_yield > 0``），本分片下载即视作「不可恢复」：
            任何后续异常不再换候选重试（否则会重复下发污染客户端/破坏 keep-alive 帧），
            直接抛 TGError，让 webdav 走「已发部分字节 → 立即中断连接」分支。
          - ``ok`` 标记：读循环只有正常 break 才置 True，异常路径都置 False，
            使 finally 把连接关掉而非放回池子，杜绝 zombie 连接。
          - ``expected_total``：记录本次应下载的字节数（end-start+1，无 Range 则 None），
            读循环末尾若小于 expected 即视为「代理少发字节（CL 虚高）」，
            立即抛 TGError（而不是静默结束、日志写「下载完成」）。
          - alive 判定延后到读完 body 后再算，并校验 ``resp.isclosed()``。
        """
        blk = int(blk or 1024 * 1024)
        n = len(self.slots)
        sd = self.slots[slot % n]
        token = sd["token"]
        idx = slot % n
        cands = self._candidates.get(idx) or [(self.api_base, self.proxy_token)]
        last = None
        t0 = time.time()  # 本分片下载总计时起点（发起 getFile 之前）
        # P2 修复：期望字节数初始值（循环内拿到响应头后会基于 Content-Length 重算，
        # 这样「无 Range 的整片下载」也能校验代理是否少发字节/CL 虚高）。
        expected_total = (end - start + 1) if (start is not None and end is not None) else None
        # 标记「已写过字节」：决定后续能否重试/换候选（P0 关键）
        yielded_any = False

        for ci, (api_base, proxy_token) in enumerate(cands):
            attempt = 0
            while attempt <= _CHUNK_RETRY:
                total_yield = 0
                conn = None
                resp = None
                try:
                    fp = self._get_file_path(file_id, token, api_base, proxy_token)
                    base = (api_base or self.api_base).rstrip("/")
                    bp = urllib.parse.urlparse(base).path or ""
                    path = f"{bp}/file/bot{token}/{fp}"
                    rng = None
                    if start is not None:
                        rng = f"bytes={start}-{end}" if end is not None else f"bytes={start}-"
                    _log(f"iter_chunk 下载: file_id={file_id} slot={slot} range={rng} path={path} "
                         f"proxy_auth={'on' if proxy_token else 'off'} "
                         f"expected_total={expected_total}")
                    # P3 修复：force_new=True 让同代理瞬断重试拿到全新 TCP
                    conn, resp = self._do_get(base, path, proxy_token, rng,
                                               timeout=60, force_new=(attempt > 0))
                    # P2 修复：拿到响应头后用 Content-Length 重算期望字节数
                    # （整片下载/无 Range 也能校验「代理静默截断 / CL 虚高」）
                    if expected_total is None:
                        _cl = resp.getheader("Content-Length")
                        if _cl is not None:
                            try:
                                expected_total = int(_cl)
                            except (TypeError, ValueError):
                                expected_total = None
                    if resp.status >= 400:
                        body = resp.read()
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = None
                        msg = f"iter_chunk HTTP {resp.status}: {body[:160]!r}"
                        code = str(resp.status)
                        _log(f"iter_chunk 代理候选 {ci} HTTP {code}: {msg}")
                        if code == "429":
                            raise TGError(msg)
                        if code in _BUSINESS_4XX:
                            raise TGError(msg)
                        last = TGError(msg)
                        _log(f"iter_chunk 代理候选 {ci} 失败({msg})，切换下一代理: api={api_base}")
                        break
                    t_first = None
                    try:
                        while True:
                            b = resp.read(blk)
                            if not b:
                                # 可能是正常 EOF，也可能是代理提前 FIN（CL 虚高/提前断流）
                                # P2 修复：代理常见「Connection: keep-alive + 提前 FIN」
                                # 二者都会让 resp.read 安静返回 b""，需要在长度层面兜底。
                                break
                            if t_first is None:
                                t_first = time.time()
                            total_yield += len(b)
                            yield b
                    except (GeneratorExit, _ClientGoneEarly):
                        # P0 修复：生成器被显式关闭（_stream_chunk 客户端断开抛 _ClientGone）
                        # 或被外层提前终止。不再 yield、不再换候选；连接对象弃用。
                        try:
                            conn.close()
                        except Exception:
                            pass
                        return
                    # P2 修复：读完循环末尾若不足 expected_total，判静默截断，抛错而非 return
                    if expected_total is not None and total_yield < expected_total:
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = None
                        msg = (f"代理少发字节: 期望={expected_total}B 实际={total_yield}B "
                               f"(Content-Length 虚高/提前 FIN)")
                        _log(f"iter_chunk 代理候选 {ci} {msg}")
                        raise TGError(msg)
                    # P1 修复：alive 只有在读完 body 且无异常 + resp 已关闭时才为 True。
                    # isclosed() 用安全调用：标准 http.client 响应都有该方法；若响应对象
                    # 不提供（测试替身/自定义响应），按「body 已读完」处理，不因此中断下载。
                    try:
                        resp_closed = bool(resp.isclosed())
                    except Exception:
                        resp_closed = True
                    alive_ok = (not getattr(resp, "will_close", False)) and resp_closed
                    self._release_conn(base, conn, alive_ok)
                    conn = None
                    dt_total = time.time() - t0
                    dt_first = (t_first - t0) if t_first is not None else dt_total
                    _log(f"iter_chunk 下载完成: file_id={file_id} bytes={total_yield} "
                         f"总耗时={dt_total:.3f}s 首字节={dt_first:.3f}s "
                         f"吞吐={_fmt_speed(total_yield, dt_total)} "
                         f"proxy={api_base}")
                    return
                except TGError:
                    # 把 tg 自己的错误原样上抛（不重试、不换候选）；连接由 finally 关掉
                    raise
                except _ClientGoneEarly:
                    # 客户端主动关：算正常结束
                    return
                except (http.client.HTTPException, OSError, socket.timeout) as e:
                    last = e
                    # P0 修复：已 yield 过字节 → 不再换候选、不再重试，直接上抛。
                    # 旧逻辑「break 到外层 for 换候选」会让下游在已写 N 字节后又从头
                    # yield 整片，超出 Content-Length → 污染客户端 / 错位 keep-alive。
                    if total_yield > 0 or no_switch:
                        _log(f"iter_chunk 代理候选 {ci} 下载中断(本段已写 {total_yield}B,"
                             f"不再换候选/重试): {type(e).__name__}: {e} api={api_base}")
                        raise TGError(f"下载分片中断(本段已写 {total_yield}B): "
                                      f"{type(e).__name__}: {e}")
                    # 未写出字节：可重试或换候选
                    if attempt < _CHUNK_RETRY:
                        attempt += 1
                        if ctx is not None:
                            try:
                                ctx.bump_down()
                            except Exception:
                                pass
                        _log(f"iter_chunk 代理候选 {ci} 瞬断重试({attempt}/{_CHUNK_RETRY}): "
                             f"{type(e).__name__}: {e} api={api_base}")
                        time.sleep(min(0.2 * attempt, 1.0))
                        continue
                    _log(f"iter_chunk 代理候选 {ci} 网络/其他错误: {type(e).__name__}: {e} "
                         f"api={api_base}")
                    break
                except Exception as e:
                    last = e
                    if total_yield > 0:
                        _log(f"iter_chunk 代理候选 {ci} 下载中断(已写 {total_yield}B,"
                             f"不再换候选/重试): {type(e).__name__}: {e} api={api_base}")
                        raise TGError(f"下载分片中断(已写 {total_yield}B): "
                                      f"{type(e).__name__}: {e}")
                    if attempt < _CHUNK_RETRY:
                        attempt += 1
                        if ctx is not None:
                            try:
                                ctx.bump_down()
                            except Exception:
                                pass
                        _log(f"iter_chunk 代理候选 {ci} 瞬断重试({attempt}/{_CHUNK_RETRY}): "
                             f"{type(e).__name__}: {e} api={api_base}")
                        time.sleep(min(0.2 * attempt, 1.0))
                        continue
                    _log(f"iter_chunk 代理候选 {ci} 网络/其他错误: {type(e).__name__}: {e} "
                         f"api={api_base}")
                    break
                finally:
                    # 兜底：任何漏掉的路径都关连接，杜绝 fd 泄漏与 zombie 入池
                    if conn is not None:
                        try:
                            conn.close()
                        except Exception:
                            pass
            # 该代理候选重试耗尽，未写出字节 → 继续尝试下一个候选（若有）
            continue
        # 所有候选与重试均失败：统一抛 TGError（绝不直接抛裸的 http.client 异常，
        # 否则 webdav 流式下发的 except _tg.TGError 捕获不到，会变成「未捕获异常(返回500)」
        # 并在已发 206 头后污染响应体）。
        if isinstance(last, Exception):
            raise TGError(f"下载所有代理候选均失败: {type(last).__name__}: {last}")
        raise TGError("下载所有代理候选均失败")

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
