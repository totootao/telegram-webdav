"""WebDAV 处理器（纯标准库实现，零第三方依赖）。

实现 Telegram 频道即存储的纯 WebDAV 访问层：客户端用标准 WebDAV 协议
（Windows 映射驱动器 / macOS Finder / rclone / cadaver / AList / OpenList）读写，
服务端把字节分片塞进 Telegram 频道，把文件树存在 SQLite。

支持方法：
  OPTIONS  PROPFIND  GET  HEAD  PUT  DELETE  MKCOL  MOVE  COPY  LOCK  UNLOCK  PROPPATCH
  POST    /telegram/webhook*  频道/群消息自动入库

设计目标：通用适配 + 健壮性
  1. 路径与尾斜杠：目录无论带不带 ``/`` 都能正确访问；PROPFIND 的 href 始终
     对集合补 ``/``（符合 RFC 4918，客户端相对路径才能算对）；``DAV_ROOT`` 挂载
     时 href 自动带挂载前缀，客户端拿到的地址才自洽。
  2. keep-alive 残包检测：AList/OpenList 的 WebDav 驱动给 PROPFIND/MKCOL/DELETE
     构造的 XML body 的 Content-Length 比真实 body 少算 1 字节（尾部 ``\\n``）。
     若沿用 keep-alive，残留字节会被当成下一个请求的起始行，服务端吐 Python 自带的
     HTML 400 页，客户端表现为 ``malformed HTTP status code "HTML>"``。
     这里每次处理完请求都读净请求体，并**非破坏性地**探测是否还有超出
     Content-Length 的残留字节：确实是垃圾（不像新请求起始行）就丢掉，连接照样复用；
     看起来像下一条请求就停手（流水线）；乱到超过上限才关连接。从根上消除边界错位。
     仍可用 ``DAV_KEEPALIVE=off`` 退回"每条连接只服务一次"的保守模式。
  3. 防御性：任何 handler 抛异常都回 500 并关闭连接，绝不把 Python traceback / HTML
     漏给客户端；写响应时客户端断连（BrokenPipe）静默处理；请求体读取带超时，
     Content-Length 虚高时不会把线程拖死。
"""
import base64
import concurrent.futures
import email.utils
import hashlib
import json
import re
import select
import socket
import threading
import time
import urllib.parse
import xml.sax.saxutils
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config as _config
import db as _db
import media as _media
import tg as _tg


def _log(msg):
    """统一的后台日志：带本地时间戳 + [webdav] 模块前缀，便于定位。

    请求级事件（PUT 收到、父目录校验、上传成败、文件落库）与异常兜底都经过这里，
    配合 [tg] 侧的 Telegram 交互日志，可完整还原一次上传/下载失败的根因。
    """
    import datetime
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}][webdav] {msg}", flush=True)


def _now_http(ts):
    return email.utils.formatdate(ts, usegmt=True)


def _now_iso(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _fmt_dur(sec):
    """秒(float) -> 人类可读时长 ``H:MM:SS.mmm``；无法表示返回 ``-``。

    日志里同时会带原始秒数，便于定位；这里只负责把「5023.678s」变成「1:23:43.678」。
    """
    if sec is None:
        return "-"
    try:
        sec = float(sec)
    except (TypeError, ValueError):
        return "-"
    if sec < 0:
        return "-"
    h = int(sec // 3600)
    m = int((sec % 3600) // 60)
    s = sec % 60
    if h:
        return f"{h:d}:{m:02d}:{s:06.3f}"
    return f"{m:d}:{s:06.3f}"


def _fmt_size(n):
    """字节数 -> ``45.2MB`` 这类可读串。"""
    try:
        n = float(n)
    except (TypeError, ValueError):
        return "-"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(n) < 1024.0:
            return f"{n:.0f}{unit}" if unit == "B" else f"{n:.1f}{unit}"
        n /= 1024.0
    return f"{n:.1f}PB"


def _fmt_speed(nbytes, seconds):
    """吞吐：``(字节数, 耗时秒) -> '12.3 MB/s'``。耗时过小/无效时返回 ``-``。"""
    try:
        seconds = float(seconds)
        if seconds <= 0:
            return "-"
        return f"{nbytes / seconds / (1024 * 1024):.1f} MB/s"
    except (TypeError, ValueError, ZeroDivisionError):
        return "-"


def _media_kind(content_type, name=""):
    """按 Content-Type / 扩展名判断是音频还是视频，返回 'video'/'audio'/'-'。

    只用于日志标注，非媒体返回 ``-``（时长解析自然也不会有结果）。
    """
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct.startswith("video/"):
        return "video"
    if ct.startswith("audio/"):
        return "audio"
    ext = name.rsplit(".", 1)[-1].lower() if name and "." in name else ""
    if ext in ("mp4", "m4v", "mov", "mkv", "webm", "ogv", "3gp", "3g2", "avi"):
        return "video"
    if ext in ("mp3", "m4a", "m4b", "flac", "wav", "wave", "ogg", "oga", "opus", "spx"):
        return "audio"
    return "-"


# 请求体读取超时（秒）。防止 Content-Length 虚高时把线程拖死。空闲 keep-alive
# 等待上限改为从配置读取（DAV_IDLE_TIMEOUT，默认 30），便于自测调小、生产按需放大。
_BODY_TIMEOUT = 300.0
# 单次最多丢掉多少个「Content-Length 之外」的垃圾字节。超过说明客户端严重错乱，
# 直接关连接，不做无谓挣扎。
_RESIDUAL_LIMIT = 4096


# 媒体时长解析的采样大小（字节）。只取首片的头部与末片的尾部，O(1) 次切片拷贝：
# 时长信息（moov/mvhd、Xing、fmt、STREAMINFO）都在文件头或文件尾，不需要全文扫描。
# tail 给得比 head 大，是因为 MP4 未做 faststart 时 moov 落在文件尾且可能较大。
_MEDIA_HEAD_SAMPLE = 512 * 1024
_MEDIA_TAIL_SAMPLE = 4 * 1024 * 1024
# 上传走「流式边收边传」的大小门槛：超过它就不把整个请求体读进内存。
# 旧路径对 1.2GB 文件的实测内存峰值是 2743MB（2.27 倍），2GB 文件足以撑爆普通容器。
_STREAM_UPLOAD_MIN_BYTES = 32 * 1024 * 1024

# 单个分片上传失败后，在 webdav 层重试的次数。
#
# 为什么需要它（大文件上传的核心痛点）：一个 1.2GB 文件会被切成 60 个分片，
# 若「任一分片失败就整体放弃」，设单分片失败率 1%，整体成功率仅 0.99^60≈55%——
# 也就是近一半概率白传。分片越多越容易挂，这正是大文件尤其容易失败的原因。
# 这里让**失败的那一片自己重试**（换 bot 槽位 + 退避），其余分片照常成功，
# 整体成功率随之回到 ~99%+，且不会产生「一片失败拖垮全部」的连锁反应。
# 注意：upload_chunk 内部已有「换槽位 + 3 次退避」的快速重试，这里是最后防线，
# 退避更长，用于扛住持续性限流/较长时间的网络抖动。
_UP_CHUNK_RETRY = 3
# 分片去重记录在库里的保留时长（秒）：默认 30 天，避免去重表无限增长。
_CHUNK_DEDUP_TTL = 30 * 24 * 3600


def _up_backoff(attempt, msg):
    """分片上传重试的退避秒数。

    429 限流优先用 Telegram 返回的 retry_after（等够时间再试，否则必然再撞限流）；
    其他错误走指数退避，上限 30s，避免长时间抖动时把请求打爆。
    """
    m = re.search(r"retry_after\D*(\d+)", msg or "")
    if m:
        return max(1, min(60, int(m.group(1))))
    return min(30, 2 ** attempt)


# ---------------- 请求级观测：超时到底卡在谁身上、重试了几次 ----------------
# 背景：标准库 http.server 在 rfile 读/写超时时，是在 *它自己* 的 handle_one_request
# 内部 except TimeoutError 后直接 log_error("Request timed out: %r") 的——外层覆写的
# handle_one_request 根本感知不到，日志里只剩一行没有主体、没有路径、没有重试信息的
# 「172.17.0.1 - - [...] Request timed out: TimeoutError('timed out')」。
# 所以这里做三件事：
#   1) 覆写 log_error 拦下这条日志，补上「哪个请求、卡在哪个阶段」；
#   2) 用 _ReqCtx 累计本次请求的分片重试次数，回答「有没有重试、重试了几次」；
#   3) 区分 keep-alive 空闲等待超时（正常回收，默认静默）与请求处理中超时（真问题）。
# 空闲超时默认不打日志（DAV_LOG_IDLE_TIMEOUT=on 可打开），否则刷屏淹掉真问题。
import os as _os

_LOG_IDLE_TIMEOUT = str(_os.getenv("DAV_LOG_IDLE_TIMEOUT", "off")).lower() == "on"


class _ReqCtx:
    """一次 WebDAV 请求的观测上下文：谁、干什么、重试了几次、跑了多久。

    刻意**不用** threading.local：上传/下载分片跑在 ThreadPoolExecutor 里，
    线程 local 传不进去；只有显式把 ctx 对象往下传（``tg.iter_chunk(ctx=...)``），
    才能把分片级重试次数累计到「发起它的那条 WebDAV 请求」上。
    """

    __slots__ = ("peer", "method", "path", "seq", "t0", "lock",
                 "up_retry", "down_retry", "up_chunks", "up_reused")

    def __init__(self, peer, method, path, seq):
        self.peer = peer
        self.method = method
        self.path = path
        self.seq = seq
        self.t0 = time.time()
        self.lock = threading.Lock()
        self.up_retry = 0      # 上传分片重试次数（含 429 退避重试）
        self.down_retry = 0    # 下载分片瞬断重试次数
        self.up_chunks = 0     # 本次真正上传到 Telegram 的分片数
        self.up_reused = 0     # 命中 SHA 去重、直接复用的分片数

    def bump_up(self, n=1):
        with self.lock:
            self.up_retry += n

    def bump_down(self, n=1):
        with self.lock:
            self.down_retry += n

    @property
    def elapsed(self):
        return time.time() - self.t0

    def key(self):
        return (self.peer, self.method, self.path)

    def retry_desc(self):
        """重试情况的一句话摘要。"""
        parts = []
        if self.up_retry:
            parts.append(f"上传分片重试{self.up_retry}次")
        if self.down_retry:
            parts.append(f"下载分片重试{self.down_retry}次")
        return "、".join(parts) if parts else "无重试(一次通过)"

    def op(self):
        return f"{self.method} {self.path}"


class _TimeoutStats:
    """按「客户端 + method + path」聚合的超时历史，用来回答「后来有重试没」。

    - 超时发生 → timeout 计数 +1；
    - 同一 key 的下一次请求进来 → attempt 计数 +1（说明客户端/上游确实重发了）。
    两者结合就能在日志里直接看出来：这个访问超时过几次、之后又被重试了几次。
    """

    _WINDOW = 30 * 60.0   # 30 分钟内的重复访问视为同一件事的「重试」

    def __init__(self):
        self._lock = threading.Lock()
        self._m = {}

    def on_timeout(self, ctx):
        """记录一次超时，返回 (该请求累计超时次数, 客户端累计尝试次数)。"""
        with self._lock:
            self._prune()
            e = self._m.setdefault(ctx.key(), {"timeout": 0, "attempt": 0, "last": 0.0})
            e["timeout"] += 1
            e["last"] = time.time()
            return e["timeout"], e["attempt"]

    def on_attempt(self, ctx):
        """新请求开始，返回 (此前累计超时次数, 本次是第几次尝试)。"""
        with self._lock:
            self._prune()
            e = self._m.setdefault(ctx.key(), {"timeout": 0, "attempt": 0, "last": 0.0})
            e["attempt"] += 1
            e["last"] = time.time()
            return e["timeout"], e["attempt"]

    def _prune(self):
        now = time.time()
        for k in [k for k, v in self._m.items() if now - v["last"] > self._WINDOW]:
            del self._m[k]


_TIMEOUT_STATS = _TimeoutStats()


class _IntegrityError(Exception):
    """下载分片时检测到内容完整性被破坏（哈希不符或被截断）。

    抛出后由 ``_serve_file`` 捕获：立刻中断连接，绝不把错数据当完整文件交给客户端。
    """

    def __init__(self, offset):
        super().__init__("integrity check failed")
        self.offset = offset


class _ClientGone(Exception):
    """客户端在流式下载途中断开连接。

    抛出后由 ``_serve_file`` 捕获：立刻停止，不再等待其余分片下载完（省掉无谓的等待与流量）。
    """

    def __init__(self, sent=0):
        super().__init__("client gone")
        self.sent = sent


# 常见 HTTP/WebDAV 方法。用于判断缓冲区开头是「一条新请求」（流水线，不能动）
# 还是「上一条请求多出来的垃圾字节」（可以安全丢掉）。
_HTTP_METHODS = frozenset(
    b"""OPTIONS GET HEAD POST PUT DELETE TRACE CONNECT PATCH
        PROPFIND PROPPATCH MKCOL COPY MOVE LOCK UNLOCK
        ACL REPORT SEARCH MKACTIVITY BASELINE-CONTROL VERSION-CONTROL
        BPROPFIND BPROPPATCH ORDERPATCH LABEL MKREDIRECTREF UPDATEREDIRECTREF
        MKWORKSPACE CHECKIN CHECKOUT UNCHECKOUT MERGE""".split()
)


def _is_timeout_err(e):
    """判断一个异常是不是「套接字读超时」而不是真的协议/逻辑错误。

    Python 的 ``socket.timeout``（3.10+ 即内置 ``TimeoutError``）在 ``SocketIO``
    上还会留下「已超时」标记，之后该连接上的任何读都会抛
    ``OSError('cannot read from timed out object')``——这条消息里也带 timed out。
    这类异常代表「空闲连接该回收了」，绝不能渲染成 500 吓客户端。
    """
    if isinstance(e, (TimeoutError, socket.timeout)):
        return True
    return isinstance(e, OSError) and "timed out" in str(e).lower()


def _guess_ct(name, fallback="application/octet-stream"):
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    return {
        "txt": "text/plain", "md": "text/markdown", "json": "application/json",
        "html": "text/html", "htm": "text/html", "css": "text/css", "js": "application/javascript",
        "png": "image/png", "jpg": "image/jpeg", "jpeg": "image/jpeg", "gif": "image/gif",
        "webp": "image/webp", "svg": "image/svg+xml", "bmp": "image/bmp",
        "mp4": "video/mp4", "webm": "video/webm", "mov": "video/quicktime", "mkv": "video/x-matroska",
        "mp3": "audio/mpeg", "ogg": "audio/ogg", "wav": "audio/wav", "flac": "audio/flac",
        "pdf": "application/pdf", "zip": "application/zip", "rar": "application/vnd.rar",
        "7z": "application/x-7z-compressed", "doc": "application/msword",
        "docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        "xls": "application/vnd.ms-excel", "xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        "ppt": "application/vnd.ms-powerpoint", "pptx": "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    }.get(ext, fallback)


# ---- file_path 预热状态（模块级，跨请求共享）----
_WARM_LOCK = threading.Lock()
_WARM_ACTIVE = 0
_WARM_MAX_ACTIVE = 2


class WebDAVHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "TelegramWebDAV/1.1"

    # ---------------- 通用 ----------------
    @property
    def app(self):
        return self.server.app

    def log_message(self, fmt, *args):
        # 安静日志：只打错误级别，避免刷屏
        if " 2" in fmt or " 1" in fmt:
            return
        super().log_message(fmt, *args)

    def log_error(self, fmt, *args):
        """拦截标准库的超时日志，补上「哪个请求 + 重试了几次」。

        http.server 在 rfile 读/写超时时，是在基类 handle_one_request **内部** 就
        except 掉 TimeoutError 并 log_error 的，外层覆写的 handle_one_request 的
        except 完全感知不到（异常根本没抛出来）。所以只能在这一层拦。
        """
        try:
            msg = fmt % args if args else fmt
        except Exception:
            msg = fmt
        if "Request timed out" in str(msg):
            self._on_request_timeout(str(msg))
            return
        super().log_error(fmt, *args)

    def parse_request(self):
        """请求行 + 头解析成功 → 建立本次请求的观测上下文。

        只有走到这里才说明「确实有一个真请求在处理」。读请求行就超时的情况
        （keep-alive 空闲等待下一条请求）不会到这里，据此把「空闲回收」与
        「请求处理中超时」区分开——前者是正常行为，后者才是要排查的问题。
        """
        ok = super().parse_request()
        if ok:
            self._req_parsed = True
            self._conn_seq += 1
            peer = self.client_address[0] if self.client_address else "-"
            self._ctx = _ReqCtx(peer, self.command, self.path, self._conn_seq)
            n_timeout, n_attempt = _TIMEOUT_STATS.on_attempt(self._ctx)
            if n_timeout and n_attempt > 1:
                _log(f"请求重传: 客户端={peer} 连接内第{self._conn_seq}个请求 "
                     f"{self.command} {self.path} —— 该访问此前已超时 {n_timeout} 次，"
                     f"这是第 {n_attempt} 次尝试"
                     + ("（已成功的分片会按 SHA 去重复用，只补传失败片）"
                        if self.command == "PUT" else ""))
        return ok

    def _on_request_timeout(self, raw):
        """超时详情。分两种：空闲连接回收（正常）与请求处理中超时（要查）。"""
        peer = self.client_address[0] if self.client_address else "-"
        port = (self.client_address[1]
                if self.client_address and len(self.client_address) > 1 else "-")

        # 请求行都还没解析出来 → 卡在「等下一条请求」，是 keep-alive 的正常回收
        if not self._req_parsed:
            if _LOG_IDLE_TIMEOUT:
                _log(f"连接空闲超时(正常回收,无需处理): 客户端={peer}:{port} "
                     f"等待下一条请求超 {self._idle_timeout_used:.0f}s 未收到数据，关闭连接 "
                     f"—— 上一个请求: {self._last_req}")
            return

        ctx = self._ctx
        if ctx is None:
            # 请求行已读到一半（raw_requestline 非空）但没解析出 method/path
            _log(f"请求超时: 客户端={peer}:{port} 卡在请求行/请求头解析阶段，"
                 f"未取得 method/path。细节={raw}")
            return

        if not self._response_started:
            if self._body_read:
                stage = "读取请求体"
            elif self.headers.get("Content-Length") or self.headers.get("Transfer-Encoding"):
                stage = "读取请求体(尚未收到任何 body)"
            else:
                stage = "读取请求头"
        else:
            stage = "写出响应给客户端"
        body_desc = ""
        if self._body_read:
            body_desc = f" 已读请求体={_fmt_size(self._body_read)}"
            try:
                clen = int(self.headers.get("Content-Length") or 0)
            except Exception:
                clen = 0
            if clen:
                body_desc += f"/共{_fmt_size(clen)}({self._body_read * 100.0 / clen:.0f}%)"

        n_timeout, n_attempt = _TIMEOUT_STATS.on_timeout(ctx)
        _log(f"请求超时(连接已关闭,需重新发起): 客户端={ctx.peer}:{port} "
             f"请求={ctx.op()} 卡在={stage}{body_desc} 已耗时={ctx.elapsed:.1f}s "
             f"重试情况={ctx.retry_desc()} "
             f"| 该访问累计超时{n_timeout}次/客户端累计尝试{n_attempt}次"
             + (f" | 重发 PUT 时已成功的分片会按 SHA 去重复用，只补传失败片"
                if ctx.method == "PUT" else "")
             + f" | 底层={raw}")
        self._last_req = f"{ctx.op()}(超时)"

    def _finish_ctx(self):
        """请求正常收尾：给空闲超时日志留一句「上一个请求」的描述。"""
        ctx = self._ctx
        if ctx is None:
            return None
        if not self._hdr_status:
            # 未产生响应（超时/中断）：描述已由超时日志写好，不覆盖
            return None
        if ctx.up_retry or ctx.down_retry:
            _log(f"请求完成(过程中有重试): 客户端={ctx.peer} {ctx.op()} "
                 f"重试={ctx.retry_desc()} 总耗时={ctx.elapsed:.1f}s")
        return f"{ctx.op()} status={self._hdr_status}"

    # ---------------- 请求级状态 ----------------
    def __init__(self, *args, **kwargs):
        self._body_read = 0
        self._hdr_status = 0
        self._hdr_conn_sent = False
        self._response_started = False
        self._request_body = None
        self._read_timeout = _BODY_TIMEOUT
        # 超时观测相关
        self._ctx = None
        self._req_parsed = False
        self._conn_seq = 0
        self._last_req = "-"
        self._idle_timeout_used = 0.0
        super().__init__(*args, **kwargs)

    def handle_one_request(self):
        """每个请求处理完都确保读净请求体并清理残包（keep-alive 兼容）。

        异常分三类处置，这一点很关键：
          - 客户端断开（ConnectionError）：静默结束，无需响应；
          - 读超时（空闲连接等待下一个请求 / SocketIO 已被标记超时）：
            这是 HTTP 服务器正常的连接回收，静默关闭即可，**绝不能回 500**——
            否则 keep-alive 复用时客户端会莫名收到 500，表现出来就是挂载失灵；
          - 其它异常：回 500 并关连接，且只吐一行纯文本，不漏 traceback/HTML。
        """
        self._body_read = 0
        self._hdr_status = 0
        self._hdr_conn_sent = False
        self._response_started = False
        self._request_body = None
        self._req_parsed = False
        self._ctx = None
        try:
            self.connection.settimeout(self.app.config.idle_timeout)
            self._idle_timeout_used = float(self.app.config.idle_timeout)
            # 请求体读超时取自 config（DAV_BODY_TIMEOUT，默认 300s）；模块常量只作兜底。
            # 之前这里一直用死常量，导致 DAV_BODY_TIMEOUT 配了也不生效——
            # 客户端慢到想放宽/收紧请求体超时时无从下手，只能干等 300s 才断。
            self._read_timeout = float(
                getattr(self.app.config, "body_timeout", 0) or _BODY_TIMEOUT)
        except Exception:
            pass
        try:
            super().handle_one_request()
            _d = self._finish_ctx()
            if _d:
                self._last_req = _d
        except (ConnectionError, TimeoutError, socket.timeout):
            # 客户端断开 / 空闲超时回收：正常行为，静默关闭，不刷日志
            self.close_connection = True
            return
        except Exception as e:  # noqa: BLE001
            if _is_timeout_err(e):
                self.close_connection = True
                return
            # 未捕获的异常：记录具体类型与原因，回 500 但绝不吐 traceback/HTML 给客户端
            import traceback
            _log(f"未捕获异常(返回500): 请求={getattr(self, 'command', '?')} "
                 f"{getattr(self, 'path', '?')} 异常={type(e).__name__}: {e}\n"
                 f"    traceback={''.join(traceback.format_exception_only(type(e), e))!s:.200}".rstrip())
            self._send_error(500, f"internal error: {type(e).__name__}")
            self.close_connection = True
        finally:
            try:
                self._drain_body()
            except Exception as e:
                _log(f"请求收尾时 _drain_body 异常(关闭连接): {type(e).__name__}: {e}")
                self.close_connection = True

    def send_response(self, code, message=None):
        """发送状态行。

        - 记录状态、标记响应已开始（异常兜底时避免重复发响应）。
        - 若配置了 ``DAV_KEEPALIVE=off``，则每条连接只服务一次（保守模式）。
        - 是否 keep-alive 由「客户端意图 + 残包检测结果」共同决定，最终在
          ``end_headers`` 里写进 ``Connection`` 头。
        """
        self._hdr_status = code
        self._hdr_conn_sent = False
        self._response_started = True
        if not self.app.config.keepalive:
            self.close_connection = True
        super().send_response(code, message)

    def end_headers(self):
        if not self._hdr_conn_sent:
            self._hdr_conn_sent = True
            try:
                self.send_header("Connection", "close" if self.close_connection else "keep-alive")
            except Exception:
                pass
        super().end_headers()

    def _send(self, status, headers, body=None):
        self.send_response(status)
        self.send_header("Date", _now_http(time.time()))
        for k, v in headers.items():
            self.send_header(k, v)
        if body is None:
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
        if "Content-Length" not in headers:
            self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if self.command != "HEAD":
            try:
                self.wfile.write(body)
            except (ConnectionError, OSError):
                # 客户端中途断开，静默结束
                pass

    def _send_error(self, code, msg):
        """兜底错误响应：保证一定发出去，且绝不吐 Python 原始 traceback。"""
        if self._response_started and self._hdr_status >= 100 and self._hdr_status < 400:
            # 响应已部分写出（如流式下载中），只能关连接
            self.close_connection = True
            return
        try:
            body = msg.encode("utf-8", "replace")
            self.send_response(code)
            self.send_header("Date", _now_http(time.time()))
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            if self.command != "HEAD":
                try:
                    self.wfile.write(body)
                except (ConnectionError, OSError):
                    pass
        except Exception:
            self.close_connection = True

    # ---------------- 请求体读取与残包检测 ----------------
    def _iter_body_exact(self, n, blk=1024 * 1024):
        """按 n 字节流式读取请求体的生成器（带超时保护，记账到 ``_body_read``）。

        与 ``_read_exact`` 的区别：不把 n 字节攒在内存里返回，而是读一块 yield 一块，
        供流式上传边收边传——1GB 文件不再需要 1GB 的服务端内存。
        """
        if n <= 0:
            return
        orig = None
        try:
            orig = self.connection.gettimeout()
            self.connection.settimeout(self._read_timeout)
        except Exception:
            pass
        left = n
        try:
            while left > 0:
                chunk = self.rfile.read(min(left, blk))
                if not chunk:
                    break  # 客户端断开或提前结束
                left -= len(chunk)
                self._body_read += len(chunk)
                yield chunk
        finally:
            try:
                self.connection.settimeout(orig)
            except Exception:
                pass

    def _read_exact(self, n):
        """读取恰好 n 字节（带超时保护），并记账到 ``_body_read``。"""
        buf = bytearray()
        if n <= 0:
            return bytes(buf)
        orig = None
        try:
            orig = self.connection.gettimeout()
            self.connection.settimeout(self._read_timeout)
        except Exception:
            pass
        try:
            while len(buf) < n:
                chunk = self.rfile.read(min(n - len(buf), 65536))
                if not chunk:
                    break  # 客户端断开或提前结束
                buf += chunk
                self._body_read += len(chunk)
        finally:
            try:
                self.connection.settimeout(orig)
            except Exception:
                pass
        return bytes(buf)

    def _read_chunked(self):
        """读取 chunked 编码的请求体（兜底，WebDAV 客户端多用 Content-Length）。"""
        buf = bytearray()
        try:
            self.connection.settimeout(self._read_timeout)
        except Exception:
            pass
        try:
            while True:
                line = self.rfile.readline(65536)
                if not line:
                    break
                line = line.split(b";", 1)[0].strip()
                if not line:
                    continue
                try:
                    size = int(line, 16)
                except ValueError:
                    break
                if size == 0:
                    break
                remaining = size
                while remaining > 0:
                    part = self.rfile.read(min(remaining, 65536))
                    if not part:
                        break
                    buf += part
                    self._body_read += len(part)
                    remaining -= len(part)
                self.rfile.readline()  # 块后 CRLF
        finally:
            try:
                self.connection.settimeout(self.app.config.idle_timeout)
            except Exception:
                pass
        return bytes(buf)

    def _peek_nonblocking(self):
        """非破坏性地看一眼「还有没有字节可读」（rfile 缓冲 or 内核缓冲）。

        为什么不能直接 ``self.rfile.peek(1)``：
          - ``peek`` 在缓冲为空时会**一直阻塞到套接字超时**——拿空闲超时（30s）去
            peek，每条请求都要白等，吞吐直接崩；
          - 更糟的是一旦触发超时，``SocketIO`` 会被永久标记成 timed out，之后这条
            连接上任何读都抛 ``OSError('cannot read from timed out object')``，
            表现出来就是莫名的 500。
        所以这里临时切成非阻塞（timeout=0）看一眼，用完立刻还原。
        """
        old = None
        try:
            old = self.connection.gettimeout()
            self.connection.settimeout(0)
        except Exception:
            pass
        try:
            return self.rfile.peek(1) or b""
        except Exception:
            return b""
        finally:
            try:
                self.connection.settimeout(old)
            except Exception:
                pass

    @staticmethod
    def _looks_like_request(buf):
        """缓冲开头是否像一条新的 HTTP 请求起始行（即流水线里的下一条请求）。

        RFC 7230 §3.5 允许请求行前先来一个空行（CRLF），所以先剥掉前导换行再判断——
        否则 AList 多发的那个 ``\\n`` 后面紧跟的合法请求会被误判成垃圾。
        """
        if not buf:
            return False
        line = buf[:1024].lstrip(b"\r\n").split(b"\n", 1)[0].rstrip(b"\r")
        parts = line.split()
        if len(parts) != 3:
            return False
        method, target, ver = parts
        if method not in _HTTP_METHODS:
            return False
        if not (target.startswith(b"/") or target == b"*" or b"://" in target):
            return False
        return ver.startswith(b"HTTP/")

    def _drain_residual(self, limit=_RESIDUAL_LIMIT):
        """清掉 Content-Length 之外多出来的垃圾字节，返回丢弃的字节数。

        AList/OpenList 等客户端偶尔把 Content-Length 少算 1 字节（XML body 尾部
        多一个 ``\\n``）。这些字节若留在连接上，keep-alive 下会被当成下一条请求的
        起始行，服务端吐 400 HTML，客户端就报
        ``malformed HTTP status code "HTML>"``。

        处理策略（比"一发现残留就关连接"更好）：
          - 不像新请求起始行的前导字节 -> 直接丢掉，连接照样能复用；
          - 撞上一条看起来合法的请求 -> 立刻停手，那是流水线，交给下一轮处理；
          - 丢到上限还在丢 -> 客户端严重错乱，关连接。
        """
        dropped = 0
        try:
            while dropped < limit:
                buf = self._peek_nonblocking()
                if not buf or self._looks_like_request(buf):
                    break
                self.rfile.read(1)
                dropped += 1
        except Exception:
            self.close_connection = True
        if dropped >= limit:
            self.close_connection = True
        return dropped

    def _consume_body(self):
        """读取整个请求体（若有），并返回字节；同时探测残留字节。

        供 PROPFIND/LOCK/PROPPATCH/DELETE/MKCOL/COPY/MOVE/webhook 等使用。
        在发送响应之前调用，这样若发现残留可立即标记关闭连接，``Connection`` 头才准确。
        """
        te = (self.headers.get("Transfer-Encoding") or "").strip().lower()
        if "chunked" in te:
            return self._read_chunked()
        cl = self.headers.get("Content-Length")
        if cl is None:
            return b""  # 无体的请求
        try:
            n = int(cl)
        except ValueError:
            n = 0
        data = self._read_exact(n)
        self._drain_residual()
        return data

    def _drain_body(self):
        """安全网：处理完请求后若仍有未读的请求体，读净它，并清理残包。"""
        try:
            if self.headers is None:
                return  # 请求行都没解析出来（空闲超时等），没什么可读的
            te = (self.headers.get("Transfer-Encoding") or "").strip().lower()
            if "chunked" in te:
                try:
                    self._read_chunked()
                except Exception:
                    self.close_connection = True
                return
            cl = self.headers.get("Content-Length")
            if cl is None:
                return
            try:
                n = int(cl)
            except ValueError:
                self.close_connection = True
                return
            left = n - self._body_read
            if left > 0:  # handler 没读净（异常提前返回等），补读
                try:
                    self.connection.settimeout(self._read_timeout)
                    while left > 0:
                        part = self.rfile.read(min(left, 262144))
                        if not part:
                            break
                        left -= len(part)
                    if left > 0:
                        self.close_connection = True
                except Exception:
                    self.close_connection = True
                finally:
                    try:
                        self.connection.settimeout(self.app.config.idle_timeout)
                    except Exception:
                        pass
            self._drain_residual()
        except Exception:
            self.close_connection = True

    def _auth_ok(self):
        cfg = self.app.config
        if not cfg.auth_enabled:
            return True
        h = self.headers.get("Authorization", "")
        if not h.startswith("Basic "):
            return False
        try:
            decoded = base64.b64decode(h[6:]).decode("utf-8", "replace")
            user, _, pw = decoded.partition(":")
        except Exception:
            return False
        return user == cfg.auth_user and pw == cfg.auth_password

    def _require_auth(self):
        if not self._auth_ok():
            self._send(
                401,
                {"WWW-Authenticate": 'Basic realm="telegram-webdav"'},
                b"401 Unauthorized",
            )
            return False
        return True

    def _normalize_path(self, raw):
        """把任意形式的请求目标规范化为内部绝对路径（无尾斜杠，根仍为 ``/``）。

        - 兼容代理发来的绝对形式 ``http://host/path``（取 path 部分）。
        - 兼容有无尾斜杠：``/foo`` 与 ``/foo/`` 都归一成 ``/foo``。
        - 兼容 ``DAV_ROOT`` 挂载：把挂载前缀剥掉。
        - 拒绝 ``..`` 路径穿越，避免越权访问挂载点之外。
        - 折叠多余 ``/``。
        """
        if raw is None:
            return None
        p = urllib.parse.urlsplit(raw).path
        p = urllib.parse.unquote(p)
        if not p.startswith("/"):
            p = "/" + p
        # 路径穿越防护
        if ".." in p.split("/"):
            return None
        p = re.sub(r"/+", "/", p)
        rp = self.app.config.root_path
        if rp not in ("", "/"):
            rp = rp.rstrip("/")
            if p == rp or p.startswith(rp + "/"):
                p = p[len(rp):] or "/"
            else:
                return None
        if p != "/":
            p = p.rstrip("/")
        return p

    def _href(self, path, is_dir):
        """生成对外 href：集合补尾斜杠，并带 ``DAV_ROOT`` 挂载前缀。"""
        rp = self.app.config.root_path
        if rp in ("", "/"):
            h = path
        else:
            h = rp.rstrip("/") + path
        if is_dir and not h.endswith("/"):
            h += "/"
        return h

    def _parse_range(self, total):
        h = self.headers.get("Range", "")
        m = re.match(r"bytes=(\d*)-(\d*)$", h.strip())
        if not m:
            return None
        s, e = m.group(1), m.group(2)
        if s == "" and e == "":
            return None
        if s == "":
            # 后缀范围 bytes=-N
            n = int(e)
            if n <= 0:
                return None
            start = max(0, total - n)
            end = total - 1
        else:
            start = int(s)
            end = int(e) if e else total - 1
        if start < 0 or end < start or start >= total:
            return "invalid"
        return (start, min(end, total - 1))

    # ---------------- OPTIONS ----------------
    def do_OPTIONS(self):
        if not self._require_auth():
            return
        self._send(
            200,
            {
                "DAV": "1, 2",
                "Allow": "OPTIONS, PROPFIND, GET, HEAD, PUT, DELETE, MKCOL, MOVE, COPY, LOCK, UNLOCK, PROPPATCH",
                "MS-Authoring-FrontPage": "none",
                "Accept-Ranges": "bytes",
            },
        )

    # ---------------- PROPFIND ----------------
    def do_PROPFIND(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None:
            self._send(404, {"Content-Type": "text/plain; charset=utf-8"}, b"404 Not Found")
            return
        # 读净请求体（含残包检测），避免 keep-alive 错位
        self._consume_body()
        depth = self.headers.get("Depth", "1")
        if depth not in ("0", "1", "infinity"):
            depth = "1"
        self_node, children = self.app.db.list_children(path, depth)
        if self_node is None:
            self._send(404, {"Content-Type": "text/plain; charset=utf-8"}, b"404 Not Found")
            return
        xml = self._build_propfind(path, self_node, children)
        self._send(
            207,
            {"Content-Type": 'application/xml; charset="utf-8"'},
            xml.encode("utf-8"),
        )
        # 响应先发给客户端，再后台预热：列目录到用户点开文件通常有几秒，
        # 足够把 getFile 的 ~1s RTT 提前消化掉，且不拖慢列目录本身。
        self._warmup_dir(children)

    def _build_propfind(self, base, self_node, children):
        items = [self_node] + children
        out = ['<?xml version="1.0" encoding="utf-8"?>']
        # xmlns:T 为本项目的自定义元数据命名空间（媒体时长等）。不支持的客户端会忽略它。
        out.append('<D:multistatus xmlns:D="DAV:" xmlns:T="urn:telegram-webdav:meta">')
        for n in items:
            is_dir = bool(n["is_dir"])
            href = self._href(n["path"], is_dir)
            out.append("  <D:response>")
            out.append(f"    <D:href>{xml.sax.saxutils.escape(href)}</D:href>")
            out.append("    <D:propstat><D:prop>")
            if is_dir:
                out.append("      <D:resourcetype><D:collection/></D:resourcetype>")
            else:
                out.append("      <D:resourcetype/>")
            etag_val = n.get("etag") or '""'
            out.append(f"      <D:getcontentlength>{n['size']}</D:getcontentlength>")
            out.append(f"      <D:getlastmodified>{_now_http(n['mtime'])}</D:getlastmodified>")
            out.append(f"      <D:creationdate>{_now_iso(n['ctime'])}</D:creationdate>")
            out.append(f"      <D:getetag>{etag_val}</D:getetag>")
            ct = n.get("content_type") or ("" if is_dir else "application/octet-stream")
            out.append(f"      <D:getcontenttype>{ct}</D:getcontenttype>")
            out.append(f"      <D:displayname>{xml.sax.saxutils.escape(n['name'])}</D:displayname>")
            # 自定义属性：媒体时长（秒，3 位小数）。仅音频/视频有，其余文件不输出该元素。
            dur = n.get("duration")
            if dur:
                out.append(f'      <T:duration>{dur:.3f}</T:duration>')
            out.append("    </D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat>")
            out.append("  </D:response>")
        out.append("</D:multistatus>")
        return "\n".join(out)

    # ---------------- GET / HEAD ----------------
    def do_GET(self):
        if not self._require_auth():
            return
        self._serve_file(head_only=False)

    def do_HEAD(self):
        if not self._require_auth():
            return
        self._serve_file(head_only=True)

    # ---------------- 并发上传 / 下载 ----------------
    def _upload_parallel(self, body_data, orig_name, multi, chunk_size, chunks_meta,
                         file_hash, head_sample_size, tail_sample_size):
        """并发上传所有分片到 Telegram：线程池切片上传，保序收集。

        - 并发数 = config._upload_workers(槽位数)（默认=bot 数，受每 bot 1 msg/s 限流约束）
        - 每分片独立算 SHA-256 供下载校验；整文件 SHA-256 按序组装
        - 任一分片失败即取消其余任务并抛出，由调用方回 502
        """
        total = len(body_data)
        n_chunks = (total + chunk_size - 1) // chunk_size
        workers = self.app.config._upload_workers(len(self.app.backend.slots))
        dedup_on = getattr(self.app.config, "chunk_dedup", True)
        _log(f"PUT 并发上传启动: 分片数={n_chunks} 并发线程={workers} "
             f"分片去重={'on' if dedup_on else 'off'}")

        def _one(ci, buf):
            chunk_sha = hashlib.sha256(buf).hexdigest()
            # 去重：这片内容若已上传过，直接复用原 file_id，跳过本次上传。
            # 大文件重传时靠它做到「只补传失败的那几片」。
            if dedup_on:
                hit = self.app.db.find_chunk_by_sha(chunk_sha, len(buf))
                if hit:
                    fid, slot, mid = hit
                    _log(f"PUT 分片[{ci}] 命中去重(跳过上传): size={len(buf)}B "
                         f"slot={slot} file_id={fid}")
                    return ci, fid, slot, mid, chunk_sha, buf, True
            chunk_name = orig_name if not multi else f"{orig_name}.part{ci:03d}"
            fid, slot, mid = self.app.backend.upload_chunk(buf, file_name=chunk_name)
            if dedup_on:
                self.app.db.put_chunk_dedup(chunk_sha, fid, slot, mid, len(buf))
            return ci, fid, slot, mid, chunk_sha, buf, False

        ctx = getattr(self, "_ctx", None)   # 请求级重试计数（超时日志要打印）

        def _one_retry(ci, buf):
            """上传单分片，失败只重试这一片（不牵连其他分片），重试耗尽才抛出。"""
            last = None
            for attempt in range(_UP_CHUNK_RETRY + 1):
                try:
                    return _one(ci, buf)
                except _tg.TGError as e:
                    last = e
                    if attempt >= _UP_CHUNK_RETRY:
                        break
                    _log(f"PUT 分片[{ci}] 上传失败，重试({attempt + 1}/{_UP_CHUNK_RETRY}): {e}")
                    if ctx is not None:
                        ctx.bump_up()
                    time.sleep(_up_backoff(attempt, str(e)))
            raise last

        results = [None] * n_chunks
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            fut_map = {
                ex.submit(_one_retry, i, body_data[i * chunk_size:(i + 1) * chunk_size]): i
                for i in range(n_chunks)
            }
            for fut in concurrent.futures.as_completed(fut_map):
                i = fut_map[fut]
                try:
                    results[i] = fut.result()
                except _tg.TGError as e:
                    # 该分片重试已耗尽，整体失败：取消尚未开始的任务，避免无谓的上传。
                    # 注意 fut_map 的 key 才是 Future（value 是分片索引），
                    # 早期版本误用 fut_map.values() 去 .cancel()，实际是在对 int 调用，
                    # 会抛 AttributeError 并掩盖真正的失败原因（429/网络错误）。
                    for f2 in fut_map:
                        f2.cancel()
                    _log(f"PUT 分片[{i}] 重试 {_UP_CHUNK_RETRY} 次后仍失败，"
                         f"已取消其余未开始任务: {e}")
                    raise
        # 按序组装：整文件哈希 + 分片元信息（带 _buf 供调用方取 head/tail 采样，落库前清除）
        reused = 0
        for res in results:
            _, fid, slot, mid, chunk_sha, buf, hit = res
            if hit:
                reused += 1
            file_hash.update(buf)
            chunks_meta.append({
                "file_id": fid, "slot": slot, "size": len(buf),
                "message_id": mid, "sha256": chunk_sha, "_buf": buf,
            })
        if reused:
            _log(f"PUT 分片复用统计: 共 {n_chunks} 片，其中 {reused} 片命中去重直接复用，"
                 f"{n_chunks - reused} 片实际上传")

    def _upload_streaming(self, body_iter, total, orig_name, multi, chunk_size,
                          chunks_meta, file_hash, head_sample_size, tail_sample_size):
        """边收边传：从 ``body_iter`` 流式取字节，攒满一片就交给线程池上传。

        为什么需要它（1GB+ 文件的硬伤）：
          旧路径先 ``_read_exact`` 把整个文件读进内存（1.2GB → 1.2GB），
          ``_upload_parallel`` 再按分片切片（又一份 1.2GB），实测 1.2GB 上传
          服务端内存峰值 **2743MB ≈ 文件大小的 2.27 倍**。2GB 文件就是 ~4.5GB，
          普通 1~2GB 内存的容器直接 OOM。

        现在内存占用 ≈ 在飞分片数 × 分片大小（默认 2×5×20MB ≈ 200MB），与文件大小无关。

        关键点：
        - 顺序读取 → 整文件 SHA-256 在提交前按序 update，结果与整文件计算一致
        - head/tail 采样：只保留首片头 512KB 与末片尾 4MB，不保留全部分片数据
        - 在飞分片数上限 = 2 × 并发数，防止 TG 上传慢、客户端快时内存堆积
        - 任一分片失败立即取消剩余任务并抛出，由调用方回 502
        """
        n_chunks = (total + chunk_size - 1) // chunk_size if total > 0 else 0
        workers = self.app.config._upload_workers(len(self.app.backend.slots))
        max_inflight = max(2, workers * 2)
        dedup_on = getattr(self.app.config, "chunk_dedup", True)
        _log(f"PUT 流式上传启动: 分片数={n_chunks} 并发线程={workers} "
             f"在飞上限={max_inflight} 分片大小={chunk_size // 1024 // 1024}MB "
             f"分片重试={_UP_CHUNK_RETRY} 分片去重={'on' if dedup_on else 'off'} "
             f"(内存占用≈在飞分片数×分片大小,与文件大小无关)")

        results = [None] * n_chunks
        head_sample = b""
        tail_sample = b""
        pending = {}

        def _one(ci, buf):
            chunk_sha = hashlib.sha256(buf).hexdigest()
            # 去重：这片内容若已上传过，直接复用原 file_id，跳过本次上传。
            # 1.2GB 重传时 60 片里 59 片命中 → 实际只补传失败那 1 片。
            if dedup_on:
                hit = self.app.db.find_chunk_by_sha(chunk_sha, len(buf))
                if hit:
                    fid, slot, mid = hit
                    _log(f"PUT 分片[{ci}] 命中去重(跳过上传): size={len(buf)}B "
                         f"slot={slot} file_id={fid}")
                    return ci, fid, slot, mid, chunk_sha, buf, True
            chunk_name = orig_name if not multi else f"{orig_name}.part{ci:03d}"
            fid, slot, mid = self.app.backend.upload_chunk(buf, file_name=chunk_name)
            if dedup_on:
                self.app.db.put_chunk_dedup(chunk_sha, fid, slot, mid, len(buf))
            return ci, fid, slot, mid, chunk_sha, buf, False

        ctx = getattr(self, "_ctx", None)   # 请求级重试计数（超时日志要打印）

        def _one_retry(ci, buf):
            """上传单分片，失败只重试这一片（不牵连其他分片），重试耗尽才抛出。"""
            last = None
            for attempt in range(_UP_CHUNK_RETRY + 1):
                try:
                    return _one(ci, buf)
                except _tg.TGError as e:
                    last = e
                    if attempt >= _UP_CHUNK_RETRY:
                        break
                    _log(f"PUT 分片[{ci}] 上传失败，重试({attempt + 1}/{_UP_CHUNK_RETRY}): {e}")
                    if ctx is not None:
                        ctx.bump_up()
                    time.sleep(_up_backoff(attempt, str(e)))
            raise last

        def _harvest(done_futs):
            """回收已完成的任务；任一分片失败即抛出。"""
            nonlocal tail_sample
            for fut in done_futs:
                ci, fid, slot, mid, chunk_sha, buf, hit = fut.result()
                results[ci] = (ci, fid, slot, mid, chunk_sha, len(buf), hit)
                if ci == n_chunks - 1:
                    tail_sample = buf[-tail_sample_size:]
                del pending[fut]

        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            ci = 0
            buf = bytearray()
            for block in body_iter:
                buf += block
                while len(buf) >= chunk_size and ci < n_chunks:
                    piece = bytes(buf[:chunk_size])
                    del buf[:chunk_size]
                    file_hash.update(piece)          # 顺序：整文件哈希与整算一致
                    if ci == 0:
                        head_sample = piece[:head_sample_size]
                    pending[ex.submit(_one_retry, ci, piece)] = ci
                    ci += 1
                    if len(pending) >= max_inflight:
                        done, _ = concurrent.futures.wait(
                            pending, return_when=concurrent.futures.FIRST_COMPLETED)
                        _harvest(done)
            if ci < n_chunks and buf:                # 最后一片（不足 chunk_size）
                piece = bytes(buf)
                file_hash.update(piece)
                if ci == 0:
                    head_sample = piece[:head_sample_size]
                pending[ex.submit(_one_retry, ci, piece)] = ci
                ci += 1
            for fut in concurrent.futures.as_completed(list(pending)):
                _harvest([fut])

        reused = 0
        for res in results:
            if res is None:
                raise _tg.TGError(f"分片 {len(chunks_meta)} 未上传成功")
            _, fid, slot, mid, chunk_sha, size, hit = res
            if hit:
                reused += 1
            chunks_meta.append({
                "file_id": fid, "slot": slot, "size": size,
                "message_id": mid, "sha256": chunk_sha,
            })
        if reused:
            _log(f"PUT 分片复用统计: 共 {n_chunks} 片，其中 {reused} 片命中去重直接复用，"
                 f"{n_chunks - reused} 片实际上传")
        return head_sample, tail_sample

    def _stream_chunk(self, entry, blk=None):
        """边下边发单分片：迭代 backend.iter_chunk 产生的字节块，直接写给客户端。

        这是「播放丝滑」的核心：从 Telegram 读到 `blk` 字节就立刻写给客户端一次，
        而不是攒够整个分片（20MB）再发——后者会让播放器每到一个分片边界就干等
        一整片的下载时间（真实网络下 2~10s），表现为周期性卡顿。

        ``blk`` 缺省由 ``TG_STREAM_BLOCK_KB`` 决定（默认 256KB）：越小数据到达越平滑、
        起播/seek 首字节越快。

        完整分片(verify=True)会增量计算 SHA-256 并在分片末尾比对（边发边校验），
        不符立即抛 _IntegrityError 中断连接；Range 切开的片段(verify=False)只转发不校验。
        返回实际写出的字节数，供收尾日志统计吞吐。
        """
        ci, c, cstart, cend, rs, re_, verify = entry
        h = hashlib.sha256() if verify else None
        sent = 0
        nblk = 0
        t_first = None
        try:
            for block in self.app.backend.iter_chunk(
                c["file_id"], c.get("slot", 0), rs, re_, blk=blk,
                ctx=getattr(self, "_ctx", None),
            ):
                if t_first is None:
                    t_first = time.time()
                if h is not None:
                    h.update(block)
                try:
                    self.wfile.write(block)
                    sent += len(block)
                    nblk += 1
                except (ConnectionError, OSError):
                    # 客户端中途断开：抛出内部信号，让调用方立刻停止（不再等其余分片）
                    raise _ClientGone(sent)
        except _tg.TGError:
            raise
        except _ClientGone:
            # 客户端主动断开是正常结束信号，必须原样传播——若被下面的兜底捕获并
            # 转成 TGError，日志会把「客户端提前断开」误报成「下载分片失败」。
            raise
        except _IntegrityError:
            raise
        except Exception as e:
            # 兜底：任何非 TGError 的异常（如代理偶发的 http.client.ResponseNotReady）
            # 都统一转成 TGError，让上层以「干净断连」处理，而不是冒出
            # 「未捕获异常(返回500)」污染已经发出 206 头的响应体。
            raise _tg.TGError(f"分片下载异常: {type(e).__name__}: {e}")
        if h is not None and h.hexdigest() != c.get("sha256"):
            raise _IntegrityError(cstart)
        return sent, nblk, t_first

    # ---- file_path 预热（不落库版提速）----
    # Telegram 的 file_path 有效期只有 1 小时，落库会拿到过期路径，所以只在内存里预热：
    # 客户端列目录(PROPFIND) / 探测(HEAD) 时就把 file_path 取回放进 50min TTL 缓存，
    # 等真正 GET 时缓存已热，getFile 的 ~1s RTT 从关键路径上消失（实测 TTFB 1.15s→0.18s）。
    # 预热全部走后台 daemon 线程 + 独立连接，不阻塞响应、不争用下载的连接池。
    # 预热并发上限：避免大目录反复列目录时堆积过多预热线程打到代理。
    def _warmup_paths(self, items, tag):
        """后台预热一批分片的 file_path。已在缓存中的会被跳过（零开销）。

        items: [(file_id, slot), ...]；tag 仅用于日志区分来源（PROPFIND / HEAD）。
        """
        if not items:
            return
        global _WARM_ACTIVE
        with _WARM_LOCK:
            if _WARM_ACTIVE >= _WARM_MAX_ACTIVE:
                _log(f"file_path 预热跳过: 来源={tag} 已有 {_WARM_ACTIVE} 个预热在执行")
                return
            _WARM_ACTIVE += 1

        def _run():
            global _WARM_ACTIVE
            try:
                self.app.backend.prefetch_paths(items)
            except Exception as e:
                _log(f"file_path 预热异常(忽略): 来源={tag} err={type(e).__name__}: {e}")
            finally:
                with _WARM_LOCK:
                    _WARM_ACTIVE -= 1

        threading.Thread(target=_run, name=f"warmup-{tag}", daemon=True).start()

    def _warmup_dir(self, nodes):
        """列目录后预热目录内文件的首片 file_path。

        只取首片：起播/打开文件的第一个字节就是它，是关键路径；
        其余分片在 GET 时由 prefetch_paths 并发预取（已在 _serve_file 中实现）。
        """
        if not getattr(self.app.config, "warmup_propfind", True):
            return
        items = []
        for n in nodes:
            if n.get("is_dir") or not n.get("chunks"):
                continue
            try:
                cs = json.loads(n["chunks"])
            except (ValueError, TypeError):
                continue
            if not cs:
                continue
            items.append((cs[0]["file_id"], cs[0].get("slot", 0)))
            if len(items) >= self.app.config.warmup_max_files:
                break
        if items:
            _log(f"file_path 预热触发: 来源=PROPFIND 文件数={len(items)} "
                 f"(仅首片,已缓存的自动跳过)")
            self._warmup_paths(items, "propfind")

    def _serve_file(self, head_only):
        path = self._normalize_path(self.path)
        if path is None:
            _log(f"GET 拒绝(404): 路径非法/越权 path={self.path!r}")
            self._send(404, {"Content-Type": "text/plain; charset=utf-8"}, b"404 Not Found")
            return
        node = self.app.db.get_node(path)
        if node is None:
            _log(f"GET 拒绝(404): 节点不存在 path={path}")
            self._send(404, {"Content-Type": "text/plain; charset=utf-8"}, b"404 Not Found")
            return
        if node["is_dir"]:
            # 目录：重定向到带尾斜杠的形式（浏览器/Windows 相对路径更稳）
            _log(f"GET 目录重定向(301): path={path} -> {self._href(path, True)}")
            self._send(
                301,
                {"Location": self._href(path, True)},
                b"301 Moved Permanently",
            )
            return
        total = node["size"]
        # 先解析再记日志：node['chunks'] 是 JSON 字符串，直接 len() 会数成字符数
        chunks = json.loads(node["chunks"]) if node.get("chunks") else []

        t0 = time.time()
        dur = node.get("duration")
        _log(f"GET 开始{' (HEAD)' if head_only else ''}: path={path} size={_fmt_size(total)} "
             f"分片数={len(chunks)} Range={self.headers.get('Range')} "
             f"类型={_media_kind(node.get('content_type'), path.rsplit('/', 1)[-1])} "
             f"时长={_fmt_dur(dur)}"
             f"{'(' + f'{dur:.3f}s' + ')' if dur is not None else ''}")

        rng = self._parse_range(total) if not head_only else None
        if rng == "invalid":
            self._send(
                416,
                {"Content-Range": f"bytes */{total}", "Content-Type": "text/plain; charset=utf-8"},
                b"416 Range Not Satisfiable",
            )
            return

        if rng is None:
            start, end, status = 0, total - 1, 200
        else:
            start, end, status = rng[0], rng[1], 206

        content_type = node.get("content_type") or "application/octet-stream"
        headers = {
            "Content-Type": content_type,
            "Accept-Ranges": "bytes",
            "Cache-Control": "no-store",
            "ETag": node.get("etag") or '""',
            "Last-Modified": _now_http(node["mtime"]),
        }
        if status == 206:
            headers["Content-Range"] = f"bytes {start}-{end}/{total}"
        headers["Content-Length"] = str(end - start + 1)

        if head_only:
            # 播放器/下载器通常「HEAD 探测 → 紧跟 GET」：趁探测把全部分片的
            # file_path 预热好，随后的 GET 直接命中缓存，省掉每片 ~1s 的 getFile。
            if getattr(self.app.config, "warmup_head", True) and chunks:
                self._warmup_paths(
                    [(c["file_id"], c.get("slot", 0)) for c in chunks], "head"
                )
            self._send(status, headers)
            return

        # 流式拼接分片
        self.send_response(status)
        self.send_header("Date", _now_http(time.time()))
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()

        if total == 0 or not chunks:
            return

        # 预先规划本次需要拉取的分片及其相对 [start,end] 的字节区间
        offset = 0
        plan = []  # (ci, c, cstart, cend, rs, re_, verify)
        for ci, c in enumerate(chunks):
            csize = c["size"]
            cstart = offset
            cend = offset + csize - 1
            offset += csize
            # 只有「完整落在请求区间内」的分片才能校验整片哈希；
            # Range 把分片切开的情形只流式转发、不校验（无完整分片可比对）。
            csha = c.get("sha256")
            verify = csha is not None and cstart >= start and cend <= end
            if cend < start or cstart > end:
                continue
            rs = max(cstart, start) - cstart
            re_ = min(cend, end) - cstart
            plan.append((ci, c, cstart, cend, rs, re_, verify))
        if not plan:
            return

        # 提速要点（已迭代）：首片在主线程「边下边发」(流式转发)，让客户端尽快拿到首字节(降低 TTFB)；
        # 其余分片交给主线程串行下载（不再用线程池并发拉取）。
        #
        # 历史版本曾用 ThreadPoolExecutor 并发拉取 rest 分片，但实测暴露三个连锁问题，导致
        # 「下一个分片下载完，下载下一个分片卡顿」：
        #   1) 按字节序 result() 阻塞：第 i 片 future.result() 未返回前，主线程无法轮到第 i+1 片
        #      result()，客户端看到的就是「分片间停顿」，即使 i+1 片早就下完躺在 future 里；
        #   2) keep-alive 连接池互锁：tg._conn_pool 里的 HTTPSConnection 在多线程同时调用
        #      iter_chunk 时被 http.client 内部序列化（甚至丢包），每个分片都需要额外的等待；
        #   3) Telegram 1 msg/s 流控：每 bot 并发打多个分片 → 频繁 429 → _send_document 切槽位
        #      重新 getFile/握手；自建代理侧也会撞并发上限（Cloudflare Worker 等）触发 504。
        # 改为主线程串行后：没有线程竞争、没有按序 result()、不触发单 bot 流控、连接池不互锁，
        # TTFB 与并发峰值都更稳。换文件/换客户端的并发仍在 ThreadingHTTPServer 层面自然并行。
        first = plan[0]
        rest = plan[1:]

        def _collect(ci, c, rs, re_):
            data = b"".join(self.app.backend.iter_chunk(
                c["file_id"], c.get("slot", 0), rs, re_,
                ctx=getattr(self, "_ctx", None),
            ))
            return ci, data

        # 单文件固定单线程下载（参数 `workers` 暂留以兼容日志，但实际不再开线程池）
        workers = 1
        # 播放关键参数：stream_all=全分片流式(默认)，blk=流式读块大小
        stream_all = getattr(self.app.config, "stream_all_chunks", True)
        blk = max(16, int(getattr(self.app.config, "stream_block_kb", 256))) * 1024
        _log(f"GET 单线程下载: path={path} 需拉分片={len(plan)} "
             f"Range={self.headers.get('Range')} "
             f"模式={'全分片流式' if stream_all else '仅首片流式(其余整片缓冲)'} "
             f"读块={blk // 1024}KB")

        def _fmt_speed_local(n, dt):
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

        sent = 0
        # 每分片时间线：用于诊断"分片间是否卡顿"。理想情况下相邻分片间隔≈0。
        tl = []  # (ci, 下载耗时, 写回耗时, 分片间隔, 字节数)
        last_done = time.time()  # 上一片处理完（写回客户端）的时刻

        # 真实环境优化：后台并发预取后续分片的 file_path，把 getFile 的 RTT 藏进下载时间里。
        #
        # 【踩过的坑】早期版本一次性把**全部** rest 分片丢去预取：60 分片的文件会瞬间发起
        # 59 个 getFile（8 路并发持续十几秒），把自建代理打满，getFile 从 0.7s 恶化到
        # 2.7~14.8s，连首片自己的 getFile 都被拖到 13s，TTFB 直接 13.959s——得不偿失。
        #
        # 改为**滑动窗口**：始终只预取「接下来 window 片」，处理完一片就往前推进一格。
        # 并发 getFile 始终 ≤ window，既消除了后续分片的等待，又不干扰首片的首字节。
        pf = None
        pf_window = max(1, int(getattr(self.app.config, "prefetch_window", 3)))
        pf_submitted = set()

        def _kick_prefetch(from_idx):
            """预取 plan[from_idx : from_idx+window] 中尚未提交过的分片。"""
            if pf is None:
                return
            items = []
            for e in plan[from_idx: from_idx + pf_window]:
                fid = e[1]["file_id"]
                if fid in pf_submitted:
                    continue
                pf_submitted.add(fid)
                items.append((fid, e[1].get("slot", 0)))
            if items:
                try:
                    pf.submit(self.app.backend.prefetch_paths, items)
                except Exception:
                    pass

        if rest and hasattr(self.app.backend, "prefetch_paths"):
            try:
                # 单线程 executor：保证同一时刻只有一批（≤window 个）getFile 在飞
                pf = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                _kick_prefetch(1)
            except Exception:
                pf = None

        try:
            # 首片：始终流式转发（边下边发，让客户端尽快拿到首字节）
            t_dl = time.time()
            n_first, nblk_first, tfb = self._stream_chunk(first, blk)
            dl_t = time.time() - t_dl
            sent += n_first
            tl.append((first[0], dl_t, 0.0, 0.0, n_first))
            _log(f"GET 分片[{first[0]}](首) 流式完成: 耗时={dl_t:.3f}s "
                 f"大小={_fmt_size(n_first)}({n_first}B) 块数={nblk_first} "
                 f"吞吐={_fmt_speed_local(n_first, dl_t)} "
                 f"首字节延迟={(tfb - t_dl):.3f}s(本片内) TTFB={(tfb - t0):.3f}s(自请求起)")
            last_done = time.time()
            # 首片已下发，预取由滑动窗口继续推进，不再阻塞等待
            # （预取失败也能正常下载，只是慢一点：iter_chunk 会自动回退到常规 getFile）
            # 其余分片：默认同样流式下发（播放丝滑的关键）；
            # 仅在 TG_STREAM_ALL_CHUNKS=off 时退回「整片缓冲后一次性写出」的旧行为。
            for idx, entry in enumerate(rest, start=1):
                ci, c, cstart, cend, rs, re_, verify = entry
                _kick_prefetch(idx + pf_window)  # 滑动窗口：往前推进预取
                t_dl = time.time()
                gap = t_dl - last_done  # 上片写回完 → 本片开始下载之间的间隔（卡顿核心指标）
                if stream_all:
                    n_sent, nblk, tfb = self._stream_chunk(entry, blk)
                    dl_t = time.time() - t_dl
                    tl.append((ci, dl_t, 0.0, gap, n_sent))
                    _log(f"GET 分片[{ci}] 流式完成: 耗时={dl_t:.3f}s 分片间隔={gap:.3f}s "
                         f"大小={_fmt_size(n_sent)}({n_sent}B) 块数={nblk} "
                         f"吞吐={_fmt_speed_local(n_sent, dl_t)}")
                    sent += n_sent
                else:
                    _, data = _collect(ci, c, rs, re_)
                    dl_t = time.time() - t_dl
                    if verify and hashlib.sha256(data).hexdigest() != c.get("sha256"):
                        raise _IntegrityError(cstart)
                    t_wr = time.time()
                    try:
                        self.wfile.write(data)
                        sent += len(data)
                    except (ConnectionError, OSError):
                        raise _ClientGone(sent)
                    wr_t = time.time() - t_wr
                    tl.append((ci, dl_t, wr_t, gap, len(data)))
                    _log(f"GET 分片[{ci}] 缓冲后写出: 下载耗时={dl_t:.3f}s "
                         f"写回耗时={wr_t:.3f}s 分片间隔={gap:.3f}s "
                         f"大小={_fmt_size(len(data))}({len(data)}B) "
                         f"吞吐={_fmt_speed_local(len(data), dl_t)}")
                last_done = time.time()
        except _ClientGone as e:
            _log(f"GET 客户端提前断开(已停止): path={path} 已发={_fmt_size(sent)} "
                 f"耗时={time.time() - t0:.2f}s 吞吐={_fmt_speed(sent, time.time() - t0)}")
            self.close_connection = True
            return
        except _IntegrityError as e:
            # 分片内容与上传时记录的不一致（Telegram 返回了损坏/截断的字节，或数据被污染）：
            # 已经可能发了一部分脏数据，立刻中断连接，绝不把错数据当完整文件交给客户端。
            _log(f"GET 分片完整性校验失败(中断连接): path={path} @offset {e.offset} "
                 f"已发={_fmt_size(sent)} "
                 f"说明=内容不符或被截断（Telegram 返回字节与原 file_id 记录的 sha256 不符）")
            self.close_connection = True
            return
        except _tg.TGError as e:
            _log(f"GET 下载分片失败(连接中断): path={path} 错误={e} 已发={_fmt_size(sent)}")
            self.close_connection = True
            return
        finally:
            if pf is not None:
                pf.shutdown(wait=False)
        dt_total = time.time() - t0
        # 卡顿诊断：相邻分片「写回完→下一片开始下载」的最大间隔；
        # 单线程串行下应≈0，若出现明显尖峰即说明某分片下载/写回异常阻塞。
        gaps = [g for (_, _, _, g, _) in tl[1:]]  # 跳过首片（无前置间隔）
        max_gap = max(gaps) if gaps else 0.0
        dl_times = [d for (_, d, _, _, _) in tl]
        _log(f"GET 完成: path={path} 状态={status} 区间={start}-{end}/{total} "
             f"已发={_fmt_size(sent)}({sent}B) 耗时={dt_total:.2f}s "
             f"吞吐={_fmt_speed(sent, dt_total)} 模式=单线程串行 "
             f"分片数={len(tl)} 最大分片间隔={max_gap:.3f}s "
             f"(卡顿指标:≈0表示分片平滑衔接) "
             f"类型={_media_kind(content_type, path.rsplit('/', 1)[-1])} "
             f"时长={_fmt_dur(dur)}")

    # ---------------- PUT ----------------
    def do_PUT(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None:
            _log(f"PUT 拒绝(403): 路径非法/越权 path={self.path!r}")
            self._send(403, {"Content-Type": "text/plain; charset=utf-8"}, b"403 Forbidden")
            return
        if self.app.backend is None:
            _log(f"PUT 拒绝(503): Telegram 未配置（backend=None）path={path}")
            self._send(503, {"Content-Type": "text/plain; charset=utf-8"},
                       "Telegram 未配置，无法存储".encode("utf-8"))
            return
        t0 = time.time()
        _log(f"PUT 收到: path={path} Content-Length={self.headers.get('Content-Length')} "
             f"Transfer-Encoding={self.headers.get('Transfer-Encoding')} "
             f"Content-Type={self.headers.get('Content-Type')} "
             f"Expect={self.headers.get('Expect')}")

        # 条件头：If-None-Match: * 表示「仅当不存在时创建」
        if_none = self.headers.get("If-None-Match", "")
        existed = self.app.db.get_node(path) is not None
        if if_none == "*" and existed:
            _log(f"PUT 拒绝(412): 已存在且 If-None-Match:* path={path}")
            self._send(412, {"Content-Type": "text/plain; charset=utf-8"},
                       b"412 Precondition Failed (already exists)")
            return

        parent = "/" if path == "/" else path.rsplit("/", 1)[0] or "/"
        pnode = self.app.db.get_node(parent)
        if pnode is None or not pnode["is_dir"]:
            _log(f"PUT 拒绝(409): 父目录不存在或非目录 path={path} parent={parent} "
                 f"parent_node={'缺失' if pnode is None else '存在但非目录'}")
            self._send(409, {"Content-Type": "text/plain; charset=utf-8"})  # Conflict: 父目录不存在
            return

        # 处理 Expect: 100-continue
        if self.headers.get("Expect", "").lower() == "100-continue":
            self.send_response_only(100)
            self.end_headers()

        # 读取请求体：优先用 Content-Length；若缺失则回退到 chunked 或流式读取。
        # 某些 WebDAV 客户端（Windows 资源管理器 / 特定配置的 rclone）可能不发送
        # Content-Length 头，此时必须通过 Transfer-Encoding 或 EOF 判断边界，
        # 否则 total=0 导致 while 循环不执行、文件数据静默丢失（创建空文件）。
        te = (self.headers.get("Transfer-Encoding") or "").strip().lower()
        cl_hdr = self.headers.get("Content-Length")
        total = int(cl_hdr) if cl_hdr is not None else 0
        # 有 Content-Length 且足够大 → 走「流式边收边传」：1GB 文件不再占 1GB 内存。
        # chunked / 无 CL 的兜底路径仍需整包读取（这类客户端上传大文件极少见）。
        _streaming = cl_hdr is not None and total > _STREAM_UPLOAD_MIN_BYTES
        if _streaming:
            _log(f"PUT 读取请求体: 模式=流式边收边传 size={total}B "
                 f"(>={_STREAM_UPLOAD_MIN_BYTES // 1024 // 1024}MB 走流式,避免整包占内存)")
            body_data = None
        elif cl_hdr is not None:
            body_data = b""
            if total > 0:
                _log(f"PUT 读取请求体: 模式=Content-Length(整包) size={total}B")
                body_data = self._read_exact(total)
                if len(body_data) != total:
                    _log(f"PUT 请求体长度不足(客户端提前断开): 期望 {total}B 实际收到 {len(body_data)}B "
                         f"path={path}")
                    raise _tg.TGError("请求体长度不足（客户端提前断开）")
        elif "chunked" in te:
            # chunked 编码：读全部块后拼接
            _log(f"PUT 读取请求体: 模式=chunked")
            body_data = self._read_chunked()
            total = len(body_data)
            _log(f"PUT chunked 读取完成: size={total}B")
        else:
            # 既无 CL 也非 chunked：对 PUT 方法尝试流式读取直到 EOF（短连接）
            # 注意：keep-alive 下无法安全判断 EOF，此处做最大努力读取
            _log(f"PUT 读取请求体: 模式=流式(无 Content-Length/Transfer-Encoding) "
                 f"path={path} keepalive={self.app.config.keepalive} "
                 f"注:keep-alive 下可能读不到 EOF，将按最大努力读取")
            body_data = b""
            try:
                while True:
                    self.connection.settimeout(self._read_timeout)
                    part = self.rfile.read(65536)
                    if not part:
                        break
                    body_data += part
                    self._body_read += len(part)
            finally:
                try:
                    self.connection.settimeout(self.app.config.idle_timeout)
                except Exception:
                    pass
            total = len(body_data)
            _log(f"PUT 流式读取完成: size={total}B")

        ct = self.headers.get("Content-Type", "").split(";")[0].strip()
        if not ct:
            ct = _guess_ct(path.rsplit("/", 1)[-1])

        chunk_size = self.app.config.chunk_size
        # 原始文件名：写进 Telegram 频道消息，让人浏览频道时看到的就是原文件名
        # （参考 otterhub-server 的做法）。单分片直接用原名；多分片加 .partNN 后缀，
        # 既保留原文件名线索，又能把不同分片区分开。
        orig_name = path.rsplit("/", 1)[-1] or "file.bin"
        multi = total > chunk_size
        chunks_meta = []
        ci = 0
        file_hash = hashlib.sha256()  # 整文件 SHA-256：边读边算，零额外内存
        head_sample = b""  # 首片头部采样（解析媒体时长用）
        tail_sample = b""  # 末片尾部采样
        n_chunks = (total + chunk_size - 1) // chunk_size if total > 0 else 0
        try:
            if total > 0:
                if _streaming:
                    # 边收边传：内存占用恒定，与文件大小无关（1GB+ 文件的关键）
                    head_sample, tail_sample = self._upload_streaming(
                        self._iter_body_exact(total), total, orig_name, multi,
                        chunk_size, chunks_meta, file_hash,
                        _MEDIA_HEAD_SAMPLE, _MEDIA_TAIL_SAMPLE)
                    # 流式路径下若客户端提前断开，实际收到的字节会少于 Content-Length
                    got = sum(c["size"] for c in chunks_meta)
                    if got != total:
                        _log(f"PUT 请求体长度不足(客户端提前断开): 期望 {total}B 实际收到 {got}B "
                             f"path={path}")
                        raise _tg.TGError("请求体长度不足（客户端提前断开）")
                else:
                    self._upload_parallel(body_data, orig_name, multi, chunk_size,
                                           chunks_meta, file_hash, _MEDIA_HEAD_SAMPLE,
                                           _MEDIA_TAIL_SAMPLE)
                    head_sample = chunks_meta[0]["_buf"][:_MEDIA_HEAD_SAMPLE] if chunks_meta else b""
                    tail_sample = chunks_meta[-1]["_buf"][-_MEDIA_TAIL_SAMPLE:] if chunks_meta else b""
                    for c in chunks_meta:
                        c.pop("_buf", None)  # 元信息落库前清掉内存引用
            # 清理残留字节（AList 类 CL 少算），必要时关闭连接
            self._drain_residual()
            _log(f"PUT 分片上传完成(并发={self.app.config._upload_workers(len(self.app.backend.slots))}): "
                 f"path={path} 分片数={n_chunks} 总大小={total}B chunk_size={chunk_size}B")
        except _tg.TGError as e:
            _log(f"PUT 上传到 Telegram 失败(返回502): path={path} 已上传分片={len(chunks_meta)}/{n_chunks} "
                 f"total={total}B 错误={e}")
            self._send(502, {"Content-Type": "text/plain; charset=utf-8"},
                       f"上传到 Telegram 失败: {e}".encode("utf-8"))
            return

        # 媒体时长（音频/视频）：本地解析，Telegram 侧仍是原字节存储，不影响分片与哈希。
        # 解析失败一律吞掉——绝不能因为元数据解析影响上传结果。
        duration = None
        dur_fmt = None
        if total > 0:
            try:
                duration, dur_fmt = _media.probe_duration_detail(
                    head_sample, tail_sample, total, path.rsplit("/", 1)[-1]
                )
            except Exception as e:
                # 文件头异常（非媒体 / 截断 / 格式无法解析）：仅影响时长元数据，不影响上传
                _log(f"PUT 媒体时长解析异常(已忽略,不影响上传): path={path} err={type(e).__name__}: {e}")
                duration = None
        # 音视频时长日志：识别出容器才打印「可读时长 + 容器」，否则明确说明未识别，
        # 便于一眼区分「非媒体文件」与「媒体文件但解析失败」。
        kind = _media_kind(ct, path.rsplit("/", 1)[-1])
        if duration is not None:
            _log(f"PUT 媒体解析: path={path} 类型={kind} 容器={dur_fmt} "
                 f"时长={_fmt_dur(duration)}({duration:.3f}s)")
        elif kind in ("audio", "video"):
            _log(f"PUT 媒体解析: path={path} 类型={kind} 未能解析出时长"
                 f"(采样不足/非标准封装/加密moov)，不影响上传")

        try:
            self.app.db.create_file(
                path, ct, chunks_meta if total > 0 else [], total,
                chunk_size=chunk_size if total > 0 else None,
                file_hash=file_hash.hexdigest() if total > 0 else None,
                duration=duration,
            )
        except Exception as e:
            _log(f"PUT 落库失败(返回500): path={path} 分片数={len(chunks_meta)} total={total} "
                 f"err={type(e).__name__}: {e}")
            self._send(500, {"Content-Type": "text/plain; charset=utf-8"},
                       f"元数据写入失败: {e}".encode("utf-8"))
            return
        _log(f"PUT 完成: path={path} 状态={'覆盖(204)' if existed else '新建(201)'} "
             f"size={_fmt_size(total)}({total}B) 分片数={len(chunks_meta)} content_type={ct} "
             f"时长={_fmt_dur(duration)}"
             f"{'(' + f'{duration:.3f}s' + ')' if duration is not None else ''} "
             f"耗时={time.time() - t0:.2f}s 吞吐={_fmt_speed(total, time.time() - t0)} "
             f"file_hash={'有' if total > 0 else '无(空文件)'}")
        self._send(204 if existed else 201, {"Content-Type": "text/plain"})

    # ---------------- DELETE ----------------
    def do_DELETE(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None or path == "/":
            self._send(403, {"Content-Type": "text/plain; charset=utf-8"}, b"403 Forbidden")
            return
        self._consume_body()  # AList 会给 DELETE 带 body，先读净
        node = self.app.db.get_node(path)
        if node is None:
            self._send(404, {"Content-Type": "text/plain; charset=utf-8"}, b"404 Not Found")
            return
        self.app.db.delete_recursive(path)
        # 注意：Telegram 不支持删除已发消息，物理分片仍留在频道（与 otterhub 一致）
        self._send(204, {"Content-Type": "text/plain"})

    # ---------------- MKCOL ----------------
    def do_MKCOL(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None or path == "/":
            self._send(403, {"Content-Type": "text/plain; charset=utf-8"}, b"403 Forbidden")
            return
        self._consume_body()  # 读净可能的 body（部分客户端会发空 XML）
        if self.app.db.get_node(path) is not None:
            self._send(405, {"Content-Type": "text/plain; charset=utf-8"})  # Method Not Allowed
            return
        parent = "/" if path == "/" else path.rsplit("/", 1)[0] or "/"
        pnode = self.app.db.get_node(parent)
        if pnode is None or not pnode["is_dir"]:
            self._send(409, {"Content-Type": "text/plain; charset=utf-8"})
            return
        self.app.db.create_dir(path)
        self._send(201, {"Content-Type": "text/plain"})

    # ---------------- MOVE / COPY ----------------
    def _parse_destination(self):
        dest = self.headers.get("Destination", "")
        if not dest:
            return None
        p = urllib.parse.urlparse(dest)
        return self._normalize_path(p.path)

    def do_MOVE(self):
        if not self._require_auth():
            return
        self._move_or_copy(copy=False)

    def do_COPY(self):
        if not self._require_auth():
            return
        self._move_or_copy(copy=True)

    def _move_or_copy(self, copy):
        src = self._normalize_path(self.path)
        dst = self._parse_destination()
        self._consume_body()  # 读净可能的 body
        if src is None or dst is None:
            self._send(400, {"Content-Type": "text/plain; charset=utf-8"}, b"400 Bad Request")
            return
        if src == dst:
            self._send(403, {"Content-Type": "text/plain; charset=utf-8"}, b"403 Forbidden")
            return
        if self.app.db.get_node(src) is None:
            self._send(404, {"Content-Type": "text/plain; charset=utf-8"}, b"404 Not Found")
            return
        if dst.startswith(src + "/"):
            self._send(423, {"Content-Type": "text/plain; charset=utf-8"})  # Locked: 不能移入自身子树
            return
        dst_existed = self.app.db.get_node(dst) is not None
        overwrite = (self.headers.get("Overwrite", "T").upper() != "F")
        if dst_existed and not overwrite:
            self._send(412, {"Content-Type": "text/plain; charset=utf-8"})  # Precondition Failed
            return
        parent = "/" if dst == "/" else dst.rsplit("/", 1)[0] or "/"
        pnode = self.app.db.get_node(parent)
        if pnode is None or not pnode["is_dir"]:
            self._send(409, {"Content-Type": "text/plain; charset=utf-8"})
            return
        ok = self.app.db.copy(src, dst) if copy else self.app.db.move(src, dst)
        if not ok:
            self._send(500, {"Content-Type": "text/plain; charset=utf-8"}, b"500 Internal Error")
            return
        self._send(204 if dst_existed else 201, {"Content-Type": "text/plain"})

    # ---------------- LOCK / UNLOCK ----------------
    def do_LOCK(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None:
            self._send(403, {"Content-Type": "text/plain; charset=utf-8"}, b"403 Forbidden")
            return
        # 刷新已有锁（If 头带 lock token）
        ifh = self.headers.get("If", "")
        m = re.search(r"<opaquelocktoken:([^>]+)>", ifh) or re.search(r'"([^"]+)"', ifh)
        token = None
        if m:
            tok = m.group(1)
            if self.app.db.get_lock(tok):
                token = tok
        if token is None:
            token = "opaquelocktoken:" + hashlib.sha1(
                (path + str(time.time()) + str(id(self))).encode()
            ).hexdigest()
        body = self._consume_body()
        owner = ""
        om = re.search(r"<D:href>([^<]*)</D:href>", body.decode("utf-8", "replace"))
        if om:
            owner = om.group(1)
        depth = self.headers.get("Depth", "0")
        timeout_hdr = self.headers.get("Timeout", "Second-3600")
        ttl = 3600
        tm = re.search(r"Second-(\d+)", timeout_hdr)
        if tm:
            ttl = int(tm.group(1))
        self.app.db.add_lock(token, path, owner, depth, ttl)
        lock_xml = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<D:prop xmlns:D="DAV:">'
            "  <D:lockdiscovery><D:activelock>"
            "    <D:lockscope><D:exclusive/></D:lockscope>"
            "    <D:locktype><D:write/></D:locktype>"
            f"   <D:depth>{depth}</D:depth>"
            f"   <D:timeout>{timeout_hdr}</D:timeout>"
            f"   <D:locktoken><D:href>{token}</D:href></D:locktoken>"
            "  </D:activelock></D:lockdiscovery>"
            "</D:prop>"
        )
        self._send(
            200,
            {
                "Content-Type": 'application/xml; charset="utf-8"',
                "Lock-Token": f"<{token}>",
            },
            lock_xml.encode("utf-8"),
        )

    def do_UNLOCK(self):
        if not self._require_auth():
            return
        token = self.headers.get("Lock-Token", "").strip("<>")
        if token:
            self.app.db.remove_lock(token)
        self._send(204, {"Content-Type": "text/plain"})

    def do_PROPPATCH(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None or self.app.db.get_node(path) is None:
            self._send(404, {"Content-Type": "text/plain; charset=utf-8"}, b"404 Not Found")
            return
        self._consume_body()  # 接受死属性写入
        xml = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<D:multistatus xmlns:D="DAV:">'
            f"  <D:response><D:href>{xml.sax.saxutils.escape(self._href(path, False))}</D:href>"
            '    <D:propstat><D:prop/>'
            "    <D:status>HTTP/1.1 200 OK</D:status></D:propstat>"
            "  </D:response>"
            "</D:multistatus>"
        )
        self._send(
            207, {"Content-Type": 'application/xml; charset="utf-8"'}, xml.encode("utf-8")
        )

    # ---------------- POST：Telegram webhook 入库 ----------------
    def do_POST(self):
        if self.path.startswith("/telegram/webhook"):
            self._handle_webhook()
            return
        self._send(405, {"Content-Type": "text/plain; charset=utf-8"}, b"405 Method Not Allowed")

    def _handle_webhook(self):
        cfg = self.app.config
        secret = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if cfg.webhook_secret and secret != cfg.webhook_secret:
            self._send(403, {"Content-Type": "text/plain; charset=utf-8"}, b"403 Forbidden")
            return
        if self.app.backend is None:
            self._send(503, {"Content-Type": "text/plain; charset=utf-8"},
                       b"503 Telegram not configured")
            return
        slot = 0
        mm = re.match(r"/telegram/webhook/(\d+)", self.path)
        if mm:
            slot = int(mm.group(1)) % max(1, len(self.app.backend.slots))

        try:
            raw = self._consume_body()
            update = json.loads(raw.decode("utf-8", "replace"))
        except Exception:
            self._send(400, {"Content-Type": "text/plain; charset=utf-8"}, b"400 Bad Request")
            return

        message = (
            update.get("message")
            or update.get("channel_post")
            or update.get("edited_message")
            or update.get("edited_channel_post")
        )
        media = _tg.TelegramBackend.extract_media(message) if message else None
        if not media:
            self._send(200, {"Content-Type": "text/plain"})  # 非媒体消息，忽略
            return

        base = cfg.import_dir
        if self.app.db.get_node(base) is None:
            self.app.db.create_dir(base)
        name = media["file_name"]
        dest = f"{base}/{name}"
        i = 1
        while self.app.db.get_node(dest) is not None:
            stem, dot, ext = name.rpartition(".")
            suffix = f"_{i}" if dot else f"_{i}"
            dest = f"{base}/{stem}{suffix}{dot}{ext}" if dot else f"{base}/{name}{suffix}"
            i += 1
        self.app.db.create_file(
            dest,
            media["content_type"],
            [{"file_id": media["file_id"], "slot": slot, "size": media["file_size"]}],
            media["file_size"],
            chunk_size=None,
        )
        self._send(200, {"Content-Type": "text/plain"},
                   f"imported {dest}".encode("utf-8"))


class App:
    """把 db / tg 后端 / config 打包进 HTTP server，供 handler 访问。"""

    def __init__(self):
        self.config = _config.config
        self.db = _db.MetaStore(self.config.db_path)
        self.backend = (
            _tg.TelegramBackend(
                self.config.slots, self.config.api_base, self.config.rate_limit,
                self.config.proxy_token, self.config.slot_rotate,
                self.config.proxy_pools,
            )
            if self.config.slots
            else None
        )


def make_server():
    app = App()
    server = ThreadingHTTPServer((app.config.host, app.config.port), WebDAVHandler)
    server.app = app
    return server


if __name__ == "__main__":
    srv = make_server()
    print("TelegramWebDAV 配置:", json.dumps(srv.app.config.summary(), ensure_ascii=False))
    if not srv.app.backend:
        print("[警告] 未检测到 Telegram 配置（TG_BOT_TOKEN/TG_CHAT_ID），PUT 将返回 503。")
    print(f"监听 http://{srv.app.config.host}:{srv.app.config.port}")
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        pass
