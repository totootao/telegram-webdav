"""WebDAV 处理器（纯标准库实现，零第三方依赖）。

实现 OtterHub "频道即存储" 的纯 WebDAV 访问层：客户端用标准 WebDAV 协议
（Windows 映射驱动器 / macOS Finder / rclone / cadaver）读写，服务端把字节
分片塞进 Telegram 频道，把文件树存在 SQLite。

支持方法：
  OPTIONS  PROPFIND  GET  HEAD  PUT  DELETE  MKCOL  MOVE  COPY  LOCK  UNLOCK  PROPPATCH
  POST    /telegram/webhook*  频道/群消息自动入库（参考 otterhub 的 webhook 导入）

下载支持 HTTP Range：把请求区间映射到分片，逐片向 Telegram 发 Range 请求并流式拼接
（与 otterhub tg-adapter.getMergedFile 同思路）。
"""
import base64
import email.utils
import hashlib
import json
import re
import time
import urllib.parse
import xml.sax.saxutils
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import config as _config
import db as _db
import tg as _tg


def _now_http(ts):
    return email.utils.formatdate(ts, usegmt=True)


def _now_iso(ts):
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


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


class WebDAVHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "TelegramWebDAV/1.0"

    # ---------------- 通用 ----------------
    @property
    def app(self):
        return self.server.app

    def log_message(self, fmt, *args):
        # 安静日志：只打错误级别，避免刷屏
        if " 2" in fmt or " 1" in fmt:
            return
        super().log_message(fmt, *args)

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
            self.send_response(401)
            self.send_header("WWW-Authenticate", 'Basic realm="telegram-webdav"')
            self.send_header("Content-Length", "0")
            self.end_headers()
            return False
        return True

    def _normalize_path(self, raw):
        p = urllib.parse.urlsplit(raw).path
        p = urllib.parse.unquote(p)
        if not p.startswith("/"):
            p = "/" + p
        if self.app.config.root_path not in ("", "/"):
            rp = self.app.config.root_path.rstrip("/")
            if p == rp or p.startswith(rp + "/"):
                p = p[len(rp):] or "/"
            else:
                return None
        p = re.sub(r"/+", "/", p)
        if p != "/":
            p = p.rstrip("/")
        return p

    def _parse_range(self, total):
        h = self.headers.get("Range", "")
        m = re.match(r"bytes=(\d*)-(\d*)$", h.strip())
        if not m:
            return None
        s, e = m.group(1), m.group(2)
        if s == "" and e == "":
            return None
        if s == "":
            n = int(e)
            if n <= 0:
                return None
            start = max(0, total - n)
            end = total - 1
        else:
            start = int(s)
            end = int(e) if e else total - 1
        if start > end or start >= total:
            return "invalid"
        return (start, min(end, total - 1))

    def _send(self, status, headers, body=None):
        self.send_response(status)
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
            self.wfile.write(body)

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
            },
        )

    # ---------------- PROPFIND ----------------
    def do_PROPFIND(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None:
            self._send(404, {"Content-Type": "text/plain"})
            return
        depth = self.headers.get("Depth", "1")
        self_node, children = self.app.db.list_children(path, depth)
        if self_node is None:
            self._send(404, {"Content-Type": "text/plain"})
            return
        body = self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
        xml = self._build_propfind(path, self_node, children)
        self._send(
            207,
            {"Content-Type": 'application/xml; charset="utf-8"'},
            xml.encode("utf-8"),
        )

    def _href(self, path, is_dir):
        h = path
        if is_dir and h != "/":
            h += "/"
        return h

    def _build_propfind(self, base, self_node, children):
        items = [self_node] + children
        out = ['<?xml version="1.0" encoding="utf-8"?>']
        out.append('<D:multistatus xmlns:D="DAV:">')
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

    def _serve_file(self, head_only):
        path = self._normalize_path(self.path)
        if path is None:
            self._send(404, {"Content-Type": "text/plain"})
            return
        node = self.app.db.get_node(path)
        if node is None:
            self._send(404, {"Content-Type": "text/plain"})
            return
        if node["is_dir"]:
            # 目录不支持 GET，返回 404（客户端用 PROPFIND 列举）
            self._send(404, {"Content-Type": "text/plain"})
            return

        total = node["size"]
        # 空文件：Telegram 无法存 0 字节，元数据打标为无分片 -> 直接返回空
        chunks = json.loads(node["chunks"]) if node.get("chunks") else []

        rng = self._parse_range(total) if not head_only else None
        if rng == "invalid":
            self._send(
                416,
                {"Content-Range": f"bytes */{total}", "Content-Type": "text/plain"},
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
            self._send(status, headers)
            return

        # 流式拼接分片
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.end_headers()

        if total == 0:
            return

        # 定位覆盖 [start,end] 的分片，逐片按相对 Range 向 TG 请求
        offset = 0
        for c in chunks:
            csize = c["size"]
            cstart = offset
            cend = offset + csize - 1
            offset += csize
            if cend < start or cstart > end:
                continue
            rs = max(cstart, start) - cstart
            re_ = min(cend, end) - cstart
            try:
                for block in self.app.backend.iter_chunk(
                    c["file_id"], c.get("slot", 0), rs, re_
                ):
                    if not block:
                        break
                    self.wfile.write(block)
            except _tg.TGError as e:
                # 已部分写出，无法回滚；记录错误
                print(f"[webdav] 下载分片失败 {path}: {e}", flush=True)
                return

    # ---------------- PUT ----------------
    def do_PUT(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None:
            self._send(403, {"Content-Type": "text/plain"})
            return
        if self.app.backend is None:
            self._send(503, {"Content-Type": "text/plain; charset=utf-8"},
                       "Telegram 未配置，无法存储".encode("utf-8"))
            return

        parent = "/" if path == "/" else path.rsplit("/", 1)[0] or "/"
        pnode = self.app.db.get_node(parent)
        if pnode is None or not pnode["is_dir"]:
            self._send(409, {"Content-Type": "text/plain"})  # Conflict: 父目录不存在
            return

        existed = self.app.db.get_node(path) is not None

        # 处理 Expect: 100-continue
        if self.headers.get("Expect", "").lower() == "100-continue":
            self.send_response_only(100)
            self.end_headers()

        total = int(self.headers.get("Content-Length", 0) or 0)
        ct = self.headers.get("Content-Type", "").split(";")[0].strip()
        if not ct:
            ct = _guess_ct(path.rsplit("/", 1)[-1])

        chunk_size = self.app.config.chunk_size
        chunks_meta = []
        try:
            remaining = total
            while remaining > 0:
                want = min(chunk_size, remaining)
                buf = b""
                while len(buf) < want:
                    part = self.rfile.read(want - len(buf))
                    if not part:
                        break
                    buf += part
                if len(buf) != want:
                    raise _tg.TGError("请求体长度不足（客户端提前断开）")
                fid, slot, mid = self.app.backend.upload_chunk(buf)
                chunks_meta.append(
                    {"file_id": fid, "slot": slot, "size": len(buf), "message_id": mid}
                )
                remaining -= len(buf)
        except _tg.TGError as e:
            self._send(502, {"Content-Type": "text/plain; charset=utf-8"},
                       f"上传到 Telegram 失败: {e}".encode("utf-8"))
            return

        self.app.db.create_file(
            path, ct, chunks_meta if total > 0 else [], total,
            chunk_size=chunk_size if total > 0 else None,
        )
        self._send(204 if existed else 201, {"Content-Type": "text/plain"})

    # ---------------- DELETE ----------------
    def do_DELETE(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None or path == "/":
            self._send(403, {"Content-Type": "text/plain"})
            return
        node = self.app.db.get_node(path)
        if node is None:
            self._send(404, {"Content-Type": "text/plain"})
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
            self._send(403, {"Content-Type": "text/plain"})
            return
        if self.app.db.get_node(path) is not None:
            self._send(405, {"Content-Type": "text/plain"})  # Method Not Allowed
            return
        parent = "/" if path == "/" else path.rsplit("/", 1)[0] or "/"
        pnode = self.app.db.get_node(parent)
        if pnode is None or not pnode["is_dir"]:
            self._send(409, {"Content-Type": "text/plain"})
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
        if src is None or dst is None:
            self._send(400, {"Content-Type": "text/plain"})
            return
        if self.app.db.get_node(src) is None:
            self._send(404, {"Content-Type": "text/plain"})
            return
        if dst.startswith(src + "/"):
            self._send(423, {"Content-Type": "text/plain"})  # Locked: 不能移入自身子树
            return
        dst_existed = self.app.db.get_node(dst) is not None
        overwrite = (self.headers.get("Overwrite", "T").upper() != "F")
        if dst_existed and not overwrite:
            self._send(412, {"Content-Type": "text/plain"})  # Precondition Failed
            return
        parent = "/" if dst == "/" else dst.rsplit("/", 1)[0] or "/"
        pnode = self.app.db.get_node(parent)
        if pnode is None or not pnode["is_dir"]:
            self._send(409, {"Content-Type": "text/plain"})
            return
        ok = self.app.db.copy(src, dst) if copy else self.app.db.move(src, dst)
        if not ok:
            self._send(500, {"Content-Type": "text/plain"})
            return
        self._send(204 if dst_existed else 201, {"Content-Type": "text/plain"})

    # ---------------- LOCK / UNLOCK ----------------
    def do_LOCK(self):
        if not self._require_auth():
            return
        path = self._normalize_path(self.path)
        if path is None:
            self._send(403, {"Content-Type": "text/plain"})
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
        owner = ""
        body = self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
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
            self._send(404, {"Content-Type": "text/plain"})
            return
        # 最小化实现：接受死属性写入，返回 207 全部成功（不持久化具体属性）
        self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
        xml = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<D:multistatus xmlns:D="DAV:">'
            f"  <D:response><D:href>{xml.sax.saxutils.escape(path)}</D:href>"
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
        self._send(405, {"Content-Type": "text/plain"})

    def _handle_webhook(self):
        cfg = self.app.config
        # 校验密钥（Telegram 在以 X-Telegram-Bot-Api-Secret-Token 头发送）
        secret = self.headers.get("X-Telegram-Bot-Api-Secret-Token", "")
        if cfg.webhook_secret and secret != cfg.webhook_secret:
            self._send(403, {"Content-Type": "text/plain"})
            return
        if self.app.backend is None:
            self._send(503, {"Content-Type": "text/plain"})
            return
        # 从路径解析 slot：/telegram/webhook 或 /telegram/webhook/1
        slot = 0
        mm = re.match(r"/telegram/webhook/(\d+)", self.path)
        if mm:
            slot = int(mm.group(1)) % max(1, len(self.app.backend.slots))

        try:
            raw = self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))
            update = json.loads(raw.decode("utf-8", "replace"))
        except Exception:
            self._send(400, {"Content-Type": "text/plain"})
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

        # 落库到导入目录（文件名去重）
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
        # 单分片：整文件已在频道里，file_id + slot 即索引
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
                self.config.proxy_token,
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
