"""本地假 Telegram Bot API：用于在没有真实 bot 的情况下端到端自测 WebDAV 服务。

它实现了本服务真正会用到的几个接口：
  POST /bot<token>/sendDocument   接收 multipart，存字节，返回 file_id
  GET  /bot<token>/getFile?file_id 返回 file_path (= file_id)
  GET  /file/bot<token>/<file_id>  按 file_id 回字节（支持 Range，验证分片拼接）
  POST /bot<token>/setWebhook  GET /bot<token>/getWebhookInfo  供 webhook 测试

真实 Telegram 行为被忠实模拟：file_id 稳定、getFile 再取 file_path、file 接口支持 Range。
"""
import re
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STORE = {}  # file_id -> bytes
FILENAMES = {}  # file_id -> 上传时声明的 filename（用于验证原始文件名透传）
_COUNTER = [0]
_LOCK = threading.Lock()


def _new_id():
    with _LOCK:
        _COUNTER[0] += 1
        return f"FAKE-{_COUNTER[0]:08d}"


def _parse_multipart(body, content_type):
    m = re.search(r"boundary=([^;]+)", content_type or "")
    if not m:
        return {}, {}
    boundary = m.group(1).strip().strip('"').encode()
    parts = body.split(b"--" + boundary)
    fields, files = {}, {}
    for part in parts:
        if part in (b"", b"--", b"\r\n", b"--\r\n"):
            continue
        if part.startswith(b"\r\n"):
            part = part[2:]
        if part.endswith(b"\r\n"):
            part = part[:-2]
        hidx = part.find(b"\r\n\r\n")
        if hidx == -1:
            continue
        head = part[:hidx].decode("utf-8", "replace")
        data = part[hidx + 4:]
        cd = re.search(r"Content-Disposition: form-data; (.*)", head)
        if not cd:
            continue
        disp = cd.group(1)
        nm = re.search(r'name="([^"]+)"', disp)
        if not nm:
            continue
        name = nm.group(1)
        fn = re.search(r'filename="([^"]*)"', disp)
        if fn:
            files[name] = (fn.group(1), data)
        else:
            fields[name] = data.decode("utf-8", "replace")
    return fields, files


class FakeTGHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, *a):
        pass

    def _send_json(self, obj, status=200):
        body = __import__("json").dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0) or 0)
        body = self.rfile.read(length) if length else b""
        ct = self.headers.get("Content-Type", "")
        p = urllib.parse.urlparse(self.path)

        if p.path.endswith("/sendDocument"):
            _, files = _parse_multipart(body, ct)
            f = files.get("document") or files.get("video") or files.get("audio")
            if not f:
                self._send_json({"ok": False, "error_code": 400, "description": "no file"}, 400)
                return
            fid = _new_id()
            STORE[fid] = f[1]
            FILENAMES[fid] = f[0]
            self._send_json({
                "ok": True,
                "result": {
                    "message_id": _COUNTER[0],
                    "document": {"file_id": fid, "file_size": len(f[1])},
                },
            })
            return

        if p.path.endswith("/setWebhook"):
            self._send_json({"ok": True, "result": True, "description": "webhook set"})
            return

        self._send_json({"ok": False, "error_code": 404, "description": "unknown"}, 404)

    def do_GET(self):
        p = urllib.parse.urlparse(self.path)
        q = urllib.parse.parse_qs(p.query)

        if p.path.endswith("/getWebhookInfo"):
            self._send_json({"ok": True, "result": {"url": "", "pending_update_count": 0}})
            return

        if p.path.endswith("/getFile"):
            fid = q.get("file_id", [""])[0]
            if fid in STORE:
                self._send_json({"ok": True, "result": {"file_id": fid, "file_path": fid}})
            else:
                self._send_json({"ok": False, "error_code": 400, "description": "file not found"}, 400)
            return

        if "/file/bot" in p.path:
            fid = p.path.rsplit("/", 1)[-1]
            data = STORE.get(fid)
            if data is None:
                self.send_response(404)
                self.send_header("Content-Length", "0")
                self.end_headers()
                return
            rng = self.headers.get("Range")
            start, end = 0, len(data) - 1
            status = 200
            if rng and rng.startswith("bytes="):
                spec = rng[len("bytes="):]
                s, _, e = spec.partition("-")
                start = int(s) if s else 0
                end = int(e) if e else len(data) - 1
                status = 206
            seg = data[start:end + 1]
            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            self.send_header("Content-Length", str(len(seg)))
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")
            self.end_headers()
            self.wfile.write(seg)
            return

        self._send_json({"ok": False, "error_code": 404, "description": "unknown"}, 404)


def start_fake(port=8899):
    srv = ThreadingHTTPServer(("127.0.0.1", port), FakeTGHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


if __name__ == "__main__":
    srv = start_fake(8899)
    print("Fake Telegram API on http://127.0.0.1:8899  (Ctrl+C 退出)")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        srv.shutdown()
