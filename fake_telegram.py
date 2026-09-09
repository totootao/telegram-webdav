"""本地假 Telegram Bot API：用于在没有真实 bot 的情况下端到端自测 WebDAV 服务。

它实现了本服务真正会用到的几个接口：
  POST /bot<token>/sendDocument   接收 multipart，存字节，返回 file_id
  GET  /bot<token>/getFile?file_id 返回 file_path (= file_id)
  GET  /file/bot<token>/<file_id>  按 file_id 回字节（支持 Range，验证分片拼接）
  POST /bot<token>/setWebhook  GET /bot<token>/getWebhookInfo  供 webhook 测试

真实 Telegram 行为被忠实模拟：file_id 稳定、getFile 再取 file_path、file 接口支持 Range。

故障注入钩子（FAIL_FLAGS + reset_fail_flags）：用于测试下载过程随机停止的 bug。
所有开关默认 None，启用时按需打开；测试结束请调用 reset_fail_flags() 复位。
"""
import re
import sys
import threading
import time
import urllib.parse
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

STORE = {}  # file_id -> bytes
FILENAMES = {}  # file_id -> 上传时声明的 filename（用于验证原始文件名透传）
_COUNTER = [0]
_LOCK = threading.Lock()


# ========== 故障注入（默认全 None，不影响已有测试） ==========
# 用法示例：
#   import fake_telegram as ftg
#   ftg.FAIL_FLAGS["send_connection_close_after"] = 1   # 第 1 次响应后开始发 Connection: close
#   ftg.FAIL_FLAGS["truncate_at"] = (2, 8 * 1024)        # 第 2 次响应只发 8KB 就断
#   ftg.FAIL_FLAGS["wrong_content_length"] = (3, 2 * 1024 * 1024)  # 第 3 次响应 CL 声明 2MB 但只发实际大小
#   ftg.reset_fail_flags()                                # 测试结束复位
FAIL_FLAGS = {
    # 第 N 次 /file/bot/... 响应后开始注入 "Connection: close" 头：
    # 触发频率 = (calls_with_close_active, set_to)
    # 表示从第 calls_with_close_active 次响应起，强行加上 "Connection: close"
    "send_connection_close_after": None,  # int: 第 N 次起加 Connection: close
    # 第 N 次响应只发 M 字节就强制关闭连接（模拟代理少发字节）
    "truncate_at": None,  # tuple: (call_index, bytes_to_send)
    # 第 N 次响应声明 Content-Length=M 但只发实际大小（模拟 CL 虚高）
    "wrong_content_length": None,  # tuple: (call_index, declared_content_length)
    # 黑洞代理：第 N 次响应 sleep S 秒后强制关闭（accept 但不发数据）
    "black_hole": None,  # tuple: (call_index, sleep_seconds)
    # 反向：同一 connection 上把已经「应保持」的连接强制 close（模拟 keep-alive FIN）
    # 通过 send_connection_close_after 即可，无需重复
}
# 简单的「第几次响应」计数（/_file/ 路径）
_FILE_HIT_COUNTER = [0]
_FILE_HIT_LOCK = threading.Lock()
# POST 计数（用于 /sendDocument 等）
_POST_HIT_COUNTER = [0]
_POST_HIT_LOCK = threading.Lock()


def reset_fail_flags():
    """重置所有故障注入开关 + 计数器，测试结束调用。"""
    FAIL_FLAGS["send_connection_close_after"] = None
    FAIL_FLAGS["truncate_at"] = None
    FAIL_FLAGS["wrong_content_length"] = None
    FAIL_FLAGS["black_hole"] = None
    with _FILE_HIT_LOCK:
        _FILE_HIT_COUNTER[0] = 0
    with _POST_HIT_LOCK:
        _POST_HIT_COUNTER[0] = 0
    print(f"[fake_tg] reset_fail_flags: hit counters reset", file=sys.stderr)


def _next_file_hit():
    with _FILE_HIT_LOCK:
        _FILE_HIT_COUNTER[0] += 1
        return _FILE_HIT_COUNTER[0]


def _next_post_hit():
    with _POST_HIT_LOCK:
        _POST_HIT_COUNTER[0] += 1
        return _POST_HIT_COUNTER[0]


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
        _next_post_hit()  # 计入 POST 调用
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
            hit_index = _next_file_hit()
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

            # === 故障注入：根据 hit_index 决定是否触发 ===
            send_close = (FAIL_FLAGS["send_connection_close_after"] is not None
                          and hit_index >= FAIL_FLAGS["send_connection_close_after"])
            # truncate / wrong_cl / black_hole：从命中点开始持续触发（>= hit_index_first）
            # 这样多个连续请求都能复现同一故障，避免 fake_telegram 全局计数导致只能用一次
            truncate = (FAIL_FLAGS["truncate_at"] is not None
                        and hit_index >= FAIL_FLAGS["truncate_at"][0])
            wrong_cl = (FAIL_FLAGS["wrong_content_length"] is not None
                        and hit_index >= FAIL_FLAGS["wrong_content_length"][0])
            black = (FAIL_FLAGS["black_hole"] is not None
                     and hit_index >= FAIL_FLAGS["black_hole"][0])

            self.send_response(status)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Accept-Ranges", "bytes")
            if status == 206:
                self.send_header("Content-Range", f"bytes {start}-{end}/{len(data)}")

            # 黑洞：先回响应头，再 sleep + 强制断开（不写 body）
            if black:
                declared_cl = str(len(seg))
                self.send_header("Content-Length", declared_cl)
                self.send_header("Connection", "close")
                self.end_headers()
                time.sleep(FAIL_FLAGS["black_hole"][1])
                try:
                    self.connection.shutdown(2)  # SHUT_RDWR
                except Exception:
                    pass
                try:
                    self.connection.close()
                except Exception:
                    pass
                return

            if truncate:
                # 声明真实长度，但实际只发 N 字节（断流）
                self.send_header("Content-Length", str(len(seg)))
                if send_close:
                    self.send_header("Connection", "close")
                self.end_headers()
                try:
                    self.wfile.write(seg[: FAIL_FLAGS["truncate_at"][1]])
                except Exception:
                    pass
                try:
                    self.connection.shutdown(1)
                except Exception:
                    pass
                return

            if wrong_cl:
                # 声明 CL 大于实际（CL 虚高）。
                # 先 shutdown(1) 半关 + 写一部分真实字节，让客户端 read() 立刻 EOF
                # 抛 IncompleteRead（模拟"代理声称 4MB 但中途 FIN"的真实形态），
                # 而不是"看似 200 但 body 不完整"的假象。
                declared = FAIL_FLAGS["wrong_content_length"][1]
                self.send_header("Content-Length", str(declared))
                if send_close:
                    self.send_header("Connection", "close")
                self.end_headers()
                try:
                    self.connection.shutdown(1)
                except Exception:
                    pass
                try:
                    self.wfile.write(seg)
                except Exception:
                    pass
                return

            self.send_header("Content-Length", str(len(seg)))
            if send_close:
                self.send_header("Connection", "close")
            self.end_headers()
            try:
                self.wfile.write(seg)
            except Exception:
                pass
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
