#!/usr/bin/env python3
"""端到端自测：启动假 Telegram + WebDAV 服务，用标准库客户端跑完整 WebDAV 流程。

覆盖：认证 / PROPFIND / MKCOL / PUT(小文件单分片) / PUT(大文件多分片) /
      GET 全量 / GET Range(头/中/尾) / HEAD / MOVE / COPY / DELETE / 空文件 / webhook 入库。

运行：python3 selftest.py
"""
import base64
import hashlib
import json
import os
import socket
import sys
import threading
import time
import urllib.request

# ---------- 选空闲端口 ----------
def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


FAKE_PORT = _free_port()
DAV_PORT = _free_port()
DB_PATH = os.path.join("/tmp", f"tgwebdav_selftest_{os.getpid()}.db")

os.environ["TG_API_BASE"] = f"http://127.0.0.1:{FAKE_PORT}"
os.environ["TG_BOT_TOKEN"] = "FAKE_TOKEN"
os.environ["TG_CHAT_ID"] = "-100FAKE"
os.environ["CHUNK_SIZE_MB"] = "20"
os.environ["DB_PATH"] = DB_PATH
os.environ["PORT"] = str(DAV_PORT)
os.environ["HOST"] = "127.0.0.1"
os.environ["DAV_USER"] = "tester"
os.environ["DAV_PASSWORD"] = "s3cr3t"
os.environ["TG_WEBHOOK_SECRET"] = "sekret"
os.environ["WEBDAV_IMPORT_DIR"] = "/telegram-import"
# 自测用短空闲超时：避免 keep-alive 空闲等待拖慢逐条独立请求的用例（生产默认 30s）。
os.environ["DAV_IDLE_TIMEOUT"] = "2"

# 必须在 import server 前设好环境变量（config 在导入时读取）
import fake_telegram as ftg
from server import make_server
import db as dbs

ftg.start_fake(FAKE_PORT)
srv = make_server()
t = threading.Thread(target=srv.serve_forever, daemon=True)
t.start()
time.sleep(0.3)

BASE = f"http://127.0.0.1:{DAV_PORT}"
AUTH = "Basic " + base64.b64encode(b"tester:s3cr3t").decode()


def req(method, path, body=None, headers=None, raw=False):
    # 对路径做安全的 URL 编码：保留 / 与已编码的 %XX，避免 urllib 拒绝含空格/中文的路径。
    _q = urllib.parse.urlsplit(path)
    _enc = urllib.parse.quote(_q.path, safe="/%")
    url = BASE + urllib.parse.urlunsplit((_q.scheme, _q.netloc, _enc, _q.query, _q.fragment))
    r = urllib.request.Request(url, data=body, method=method)
    if headers:
        for k, v in headers.items():
            r.add_header(k, v)
    r.add_header("Authorization", AUTH)
    # 每条用例用独立短连接（声明 close），避免服务端 keep-alive 空闲等待拖慢自测；
    # keep-alive 本身由下面的裸 socket 用例（test 18/19）单独验证。
    r.add_header("Connection", "close")
    try:
        with urllib.request.urlopen(r, timeout=120) as resp:
            data = resp.read()
            return resp.status, dict(resp.headers), data
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


results = []
def check(name, cond, extra=""):
    results.append((name, cond, extra))
    print(("PASS " if cond else "FAIL ") + name + (("  -> " + extra) if extra and not cond else ""))


# 1) 未带认证应 401
r = urllib.request.Request(BASE + "/", method="PROPFIND")
try:
    urllib.request.urlopen(r, timeout=5).read()
    st = 200
except urllib.error.HTTPError as e:
    st = e.code
check("auth.required(401)", st == 401, f"status={st}")

# 2) PROPFIND 根
st, h, b = req("PROPFIND", "/", body=b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/></D:prop></D:propfind>', headers={"Depth": "1"})
check("propfind.root(207)", st == 207, f"status={st}")

# 3) MKCOL
st, h, b = req("MKCOL", "/test")
check("mkcol(201)", st == 201, f"status={st}")

# 4) PUT 小文件（5MB -> 单分片）
small = os.urandom(5 * 1024 * 1024)
st, h, b = req("PUT", "/test/small.bin", body=small, headers={"Content-Type": "application/octet-stream"})
check("put.small(201)", st == 201, f"status={st}")
node = dbs.MetaStore(DB_PATH).get_node("/test/small.bin")
chk = json.loads(node["chunks"]) if node and node.get("chunks") else []
check("put.small.single_chunk", node is not None and len(chk) == 1, f"chunks={len(chk)}")
check("put.small.size", node is not None and node["size"] == len(small), f"size={node['size'] if node else None}")

# 5) PUT 大文件（45MB -> 多分片）
big = os.urandom(45 * 1024 * 1024)
st, h, b = req("PUT", "/test/big.bin", body=big, headers={"Content-Type": "application/octet-stream"})
check("put.big(201)", st == 201, f"status={st}")
meta = dbs.MetaStore(DB_PATH).get_node("/test/big.bin")
chunks = json.loads(meta["chunks"]) if meta and meta.get("chunks") else []
check("put.big.multi_chunk", len(chunks) >= 2, f"chunks={len(chunks)}")
check("put.big.size", meta is not None and meta["size"] == len(big), f"size={meta['size'] if meta else None}")
check("put.big.chunk_sum", sum(c["size"] for c in chunks) == len(big), f"sum={sum(c['size'] for c in chunks)}")

# 6) GET 全量 + sha 一致
st, h, b = req("GET", "/test/big.bin")
check("get.full(200)", st == 200, f"status={st}, len={len(b)}")
check("get.full.sha", st == 200 and hashlib.sha256(b).hexdigest() == hashlib.sha256(big).hexdigest(),
      "" if st == 200 and hashlib.sha256(b).hexdigest() == hashlib.sha256(big).hexdigest() else "sha mismatch")

# 7) GET Range 头 100 字节
st, h, b = req("GET", "/test/big.bin", headers={"Range": "bytes=0-99"})
check("get.range.head(206)", st == 206 and b == big[0:100], f"status={st}, len={len(b)}")

# 8) GET Range 中段（跨分片：第 1 块尾 + 第 2 块头）
a, c = 20 * 1024 * 1024 - 50, 20 * 1024 * 1024 + 49  # 跨 20MB 边界
st, h, b = req("GET", "/test/big.bin", headers={"Range": f"bytes={a}-{c}"})
check("get.range.mid_cross_chunk(206)", st == 206 and b == big[a:c + 1], f"status={st}, len={len(b)}")

# 9) HEAD 头
st, h, b = req("HEAD", "/test/big.bin")
check("head.content_length", st == 200 and h.get("Content-Length") == str(len(big)), f"cl={h.get('Content-Length')}")

# 10) MOVE
st, h, b = req("MOVE", "/test/big.bin", headers={"Destination": BASE + "/test/moved.bin", "Overwrite": "T"})
check("move(204)", st in (201, 204), f"status={st}")
check("move.src_gone", dbs.MetaStore(DB_PATH).get_node("/test/big.bin") is None)
check("move.dst_present", dbs.MetaStore(DB_PATH).get_node("/test/moved.bin") is not None)

# 11) COPY
st, h, b = req("COPY", "/test/small.bin", headers={"Destination": BASE + "/test/copied.bin", "Overwrite": "T"})
check("copy(201)", st in (201, 204), f"status={st}")
st, h, b2 = req("GET", "/test/copied.bin")
check("copy.content_equal", b2 == small, f"len={len(b2)}")

# 12) 空文件
st, h, b = req("PUT", "/test/empty.bin", body=b"", headers={"Content-Type": "application/octet-stream"})
check("put.empty(201)", st == 201, f"status={st}")
st, h, b = req("GET", "/test/empty.bin")
check("get.empty(200,0)", st == 200 and len(b) == 0, f"status={st}, len={len(b)}")

# 12b) 上传到 Telegram 的文件名应与原始文件名一致（参考 otterhub-server：file_name 一路透传）。
# 单分片直接用原名；多分片用「原名.partNN」保留原名线索并区分分片。
req("MKCOL", "/names")
ftg.FILENAMES.clear()
st, h, b = req("PUT", "/names/report 2024.pdf", body=b"x" * 1234,
               headers={"Content-Type": "application/pdf"})
check("put.name.single(201)", st == 201, f"status={st}")
_n = dbs.MetaStore(DB_PATH).get_node("/names/report 2024.pdf")
_nc = json.loads(_n["chunks"]) if _n and _n.get("chunks") else []
check("put.name.single.fname", bool(_nc) and ftg.FILENAMES.get(_nc[0]["file_id"]) == "report 2024.pdf",
      f"fname={ftg.FILENAMES.get(_nc[0]['file_id']) if _nc else None}")
ftg.FILENAMES.clear()
st, h, b = req("PUT", "/names/data.zip", body=b"y" * (45 * 1024 * 1024),
               headers={"Content-Type": "application/zip"})
check("put.name.multi(201)", st == 201, f"status={st}")
_n2 = dbs.MetaStore(DB_PATH).get_node("/names/data.zip")
_nc2 = json.loads(_n2["chunks"]) if _n2 and _n2.get("chunks") else []
_exp = {f"data.zip.part{i:03d}" for i in range(len(_nc2))}
_got = {ftg.FILENAMES.get(c["file_id"]) for c in _nc2}
check("put.name.multi.fname", _got == _exp, f"got={sorted(_got)}")

# 13) DELETE
st, h, b = req("DELETE", "/test/small.bin")
check("delete(204)", st == 204, f"status={st}")
check("delete.gone", dbs.MetaStore(DB_PATH).get_node("/test/small.bin") is None)

# 14) webhook 入库
ftg.STORE["FAKE-WEBHOOK1"] = b"hello from channel"
upd = {
    "update_id": 1,
    "message": {
        "message_id": 777,
        "document": {"file_id": "FAKE-WEBHOOK1", "file_name": "chan.txt",
                     "file_size": 18, "mime_type": "text/plain"},
    },
}
st, h, b = req("POST", "/telegram/webhook", body=json.dumps(upd).encode(),
               headers={"Content-Type": "application/json", "X-Telegram-Bot-Api-Secret-Token": "sekret"})
check("webhook.import(200)", st == 200, f"status={st}")
wn = dbs.MetaStore(DB_PATH).get_node("/telegram-import/chan.txt")
check("webhook.node_present", wn is not None, f"node={wn}")
if wn:
    st, h, b = req("GET", "/telegram-import/chan.txt")
    check("webhook.content", b == b"hello from channel", f"got={b!r}")

# 15) PROPFIND 子目录列举
st, h, b = req("PROPFIND", "/test", body=b'<D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/></D:prop></D:propfind>', headers={"Depth": "1"})
check("propfind.depth1(207)", st == 207, f"status={st}")

# 15b) 完整性：PUT 时计算整文件 SHA-256 与每分片 SHA-256 并入库。
#      用 /test/copied.bin（small.bin 的副本，未被删除），顺带验证 COPY 会把哈希一并带走。
_fn = dbs.MetaStore(DB_PATH).get_node("/test/copied.bin")
_fexp = hashlib.sha256(small).hexdigest()
check("integrity.file_hash_stored", _fn is not None and _fn.get("file_hash") == _fexp,
      f"got={_fn.get('file_hash') if _fn else None} exp={_fexp}")
_fch = json.loads(_fn["chunks"])[0] if _fn and _fn.get("chunks") else {}
check("integrity.chunk_hash_stored", _fch.get("sha256") == _fexp,
      f"chunk_sha={_fch.get('sha256')}")

# 15c) 完整性：损坏某分片后 GET 应被服务端在分片边界处中断连接（而非把错数据当完整文件）。
#      用已存在、≥2 分片的 /test/moved.bin（45MB）做脏数据注入（翻转 chunk0 全部字节）。
_mn = dbs.MetaStore(DB_PATH).get_node("/test/moved.bin")
_mc = json.loads(_mn["chunks"]) if _mn and _mn.get("chunks") else []
_c0 = _mc[0]["file_id"]
_orig = ftg.STORE.get(_c0)
if _orig:
    # 只翻转第 1 个字节：SHA-256 会完全不同，但不用做 20MB 的逐字节循环
    ftg.STORE[_c0] = bytes([_orig[0] ^ 0xFF]) + _orig[1:]
_truncated = False
try:
    st, h, b = req("GET", "/test/moved.bin")
    # 若没抛异常：声明总字节未收满才算"被检测到"
    _truncated = (len(b) < _mn["size"])
except Exception:
    # urlopen 在 Content-Length 未收满时抛 IncompleteRead -> 连接被提前中断
    _truncated = True
finally:
    if _orig is not None:
        ftg.STORE[_c0] = _orig  # 还原，避免影响后续用例
check("integrity.corrupt_chunk_detected", _truncated,
      f"size={_mn['size'] if _mn else None} truncated={_truncated}")

# ----------------------------------------------------------------------------
# 16~20) 健壮性：尾斜杠双向 / 目录重定向 / keep-alive / 缺陷客户端残包 / DAV_ROOT
# ----------------------------------------------------------------------------
import socket as _sock
import re as _re

_PROPFIND_BODY = b'<D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/></D:prop></D:propfind>'


class _Raw:
    """极简裸 socket 客户端：用于精确控制 Content-Length 与 keep-alive（验证残包场景）。"""
    def __init__(self, port):
        self.s = _sock.socket(_sock.AF_INET, _sock.SOCK_STREAM)
        self.s.connect(("127.0.0.1", port))
        self.s.settimeout(15)
        self.buf = b""

    def _recv_until(self, token):
        while token not in self.buf:
            d = self.s.recv(4096)
            if not d:
                break
            self.buf += d

    def read_line(self):
        self._recv_until(b"\r\n")
        i = self.buf.find(b"\r\n")
        if i < 0:
            line = self.buf
            self.buf = b""
            return line
        line = self.buf[:i]
        self.buf = self.buf[i + 2:]
        return line

    def read_response(self):
        status = self.read_line().decode("latin1")
        headers = {}
        while True:
            line = self.read_line()
            if not line:
                break
            k, _, v = line.partition(b":")
            headers[k.decode().strip().lower()] = v.decode().strip()
        cl = headers.get("content-length")
        body = b""
        if cl is not None:
            n = int(cl)
            while len(self.buf) < n:
                d = self.s.recv(4096)
                if not d:
                    break
                self.buf += d
            body = self.buf[:n]
            self.buf = self.buf[n:]
        return status, headers, body

    def send_raw(self, data):
        self.s.sendall(data)

    def close(self):
        self.s.close()


def raw_propfind(raw, path, cl=None, keepalive=True, body=_PROPFIND_BODY):
    if cl is None:
        cl = len(body)
    req = (
        f"PROPFIND {path} HTTP/1.1\r\n"
        f"Host: 127.0.0.1\r\n"
        f"Authorization: {AUTH}\r\n"
        f"Content-Type: application/xml\r\n"
        f"Content-Length: {cl}\r\n"
        f"Connection: {'keep-alive' if keepalive else 'close'}\r\n"
        f"Depth: 1\r\n\r\n"
    ).encode() + body
    raw.send_raw(req)


def _first_href(xml_bytes):
    m = _re.search(rb"<D:href>([^<]*)</D:href>", xml_bytes)
    return m.group(1).decode() if m else None


# 16) 尾斜杠双向：/test 与 /test/ 都应 207，且自节点 href 带尾斜杠
st, h, b = req("PROPFIND", "/test", body=_PROPFIND_BODY, headers={"Depth": "1"})
check("trailingslash.no_slash(207)", st == 207, f"status={st}")
href_ns = _first_href(b)
check("trailingslash.no_slash.href_has_slash",
      href_ns == "/test/", f"href={href_ns!r}")
st, h, b = req("PROPFIND", "/test/", body=_PROPFIND_BODY, headers={"Depth": "1"})
check("trailingslash.with_slash(207)", st == 207, f"status={st}")
check("trailingslash.with_slash.href_has_slash",
      _first_href(b) == "/test/", f"href={_first_href(b)!r}")

# 17) 目录 GET 重定向到带尾斜杠的形式（301）
st, h, b = req("GET", "/test")
check("dir_get.redirect(301)", st == 301 and h.get("Location") == "/test/",
      f"status={st} loc={h.get('Location')}")

# 18) keep-alive：好客户端（CL 正确）顺序复用同一条连接，两个 PROPFIND 都正常
rk = _Raw(DAV_PORT)
raw_propfind(rk, "/", keepalive=True)
s1, h1, b1 = rk.read_response()
raw_propfind(rk, "/test", keepalive=True)
s2, h2, b2 = rk.read_response()
rk.close()
check("keepalive.reuse.both_207",
      "207" in s1 and "207" in s2, f"s1={s1!r} s2={s2!r}")
check("keepalive.header_keepalive",
      h1.get("connection") == "keep-alive", f"conn={h1.get('connection')}")

# 18b) 真·流水线：两个请求一次性发出（不等第一个响应），都应正常返回。
#      残包清理必须能认出"这是下一条请求"，不能把流水线吃掉。
rp = _Raw(DAV_PORT)
raw_propfind(rp, "/", keepalive=True)
raw_propfind(rp, "/test", keepalive=True)  # 立刻发第二条
p1, ph1, pb1 = rp.read_response()
p2, ph2, pb2 = rp.read_response()
rp.close()
check("keepalive.pipeline.both_207",
      "207" in p1 and "207" in p2, f"s1={p1!r} s2={p2!r}")
check("keepalive.pipeline.hrefs",
      _first_href(pb1) == "/" and _first_href(pb2) == "/test/",
      f"h1={_first_href(pb1)!r} h2={_first_href(pb2)!r}")

# 19) 缺陷客户端：Content-Length 少算 1 字节（模拟 AList/OpenList）。
#     服务端应把多出来的那个字节吞掉：本次仍 207，且同一条连接继续可用。
rb = _Raw(DAV_PORT)
raw_propfind(rb, "/", cl=len(_PROPFIND_BODY) - 1, keepalive=True)  # 少 1 字节
sb, hb, bb = rb.read_response()
check("broken_cl.bad_cl_still_207", "207" in sb, f"status={sb!r}")
# 同一条连接紧接着发一个正确请求：残包被清掉的话这里必须是 207（否则说明错位了）
raw_propfind(rb, "/test", keepalive=True)
s2b, h2b, b2b = rb.read_response()
rb.close()
check("broken_cl.same_conn_next_ok", "207" in s2b, f"status={s2b!r}")
check("broken_cl.no_html_leak", b"DOCTYPE HTML" not in b2b,
      "response contained HTML error page")
# 新连接也应立即可用
st, h, b = req("PROPFIND", "/", body=_PROPFIND_BODY, headers={"Depth": "1"})
check("broken_cl.next_conn_ok(207)", st == 207 and _first_href(b) == "/",
      f"status={st}")

# 20) DAV_ROOT 挂载：href 自动带前缀，挂载点之外 404
import config as cfgmod
_root_port = _free_port()
_root_db = os.path.join("/tmp", f"tgwebdav_root_{os.getpid()}.db")
os.environ["DAV_ROOT"] = "/dav"
os.environ["PORT"] = str(_root_port)
os.environ["DB_PATH"] = _root_db
cfgmod.config = cfgmod.Config()
import webdav as _webdav
_webdav._config.config = cfgmod.config
srv_root = _webdav.make_server()
t_root = threading.Thread(target=srv_root.serve_forever, daemon=True)
t_root.start()
time.sleep(0.3)
rr = _Raw(_root_port)
raw_propfind(rr, "/dav", keepalive=True)
sr, hr, br = rr.read_response()
rr.close()
check("davroot.mount_href_prefixed", _first_href(br) == "/dav/",
      f"href={_first_href(br)!r}")
# 挂载点之外（非 /dav 前缀）应 404
rr2 = _Raw(_root_port)
raw_propfind(rr2, "/", keepalive=True)
sr2, hr2, br2 = rr2.read_response()
rr2.close()
check("davroot.outside_404", "404" in sr2, f"status={sr2!r}")
srv_root.shutdown()

srv.shutdown()
passed = sum(1 for _, c, _ in results if c)
total = len(results)
print("\n" + "=" * 40)
print(f"结果: {passed}/{total} 通过")
for name, c, extra in results:
    if not c:
        print("  FAIL:", name, extra)
sys.exit(0 if passed == total else 1)
