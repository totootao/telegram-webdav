#!/usr/bin/env python3
"""真实场景端到端测试：在「慢速 Telegram 网络」下验证单文件串行下载的平滑度与健壮性。

为什么需要这个测试（与 selftest.py 的区别）：
  selftest.py 用零延迟的假 Telegram，只能验证逻辑正确性；本测试额外给每个分片
  注入人为网络延迟（模拟真实 Telegram / 自建代理的单分片下载耗时 100~300ms），
  从而能在「接近真实」的时序下验证：
    1) 单文件串行下载时，相邻分片「写回完→下一片开始」的间隔是否≈0（无卡顿）；
    2) 多客户端/多文件并发下载是否仍能在 ThreadingHTTPServer 层面自然并行；
    3) Range 拖拽 / 断点续传 / 完整性校验在慢速下依然正确。

运行：python3 real_test.py
输出：控制台报表 + /workspace/telegram-webdav/TEST_REPORT.md
"""
import base64
import hashlib
import io
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# ============ 可调参数（模拟真实环境） ============
CHUNK_DELAY = float(os.environ.get("REALTEST_CHUNK_DELAY", "0.15"))  # 每个分片下载的人为延迟(s)
CHUNK_MB = int(os.environ.get("REALTEST_CHUNK_MB", "20"))            # 分片大小(MB)
FAKE_PORT = 0
DAV_PORT = 0


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


FAKE_PORT = _free_port()
DAV_PORT = _free_port()
DB_PATH = os.path.join("/tmp", f"tgwebdav_realtest_{os.getpid()}.db")
LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "server_test.log")

os.environ["TG_API_BASE"] = f"http://127.0.0.1:{FAKE_PORT}"
os.environ["TG_BOT_TOKEN"] = "FAKE_TOKEN"
os.environ["TG_CHAT_ID"] = "-100FAKE"
os.environ["CHUNK_SIZE_MB"] = str(CHUNK_MB)
os.environ["DB_PATH"] = DB_PATH
os.environ["PORT"] = str(DAV_PORT)
os.environ["HOST"] = "127.0.0.1"
os.environ["DAV_USER"] = "tester"
os.environ["DAV_PASSWORD"] = "s3cr3t"
os.environ["TG_WEBHOOK_SECRET"] = "sekret"
os.environ["DAV_IDLE_TIMEOUT"] = "2"

import fake_telegram as ftg

# 给 /file/ 下载接口注入人为延迟，模拟真实 Telegram 单分片网络耗时
_orig_do_get = ftg.FakeTGHandler.do_GET


def _slow_do_get(self):
    p = urllib.parse.urlparse(self.path)
    if "/file/bot" in p.path:
        time.sleep(CHUNK_DELAY)
    return _orig_do_get(self)


ftg.FakeTGHandler.do_GET = _slow_do_get

from server import make_server
import db as _db

ftg.start_fake(FAKE_PORT)
srv = make_server()
_srv_thread = threading.Thread(target=srv.serve_forever, daemon=True)
_srv_thread.start()
time.sleep(0.3)

BASE = f"http://127.0.0.1:{DAV_PORT}"
AUTH = "Basic " + base64.b64encode(b"tester:s3cr3t").decode()


def req(method, path, body=None, headers=None, timeout=180):
    _q = urllib.parse.urlsplit(path)
    _enc = urllib.parse.quote(_q.path, safe="/%")
    url = BASE + urllib.parse.urlunsplit((_q.scheme, _q.netloc, _enc, _q.query, _q.fragment))
    r = urllib.request.Request(url, data=body, method=method)
    if headers:
        for k, v in headers.items():
            r.add_header(k, v)
    r.add_header("Authorization", AUTH)
    r.add_header("Connection", "close")
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def _sha(b):
    return hashlib.sha256(b).hexdigest()


# ============ 结果收集 ============
results = []


def check(name, cond, extra=""):
    results.append((name, cond, extra))
    print(("PASS " if cond else "FAIL ") + name + (("  -> " + extra) if extra and not cond else ""))


def _parse_max_gap(log_path):
    """从服务端日志解析最近一次 GET 的最大分片间隔（卡顿核心指标）。"""
    try:
        with open(log_path, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return None
    for line in reversed(lines):
        if "最大分片间隔" in line:
            import re
            m = re.search(r"最大分片间隔=([\d.]+)s", line)
            if m:
                return float(m.group(1))
    return None


# 把服务端日志（webdav/tg 的 _log）写到文件 + 控制台
class _Tee(io.TextIOBase):
    def __init__(self, a, b):
        self.a = a
        self.b = b
        self._buf = ""

    def write(self, s):
        self.a.write(s)
        self.b.write(s)
        self.b.flush()
        self.a.flush()
        return len(s)

    def flush(self):
        self.a.flush()
        self.b.flush()


_logf = open(LOG_PATH, "w", encoding="utf-8")
sys.stdout = _Tee(sys.stdout, _logf)

print("=" * 60)
print(f"真实场景测试: 单分片延迟={CHUNK_DELAY}s 分片大小={CHUNK_MB}MB "
      f"fake_tg=:{FAKE_PORT} dav=:{DAV_PORT}")
print("=" * 60)

# ---------- 准备：建目录 ----------
req("MKCOL", "/test")
req("MKCOL", "/movies")

# ---------- S1: 多分片大文件端到端（85MB → 5 片） ----------
NCHUNKS = 5
big = os.urandom(NCHUNKS * CHUNK_MB * 1024 * 1024)
big_sha = _sha(big)
t0 = time.time()
st, h, b = req("PUT", "/movies/big.bin", body=big, headers={"Content-Type": "application/octet-stream"})
dt_put = time.time() - t0
node = _db.MetaStore(DB_PATH).get_node("/movies/big.bin")
chunks = json.loads(node["chunks"]) if node and node.get("chunks") else []
check("put.big.multi_chunk", st == 201 and len(chunks) == NCHUNKS, f"status={st} chunks={len(chunks)} put={dt_put:.2f}s")

t0 = time.time()
st, h, b = req("GET", "/movies/big.bin")
dt_get = time.time() - t0
check("get.big.full(200)", st == 200, f"status={st}")
check("get.big.bytes_identical", b == big, f"len={len(b)} expect={len(big)}")
check("get.big.sha256_identical", _sha(b) == big_sha, "")
gap1 = _parse_max_gap(LOG_PATH)
check("get.big.max_gap_low", gap1 is not None and gap1 < 0.05,
      f"max_gap={gap1}s (卡顿指标,应≈0)")

# ---------- S2: Range 头部 ----------
st, h, b = req("GET", "/movies/big.bin", headers={"Range": "bytes=0-1048575"})
check("get.range.head(206)", st == 206 and b == big[0:1048576], f"status={st} len={len(b)}")

# ---------- S3: Range 中部跨片（第 2~3 片之间） ----------
mid_a = CHUNK_MB * 1024 * 1024 + 1024
mid_b = CHUNK_MB * 1024 * 1024 + 2 * 1024 * 1024
st, h, b = req("GET", "/movies/big.bin",
               headers={"Range": f"bytes={mid_a}-{mid_a + 2*1024*1024 - 1}"})
check("get.range.mid_cross_chunk(206)", st == 206 and b == big[mid_a:mid_a + 2*1024*1024],
      f"status={st} len={len(b)}")

# ---------- S4: Range 尾部 ----------
st, h, b = req("GET", "/movies/big.bin", headers={"Range": "bytes=-2097152"})
check("get.range.tail(206)", st == 206 and b == big[-2097152:], f"status={st} len={len(b)}")

# ---------- S5: 慢速网络下分片平滑度（多分片 + 延迟已生效） ----------
# 复用 S1 的 big.bin（5 片），再额外传一个 8 片文件，强调分片多时依旧平滑
big2 = os.urandom(8 * CHUNK_MB * 1024 * 1024)
t0 = time.time()
req("PUT", "/movies/big2.bin", body=big2, headers={"Content-Type": "application/octet-stream"})
dt_get2 = time.time()
st, h, b = req("GET", "/movies/big2.bin")
dt_get2 = time.time() - t0
check("get.big2.full(200)", st == 200 and b == big2, f"status={st} len={len(b)}")
gap2 = _parse_max_gap(LOG_PATH)
check("get.big2.max_gap_low", gap2 is not None and gap2 < 0.05,
      f"max_gap={gap2}s (8分片串行,应≈0)")

# ---------- S6: 多客户端并发下载【不同文件】（验证 ThreadingHTTPServer 层并发） ----------
# 准备 4 个不同文件
files = {}
for i in range(4):
    data = os.urandom(3 * CHUNK_MB * 1024 * 1024)
    req("PUT", f"/movies/c{i}.bin", body=data, headers={"Content-Type": "application/octet-stream"})
    files[f"/movies/c{i}.bin"] = data

_t0 = time.time()
_ok = {}
_errors = {}


def _get_one(path):
    try:
        s, hh, bb = req("GET", path)
        _ok[path] = (s, bb)
    except Exception as e:
        _errors[path] = str(e)


ths = [threading.Thread(target=_get_one, args=(p,)) for p in files]
_tc = time.time()
for th in ths:
    th.start()
for th in ths:
    th.join()
dt_conc = time.time() - _tc
all_ok = all(s == 200 and bb == files[p] for p, (s, bb) in _ok.items()) and not _errors
check("concurrent.diff_files.all_200_and_identical", all_ok,
      f"errors={_errors} ok={len(_ok)}/{len(files)} wall={dt_conc:.2f}s")
# 单文件全量下载基准耗时（串行 3 片 × 延迟）
single_baseline = 3 * CHUNK_DELAY
check("concurrent.parallelism_preserved",
      dt_conc < single_baseline * 3.0,
      f"wall={dt_conc:.2f}s 单文件基准≈{single_baseline:.2f}s "
      f"(4路并发墙钟应接近单文件基准,证明服务端层并发未被串行化)")

# ---------- S7: 并发同文件 + 不同 Range（多用户同时看视频拖拽） ----------
def _get_range(path, rng):
    try:
        s, hh, bb = req("GET", path, headers={"Range": rng})
        _ok[path + rng] = (s, bb)
    except Exception as e:
        _errors[path + rng] = str(e)


ranges = ["bytes=0-1048575", "bytes=-2097152",
          f"bytes={CHUNK_MB*1024*1024}-", f"bytes={2*CHUNK_MB*1024*1024}-{3*CHUNK_MB*1024*1024}"]
_ok.clear()
_errors.clear()
ths = [threading.Thread(target=_get_range, args=("/movies/big.bin", r)) for r in ranges]
for th in ths:
    th.start()
for th in ths:
    th.join()
all_ok7 = all(s == 206 for s, _ in _ok.values()) and not _errors
check("concurrent.same_file_ranges.all_206", all_ok7, f"errors={_errors} ok={len(_ok)}/{len(ranges)}")

# ---------- S8: 客户端中途断开 + 断点续传 ----------
# 用一个会中途关闭连接的自定义请求：先请求全量，读到约一半时强行断开
def _half_get(path):
    _q = urllib.parse.urlsplit(path)
    _enc = urllib.parse.quote(_q.path, safe="/%")
    url = BASE + urllib.parse.urlunsplit((_q.scheme, _q.netloc, _enc, _q.query, _q.fragment))
    r = urllib.request.Request(url, method="GET")
    r.add_header("Authorization", AUTH)
    got = 0
    try:
        with urllib.request.urlopen(r, timeout=180) as resp:
            while True:
                chunk = resp.read(1024 * 1024)
                if not chunk:
                    break
                got += len(chunk)
                if got >= len(big) // 2:
                    raise _ClientHalf("read half, abort")
    except _ClientHalf:
        return got
    except Exception:
        return got
    return got


class _ClientHalf(Exception):
    pass


half = _half_get("/movies/big.bin")
check("client.half_disconnect", half >= len(big) // 2, f"got={half} half={len(big)//2}")
# 断点续传：从 half 处接着下
st, h, b_resume = req("GET", "/movies/big.bin", headers={"Range": f"bytes={half}-"})
resume_ok = st == 206 and b_resume == big[half:]
check("resume.from_half(206_identical)", resume_ok, f"status={st} len={len(b_resume)}")

# ---------- S9: 空文件 ----------
st, h, b = req("PUT", "/test/empty.bin", body=b"", headers={"Content-Type": "application/octet-stream"})
st, h, b = req("GET", "/test/empty.bin")
check("get.empty(200_zero)", st == 200 and b == b"", f"status={st} len={len(b)}")

# ---------- S10: 完整性（故意污染一个分片，应被检测） ----------
node = _db.MetaStore(DB_PATH).get_node("/movies/big.bin")
chunks = json.loads(node["chunks"])
fid0 = chunks[0]["file_id"]
with ftg._LOCK:
    ftg.STORE[fid0] = bytes((x ^ 0xFF) for x in ftg.STORE[fid0])  # 翻转首片字节
# 完整性失败会中断连接：urllib 可能抛 HTTPError / URLError / 返回截断数据，均视为「已检测」
st, h, b = 0, {}, b""
try:
    st, h, b = req("GET", "/movies/big.bin", timeout=60)
except Exception:
    st, b = 0, b""  # 连接被服务端主动中断
corrupt_detected = (st != 200) or (b != big)
check("integrity.corrupt_chunk_detected", corrupt_detected, f"status={st} len={len(b)}")
# 还原，避免影响后续
with ftg._LOCK:
    ftg.STORE[fid0] = big[0:CHUNK_MB * 1024 * 1024]

# ============ 汇总 ============
passed = sum(1 for _, c, _ in results if c)
total = len(results)
print("\n" + "=" * 60)
print(f"结果: {passed}/{total} 通过")
print("=" * 60)

# 写报告
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "TEST_REPORT.md"), "w", encoding="utf-8") as f:
    f.write("# 真实场景测试报告\n\n")
    f.write(f"- 测试时间：{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"- 单分片人为延迟：`{CHUNK_DELAY}s`（模拟真实 Telegram / 自建代理单分片下载耗时）\n")
    f.write(f"- 分片大小：`{CHUNK_MB}MB`\n")
    f.write(f"- 下载模式：**单线程串行**（首片边下边发，其余分片主线程依次下载→校验→回写）\n")
    f.write(f"- 结论：**{passed}/{total} 通过**\n\n")
    f.write("## 卡顿诊断（核心指标）\n\n")
    f.write("| 文件 | 分片数 | 最大分片间隔 | 判定 |\n")
    f.write("|---|---|---|---|\n")
    f.write(f"| /movies/big.bin (85MB) | {NCHUNKS} | {gap1:.3f}s | {'平滑' if (gap1 is not None and gap1 < 0.05) else '异常'} |\n")
    f.write(f"| /movies/big2.bin (160MB) | 8 | {gap2:.3f}s | {'平滑' if (gap2 is not None and gap2 < 0.05) else '异常'} |\n\n")
    f.write("> 单线程串行下，相邻分片「写回完→下一片开始」的间隔应≈0；\n")
    f.write("> 若出现明显尖峰（>100ms）即说明某分片下载异常阻塞。\n\n")
    f.write("## 详细结果\n\n")
    f.write("| 用例 | 结果 | 说明 |\n")
    f.write("|---|---|---|\n")
    for name, cond, extra in results:
        f.write(f"| {name} | {'PASS' if cond else 'FAIL'} | {extra} |\n")

sys.stdout = sys.__stdout__
_logf.close()
print(f"报告已生成: TEST_REPORT.md  (服务端日志: server_test.log)")
