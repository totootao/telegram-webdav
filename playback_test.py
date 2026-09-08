#!/usr/bin/env python3
"""音视频播放场景 A/B 测试：量化「全分片流式」对播放平滑度的改善。

为什么单独测这个：
  selftest / real_test 只能验证"数据正确"和"分片间隔≈0"，但**播放器感受的是
  「数据到达的平滑度」**。若服务端把每个 20MB 分片整片缓冲到内存再一次性写出，
  播放器会经历「一批数据 → 干等一整片下载 → 又一批数据」，即周期性卡顿。

本测试用一个**限速的假 Telegram**（按设定带宽分块下发，模拟真实 Telegram 下载速度），
从播放器视角测量：
  - TTFB              : 请求发出 → 收到第一个字节（起播速度）
  - read 间隔 max/p95 : 相邻两次收到数据的间隔（卡顿的核心量化指标）
  - 卡顿次数(>200ms)  : 人眼可感知的停顿次数
  A/B 对比 TG_STREAM_ALL_CHUNKS = off(仅首片流式) 与 on(全分片流式)。

运行：python3 playback_test.py
输出：控制台对比表 + PLAYBACK_REPORT.md
"""
import base64
import hashlib
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

# ============ 可调参数 ============
RATE_MBPS = float(os.environ.get("PLAYBACK_RATE_MBPS", "12"))   # 模拟 Telegram 下载带宽(MB/s)
CHUNK_MB = int(os.environ.get("PLAYBACK_CHUNK_MB", "20"))       # 分片大小(MB)
NCH = int(os.environ.get("PLAYBACK_NCHUNKS", "3"))              # 分片数
STALL_MS = 200  # 判定"可感知卡顿"的间隔阈值(ms)

RATE_BPS = RATE_MBPS * 1024 * 1024


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


FAKE_PORT = _free_port()
DAV_PORT = _free_port()
DB_PATH = os.path.join("/tmp", f"tgwebdav_playback_{os.getpid()}.db")

os.environ["TG_API_BASE"] = f"http://127.0.0.1:{FAKE_PORT}"
os.environ["TG_BOT_TOKEN"] = "FAKE_TOKEN"
os.environ["TG_CHAT_ID"] = "-100FAKE"
os.environ["CHUNK_SIZE_MB"] = str(CHUNK_MB)
os.environ["DB_PATH"] = DB_PATH
os.environ["PORT"] = str(DAV_PORT)
os.environ["HOST"] = "127.0.0.1"
os.environ["DAV_USER"] = "tester"
os.environ["DAV_PASSWORD"] = "s3cr3t"
os.environ["DAV_IDLE_TIMEOUT"] = "2"

import fake_telegram as ftg


# ---------- 限速版假 Telegram：按带宽分块下发，模拟真实下载速度 ----------
class ThrottledTGHandler(ftg.FakeTGHandler):
    """在 /file/ 下载时按 RATE_BPS 限速分块发送（其余行为完全复用父类）。"""

    def do_GET(self):
        p = urllib.parse.urlparse(self.path)
        if "/file/bot" not in p.path:
            return super().do_GET()
        fid = p.path.rsplit("/", 1)[-1]
        data = ftg.STORE.get(fid)
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
        # 限速下发：每 64KB 后按带宽 sleep，让数据"一点点"到达
        piece = 64 * 1024
        off = 0
        while off < len(seg):
            b = seg[off:off + piece]
            self.wfile.write(b)
            try:
                self.wfile.flush()
            except Exception:
                pass
            off += len(b)
            time.sleep(len(b) / RATE_BPS)


_orig_start = ftg.start_fake


def _start_throttled(port):
    from http.server import ThreadingHTTPServer
    srv = ThreadingHTTPServer(("127.0.0.1", port), ThrottledTGHandler)
    t = threading.Thread(target=srv.serve_forever, daemon=True)
    t.start()
    return srv


from server import make_server

_start_throttled(FAKE_PORT)
srv = make_server()
cfg = srv.app.config
threading.Thread(target=srv.serve_forever, daemon=True).start()
time.sleep(0.3)

BASE = f"http://127.0.0.1:{DAV_PORT}"
AUTH = "Basic " + base64.b64encode(b"tester:s3cr3t").decode()


def _req_raw(method, path, headers=None, read_size=65536):
    """发请求并按 read_size 循环读，记录每次读到数据的时刻（播放器视角）。"""
    url = BASE + path
    r = urllib.request.Request(url, method=method)
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    r.add_header("Authorization", AUTH)
    r.add_header("Connection", "close")
    t0 = time.time()
    try:
        resp = urllib.request.urlopen(r, timeout=300)
    except urllib.error.HTTPError as e:
        return None, e.code, {}, b""
    status = resp.status
    hd = dict(resp.headers)
    stamps = []
    buf = []
    total = 0
    while True:
        b = resp.read(read_size)
        if not b:
            break
        stamps.append(time.time())
        buf.append(b)
        total += len(b)
    resp.close()
    data = b"".join(buf)
    return (t0, stamps, total, data), status, hd, data


def _metrics(t0, stamps):
    """从读取时间戳计算：TTFB / 间隔 max,p95,avg / 卡顿次数。"""
    if not stamps:
        return dict(ttfb=0, mx=0, p95=0, avg=0, stalls=0, dur=0)
    ttfb = stamps[0] - t0
    ivs = [stamps[i] - stamps[i - 1] for i in range(1, len(stamps))]
    dur = stamps[-1] - t0
    if not ivs:
        return dict(ttfb=ttfb, mx=0, p95=0, avg=0, stalls=0, dur=dur)
    srt = sorted(ivs)
    p95 = srt[min(len(srt) - 1, int(len(srt) * 0.95))]
    stalls = sum(1 for x in ivs if x * 1000 > STALL_MS)
    return dict(ttfb=ttfb, mx=max(ivs), p95=p95, avg=sum(ivs) / len(ivs),
                stalls=stalls, dur=dur)


# ---------- 准备：上传一个"视频" ----------
req = urllib.request.Request
video = os.urandom(NCH * CHUNK_MB * 1024 * 1024)
vsha = hashlib.sha256(video).hexdigest()
_r = req(BASE + "/movies", method="MKCOL")
_r.add_header("Authorization", AUTH)
try:
    urllib.request.urlopen(_r, timeout=10).read()
except urllib.error.HTTPError:
    pass
_r = req(BASE + "/movies/v.mp4", data=video, method="PUT")
_r.add_header("Authorization", AUTH)
_r.add_header("Content-Type", "video/mp4")
urllib.request.urlopen(_r, timeout=300).read()

SIZE = len(video)
print("=" * 78)
print(f"播放场景 A/B 测试  文件={NCH*CHUNK_MB}MB({NCH}片)  模拟带宽={RATE_MBPS}MB/s  "
      f"卡顿阈值={STALL_MS}ms")
print("=" * 78)

SCENARIOS = [
    ("起播 bytes=0-", {"Range": f"bytes=0-"}, video),
    ("seek 中部 bytes=60%-", {"Range": f"bytes={int(SIZE*0.6)}-"}, video[int(SIZE * 0.6):]),
    ("尾部 seek bytes=-2MB", {"Range": "bytes=-2097152"}, video[-2097152:]),
]

report = {}

for mode, flag in (("A 仅首片流式(旧)", False), ("B 全分片流式(新)", True)):
    cfg.stream_all_chunks = flag
    cfg.stream_block_kb = 256
    print(f"\n--- {mode} ---")
    rows = []
    for name, hdr, expect in SCENARIOS:
        (triple, status, hd, data) = _req_raw("GET", "/movies/v.mp4", headers=hdr)
        if triple is None:
            print(f"  {name}: HTTP {status}")
            rows.append((name, status, None, False))
            continue
        t0, stamps, total, data = triple
        m = _metrics(t0, stamps)
        ok = (status in (200, 206)) and (data == expect)
        rows.append((name, status, m, ok))
        print(f"  {name}: status={status} TTFB={m['ttfb']*1000:.0f}ms "
              f"max间隔={m['mx']*1000:.0f}ms p95={m['p95']*1000:.0f}ms "
              f"卡顿{STALL_MS}ms+={m['stalls']}次 耗时={m['dur']:.2f}s 字节一致={ok}")
    report[mode] = rows

# ---------- A/B 汇总 ----------
print("\n" + "=" * 78)
print("A/B 对比（数值越低越丝滑）")
print("=" * 78)
print(f"{'场景':<24}{'模式':<18}{'TTFB':>10}{'max间隔':>12}{'p95间隔':>12}{'卡顿次数':>10}")
a_rows = report["A 仅首片流式(旧)"]
b_rows = report["B 全分片流式(新)"]
summary = []
for i, (name, _hdr, _exp) in enumerate(SCENARIOS):
    for label, rows in (("A 旧(仅首片流式)", a_rows), ("B 新(全分片流式)", b_rows)):
        _, st, m, ok = rows[i]
        if m is None:
            continue
        print(f"{name:<24}{label:<18}{m['ttfb']*1000:>9.0f}ms{m['mx']*1000:>11.0f}ms"
              f"{m['p95']*1000:>11.0f}ms{m['stalls']:>9}次")
        summary.append((name, label, m, ok))

# ---------- 结论判定 ----------
def _mx_of(mode, i):
    m = report[mode][i][2]
    return m["mx"] if m else 0.0


improved = []
for i, (name, _hdr, _exp) in enumerate(SCENARIOS):
    a, b = _mx_of("A 仅首片流式(旧)", i), _mx_of("B 全分片流式(新)", i)
    if a > 0:
        improved.append((name, a, b, (a - b) / a * 100))
    else:
        improved.append((name, a, b, 0.0))

print("\n最大间隔改善（起播/seek 的等待时间下降幅度）:")
all_better = True
for name, a, b, pct in improved:
    print(f"  {name:<24} {a*1000:>8.0f}ms → {b*1000:>8.0f}ms   下降 {pct:.1f}%")
    if b > a * 1.05:  # 允许 5% 噪声
        all_better = False

data_ok = all(r[3] for r in a_rows + b_rows if r[2] is not None)
print(f"\n字节一致性: {'全部通过' if data_ok else '存在不一致'}")
print(f"结论: {'B(全分片流式) 全面优于 A' if (all_better and data_ok) else '需复查'}")

# ---------- 写报告 ----------
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "PLAYBACK_REPORT.md"),
          "w", encoding="utf-8") as f:
    f.write("# 音视频播放场景测试报告（A/B）\n\n")
    f.write(f"- 测试时间：{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"- 测试文件：`{NCH*CHUNK_MB}MB`（{NCH} 个 {CHUNK_MB}MB 分片）\n")
    f.write(f"- 模拟 Telegram 下载带宽：**{RATE_MBPS} MB/s**（限速假 Telegram，分块下发）\n")
    f.write("- 服务器流式读块：`256KB`（`TG_STREAM_BLOCK_KB`）\n")
    f.write(f"- 卡顿判定阈值：单次等待 **>{STALL_MS}ms**\n\n")
    f.write("## 结论\n\n")
    f.write(f"- 字节一致性：**{'全部通过' if data_ok else '存在不一致'}**\n")
    f.write(f"- 最大等待间隔：{'B 全面优于 A' if all_better else '需复查'}\n\n")
    f.write("## A/B 对比\n\n")
    f.write("| 场景 | 模式 | TTFB | max间隔 | p95间隔 | >" + str(STALL_MS) + "ms卡顿 | 字节一致 |\n")
    f.write("|---|---|---|---|---|---|---|\n")
    for name, label, m, ok in summary:
        f.write(f"| {name} | {label} | {m['ttfb']*1000:.0f}ms | {m['mx']*1000:.0f}ms | "
                f"{m['p95']*1000:.0f}ms | {m['stalls']}次 | {'是' if ok else '否'} |\n")
    f.write("\n## 改善幅度（最大等待间隔）\n\n")
    f.write("| 场景 | A 旧 | B 新 | 下降 |\n")
    f.write("|---|---|---|---|\n")
    for name, a, b, pct in improved:
        f.write(f"| {name} | {a*1000:.0f}ms | {b*1000:.0f}ms | {pct:.1f}% |\n")

print("\n报告已生成: PLAYBACK_REPORT.md")
