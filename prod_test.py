#!/usr/bin/env python3
"""生产环境（真实 Telegram）端到端测试。

与 selftest/real_test/playback_test 的区别：那三个起本地假 Telegram，
本脚本直接打**已运行的真实服务**（真实 bot 池 + 自建 TG 代理 + 真实频道）。

前置：容器已在运行，例如
  docker run -d --name tg-webdav -p 10010:8080 ... totootao/telegram-webdav:latest

覆盖：
  认证 / PROPFIND / MKCOL / PUT 多分片(真实上传) / GET 全量+SHA256 /
  Range(头/中跨片/尾) / 播放平滑度(TTFB、读间隔、卡顿次数) / seek /
  并发 Range / 断点续传 / 空文件 / 吞吐

运行：python3 prod_test.py
输出：控制台 + PROD_REPORT.md
"""
import base64
import hashlib
import os
import statistics
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = os.environ.get("PROD_BASE", "http://127.0.0.1:10010")
USER = os.environ.get("PROD_USER", "totootao")
PASS = os.environ.get("PROD_PASS", "Hhangxing963.")
BIG_MB = int(os.environ.get("PROD_BIG_MB", "60"))   # 主测试文件(默认60MB→3个20MB分片)
STALL_MS = float(os.environ.get("PROD_STALL_MS", "300"))  # 真实网络抖动大，卡顿阈值放宽到300ms

AUTH = "Basic " + base64.b64encode(f"{USER}:{PASS}".encode()).decode()
results = []


def check(name, cond, extra=""):
    results.append((name, bool(cond), extra))
    print(("PASS " if cond else "FAIL ") + name + (("  -> " + extra) if extra else ""))


def req(method, path, body=None, headers=None, timeout=600):
    url = BASE + urllib.parse.quote(path, safe="/%")
    r = urllib.request.Request(url, data=body, method=method)
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    r.add_header("Authorization", AUTH)
    r.add_header("Connection", "close")
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, dict(resp.headers), resp.read()
    except urllib.error.HTTPError as e:
        return e.code, dict(e.headers), e.read()


def req_stream(path, headers=None, read_size=65536, timeout=900):
    """流式读取并记录每次读到数据的时刻（播放器视角）。"""
    url = BASE + urllib.parse.quote(path, safe="/%")
    r = urllib.request.Request(url, method="GET")
    for k, v in (headers or {}).items():
        r.add_header(k, v)
    r.add_header("Authorization", AUTH)
    r.add_header("Connection", "close")
    t0 = time.time()
    resp = urllib.request.urlopen(r, timeout=timeout)
    status = resp.status
    hd = dict(resp.headers)
    stamps, buf, total = [], [], 0
    while True:
        b = resp.read(read_size)
        if not b:
            break
        stamps.append(time.time())
        buf.append(b)
        total += len(b)
    resp.close()
    return (t0, stamps, total, b"".join(buf)), status, hd


def metrics(t0, stamps, total, dur=None):
    if not stamps:
        return dict(ttfb=0, mx=0, p95=0, avg=0, stalls=0, severe=0, dur=0, thr=0)
    ttfb = stamps[0] - t0
    ivs = [stamps[i] - stamps[i - 1] for i in range(1, len(stamps))]
    d = (stamps[-1] - t0) or 1e-9
    if not ivs:
        return dict(ttfb=ttfb, mx=0, p95=0, avg=0, stalls=0, severe=0, dur=d,
                    thr=total / d / 1024 / 1024)
    srt = sorted(ivs)
    p95 = srt[min(len(srt) - 1, int(len(srt) * 0.95))]
    return dict(ttfb=ttfb, mx=max(ivs), p95=p95, avg=sum(ivs) / len(ivs),
                stalls=sum(1 for x in ivs if x * 1000 > STALL_MS),
                severe=sum(1 for x in ivs if x > 1.0),
                dur=d, thr=total / d / 1024 / 1024)


print("=" * 74)
print(f"生产环境真实测试  {BASE}  主文件={BIG_MB}MB  卡顿阈值={STALL_MS:.0f}ms")
print("=" * 74)

# 1) 认证
try:
    urllib.request.urlopen(urllib.request.Request(BASE + "/", method="PROPFIND"), timeout=20).read()
    st = 200
except urllib.error.HTTPError as e:
    st = e.code
check("auth.required(401)", st == 401, f"status={st}")

# 2) PROPFIND 根
st, h, b = req("PROPFIND", "/", body=b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/></D:prop></D:propfind>', headers={"Depth": "1"})
check("propfind.root(207)", st == 207, f"status={st}")

# 3) MKCOL
req("MKCOL", "/prodtest")
st, h, b = req("MKCOL", "/prodtest")
check("mkcol(201/405已存在)", st in (201, 405), f"status={st}")

# 4) PUT 大文件（真实上传 → 真实频道）
# 复用本地缓存的测试数据：若服务端已有同名同大小文件则跳过上传，
# 避免每次重跑都往真实频道再发一遍分片（Telegram 消息不可删，尽量少留垃圾）。
BIG_CACHE = "/tmp/prod_big.bin"
PROPFIND_SIZE = (b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:">'
                 b'<D:prop><D:getcontentlength/></D:prop></D:propfind>')
need_put = True
if os.path.exists(BIG_CACHE) and os.path.getsize(BIG_CACHE) == BIG_MB * 1024 * 1024:
    big = open(BIG_CACHE, "rb").read()
    st_p, _, b_p = req("PROPFIND", "/prodtest/big.bin", body=PROPFIND_SIZE,
                       headers={"Depth": "0"})
    if st_p == 207 and str(len(big)) in b_p.decode("utf-8", "replace"):
        need_put = False
else:
    big = os.urandom(BIG_MB * 1024 * 1024)

if need_put:
    t0 = time.time()
    st, h, b = req("PUT", "/prodtest/big.bin", body=big,
                   headers={"Content-Type": "application/octet-stream"})
    dt_put = time.time() - t0
    # 新建返回 201，覆盖已有资源返回 204（符合 WebDAV 规范）
    check("put.big(201/204)", st in (201, 204), f"status={st}")
    print(f"    上传: {BIG_MB}MB 耗时={dt_put:.2f}s 吞吐={BIG_MB/dt_put:.2f} MB/s")
    try:
        with open(BIG_CACHE, "wb") as _f:
            _f.write(big)
    except Exception:
        pass
else:
    dt_put = 0.0
    check("put.big(复用已有文件)", True, "跳过上传，避免重复占用频道")
big_sha = hashlib.sha256(big).hexdigest()

# 5) 分片数（PROPFIND 拿不到，改用 Range 探测 + 下面 GET 校验）
st, h, b = req("PROPFIND", "/prodtest/big.bin",
               body=b'<?xml version="1.0"?><D:propfind xmlns:D="DAV:"><D:prop><D:getcontentlength/></D:prop></D:propfind>',
               headers={"Depth": "0"})
size_ok = (st == 207) and (str(len(big)) in b.decode("utf-8", "replace"))
check("propfind.size_matches", size_ok, f"status={st}")

# 6) GET 全量 + SHA256
(triple, st, hd) = req_stream("/prodtest/big.bin")
t0, stamps, total, data = triple
m_full = metrics(t0, stamps, total)
check("get.big.full(200)", st == 200, f"status={st}")
check("get.big.sha256_identical", hashlib.sha256(data).hexdigest() == big_sha,
      f"len={len(data)}/{len(big)}")
check("get.big.bytes_identical", data == big, "")
print(f"    全量下载: {m_full['dur']:.2f}s 吞吐={m_full['thr']:.2f} MB/s "
      f"TTFB={m_full['ttfb']*1000:.0f}ms")

# 7) Range 三类
for label, rng, expect in (
    ("head", "bytes=0-1048575", big[0:1048576]),
    ("mid_cross", f"bytes={20*1024*1024+1024}-{22*1024*1024+1023}", big[20*1024*1024+1024:22*1024*1024+1024]),
    ("tail", "bytes=-2097152", big[-2097152:]),
):
    st, h, b = req("GET", "/prodtest/big.bin", headers={"Range": rng}, timeout=600)
    check(f"get.range.{label}(206)", st == 206 and b == expect, f"status={st} len={len(b)}")

# 8) 播放：起播(bytes=0-) 平滑度 —— 真实带宽下的核心指标
(triple, st, hd) = req_stream("/prodtest/big.bin", headers={"Range": "bytes=0-"})
t0, stamps, total, data = triple
m_play = metrics(t0, stamps, total)
check("playback.start(206)", st == 206, f"status={st}")
check("playback.start.bytes_ok", data == big, f"len={len(data)}")
print(f"    起播: TTFB={m_play['ttfb']*1000:.0f}ms 最大等待={m_play['mx']*1000:.0f}ms "
      f"p95={m_play['p95']*1000:.0f}ms 卡顿>{STALL_MS:.0f}ms={m_play['stalls']}次 "
      f">1s={m_play['severe']}次 吞吐={m_play['thr']:.2f}MB/s")

# 9) seek 中部
mid = int(len(big) * 0.6)
(triple, st, hd) = req_stream("/prodtest/big.bin", headers={"Range": f"bytes={mid}-"})
t0, stamps, total, data = triple
m_seek = metrics(t0, stamps, total)
check("playback.seek(206)", st == 206 and data == big[mid:], f"status={st} len={len(data)}")
print(f"    seek 60%: TTFB={m_seek['ttfb']*1000:.0f}ms 最大等待={m_seek['mx']*1000:.0f}ms "
      f"卡顿={m_seek['stalls']}次 吞吐={m_seek['thr']:.2f}MB/s")

# 10) 并发 4 路 Range（多用户同时拖拽）
ranges = ["bytes=0-1048575", "bytes=-2097152",
          f"bytes={20*1024*1024}-", f"bytes={40*1024*1024}-{45*1024*1024}"]
_ok, _err = {}, {}


def _one(rng):
    try:
        s, hh, bb = req("GET", "/prodtest/big.bin", headers={"Range": rng}, timeout=600)
        _ok[rng] = (s, bb)
    except Exception as e:
        _err[rng] = str(e)


ths = [threading.Thread(target=_one, args=(r,)) for r in ranges]
tc = time.time()
for t in ths:
    t.start()
for t in ths:
    t.join()
wall = time.time() - tc
check("concurrent.ranges.all_206", all(s == 206 for s, _ in _ok.values()) and not _err,
      f"ok={len(_ok)}/{len(ranges)} err={_err} wall={wall:.2f}s")

# 11) 断点续传
half = len(big) // 2
st, h, b = req("GET", "/prodtest/big.bin", headers={"Range": f"bytes={half}-"}, timeout=600)
check("resume.from_half(206)", st == 206 and b == big[half:], f"status={st} len={len(b)}")

# 12) 空文件
req("PUT", "/prodtest/empty.bin", body=b"", headers={"Content-Type": "application/octet-stream"})
st, h, b = req("GET", "/prodtest/empty.bin")
check("get.empty(200_zero)", st == 200 and b == b"", f"status={st} len={len(b)}")

# ---------- 汇总 ----------
passed = sum(1 for _, c, _ in results if c)
total_n = len(results)
print("\n" + "=" * 74)
print(f"结果: {passed}/{total_n} 通过")
print("=" * 74)

avg_thr = statistics.mean([m_full["thr"], m_play["thr"]])
with open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "PROD_REPORT.md"),
          "w", encoding="utf-8") as f:
    f.write("# 生产环境（真实 Telegram）测试报告\n\n")
    f.write(f"- 测试时间：{time.strftime('%Y-%m-%d %H:%M:%S')}\n")
    f.write(f"- 服务地址：`{BASE}`（容器 tg-webdav，5 bot 池 + 自建 TG 代理）\n")
    f.write(f"- 主测试文件：`{BIG_MB}MB`（按 20MB 分片 → {BIG_MB//20} 片，真实上传/下载）\n")
    f.write(f"- 卡顿阈值：单次等待 >{STALL_MS:.0f}ms；严重卡顿 >1s\n")
    f.write(f"- 结论：**{passed}/{total_n} 通过**\n\n")
    f.write("## 真实性能\n\n")
    f.write("| 指标 | 数值 |\n|---|---|\n")
    _put_spd = f"{BIG_MB/dt_put:.2f} MB/s" if dt_put else "-"
    f.write(f"| 上传 {BIG_MB}MB | {dt_put:.2f}s ({_put_spd}) |\n")
    f.write(f"| 全量下载 | {m_full['dur']:.2f}s ({m_full['thr']:.2f} MB/s) |\n")
    f.write(f"| 起播吞吐 | {m_play['thr']:.2f} MB/s |\n")
    f.write(f"| seek 吞吐 | {m_seek['thr']:.2f} MB/s |\n")
    f.write(f"| 平均吞吐 | {avg_thr:.2f} MB/s |\n\n")
    f.write("## 播放平滑度（真实带宽）\n\n")
    f.write("| 场景 | TTFB | 最大等待 | p95 | 卡顿>{}ms | 严重>1s | 吞吐 |\n".format(int(STALL_MS)))
    f.write("|---|---|---|---|---|---|---|\n")
    f.write(f"| 起播 bytes=0- | {m_play['ttfb']*1000:.0f}ms | {m_play['mx']*1000:.0f}ms | "
            f"{m_play['p95']*1000:.0f}ms | {m_play['stalls']}次 | {m_play['severe']}次 | "
            f"{m_play['thr']:.2f}MB/s |\n")
    f.write(f"| seek 60% | {m_seek['ttfb']*1000:.0f}ms | {m_seek['mx']*1000:.0f}ms | "
            f"{m_seek['p95']*1000:.0f}ms | {m_seek['stalls']}次 | {m_seek['severe']}次 | "
            f"{m_seek['thr']:.2f}MB/s |\n")
    f.write(f"| 全量(无Range) | {m_full['ttfb']*1000:.0f}ms | {m_full['mx']*1000:.0f}ms | "
            f"{m_full['p95']*1000:.0f}ms | {m_full['stalls']}次 | {m_full['severe']}次 | "
            f"{m_full['thr']:.2f}MB/s |\n\n")
    f.write("## 详细用例\n\n| 用例 | 结果 | 说明 |\n|---|---|---|\n")
    for name, cond, extra in results:
        f.write(f"| {name} | {'PASS' if cond else 'FAIL'} | {extra} |\n")

print("报告已生成: PROD_REPORT.md")
