"""视频下载速度实测（真实 Telegram + 自建代理，走容器里的生产实例）。

只报一个「平均吞吐」是骗人的 —— 播放器感受的是**数据到达的平滑度**。
所以这里逐块（256KB）记录时间戳，除了总吞吐还给出：
  - TTFB（首字节延迟）：点开/拖动后干等多久
  - 每秒速度曲线：有没有周期性掉速（分片边界最容易暴露）
  - 块间隔 p95 / 最大：卡顿就藏在这里
  - >300ms / >1s 的停顿次数：对应人眼可感知的「卡一下」

单次测量噪声很大（实测 TTFB 会在 340ms~2.3s 之间跳），所以**每个场景跑多轮取中位数**，
并额外做「并发阶梯」——单路 / 2 路 / 4 路，用来判断并发到底能不能提速。

用法： python3.11 speed_test.py [轮数，默认3]
"""
import base64
import http.client
import statistics
import sys
import threading
import time

HOST = "127.0.0.1"
PORT = 10010
USER = "totootao"
PASS = "Hhangxing963."
AUTH = base64.b64encode(f"{USER}:{PASS}".encode()).decode()

PATH = "/prodtest/big.bin"      # 60MB / 3 分片(20MB)，等效 60MB 视频
TOTAL = 62914560
BLK = 256 * 1024                # 播放器实际拿到的块大小量级
GAP = 1.5                       # 场景之间的冷却，避免上一轮的余波影响下一轮

STALL_MS = 300                  # 人眼可感知的卡顿阈值
BIG_STALL_MS = 1000


def _conn():
    return http.client.HTTPConnection(HOST, PORT, timeout=300)


def fetch(rng=None, limit_bytes=None):
    """按 rng 发 GET，逐块读，返回原始统计。"""
    c = _conn()
    h = {"Authorization": f"Basic {AUTH}"}
    if rng:
        h["Range"] = rng
    t0 = time.time()
    c.request("GET", PATH, headers=h)
    r = c.getresponse()

    ttfb = None
    got = 0
    marks = []          # (累计字节, 距 t0 秒)
    gaps = []           # 相邻块之间的间隔(ms)
    last_t = t0
    while True:
        b = r.read(BLK)
        if not b:
            break
        now = time.time()
        if ttfb is None:
            ttfb = now - t0
        got += len(b)
        marks.append((got, now - t0))
        gaps.append((now - last_t) * 1000.0)
        last_t = now
        if limit_bytes and got >= limit_bytes:
            break
    dt = time.time() - t0
    c.close()
    return {"status": r.status, "bytes": got, "elapsed": dt, "ttfb": ttfb,
            "marks": marks, "gaps": gaps}


def metrics(s):
    mb = s["bytes"] / 1048576.0
    spd = mb / s["elapsed"] if s["elapsed"] else 0
    g = s["gaps"][1:] or [0.0]          # 首块的「间隔」含 TTFB，不计入平滑度
    gs = sorted(g)
    p95 = gs[int(len(gs) * 0.95)] if gs else 0
    return {"mb": mb, "spd": spd, "ttfb": (s["ttfb"] or 0) * 1000,
            "max_gap": max(g) if g else 0, "p95": p95,
            "stalls": len([x for x in g if x > STALL_MS]),
            "big": len([x for x in g if x > BIG_STALL_MS]),
            "marks": s["marks"]}


def med(xs):
    return statistics.median(xs) if xs else 0


def multi(name, rounds, fn):
    """跑 rounds 轮，打印每轮 + 中位数。"""
    rs = [metrics(fn()) for _ in range(rounds)]
    time.sleep(GAP)
    print(f"\n  【{name}】 {rounds} 轮")
    print(f"    {'#':<3}{'速度':>11}{'TTFB':>9}{'最大间隔':>10}{'p95':>8}{'卡顿>300ms':>10}")
    for i, r in enumerate(rs, 1):
        print(f"    {i:<3}{r['spd']:>8.2f}MB/s{r['ttfb']:>8.0f}ms"
              f"{r['max_gap']:>9.0f}ms{r['p95']:>7.0f}ms{r['stalls']:>9}次")
    m = {"spd": med([r["spd"] for r in rs]), "ttfb": med([r["ttfb"] for r in rs]),
         "max_gap": med([r["max_gap"] for r in rs]), "p95": med([r["p95"] for r in rs]),
         "stalls": med([r["stalls"] for r in rs]), "marks": rs[-1]["marks"]}
    print(f"    中位数 → {m['spd']:.2f} MB/s ({m['spd']*8:.0f} Mbps)  "
          f"TTFB {m['ttfb']:.0f}ms  最大间隔 {m['max_gap']:.0f}ms  "
          f"p95 {m['p95']:.0f}ms  卡顿 {m['stalls']:.0f}次")
    return m


def curve(name, marks):
    buckets = {}
    for nbytes, t in marks:
        buckets[int(t)] = nbytes
    print(f"    —— {name} 逐秒速度（最后一轮）——")
    prev_t, prev_b = 0, 0
    for sec in sorted(buckets):
        db = buckets[sec] - prev_b
        print(f"      {sec:>3}s  {db/1048576.0:6.2f}MB/s  " + "█" * max(1, int(db / 1048576.0)))
        prev_t, prev_b = sec, buckets[sec]


def ladder(nconn, rounds=2):
    """nconn 路并发，返回聚合吞吐中位数(MB/s)。"""
    vals = []
    for _ in range(rounds):
        seg = TOTAL // nconn
        out = {}
        t0 = time.time()

        def one(i):
            st = i * seg
            en = (st + seg - 1) if i < nconn - 1 else TOTAL - 1
            out[i] = fetch(rng=f"bytes={st}-{en}")

        ths = [threading.Thread(target=one, args=(i,)) for i in range(nconn)]
        [t.start() for t in ths]
        [t.join() for t in ths]
        wall = time.time() - t0
        tot = sum(v["bytes"] for v in out.values())
        vals.append(tot / 1048576.0 / wall)
        time.sleep(GAP)
    return med(vals), vals


def main():
    rounds = int(sys.argv[1]) if len(sys.argv) > 1 else 3
    print("=" * 74)
    print(f"视频下载速度实测   http://{HOST}:{PORT}{PATH}   {TOTAL/1048576:.0f}MB / 3 分片")
    print(f"每场景 {rounds} 轮取中位数（单次噪声大：TTFB 实测在 340ms~2.3s 之间跳）")
    print("=" * 74)

    R = {}
    R["全量"] = multi("A 全量下载（无 Range）", 2, lambda: fetch())
    curve("全量", R["全量"]["marks"])

    R["播放式"] = multi("B 播放式顺序下载（Range: bytes=0-，模拟播放器）", rounds,
                        lambda: fetch(rng="bytes=0-"))
    curve("播放式", R["播放式"]["marks"])

    print("\n[C] 拖动进度条 seek（每处读 8MB）")
    for pct in (30, 60, 90):
        off = int(TOTAL * pct / 100)
        R[f"seek{pct}"] = multi(f"C seek {pct}% @{off//1048576}MB", rounds,
                                lambda o=off: fetch(rng=f"bytes={o}-",
                                                    limit_bytes=8 * 1024 * 1024))

    print("\n[D] 并发阶梯：并发到底能不能提速？")
    lad = {}
    for n in (1, 2, 4):
        v, allv = ladder(n)
        lad[n] = v
        print(f"    {n} 路并发: 聚合 {v:6.2f} MB/s ({v*8:6.1f} Mbps)   各轮={[f'{x:.1f}' for x in allv]}")

    print("\n" + "=" * 74)
    print("汇总（中位数）")
    print("=" * 74)
    print(f"  {'场景':<16}{'速度':>11}{'Mbps':>8}{'TTFB':>9}{'最大间隔':>10}{'p95':>8}{'卡顿':>7}")
    print("  " + "-" * 68)
    for k in ("全量", "播放式", "seek30", "seek60", "seek90"):
        r = R[k]
        label = {"全量": "A 全量", "播放式": "B 播放式", "seek30": "C seek 30%",
                 "seek60": "C seek 60%", "seek90": "C seek 90%"}[k]
        print(f"  {label:<16}{r['spd']:>8.2f}MB/s{r['spd']*8:>7.0f}{r['ttfb']:>8.0f}ms"
              f"{r['max_gap']:>9.0f}ms{r['p95']:>7.0f}ms{r['stalls']:>6.0f}次")
    for n in (1, 2, 4):
        print(f"  {'D ' + str(n) + '路并发':<16}{lad[n]:>8.2f}MB/s{lad[n]*8:>7.0f}"
              f"{'-':>9}{'-':>10}{'-':>8}{'-':>7}")

    print("\n判读：")
    print("  · 持续吞吐看 B（播放式），它决定「播放会不会追不上码率」")
    print("  · TTFB 决定「点开/拖动后干等多久」，目标 ≈340ms（代理+TG 固定 RTT）")
    print("  · 并发阶梯若随路数上升 → 加并发有效；若持平或下降 → 瓶颈在代理/TG 侧")
    return 0


if __name__ == "__main__":
    sys.exit(main())
