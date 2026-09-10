"""代理/优选 IP 负载均衡的单元测试（纯逻辑，不联网）。

运行：python3 proxylb_test.py
"""
import os
import sys
import time

import proxylb as P

FAILS = []


def check(cond, msg):
    if cond:
        print(f"  [OK] {msg}")
    else:
        print(f"  [FAIL] {msg}")
        FAILS.append(msg)


def mk(mode, ips=None, **env):
    for k in list(os.environ):
        if k.startswith("TG_PROXY_LB_"):
            del os.environ[k]
    os.environ.pop("TG_PROXY_IPS", None)
    for k, v in env.items():
        os.environ["TG_PROXY_LB_" + k.upper()] = str(v)
    if ips:
        os.environ["TG_PROXY_IPS"] = ips
    return P.ProxyLB(mode)


A = "https://a.example/tg"
B = "https://b.example/tg"

print("== 1. off（默认）行为与旧版一致 ==")
lb = mk("off", "a.example=1.1.1.1,2.2.2.2")
check(lb.enabled is False, "off: enabled=False")
check(lb.expand(A, "tk") == [(A, "tk", None)], "off: 不做 IP 展开（TG_PROXY_IPS 被忽略）")
routes = [(A, "t", None), (B, "t", None)]
check(lb.order(routes) == routes, "off: 顺序原样返回")
# 记了一堆账之后依然不变（off 下 begin/end 必须完全空操作）
lb.begin(routes[0])
lb.end(routes[0], ok=True, nbytes=10 << 20, seconds=1.0)
lb.end(routes[1], ok=False)
check(lb.order(routes) == routes, "off: 记账后顺序仍不变")
check(lb._stats == {}, "off: 不产生任何统计状态")

print("== 2. TG_PROXY_IPS 解析 ==")
per, glob = P.parse_ip_map("a.example=1.1.1.1,2.2.2.2;b.example=3.3.3.3")
check(per == {"a.example": ["1.1.1.1", "2.2.2.2"], "b.example": ["3.3.3.3"]}, "按域名解析")
per2, glob2 = P.parse_ip_map("1.1.1.1,2.2.2.2")
check(glob2 == ["1.1.1.1", "2.2.2.2"] and per2 == {}, "全局 IP 列表")
per3, _ = P.parse_ip_map("https://a.example/tg=9.9.9.9")
check(per3.get("a.example") == ["9.9.9.9"], "apiBase 写法也能匹配到主机名")
check(P.host_of("https://A.Example.com/tg") == "a.example.com", "host_of 大小写归一")

print("== 3. IP 展开 ==")
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2")
check([r[2] for r in lb.expand(A, "tk")] == ["1.1.1.1", "2.2.2.2", None],
      "展开 2 个优选 IP + 1 条 DNS 兜底")
lb = mk("fastest", "a.example=1.1.1.1", keep_dns="off")
check([r[2] for r in lb.expand(A, "tk")] == ["1.1.1.1"], "keep_dns=off 时不留 DNS 兜底")
lb = mk("fastest")
check(lb.expand(A, "tk") == [(A, "tk", None)], "没配 TG_PROXY_IPS 时不展开")

print("== 4. 冷启动：不能锁死在第一条（否则择优退化成永远第一条）==")
# rr：第一条成功后即可轮转（先按配置顺序走一次没问题）
lb = mk("rr", "a.example=1.1.1.1,2.2.2.2")
rs = lb.expand(A, "tk")
check(lb.order(rs) == rs, "rr: 首请求按配置顺序")
# weighted：冷启动必须随机，否则其余路由永远拿不到样本
lb = mk("weighted", "a.example=1.1.1.1,2.2.2.2")
rs = lb.expand(A, "tk")
seen = set()
for _ in range(200):
    seen.add(lb.order(rs)[0][2])
check(len(seen) == 3, f"weighted: 冷启动会随机探索到全部路由 {sorted(map(str, seen))}")
# fastest：ε-greedy 在冷启动也生效
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2", explore=1.0)
rs = lb.expand(A, "tk")
seen = set()
for _ in range(200):
    seen.add(lb.order(rs)[0][2])
check(len(seen) == 3, f"fastest: explore=1.0 时冷启动也能采样全部路由")
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2", explore=0.0)
check(lb.order(lb.expand(A, "tk")) == lb.expand(A, "tk"),
      "fastest: explore=0 且无样本时保持配置顺序（可预测的旧行为）")

print("== 5. fastest：按 EWMA 吞吐择优 ==")
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2", explore=0.0)
rs = lb.expand(A, "tk")
slow, fast, dns = rs
for _ in range(5):
    lb.begin(slow); lb.end(slow, ok=True, nbytes=10 << 20, seconds=10.0)   # 1MB/s
    lb.begin(fast); lb.end(fast, ok=True, nbytes=10 << 20, seconds=0.5)    # 20MB/s
check(lb.order(rs)[0] == fast, "fastest: 快的 IP 排到第一")
check(lb.order(rs)[-1] == dns, "fastest: 无样本的 DNS 兜底排最后")

print("== 6. fastest：在途惩罚让并发铺开，不全挤一条 ==")
# penalty=1.0 → score ≈ 「该路由给每条在途请求的公平份额」= tp / (1 + inflight)
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2", explore=0.0, penalty=1.0)
rs = lb.expand(A, "tk")
slow, fast, dns = rs
lb.begin(slow); lb.end(slow, ok=True, nbytes=10 << 20, seconds=1.0)   # 10MB/s
lb.begin(fast); lb.end(fast, ok=True, nbytes=10 << 20, seconds=0.5)   # 20MB/s
check(lb.order(rs)[0] == fast, "fastest: 空闲时走快 IP")
for _ in range(8):
    lb.begin(fast)                 # 快 IP 上压了 8 个在途
first = lb.order(rs)[0]
check(first == slow,
      f"fastest: 快 IP 挤了 8 条后让位给慢 IP（20/(1+8)=2.2 < 10，实际={first[2]}）")
for _ in range(8):
    lb.end(fast, ok=True)   # 只释放在途，不带样本
check(lb.order(rs)[0] == fast, "fastest: 在途释放后回到快 IP")

print("== 6b. weighted：无样本路由拿「均值先验」，不会永远不被采样 ==")
lb = mk("weighted", "a.example=1.1.1.1,2.2.2.2")
rs = lb.expand(A, "tk")
slow, fast, dns = rs
lb.begin(fast); lb.end(fast, ok=True, nbytes=10 << 20, seconds=0.5)
cnt = {slow[2]: 0, fast[2]: 0, dns[2]: 0}
for _ in range(300):
    cnt[lb.order(rs)[0][2]] += 1
check(cnt[slow[2]] > 20 and cnt[dns[2]] > 20,
      f"weighted: 未测过的路由仍被持续采样（否则永远拿不到样本） {cnt}")

print("== 7. weighted：按吞吐加权随机 ==")
lb = mk("weighted", "a.example=1.1.1.1,2.2.2.2")
rs = lb.expand(A, "tk")
slow, fast, dns = rs
lb.begin(slow); lb.end(slow, ok=True, nbytes=10 << 20, seconds=10.0)
lb.begin(fast); lb.end(fast, ok=True, nbytes=10 << 20, seconds=0.5)
cnt = {slow[2]: 0, fast[2]: 0, dns[2]: 0}
for _ in range(400):
    cnt[lb.order(rs)[0][2]] += 1
check(cnt["2.2.2.2"] > cnt["1.1.1.1"] * 5,
      f"weighted: 快 IP 被选中次数明显多于慢 IP {cnt}")
check(cnt[None] > cnt["1.1.1.1"],
      f"weighted: 未测过的 DNS 兜底拿均值先验（比慢 IP 更常被采样） {cnt}")

print("== 8. rr：严格轮询 ==")
lb = mk("rr", "a.example=1.1.1.1,2.2.2.2")
rs = lb.expand(A, "tk")
lb.begin(rs[0]); lb.end(rs[0], ok=True, nbytes=10 << 20, seconds=1.0)
lb.begin(rs[1]); lb.end(rs[1], ok=True, nbytes=10 << 20, seconds=1.0)
seq = [lb.order(rs)[0][2] for _ in range(6)]
check(seq == ["1.1.1.1", "2.2.2.2", None] * 2, f"rr: 依次轮转 {seq}")

print("== 8b. 小请求不参与吞吐统计（getFile 只有几百字节） ==")
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2", explore=0.0)
rs = lb.expand(A, "tk")
lb.begin(rs[0]); lb.end(rs[0], ok=True, nbytes=10 << 20, seconds=0.5)   # 20MB/s
lb.begin(rs[1]); lb.end(rs[1], ok=True, nbytes=300, seconds=0.3)        # 1KB/s（噪声）
check(lb._st(rs[1]).tp == 0.0, "小请求样本被丢弃（tp 仍为 0）")
check(lb.order(rs)[0] == rs[0], "快 IP 未被噪声样本拖垮")

print("== 9. 连续失败 → 冷却；冷却结束自动半开 ==")
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2", explore=0.0, fails=3, cooldown=60,
        cooldown_max=300)
rs = lb.expand(A, "tk")
lb.begin(rs[0]); lb.end(rs[0], ok=True, nbytes=10 << 20, seconds=0.5)
lb.begin(rs[1]); lb.end(rs[1], ok=True, nbytes=10 << 20, seconds=5.0)
check(lb.order(rs)[0] == rs[0], "正常时走快的")
for _ in range(3):
    lb.begin(rs[0]); lb.end(rs[0], ok=False)
check(lb.order(rs)[0] == rs[1], "连续失败 3 次后该路由被跳过")
check(lb.order(rs)[0][2] != rs[0][2], "冷却期内不再当选")
lb._stats[(rs[0][0], rs[0][2])].cooldown_until = time.time() - 1
check(lb.order(rs)[0] == rs[0], "冷却到期后自动恢复（半开探测）")

print("== 10. 全部冷却 → 放开（绝不比 off 更差） ==")
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2")
rs = lb.expand(A, "tk")
for r in rs:
    for _ in range(5):
        lb.begin(r); lb.end(r, ok=False)
out = lb.order(rs)
check([r[2] for r in out] == [r[2] for r in rs], "全被冷却时按配置顺序全量返回")

print("== 11. 429/4xx 是 neutral（不污染路由健康度） ==")
lb = mk("fastest", "a.example=1.1.1.1,2.2.2.2", explore=0.0, fails=3)
rs = lb.expand(A, "tk")
lb.begin(rs[0]); lb.end(rs[0], ok=True, nbytes=10 << 20, seconds=0.5)
for _ in range(10):
    lb.begin(rs[0]); lb.end(rs[0], ok=False, neutral=True)
check(lb.order(rs)[0] == rs[0], "neutral 失败不触发冷却（该路由仍是首选）")

print("== 12. 在途计数不会泄漏 ==")
lb = mk("fastest", "a.example=1.1.1.1")
rs = lb.expand(A, "tk")
for _ in range(20):
    lb.begin(rs[0])
    lb.end(rs[0], ok=True, nbytes=100, seconds=0.1)
check(lb._st(rs[0]).inflight == 0, "begin/end 配对后在途归零")

print()
if FAILS:
    print(f"FAILED: {len(FAILS)} 项")
    for f in FAILS:
        print("  - " + f)
    sys.exit(1)
print("全部通过")
