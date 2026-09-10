"""代理 / 优选 IP 的「选路 + 记账」模块（负载均衡）。

默认关闭（``TG_PROXY_LB=off``），关闭时本模块对 tg.py 的行为**零影响**。

为什么要做（来自真实压测，详见 PROXY_LB_REPORT.md / CF_PREFERRED_IP_REPORT.md）：
  * 真正拉开差距的不是「代理域名多」，而是「IP 快不快」。同一套 CF Pages 域名，
    不同优选 IP 的单车道吞吐能差 20 倍（23.2 / 6.2 / 2.6 / 1.1 MB/s），
    而现有的候选模型只有「域名」这一层，压根没法表达「同一个域名走不同 IP」。
    所以本模块把候选从「域名」展开成「域名 × IP」。
  * 单车道有上限：实测链路级天花板约 57~62 MB/s（不是 Worker 级），
    所以并发时必须多车道；``fastest`` 因此带「在途请求惩罚」，
    不是无脑把所有请求堆到第一名上。

关闭时的等价性保证（这三条是「默认当前模式」的硬承诺）：
  1. ``expand()`` 不展开 IP，只留下 ``ip=None`` 的 DNS 路由（与旧版 2 元组等价）；
  2. ``order()`` 原样返回，顺序 = 配置顺序 = 主备 + 失败切换，不做任何打乱；
  3. 不记账、不改连接池 key、不碰 urlopen 路径。

开启后新增的两种能力：
  * **IP 直连**：TCP 连到指定 IP，SNI / 证书校验 / Host 头仍用原域名
    （Cloudflare Pages 靠 SNI 路由，这是能直连优选 IP 的前提）；
  * **选路**：按 EWMA 吞吐加权/择优，连续失败的节点进入冷却并周期性半开探测。
"""
import http.client
import os
import random
import re
import socket
import ssl
import threading
import time
import urllib.parse
import urllib.request

# 可选模式：
#   off      —— 关闭（默认）。与引入本模块前逐字节一致。
#   fastest  —— 选 EWMA 吞吐最高的健康候选；按在途请求数惩罚，并发时自动分散车道；
#               并以 TG_PROXY_LB_EXPLORE 的概率随机探测其他候选（ε-greedy，防冷启动锁死）。
#   weighted —— 按 EWMA 吞吐加权随机（平滑分摊，多车道并发时最稳）。
#   rr       —— 严格轮询（适合「几个 IP 速度差不多」的场景）。
MODES = ("off", "fastest", "weighted", "rr")

_DEFAULT_MODE = "off"


def _envf(name, default, lo=0.0, hi=1e12):
    try:
        v = float(os.environ.get(name, "") or default)
    except Exception:
        v = default
    return max(lo, min(hi, v))


def _envi(name, default, lo=0, hi=1000000):
    try:
        v = int(float(os.environ.get(name, "") or default))
    except Exception:
        v = default
    return max(lo, min(hi, v))


def _envon(name, default=True):
    return (os.environ.get(name, "on" if default else "off").strip().lower()
            not in ("0", "off", "false", "no"))


def host_of(api_base):
    """从 api_base 取出小写主机名（用于匹配 TG_PROXY_IPS 的键）。"""
    s = (api_base or "").strip()
    if not s:
        return ""
    if "://" not in s:
        s = "https://" + s
    try:
        return (urllib.parse.urlparse(s).hostname or "").strip().lower()
    except Exception:
        return ""


def parse_ip_map(raw):
    """解析 ``TG_PROXY_IPS``，返回 (按域名映射, 全局 IP 列表)。

    支持（分号或换行分隔多条）：
      ``tg-proxy.pages.dev=155.117.224.63,1.1.1.1``  —— 只给该域名配优选 IP
      ``https://tg.example.com/tg=1.2.3.4``           —— 也可以直接写完整 apiBase
      ``155.117.224.63,43.175.131.30``                —— 全局，作用于所有代理域名

    域名匹配按主机名（大小写不敏感），api_base 里的路径前缀不影响匹配。
    """
    per_host = {}
    glob = []
    if not raw or not raw.strip():
        return per_host, glob
    for chunk in re.split(r"[;\n]", raw):
        chunk = chunk.strip()
        if not chunk:
            continue
        if "=" in chunk:
            k, v = chunk.split("=", 1)
            h = host_of(k)
            if not h:
                continue
            bucket = per_host.setdefault(h, [])
            for ip in (x.strip() for x in v.split(",")):
                if ip and ip not in bucket:
                    bucket.append(ip)
        else:
            for ip in (x.strip() for x in chunk.split(",")):
                if ip and ip not in glob:
                    glob.append(ip)
    return per_host, glob


class _Stat:
    """单个 (域名, IP) 路由的运行时统计。"""
    __slots__ = ("tp", "lat", "inflight", "fails", "cooldown_until",
                 "ok_count", "fail_count", "bytes", "last_ok")

    def __init__(self):
        self.tp = 0.0            # EWMA 吞吐（字节/秒）
        self.lat = 0.0           # EWMA 首字节延迟（秒）
        self.inflight = 0        # 当前在途请求数
        self.fails = 0           # 连续失败次数
        self.cooldown_until = 0.0
        self.ok_count = 0
        self.fail_count = 0
        self.bytes = 0
        self.last_ok = 0.0


class ProxyLB:
    """候选路由的选路与健康度记账。

    线程安全：所有共享状态都在 ``self._lock`` 下访问。
    """

    def __init__(self, mode=None, log=None):
        self._log = log or (lambda msg: None)
        mode = (mode if mode is not None
                else os.environ.get("TG_PROXY_LB", _DEFAULT_MODE))
        mode = str(mode or _DEFAULT_MODE).strip().lower()
        if mode not in MODES:
            mode = _DEFAULT_MODE
        self.mode = mode
        per_host, glob = parse_ip_map(os.environ.get("TG_PROXY_IPS", ""))
        self.ip_map = per_host
        self.global_ips = glob
        # EWMA 平滑系数：越大越"跟手"，越小越稳
        self.alpha = _envf("TG_PROXY_LB_ALPHA", 0.3, 0.01, 1.0)
        # 在途惩罚：score = tp / (1 + inflight * pen)。
        # 单车道有吞吐上限，全堆到"最快"那一条反而更慢，pen>0 才能让并发自动铺开。
        self.inflight_pen = _envf("TG_PROXY_LB_PENALTY", 0.5, 0.0, 10.0)
        # ε-greedy 探索概率：避免"第一名永远是它 → 其他候选永远没样本"的冷启动锁死
        self.explore = _envf("TG_PROXY_LB_EXPLORE", 0.1, 0.0, 1.0)
        # 连续失败多少次后进入冷却
        self.max_fails = _envi("TG_PROXY_LB_FAILS", 3, 1, 1000)
        self.cooldown = _envf("TG_PROXY_LB_COOLDOWN", 60.0, 1.0, 86400.0)
        self.cooldown_cap = _envf("TG_PROXY_LB_COOLDOWN_MAX", 300.0, 1.0, 86400.0)
        # 小于这个体积的请求**不参与吞吐统计**：getFile 只有几百字节，
        # 一次 RTT 抖动就能算出 "几百 B/s"，会把这条路由的 EWMA 直接打死。
        # 真正决定体验的是分片上传(20MB)/分片下载(数 MB)，它们才配说话。
        self.min_bytes = _envi("TG_PROXY_LB_MIN_BYTES", 256 * 1024, 0, 1 << 40)
        # 是否保留「走 DNS 解析」的兜底路由（优选 IP 全挂时还有条退路）
        self.keep_dns = _envon("TG_PROXY_LB_KEEP_DNS", True)
        self._lock = threading.Lock()
        self._stats = {}
        self._rr = 0
        self._orders = 0          # order() 调用计数，用于周期性打点
        # 每多少次选路打一次统计快照（排查"为什么总走这条 IP"时很有用）
        self.log_every = _envi("TG_PROXY_LB_LOG_EVERY", 100, 1, 1000000)

    @property
    def enabled(self):
        return self.mode != "off"

    def describe(self):
        """一行配置摘要，供启动日志用。"""
        if not self.enabled:
            return "off(默认: 按序主备+失败切换, TG_PROXY_IPS 被忽略)"
        ips = sum(len(v) for v in self.ip_map.values()) + len(self.global_ips)
        return (f"{self.mode} ips={ips} penalty={self.inflight_pen} "
                f"explore={self.explore} cooldown={self.cooldown}s "
                f"keep_dns={'on' if self.keep_dns else 'off'} "
                f"min_bytes={self.min_bytes}")

    # ---------- 候选展开 ----------
    def ips_for(self, api_base):
        if not self.enabled:
            return []
        return list(self.ip_map.get(host_of(api_base)) or self.global_ips or [])

    def expand(self, api_base, proxy_token):
        """把一个「域名候选」展开成 [(api_base, proxy_token, ip), ...]。

        ip=None 表示走 DNS（也是关闭模式下的唯一形态）。
        """
        ips = self.ips_for(api_base)
        if not ips:
            return [(api_base, proxy_token, None)]
        out = [(api_base, proxy_token, ip) for ip in ips]
        if self.keep_dns:
            out.append((api_base, proxy_token, None))
        return out

    # ---------- 选路 ----------
    def _st(self, route):
        key = (route[0], route[2] if len(route) > 2 else None)
        st = self._stats.get(key)
        if st is None:
            st = _Stat()
            self._stats[key] = st
        return st

    def order(self, routes):
        """按当前模式给候选排序，返回新列表（不改动入参）。

        三条硬性原则：
          1. 谁都没测过（冷启动）时不能把请求锁死在第一条上——否则其他路由
             永远拿不到样本，"择优"就退化成"永远用配置里第一条"；
          2. 已经测过的路由按 EWMA 吞吐说话，没测过的给一个「等于当前均值」的
             乐观先验（不是 0），保证它有机会被采样；
          3. 候选全在冷却时按配置顺序全量返回，绝不比关掉 LB 更差。
        """
        routes = list(routes)
        if not self.enabled or len(routes) <= 1:
            return routes
        # 周期性打一次统计快照（排查"为什么总走这条 IP"时全靠它）
        self._orders += 1
        if self._orders % self.log_every == 1:
            self._log("代理路由统计: " + self.snapshot())
        now = time.time()
        with self._lock:
            healthy = [r for r in routes if self._st(r).cooldown_until <= now]
            if not healthy:
                # 全在冷却：按配置顺序全量返回。此时"选路"已无意义，
                # 保持确定性和可预测性（绝不比关掉 LB 更差）比随机乱试更有价值。
                return routes
            pool = healthy
            if len(pool) <= 1:
                return pool
            scored = []
            for r in pool:
                st = self._st(r)
                sc = 0.0
                if st.tp > 0:
                    # 在途惩罚：单车道有吞吐上限，全堆到"最快"那条反而更慢，
                    # 除以 (1 + 在途数 × pen) 后并发会自动铺开到次快的车道。
                    sc = st.tp / (1.0 + max(0, st.inflight) * self.inflight_pen)
                scored.append((r, sc))
            positive = [sc for _r, sc in scored if sc > 0]

            if self.mode == "rr":
                if not positive:
                    return pool            # 冷启动：先按配置顺序走，第一条成功后即可轮转
                k = self._rr % len(scored)
                self._rr = (self._rr + 1) % 1000000
                return [r for r, _ in scored[k:]] + [r for r, _ in scored[:k]]

            if self.mode == "weighted":
                if not positive:
                    # 冷启动：等概率随机。若这里退回配置顺序，则第一条会一直被选中、
                    # 其余路由永远没有样本，加权将永久退化成"只走第一条"。
                    shuffled = [r for r, _ in scored]
                    random.shuffle(shuffled)
                    return shuffled
                # 无样本路由给「均值先验」，否则权重≈0 等于永不采样
                prior = sum(positive) / len(positive)
                keyed = []
                for r, sc in scored:
                    w = sc if sc > 0 else prior
                    u = random.random()
                    if u <= 0.0:
                        u = 1e-12
                    # Efraimidis-Spirakis 加权无放回采样：key = u^(1/w)，取最大的若干个
                    keyed.append((u ** (1.0 / w), r))
                keyed.sort(key=lambda x: -x[0])
                return [r for _k, r in keyed]

            # fastest
            if self.explore > 0.0 and random.random() < self.explore:
                # ε-greedy：随机挑一条去采样（含冷启动），避免"第一名永远是它"
                i = random.randrange(len(scored))
                pick = scored.pop(i)
                rest = [r for r, _ in scored]
                if positive:
                    rest.sort(key=lambda r: -self._st(r).tp
                              / (1.0 + max(0, self._st(r).inflight) * self.inflight_pen))
                return [pick[0]] + rest
            if not positive:
                return pool
            scored.sort(key=lambda x: -x[1])   # 稳定排序：同分保持配置顺序
            return [r for r, _ in scored]

    # ---------- 记账 ----------
    def begin(self, route):
        """请求开始：在途 +1。"""
        if not self.enabled or route is None:
            return
        with self._lock:
            self._st(route).inflight += 1

    def end(self, route, ok=True, nbytes=0, seconds=0.0, ttfb=None, neutral=False):
        """请求结束：在途 -1 并更新统计。

        ``neutral=True`` 表示「这次失败不能怪这条路由」（429 / 4xx 业务错误是
        bot 级或请求级问题，换 IP 也没用），只减在途、不计失败。
        """
        if not self.enabled or route is None:
            return
        now = time.time()
        with self._lock:
            st = self._st(route)
            if st.inflight > 0:
                st.inflight -= 1
            if not ok:
                st.fail_count += 1
                if neutral:
                    return
                st.fails += 1
                if st.fails >= self.max_fails:
                    back = min(
                        self.cooldown * (2 ** (st.fails - self.max_fails)),
                        self.cooldown_cap,
                    )
                    st.cooldown_until = now + back
                    self._log(
                        f"代理路由进入冷却: {host_of(route[0]) or route[0]}"
                        f"@{route[2] or 'dns'} 连续失败={st.fails} 冷却={back:.0f}s"
                    )
                return
            st.ok_count += 1
            st.fails = 0
            st.cooldown_until = 0.0
            st.last_ok = now
            if nbytes and nbytes > 0:
                st.bytes += int(nbytes)
            if nbytes >= self.min_bytes and seconds > 0:
                tp = float(nbytes) / float(seconds)
                st.tp = tp if st.tp <= 0 else (self.alpha * tp + (1 - self.alpha) * st.tp)
            if ttfb is not None and ttfb >= 0:
                st.lat = (ttfb if st.lat <= 0
                          else (self.alpha * ttfb + (1 - self.alpha) * st.lat))

    def cooling_down(self, route):
        """该路由是否还在冷却中（>0 表示剩余秒数）。"""
        if not self.enabled or route is None:
            return 0.0
        with self._lock:
            st = self._stats.get((route[0], route[2] if len(route) > 2 else None))
        if st is None:
            return 0.0
        return max(0.0, st.cooldown_until - time.time())

    def snapshot(self):
        """给日志/排查用的一行摘要（不持锁过久）。"""
        if not self.enabled:
            return "lb=off"
        with self._lock:
            parts = []
            for (base, ip), st in self._stats.items():
                # begin() 会为在途路由建条目；没样本的不刷屏
                if st.ok_count == 0 and st.fail_count == 0 and st.tp <= 0:
                    continue
                cool = max(0.0, st.cooldown_until - time.time())
                parts.append(
                    f"{host_of(base) or base}@{ip or 'dns'}:"
                    f"tp={st.tp / 1048576:.1f}MB/s"
                    f"{'' if st.lat <= 0 else f'/ttfb={st.lat * 1000:.0f}ms'}"
                    f" ok={st.ok_count} fail={st.fail_count}"
                    f"{f' cool={cool:.0f}s' if cool > 0 else ''}"
                )
        return "lb[" + self.mode + "] " + (" | ".join(parts) if parts else "无样本")


# ---------------------------------------------------------------------------
# IP 直连（SNI 保持原域名）
# ---------------------------------------------------------------------------
class PinnedHTTPConnection(http.client.HTTPConnection):
    """TCP 连到指定 IP，但 Host 头仍是原域名。"""

    def __init__(self, host, port=None, pin_ip=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT, **kw):
        self._pin_ip = pin_ip or None
        super().__init__(host, port, timeout=timeout, **kw)

    def connect(self):
        if not self._pin_ip:
            return super().connect()
        self.sock = socket.create_connection(
            (self._pin_ip, self.port), self.timeout,
            getattr(self, "source_address", None),
        )


class PinnedHTTPSConnection(PinnedHTTPConnection, http.client.HTTPSConnection):
    """TCP 连到指定 IP，但 SNI / 证书校验 / Host 头仍用原域名。

    Cloudflare Pages / Workers 是按 SNI 做路由的：只要 SNI 对，连哪个边缘 IP 都能
    正确落到对应的项目上。这也是「优选 IP」能生效的前提。
    """

    def connect(self):
        if not self._pin_ip:
            return http.client.HTTPSConnection.connect(self)
        sock = socket.create_connection(
            (self._pin_ip, self.port), self.timeout,
            getattr(self, "source_address", None),
        )
        ctx = getattr(self, "_context", None)
        if ctx is None:
            ctx = ssl.create_default_context()
        try:
            self.sock = ctx.wrap_socket(
                sock, server_hostname=(self._tunnel_host or self.host)
            )
        except Exception:
            try:
                sock.close()
            except Exception:
                pass
            raise


class _PinnedHTTPHandler(urllib.request.HTTPHandler):
    def __init__(self, pin_ip):
        urllib.request.HTTPHandler.__init__(self)
        self._pin_ip = pin_ip

    def http_open(self, req):
        return self.do_open(self._mk, req)

    def _mk(self, host, **kw):
        return PinnedHTTPConnection(host, pin_ip=self._pin_ip, **kw)


class _PinnedHTTPSHandler(urllib.request.HTTPSHandler):
    def __init__(self, pin_ip):
        urllib.request.HTTPSHandler.__init__(self)
        self._pin_ip = pin_ip

    def https_open(self, req):
        return self.do_open(self._mk, req)

    def _mk(self, host, **kw):
        return PinnedHTTPSConnection(host, pin_ip=self._pin_ip, **kw)


def urlopen_pinned(req, timeout, pin_ip):
    """带 IP 直连的 urlopen；``pin_ip`` 为空时与 ``urlopen`` 完全等价。"""
    if not pin_ip:
        return urllib.request.urlopen(req, timeout=timeout)
    opener = urllib.request.build_opener(
        _PinnedHTTPSHandler(pin_ip), _PinnedHTTPHandler(pin_ip)
    )
    return opener.open(req, timeout=timeout)
