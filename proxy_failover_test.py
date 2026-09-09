#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
"""多 TG 代理候选：构建回归 + 真实故障切换验证。

背景：新增第二个 TG 代理（otterhub）时发现 ``_build_candidates`` 的兜底逻辑写成了
``if not cands: cands.append(api_base)`` —— 一旦配了 TG_PROXY_POOLS，TG_API_BASE 里
配的主代理就被整个丢掉：想「多一个备用」结果变成「换掉主用」。本测试把该回归点钉住，
并验证「首候选不可达时能自动切到下一候选、全部不可达时报错而不是静默返回空」。

用法（需要 TG_* / DAV_* 环境变量，B 组联网用例才跑）::

    source <container-env>.sh && python3.11 proxy_failover_test.py
"""
import io
import os
import sys
import contextlib

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import tg
from tg import TelegramBackend, TGError

GLOBAL_BASE = "https://tg.totootao.top/tg"
POOL_BASE = "https://otterhub-tg-proxy-3uj.pages.dev/tg"
DEAD_BASE = "http://127.0.0.1:9/tg"  # 必连不上（端口没人听）

_RESULTS = []


def check(name, cond, detail=""):
    _RESULTS.append((name, bool(cond)))
    print(("  \033[32mPASS\033[0m " if cond else "  \033[31mFAIL\033[0m ")
          + name + (("  -> " + detail) if detail else ""))


def _mk(api_base, proxy_token, pools, slots=None):
    slots = slots if slots is not None else [{"token": "T", "chat_id": "C"}]
    return TelegramBackend(slots=slots, api_base=api_base, proxy_token=proxy_token,
                           proxy_pools=pools, rate_limit=0, rotate=True)


# ---------------------------------------------------------------- A 组：构建候选（纯单元，不联网）
def test_a_build():
    print("\n[A] 候选列表构建（不联网）")
    tok, ptok = "GLOBAL-TOKEN", "POOL-TOKEN"

    b = _mk(GLOBAL_BASE, tok, [])
    c = b._build_candidates(b.slots[0])
    check("A1 未配 TG_PROXY_POOLS：候选=[全局默认]",
          c == [(GLOBAL_BASE, tok)], str(c))

    b = _mk(GLOBAL_BASE, tok, [(POOL_BASE, ptok)])
    c = b._build_candidates(b.slots[0])
    check("A2 配了 TG_PROXY_POOLS：全局默认仍保留在候选里（本次修复的回归点）",
          c == [(POOL_BASE, ptok), (GLOBAL_BASE, tok)], str(c))

    b = _mk(GLOBAL_BASE, tok, [(GLOBAL_BASE, tok)])
    c = b._build_candidates(b.slots[0])
    check("A3 池里配的和全局默认相同：去重后只有 1 个",
          c == [(GLOBAL_BASE, tok)], str(c))

    b = _mk(GLOBAL_BASE, tok, [(POOL_BASE, ptok)])
    c = b._build_candidates({"token": "T", "chat_id": "C",
                             "api_base": "https://slot-only.example/tg"})
    check("A4 槽位自带 apiBase：自带在前，池次之，全局默认兜底",
          c == [("https://slot-only.example/tg", tok), (POOL_BASE, ptok),
                (GLOBAL_BASE, tok)], str(c))

    b = _mk(GLOBAL_BASE, tok, [(POOL_BASE, ptok)])
    c = b._build_candidates({"token": "T", "chat_id": "C", "api_base": GLOBAL_BASE})
    check("A5 槽位自带 == 全局默认：结尾不再重复追加",
          c == [(GLOBAL_BASE, tok), (POOL_BASE, ptok)], str(c))


# ---------------------------------------------------------------- B 组：真实联网
def test_b_real():
    print("\n[B] 真实代理连通性与故障切换")
    try:
        from config import Config
        cfg = Config()
    except Exception as e:
        print("  跳过：无法加载配置 (%s)" % e)
        return
    if not cfg.slots or not cfg.proxy_pools:
        print("  跳过：环境变量里没有 TG_BOT_POOLS / TG_PROXY_POOLS")
        return

    fid = os.environ.get("TG_TEST_FILE_ID",
                         "BQACAgUAAx0EXFWnCwACUT9qoJ5DV_qWCuOm47OJKOGcOjRf5wAChCY"
                         "AAi39CFU477pao1QfuT0E")
    n = 256 * 1024
    be = TelegramBackend(slots=cfg.slots, api_base=cfg.api_base,
                         proxy_token=cfg.proxy_token, proxy_pools=cfg.proxy_pools,
                         rate_limit=0, rotate=cfg.slot_rotate)
    c0 = be._candidates[0]
    expect = len(cfg.proxy_pools) + 1  # 池里的候选 + 全局默认兜底
    check("B1 生产配置下每个 bot 都有 %d 个代理候选（池 %d + 全局兜底 1）"
          % (expect, len(cfg.proxy_pools)),
          all(len(be._candidates[i]) == expect for i in range(len(cfg.slots))),
          "槽位0候选=%s" % ([b for b, _ in c0],))

    def _read(cands, slot=0):
        be._candidates[0] = cands
        buf = io.BytesIO()
        for b in be.iter_chunk(fid, slot, start=0, end=n - 1, blk=64 * 1024):
            buf.write(b)
        return buf.getvalue()

    data = _read(list(c0))
    check("B2 正常下载：拿到 %d 字节" % n, len(data) == n,
          "实际 %d 字节" % len(data))

    dead_first = [(DEAD_BASE, c0[0][1])] + list(c0)
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(buf):
            data2 = _read(dead_first)
        out = buf.getvalue()
        ok = len(data2) == n and data2 == data
        check("B3 首候选不可达：自动切到下一候选且数据一致", ok,
              "实际 %d 字节" % len(data2))
        check("B4 切换过程有日志记录（能看到「切换下一代理」）",
              "切换下一代理" in out or "瞬断重试" in out)
    except Exception as e:
        check("B3 首候选不可达：自动切到下一候选且数据一致", False,
              "抛异常 %s: %s" % (type(e).__name__, e))
        check("B4 切换过程有日志记录", False, "上一步已抛异常")

    try:
        with contextlib.redirect_stdout(io.StringIO()), \
             contextlib.redirect_stderr(io.StringIO()):
            _read([(DEAD_BASE, c0[0][1])])
        check("B5 全部候选不可达：必须抛错，不能静默返回空", False, "竟然没抛异常")
    except TGError as e:
        check("B5 全部候选不可达：必须抛错，不能静默返回空", True,
              "TGError: %s" % str(e)[:60])
    except Exception as e:
        check("B5 全部候选不可达：必须抛错，不能静默返回空", True,
              "%s: %s" % (type(e).__name__, str(e)[:60]))


def main():
    test_a_build()
    test_b_real()
    ok = sum(1 for _, p in _RESULTS if p)
    total = len(_RESULTS)
    print("\n%d/%d 通过" % (ok, total))
    if ok != total:
        print("失败项：" + ", ".join(n for n, p in _RESULTS if not p))
    return 0 if ok == total else 1


if __name__ == "__main__":
    sys.exit(main())
