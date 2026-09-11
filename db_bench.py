#!/usr/bin/env python3
"""db.py 大数据操作基准：模拟「万级文件目录」场景下的核心元数据操作耗时。

覆盖场景（对应 DB_OPT_REPORT.md 的优化点）：
  1. 批量建文件 create_file   —— 写入吞吐（单行 upsert + commit）
  2. 列目录 depth=1           —— PROPFIND 最常用路径（优化：parent_path 点查）
  3. 列目录 depth=infinity    —— 递归列全子树（优化：甩掉 chunks 大 JSON）
  4. get_node 含/不含 chunks  —— 存在性检查轻量化
  5. move 子树                —— 优化：单锁 + executemany（旧版逐行 INSERT）
  6. copy 子树                —— 同上
  7. delete_recursive 子树    —— 单条 LIKE DELETE（LIKE 转义修复）
  8. chunk_dedup 写/查        —— 去重表吞吐
  9. 过期去重记录清理          —— 优化：分批提交（旧版单条大事务）

运行：python3 db_bench.py [--nodes 5000] [--runs 5]
说明：脚本只压 db 层（不起 HTTP/Telegram），结果受机器性能影响，
绝对值仅供相对对比（旧版 vs 新版同机同参数跑分）。
"""
import argparse
import json
import os
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import MetaStore  # noqa: E402


def _fake_chunks(i, n_chunks=8):
    """构造第 i 个文件的分片元数据（形态与真实落库一致：file_id/slot/size/message_id）。

    默认 8 片 ≈ 400B JSON/文件：接近真实大文件的分片列表规模，
    用于体现「列目录甩掉 chunks 大字段」的收益。
    """
    return [
        {"file_id": f"BQACAgIAAx0CAPIK{k * i:012d}", "slot": k % 4, "size": 20971520,
         "message_id": 1000 + i * 10 + k}
        for k in range(n_chunks)
    ]


class Bench:
    def __init__(self, nodes, runs):
        self.nodes = nodes
        self.runs = runs
        self.rows = []
        self.tmp = tempfile.mkdtemp(prefix="tgwd_bench_")
        self.store = MetaStore(os.path.join(self.tmp, "bench.db"))

    def record(self, name, elapsed, unit="ms"):
        val = elapsed * 1000 if unit == "ms" else elapsed * 1e6
        self.rows.append((name, val, unit))
        print(f"  {name:<38} {val:>12.2f} {unit}")

    def report(self):
        w = max(len(r[0]) for r in self.rows) + 2
        print("\n" + "=" * 64)
        print("结果汇总（值越小越好）")
        print("=" * 64)
        for name, val, unit in self.rows:
            print(f"  {name:<{w}} {val:>12.2f} {unit}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--nodes", type=int, default=5000, help="单目录文件数（默认 5000）")
    ap.add_argument("--runs", type=int, default=5, help="读类场景重复次数（默认 5）")
    args = ap.parse_args()
    b = Bench(args.nodes, args.runs)
    st, N, R = b.store, args.nodes, args.runs

    st.create_dir("/bench")
    st.create_dir("/bench/big")

    # 1) 批量建文件
    t0 = time.perf_counter()
    for i in range(N):
        st.create_file(f"/bench/big/file_{i:05d}.bin", "application/octet-stream",
                       _fake_chunks(i), 50 * 1024 * 1024, chunk_size=20971520,
                       file_hash=f"{'%064x' % i}")
    b.record(f"create_file x{N}（批量建库）", time.perf_counter() - t0, "ms")

    # 1.5) 预置大子树（不计时）：再复制两份，让 /bench 子树共 ~3N 个节点，
    #      复现「列浅层目录但子树巨大」的真实负载
    assert st.copy("/bench/big", "/bench/copy_a")
    assert st.copy("/bench/big", "/bench/copy_b")

    # 2) 浅目录列目录 depth=1（核心场景：直接子节点少、子树巨大）
    t0 = time.perf_counter()
    for _ in range(R):
        self_node, children = st.list_children("/bench", "1")
        assert len(children) == 3, f"depth=1 子节点数错误: {len(children)}"
    b.record(f"list_children 浅目录大子树 x{R}", time.perf_counter() - t0, "ms")

    # 2.1) 深目录列目录 depth=1（结果集 5000 行，验证无回退）
    t0 = time.perf_counter()
    for _ in range(R):
        self_node, children = st.list_children("/bench/big", "1")
        assert len(children) == N, f"depth=1 子节点数错误: {len(children)}"
    b.record(f"list_children depth=1({N}子) x{R}", time.perf_counter() - t0, "ms")

    # 3) 列目录 depth=infinity（递归全子树 ~3N 节点）
    t0 = time.perf_counter()
    for _ in range(R):
        self_node, children = st.list_children("/bench", "infinity")
        assert len(children) >= 2 * N, f"infinity 子树错误: {len(children)}"
    b.record(f"list_children infinity(3N) x{R}", time.perf_counter() - t0, "ms")

    # 4) get_node 含/不含 chunks（单个大文件的点查）
    #    （with_chunks 为新版参数；旧版代码跑基准时自动回退/跳过）
    t0 = time.perf_counter()
    for _ in range(R):
        try:
            st.get_node("/bench/big/file_00000.bin", with_chunks=True)
        except TypeError:
            st.get_node("/bench/big/file_00000.bin")
    b.record(f"get_node 含chunks   x{R}", time.perf_counter() - t0, "ms")
    try:
        t0 = time.perf_counter()
        for _ in range(R):
            st.get_node("/bench/big/file_00000.bin", with_chunks=False)
        b.record(f"get_node 轻量(无chunks) x{R}", time.perf_counter() - t0, "ms")
    except TypeError:
        print("  get_node 轻量(无chunks) x%d   跳过（旧版无此参数）" % R)

    # 5) move 子树（A->B->A 交替，共 R 次）
    t0 = time.perf_counter()
    for i in range(R):
        src, dst = ("/bench/big", "/bench/moved") if i % 2 == 0 else ("/bench/moved", "/bench/big")
        assert st.move(src, dst)
    b.record(f"move 子树({N}文件) x{R}", time.perf_counter() - t0, "ms")
    # R 次交替后数据实际所在目录（奇数次停在 moved，偶数次回到 big）
    src_dir = "/bench/big" if R % 2 == 0 else "/bench/moved"

    # 6) copy 子树 R 次（每次覆盖同一目标）
    t0 = time.perf_counter()
    for i in range(R):
        assert st.copy(src_dir, "/bench/copy")
    b.record(f"copy 子树({N}文件) x{R}", time.perf_counter() - t0, "ms")

    # 7) delete_recursive 子树 R 次（删掉预置的两份大拷贝 + 一份工作拷贝）
    t0 = time.perf_counter()
    st.delete_recursive("/bench/copy_a")
    st.delete_recursive("/bench/copy_b")
    b.record(f"delete_recursive({2 * N}文件)", time.perf_counter() - t0, "ms")

    # 8) chunk_dedup 写/查吞吐
    M = 20000
    rows = [(f"{i:064x}", f"BQACAgIA{i}", i % 4, 1000 + i, 20971520) for i in range(M)]
    t0 = time.perf_counter()
    for sha, fid, slot, mid, size in rows:
        st.put_chunk_dedup(sha, fid, slot, mid, size)
    dt = time.perf_counter() - t0
    b.record(f"put_chunk_dedup x{M}", dt, "ms")
    print(f"    -> {M / dt:,.0f} 写/秒")
    t0 = time.perf_counter()
    for sha, *_ in rows:
        assert st.find_chunk_by_sha(sha, 20971520)
    dt = time.perf_counter() - t0
    b.record(f"find_chunk_by_sha x{M}", dt, "ms")
    print(f"    -> {M / dt:,.0f} 查/秒")

    # 9) 过期清理（2 万条全部过期，新版分批提交 / 旧版单条大事务）
    t0 = time.perf_counter()
    try:
        n = st.purge_expired_chunks(ttl_seconds=0, batch_size=5000)
    except TypeError:  # 旧版签名无 batch_size
        n = st.purge_expired_chunks(0)
    b.record(f"purge_expired_chunks({n} 行)", time.perf_counter() - t0, "ms")

    # 10) 周期维护（optimize + checkpoint，旧版无此方法自动跳过）
    try:
        t0 = time.perf_counter()
        stats = st.maintenance(chunk_ttl_seconds=0)
        b.record("maintenance() 全流程", time.perf_counter() - t0, "ms")
        print(f"    -> {stats}")
    except AttributeError:
        print("  maintenance() 全流程           跳过（旧版无此方法）")

    b.report()
    st.close()


if __name__ == "__main__":
    main()
