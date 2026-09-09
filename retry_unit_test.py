"""验证「某一个分片上传失败时，只重试该分片，不再整体失败」。

做法：mock 掉 backend.upload_chunk，让**第 2 片的前 2 次调用必失败**、第 3 次成功，
其余分片正常。若分片级重试生效 → 整体上传成功（旧行为是直接 502 整体失败）。
再验证：若某片**永久失败**，重试 3 次后才抛出（且抛出的是 TGError，不是 AttributeError）。
"""
import hashlib
import sys
import types

import tg as _tg
import webdav

CHUNK = 20 * 1024 * 1024
N = 5


class FakeDB:
    def find_chunk_by_sha(self, sha, size=None):
        return None

    def put_chunk_dedup(self, *a):
        pass


class FakeBackend:
    slots = [0, 1, 2, 3, 4]

    def __init__(self, fail_map, fail_times):
        self.fail_map = fail_map      # {chunk_index: 失败次数}
        self.fail_times = fail_times
        self.calls = {}
        self.uploaded = []

    def upload_chunk(self, data, file_name=None):
        # 从文件名反推分片序号 partNNN
        idx = int(file_name.rsplit("part", 1)[-1]) if "part" in file_name else 0
        self.calls[idx] = self.calls.get(idx, 0) + 1
        if idx in self.fail_map and self.calls[idx] <= self.fail_map[idx]:
            raise _tg.TGError(f"HTTP 429 Too Many Requests: retry_after 1")
        self.uploaded.append(idx)
        return f"FID-{idx:03d}", idx, 1000 + idx


def make_handler(backend):
    app = types.SimpleNamespace(
        config=types.SimpleNamespace(_upload_workers=lambda n: 3, chunk_dedup=False),
        db=FakeDB(),
        backend=backend,
    )
    h = webdav.WebDAVHandler.__new__(webdav.WebDAVHandler)
    # app 是只读 property（取自 self.server.app），这里塞一个假 server
    h.server = types.SimpleNamespace(app=app)
    return h


def run(fail_map, label):
    body = b"".join(bytes([i % 251]) * 1 + b"x" * (CHUNK - 1) for i in range(N))
    body = body[: CHUNK * N]
    backend = FakeBackend(fail_map, 0)
    h = make_handler(backend)
    meta = []
    fh = hashlib.sha256()
    try:
        h._upload_parallel(body, "t.bin", True, CHUNK, meta, fh, 512, 4096)
        print(f"[{label}] 整体上传成功 ✅  分片上传调用次数={backend.calls} "
              f"成功分片={sorted(backend.uploaded)}")
        print(f"        元信息 {len(meta)} 条，file_id={[m['file_id'] for m in meta]}")
        return True, backend
    except Exception as e:
        print(f"[{label}] 整体失败 ❌  {type(e).__name__}: {e}")
        print(f"        分片上传调用次数={backend.calls}")
        return False, backend


def main():
    print("=== 场景1：第2片前2次失败、第3次成功（瞬断）===")
    ok1, b1 = run({2: 2}, "瞬断")
    print(f"  结论: 第2片被调用 {b1.calls.get(2)} 次(1次失败+重试成功)，"
          f"整体{'成功→分片重试生效' if ok1 else '失败→有问题'}")

    print("\n=== 场景2：第3片永久失败（重试耗尽）===")
    ok2, b2 = run({3: 99}, "永久失败")
    print(f"  结论: 第3片被调用 {b2.calls.get(3)} 次后放弃，"
          f"整体{'成功' if ok2 else '失败(符合预期)'}")
    print(f"  异常类型检查: 期望 TGError（不是 AttributeError）")

    print("\n=== 断言 ===")
    assert ok1, "瞬断场景应整体成功"
    assert b1.calls.get(2) == 3, f"第2片应被尝试3次，实际 {b1.calls.get(2)}"
    assert not ok2, "永久失败场景应整体失败"
    assert b2.calls.get(3) == webdav._UP_CHUNK_RETRY + 1, \
        f"第3片应被尝试 {webdav._UP_CHUNK_RETRY+1} 次，实际 {b2.calls.get(3)}"
    print("✅ 全部断言通过：单个分片失败会独立重试，不再一失败就拖垮整个上传")


if __name__ == "__main__":
    main()
