# 真实场景测试报告

- 测试时间：2026-09-09 10:39:52
- 单分片人为延迟：`0.15s`（模拟真实 Telegram / 自建代理单分片下载耗时）
- 分片大小：`20MB`
- 下载模式：**单线程串行**（首片边下边发，其余分片主线程依次下载→校验→回写）
- 结论：**17/17 通过**

## 卡顿诊断（核心指标）

| 文件 | 分片数 | 最大分片间隔 | 判定 |
|---|---|---|---|
| /movies/big.bin (85MB) | 5 | 0.000s | 平滑 |
| /movies/big2.bin (160MB) | 8 | 0.000s | 平滑 |

> 单线程串行下，相邻分片「写回完→下一片开始」的间隔应≈0；
> 若出现明显尖峰（>100ms）即说明某分片下载异常阻塞。

## 详细结果

| 用例 | 结果 | 说明 |
|---|---|---|
| put.big.multi_chunk | PASS | status=201 chunks=5 put=4.09s |
| get.big.full(200) | PASS | status=200 |
| get.big.bytes_identical | PASS | len=104857600 expect=104857600 |
| get.big.sha256_identical | PASS |  |
| get.big.max_gap_low | PASS | max_gap=0.0s (卡顿指标,应≈0) |
| get.range.head(206) | PASS | status=206 len=1048576 |
| get.range.mid_cross_chunk(206) | PASS | status=206 len=2097152 |
| get.range.tail(206) | PASS | status=206 len=2097152 |
| get.big2.full(200) | PASS | status=200 len=167772160 |
| get.big2.max_gap_low | PASS | max_gap=0.0s (8分片串行,应≈0) |
| concurrent.diff_files.all_200_and_identical | PASS | errors={} ok=4/4 wall=0.56s |
| concurrent.parallelism_preserved | PASS | wall=0.56s 单文件基准≈0.45s (4路并发墙钟应接近单文件基准,证明服务端层并发未被串行化) |
| concurrent.same_file_ranges.all_206 | PASS | errors={} ok=4/4 |
| client.half_disconnect | PASS | got=52428800 half=52428800 |
| resume.from_half(206_identical) | PASS | status=206 len=52428800 |
| get.empty(200_zero) | PASS | status=200 len=0 |
| integrity.corrupt_chunk_detected | PASS | status=0 len=0 |
