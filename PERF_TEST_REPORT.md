# telegram-webdav 真实场景压测与内存/CPU 优化报告

- 被测仓库：https://github.com/totootao/telegram-webdav （commit `ed74f88`）
- 测试时间：2026-09-10
- 测试方式：**真实 Telegram 环境**（5 个真实 bot + 3 个真实 CF Pages 代理，即用生产 `docker run` 那套参数），全链路 HTTP 请求，非 mock
- 测试机：Ubuntu 22.04 / 32 核 / 123GB 内存（内存充足，因此本报告关注的是**进程自身的内存放大倍数**，不是"能不能跑起来"）

---

## 一、结论速览

| 指标（真实场景） | 优化前 | 优化后 | 变化 |
|---|---|---|---|
| 150MB 新文件上传耗时 | **246.7 s**（0.64 MB/s） | **44.0 s**（3.58 MB/s） | **-82%** |
| 150MB 上传内存峰值 | **758 MB** | **327 MB** | **-57%** |
| 150MB 上传完成后驻留 RSS | 378 MB（不回落） | **142 MB** | -62% |
| 8 客户端并发（8×8MB）内存峰值 | 378 MB | **39 MB** | **-90%** |
| 30 次连续小文件上传后 RSS | 持续增长 | **36.5 MB（零增长）** | 无泄漏 |
| 单分片挂起最坏耗时 | 166 s / 246 s（无任何干预） | **60 s 掐断 → 换代理 → 7.6 s 成功** | 长尾从 4 分钟压到 1 分钟内 |
| 空闲 RSS / 线程数 | 35 MB / 2 | **31.8 MB / 2** | — |
| 空闲 CPU 占用率 | 约 0.3% | **0.23%**（30 s 仅 0.07 CPU 秒） | 已极低，仅做空转降频 |

> CPU 不是本项目的问题：全程压测累计 CPU 约 3 秒 / 15 分钟，属纯 IO 服务正常水平。
> **真正的痛点在内存放大和上传长尾**，本报告的全部优化都落在实处。

---

## 二、真实场景测试结果

### 2.1 功能回归（优化后全部通过）

| # | 场景 | 结果 |
|---|---|---|
| 1 | PUT 100KB 小文件 + GET 回读 | 201 / md5 一致 ✅ |
| 2 | PUT 50MB（3 分片）+ GET 回读 | md5 一致 ✅ |
| 3 | PUT 150MB（8 分片）+ GET 回读 | md5 一致 ✅ |
| 4 | Range 请求（视频 seek 模拟，取中段 1MB） | 206 + 字节比对一致 ✅ |
| 5 | 空文件 PUT/GET | 201 / 200 0B ✅ |
| 6 | 中文 + 特殊字符文件名 | 201 ✅ |
| 7 | MKCOL / DELETE / MOVE / PROPFIND Depth:1 | 201/204/201/207 ✅ |
| 8 | 错误密码鉴权 | 401 ✅ |
| 9 | 分片去重（同内容重复上传 150MB） | **0.295 s 秒传** ✅ |
| 10 | 超时重传片的数据一致性 | md5 一致 ✅ |

### 2.2 发现的问题（按严重度）

**P0-1 上传分片被代理挂起时无任何时间上限（最严重）**
- 现象：8 客户端并发时 7 个 4~11 s 完成，第 8 个耗时 **166.9 s**；150MB 文件（8 分片）中单片卡 **246 s**，其余 7 片 5~15 s 完成 → 整个上传从 ~15 s 恶化到 247 s。
- 证据（服务日志，同一文件 8 个分片）：
  ```
  22:34:35 sendDocument 开始 part000..part004
  22:34:40~22:34:50  part001/003/000/002/006/005/007 全部成功
  22:38:41 sendDocument 成功 part004   ← 卡了 246 s
  ```
- 根因：`urlopen(timeout=240)` 的 socket 超时只覆盖"单次读/写间隔"，挡不住**完全无响应**的挂起；且挂起后同 bot 的后续请求被队头阻塞（日志中 `message_id` 连续：31499 → 31500，前一条不返回，后一条永远排队）。挂起期间应用层零干预。
- 已修：新增 `TG_HTTP_TIMEOUT`（默认 60 s），超时立即换代理/换 bot 重传。实测生效：
  ```
  22:46:46 sendDocument 开始 p8.bin 代理=otterhub
  22:47:47 sendDocument 挂起超时(60s) → 换代理/换bot重传
  22:47:47 sendDocument 开始 p8.bin 代理=tg-proxy-b4t
  22:47:55 sendDocument 成功（7.60s）
  ```

**P0-2 multipart 构造把每个 20MB 分片放大成 3 份内存**
- 现象：150MB（8 分片）上传内存峰值 **758 MB**，而代码注释宣称"≈在飞分片数 × 分片大小 ≈ 200MB"。
- 根因：`_build_multipart` 用 `body += crlf + data + crlf`。bytes 不可变，`crlf+data+crlf` 会先复制一份 20MB 临时对象，`+=` 再复制一份 → 每片瞬时 `data(20MB) + 临时(20MB) + 结果(20MB)`，8 片并发 ≈ 480 MB，与实测 758 MB 吻合。
- 已修：改为 parts 列表 + 一次性 `b"".join`（已验证输出字节与旧版完全一致，含 boundary/CRLF 格式）。

**P1-3 流式上传切片多一次全量拷贝**
- `piece = bytes(buf[:chunk_size])`：`buf[:cs]` 复制一次 bytearray，`bytes()` 再复制一次。改为 `bytes(memoryview(buf)[:cs])`，每片省一次 20MB 拷贝。

**P1-4 在飞分片上限偏大**
- `max_inflight = workers*2`（5 并发 = 10 片 = 200 MB 纯数据缓冲）。改为 `workers + 2`（默认 7），并支持 `TG_UPLOAD_INFLIGHT` 覆盖。
- 实测权衡：`TG_UPLOAD_INFLIGHT=4` 时峰值 301 MB（仅再降 26 MB）但吞吐掉 42%（76 s vs 44 s），**默认 7 是更好的平衡点**；内存极度紧张的小 VPS 才建议调到 4。

**P1-5 内存只涨不回落（监控容易误判为泄漏）**
- 20MB 分片由 malloc 直接分配，free 后 glibc 未必归还 OS。实测并发后 RSS 常驻 378 MB 不回落。
- 已修：每请求收尾调用 glibc `malloc_trim(0)`（非 Linux/glibc 静默 no-op）。
- 踩坑记录：最初只在大请求（>32MB）后 trim，结果并发 8 个 8MB 请求单个都不达阈值 → RSS 卡在 293 MB；改为**每请求无条件 trim** 后，同场景回落到 35.8 MB。

**P2-6 预热线程空闲期空转**
- `_warm_loop` 空闲时仍每 2 s 醒一次判断。已改为无活动 5 分钟后 sleep 60 s，唤醒频率从 0.5 次/秒降到 1 次/分钟（CPU 收益小，主要减少无谓唤醒）。

**P2-7（环境问题，非代码缺陷）某个 bot 反复挂起**
- slot=1 的 bot（`8981700038` / chat `-1001929321614`）在两次独立压测中都是挂起的那一片（166 s、246 s、68 s）。
- 但用 curl 直连该 bot 发同一个 20MB 文件只要 **9.99 s** → bot 本身正常，是链路（CF 代理 → Telegram）侧对"大 body POST"的排队/风控。
- 建议：观察该 bot；若持续异常，从 `TG_BOT_POOLS` 中替换，或把 5 个 bot 降为 4 个（少一个不稳定因子）。

**P2-8（澄清，不是 bug）下载速度慢是代理带宽，不是代码**
- 下载 50MB 实测 2.8 MB/s，看似慢。但用 curl **直连同一个代理**下同一分片只有 **0.94 MB/s**，经本服务反而更快（3.7 MB/s，连接池复用 + 预热生效）。
- 结论：瓶颈是 CF Pages 代理的单连接跨境速率，与仓库既有 `PROXY_LB_REPORT.md` 的结论一致（单代理 12 路 66~73 MB/s 未饱和，**瓶颈在单连接速率而非代理带宽**）。不要为此改代码。
- 补充发现：3 个代理是**主备模式**（正常永远走候选 0），所有上传流量集中在一个 CF Pages 实例上——这是大并发 POST 排队的长尾诱因之一。若要分散，建议按 `PROXY_LB_REPORT.md` 的 P0 建议做"延迟感知主备"，而不是均分轮询。

**P2-9 DELETE 不回收 Telegram 空间（空间只增不减）**
- `do_DELETE` 只删 SQLite 元数据，代码注释写"Telegram 不支持删除已发消息"。实际上 **Bot API 有 `deleteMessage`**，bot 可以删除自己发出的消息（频道消息无 48 小时限制）。
- 后果：每次覆盖写、删除、以及**超时重传后被 TG 实际接收的孤儿分片**，都会永久占用频道空间。长期运行（尤其反复覆盖同名大文件）会给频道留下一堆取不回也删不掉的分片。
- 建议（未实施，需决策）：删除/覆盖时按 `message_id` 调 `deleteMessage` 回收；孤儿分片可在 dedup 表中记录并按 TTL 清理。

**⚠️ 本次压测在真实频道留下的分片需要手动清理**
- 压测用真实 bot 上传了约 **1.2 GB** 测试数据（多个 150MB 文件 + 80 余个小文件）。WebDAV 侧元数据已全部 DELETE，**但 Telegram 频道里的分片消息仍在**（见 P2-9）。
- 清理方式：在 Telegram 客户端里按文件名搜索 `.bin` / `part00` 批量删除，或等 bot 侧实现 `deleteMessage` 回收。

---

## 三、改动清单（4 个文件）

| 文件 | 改动 |
|---|---|
| `tg.py` | ① `_build_multipart` 改 join，消除每片 20MB 的重复拷贝；② 新增 `_HTTP_TIMEOUT`（`TG_HTTP_TIMEOUT`，默认 60s）与 `_is_timeout_err()`，超时不当作普通网络错误重试同代理，而是立即换代理/换 bot；③ 超时切换打日志；④ warm loop 空闲降频 |
| `webdav.py` | ① `_upload_streaming` / `_upload_parallel` 用 memoryview 单次拷贝；② 在飞上限 `workers*2` → `workers+2` 且支持 `TG_UPLOAD_INFLIGHT`；③ 每请求收尾 `malloc_trim`（`_maybe_trim`，非 glibc 平台自动 no-op）；④ 补 `import os`（新增代码用到） |
| `.env.example` | 新增 `TG_UPLOAD_INFLIGHT` / `TG_HTTP_TIMEOUT` 说明 |
| 本报告 | `PERF_TEST_REPORT.md` |

**兼容性**：未改任何对外行为、协议语义与默认值以外的配置。两处新环境变量都有安全默认值；`TG_HTTP_TIMEOUT` 只对"完全挂起"生效，慢速但持续出字节的链路不受影响。

---

## 四、复现方法

```bash
# 1) 起服务（生产参数，本地跑，等价于 docker run 内的 python run.py）
HOST=127.0.0.1 PORT=8080 DB_PATH=/tmp/tg.db \
DAV_USER=totootao DAV_PASSWORD='Hhangxing963.' \
TG_PROXY_POOLS='...' TG_BOT_POOLS='...' \
TG_UPLOAD_CONCURRENCY=5 TG_DOWNLOAD_CONCURRENCY=5 \
python3.11 run.py

# 2) 压测
head -c 157286400 /dev/urandom > t.bin
curl -u user:pass -X PUT --data-binary @t.bin http://127.0.0.1:8080/t.bin
# 并发 8 客户端
for i in $(seq 1 8); do curl -u user:pass -X PUT --data-binary @c$i.bin http://127.0.0.1:8080/c$i.bin & done; wait
# 3) 监控（每秒采样 RSS/CPU/线程/句柄）
```

## 五、遗留建议（未实施，供决策）

1. **极限内存场景**：若容器只有 256~512MB，设 `TG_UPLOAD_INFLIGHT=3~4` + `CHUNK_SIZE_MB=10`（分片减半，内存同比减半），代价是吞吐下降。
2. **彻底消灭 multipart 拷贝**：改用 `http.client` 直接分块写请求体（headers + data + tail 流式发），每片内存可从 ~40MB 降到 ~20MB。已验证 urllib 对 file-like body 支持不可靠（会走 chunked 编码导致失败），故本次未改，风险与收益不匹配。
3. **延迟感知主备**：给代理候选加 EWMA 吞吐 + 健康摘除，解决"半死不活代理"——这是 `PROXY_LB_REPORT.md` 已列 P0，本次未实施。
