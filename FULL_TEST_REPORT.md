# 全量真实场景测试报告

> 时间：2026-09-10
> 环境：真实 Telegram bot ×5（用户频道）+ 真实代理 ×3 + Docker 容器（最新代码 `main`）
> 目标：尽可能暴露真实缺陷并修复

---

## 一、测试总览

在真实链路（真实 bot、真实代理、真实网络）上跑了 **全部现有套件 + 两个新写的全量/边界专项**，共约 **230 个用例**。

| 测试套件 | 用例数 | 结果 | 说明 |
|---|---|---|---|
| `full_real_test.py`（新） | 76 | **全通过** | 目录/文件大小矩阵/Range/文件名/MOVE·COPY/错误处理/并发/中断/覆盖写 |
| `edge_real_test.py`（新） | 26 | **全通过** | Range 跨分片边界(14例)/断点续传/深层路径/大目录/12路并发/路径遍历安全/OPTIONS |
| `bigfile_test.py`（1GB） | 14 | **全通过** | 1GB 上传下载 + 内存 + Range + seek + 续传 + 并发 |
| `prod_test.py`（60MB） | 17 | **全通过** | 生产级全场景 |
| `download_stop_test.py` | 16 | **全通过** | 下载停止专项（池污染/CL虚高/已写不重复/黑洞） |
| `timeout_log_test.py` | 18 | **全通过** | 修复后（见问题 1） |
| `real_test.py` | 17 | **全通过** | |
| `proxy_failover_test.py` | 7 | **全通过** | 代理故障切换 |
| `selftest.py` | 70/71 | 1 例失败 | **仓库既有、与本次改动无关**（原基线即如此） |
| `dedup` / `dedup_partial` / `playback` / `retry_unit` | — | **全通过** | 去重/播放/上传重试 |
| chunked 上传（无 Content-Length） | 1 | **通过** | `201` + SHA 一致 |
| `Expect: 100-continue` 握手 | 1 | **通过** | 正确返回 `100 Continue` |
| chaos 网络抖动（温和+多候选） | 8 | **全通过** | 见抖动报告 4.4 节 |

**结论：服务端功能面非常稳健，未发现数据正确类缺陷。**

---

## 二、重点验证到的能力

### 文件大小矩阵（全部上传→SHA 校验→HEAD→PROPFIND→Range→删除）
`0B / 1B / 1KB / 1MB / 20MB(分片边界) / 45MB(3分片) / 100MB(5分片)` —— 每个尺寸的
**字节数与 SHA-256 均完全一致**，空文件(0B)也正确（200 + 空 body，无异常）。

### Range 跨分片边界（off-by-one 高发区，14 例全对）
用 45MB/20MB 分片构造：片0=`0-20971519`、片1=`20971520-41943039`、片2=`...`。
验证了「边界前 1 字节」「跨片 0→片1」「跨边界 ±10」「跨整个片1」「跨 3 片(10MB-40MB)」「最后 1 字节」「后缀 `bytes=-1` / `bytes=-20971521`」「整文件」——**内容与 `Content-Range` 全部精确匹配**，无 off-by-one。

### 1GB 大文件（关键指标）
| 指标 | 结果 |
|---|---|
| 上传 | 12.17 MB/s，**内存增量 0MB**（流式，未整包缓冲） |
| 全量下载 | 13.81 MB/s，TTFB 4.9s，**内存增量 0MB** |
| SHA-256 | 一致 |
| Range 首/中/尾 | 全对 |
| 起播 / seek TTFB | 0.348s / 1.081s |
| 断点续传 | 两段各 100MB 拼接正确 |
| 4 路并发 Range | 墙钟 2.30s，全部 206 |

### 安全与健壮
- **路径遍历防护**：`/../../../etc/passwd` 与 URL 编码变体均 `404/403`，**未泄露** `/etc/passwd`。
- **客户端中途断开**后，服务端仍健康（后续 GET 200 + SHA 一致、PROPFIND 207）。
- **12 路并发** GET / Range 全部字节正确。
- **chunked 上传 / Expect:100-continue** 均正常。

---

## 三、发现并修复的问题

### 问题 1：`_do_get` 新增 `force_new` 形参导致旧 mock 崩溃（已修）
给 `_do_get` 加 `force_new`（P3 修复）后，`timeout_log_test.py` 的桩函数签名未同步，抛
`TypeError: fake_do_get() got an unexpected keyword argument 'force_new'`。

修复两处：
- `timeout_log_test.py`：mock 同步 `force_new` 与 `isclosed`。
- `tg.py`：`resp.isclosed()` 改为**安全调用**（`try/except`）。标准 `http.client` 响应都有该方法，但任何非标准响应对象（测试替身/自定义响应）原先会 `AttributeError` 中断下载；现在按「body 已读完」兜底，更健壮。

修复后 `timeout_log_test` **18/18 通过**。

### 问题 2：`warmup_test.py` 硬编码生产 DB 路径，换环境必失败（已修）
`DB_PATH` 写死 `/home/docker/data/tg-webdav/telegram_webdav.db`，在沙箱/其他机器服务起不来、请求全 `404`。
改为 `os.environ.get("WARM_DB", "<原生产路径>")` —— 生产机行为不变，其他环境用 `WARM_DB` 覆盖即可。

> 说明：`speed_test` 依赖 `prod_test` 预置的 `/prodtest/big.bin`，`prod_test` 自给自足（自建目录+上传）。
> 这两个并非缺陷，按「先跑 prod_test 再跑 speed_test」的顺序即可。

---

## 四、性能观察（非数据错误，供优化参考）

1. **seek 到中段偏慢**：`speed_test` 显示 seek 30%/60% 时 TTFB 达 **1.0–1.5s**、吞吐降到 2.8–3.7 MB/s；而 seek 90% 仅 385ms / 8.12 MB/s。
   原因：中段 Range 起点落在分片中间，需先 `getFile` 取 file_path（未命中缓存时是一次代理 RTT）。
   建议：延长 `file_path` 缓存 TTL 或扩大 seek 时的预取窗口。
2. **并发增益递减**：1 路 12.6 → 2 路 23.3 → 4 路 27.3 MB/s，瓶颈在代理/TG 侧而非服务端（服务端未被串行化）。

---

## 五、结论

- 经过 ~230 个真实用例（含 1GB 大文件、跨分片边界、并发、安全、中断、chunked、抖动）验证，
  **未发现数据正确性缺陷**；修复了 2 个问题（1 个是我此前修复引入的测试不兼容 + 健壮性加固，1 个测试可移植性）。
- 服务端 **流式处理彻底**（1GB 文件内存增量 0MB），分片拼接、Range 语义、代理自愈均正确。
- 遗留观察是 seek 中段 TTFB 偏高（性能优化项，非 bug）。

---

## 六、最终回归（代码冻结后重跑，全部真实环境）

镜像用**当前源码重新构建**（`docker build -t totootao/telegram-webdav:latest .`），
容器以生产配置启动（5 bot 池 + 3 代理候选 + 并发 5/5），跑完即删：

| 套件 | 结果 | 关键结论 |
|---|---|---|
| `download_stop_test.py` | **16/16** | 下载停止专项（离线单元） |
| `timeout_log_test.py` | **18/18** | 超时/重试日志与语义 |
| `retry_unit_test.py` | 通过 | 分片独立重试不拖垮整体 |
| `proxy_failover_test.py` | **7/7** | 代理候选生成与故障切换 |
| `dedup_test.py` | 通过 | 100MB 重传：27.13s → **0.12s**（232.8x 去重生效），SHA 一致 |
| `dedup_partial_test.py` | 通过 | 5 片删 2 片去重记录 → 只补传 2 片（10.80s vs 27.13s），SHA 一致 |
| `full_real_test.py` | **76/76** | 全量真实场景 |
| `edge_real_test.py` | **26/26** | 边界/并发/安全（含 12 路并发、路径遍历拦截） |

补充观察：
- 全程服务端日志 **无 ERROR / Traceback**；容器内存峰值 **215.8MiB**（跑过 100MB×2 上传 + 12 路并发）。
- 测试产生的真实数据（`/fulltest` `/edgetest` `/deduptest`）已全部 `DELETE` 返回 204 清理干净，容器已停止。

> 至此「全量真实场景测试 + 修复」闭环完成：代码已提交并推送到 `main`（`0d59d7d..9220756` 及之后提交）。
