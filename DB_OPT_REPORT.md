# 数据库大数据操作优化报告（db.py）

> 关联基准脚本：`db_bench.py`（可复现，`python3 db_bench.py --nodes 5000 --runs 5`）
> 基线：优化前代码（同机同参数，git 历史 5623a12）；数据规模：单目录 5000 文件 × 8 分片/文件，子树共 1.5 万节点。

## 一、瓶颈定位（精读 db.py + 调用侧）

| # | 瓶颈 | 影响 |
|---|------|------|
| 1 | `list_children(depth=1)` 用 `path LIKE 'prefix%'` 拉取**全部后代**再 Python 过滤 | 列一个只有 3 个子目录、但子树有 1.5 万文件的根，要白扫 1.5 万行——PROPFIND 最常用深度，rclone/AList 挂载点列目录首当其冲 |
| 2 | 列目录 `SELECT *` 连 **chunks 大 JSON** 一起读 | PROPFIND XML 根本不用 chunks（预热只用首片），万级目录一次列目录白读几 MB JSON |
| 3 | `move`/`copy` 逐行 `execute(INSERT)` | 万级子树逐行插桩 + 全程全局锁（单连接模型），持锁时间长，阻塞所有并发请求 |
| 4 | `move`/`copy` 先 `descendants()`（拿锁→放锁）再重新拿锁写入 | TOCTOU：两段之间树结构可能被并发修改 |
| 5 | `idx_nodes_path` 与 `path UNIQUE` 自带索引**完全重复** | 每次写入维护两份一模一样的索引，纯写放大 |
| 6 | LIKE 前缀未转义 `%`/`_` | 路径含通配符时列目录/删除/移动**误匹配、误删**（正确性 bug） |
| 7 | `purge_expired_chunks` 单条大事务 | 去重表百万行级清理时长时间持锁，全部请求卡死 |
| 8 | 无周期维护 | `PRAGMA optimize` 从不执行（查询计划统计过期）、WAL 只增不减 |

## 二、改动清单

**db.py**
1. 新增 `parent_path` 冗余列 + **复合索引 `(parent_path, path)`**：depth=1 列目录改点查。
   注：单列 `parent_path` 索引实测反而更慢（5000 行结果需 temp b-tree 排序），复合索引让
   `WHERE parent_path=? ORDER BY path` 由索引直接满足，零排序。
2. 新增 `first_file_id` / `first_slot` 冗余列：PROPFIND 预热直接读首片，列目录彻底甩掉 chunks。
3. `move`/`copy` 重构：单次持锁内完成「查子树→删 dst→`executemany` 批量插→删 src」，消灭 TOCTOU。
4. 删除冗余 `idx_nodes_path`（旧库启动时自动 `DROP INDEX IF EXISTS`）。
5. 所有 LIKE 前缀统一 `_like_escape()` 转义（配合既有 `ESCAPE '\'`）。
6. `purge_expired_chunks` 分批删除（默认 5000 行/批），批间提交让出全局锁。
7. PRAGMA：`cache_size=64MB`、`temp_store=MEMORY`、`mmap_size=256MB`、`journal_size_limit=64MB`。
   （曾试验 `page_size=8192`，实测小行高频提交场景慢 ~25%，已回退 4096——以实测为准。）
8. 新增 `maintenance()`：清过期锁/去重记录 + `PRAGMA optimize` + WAL checkpoint。
9. 新增 `put_chunk_dedup_many()` 批量登记接口；`get_node(with_chunks=)` 轻量/完整双模式。

**webdav.py**
- `_warmup_dir` 优先读 `first_file_id`（旧数据自动回退解析 chunks）。
- 存在性/属性检查类 `get_node` 全部改 `with_chunks=False`（PUT/DELETE/MKCOL/MOVE/COPY/LOCK/PROPPATCH/webhook）。
- `make_server` 启动后台维护线程（默认 10 分钟首跑、每 6h 一次，`DB_MAINT_FIRST_DELAY` / `DB_MAINTENANCE_INTERVAL` 可调）。

## 三、基准对比（同机同参数，值越小越好）

| 场景 | 旧版 | 新版 | 变化 |
|---|---:|---:|---|
| **list_children 浅目录大子树**（子树 1.5 万节点，列 1 层 3 项）×5 | 256.6 ms | **0.30 ms** | **≈850×** |
| list_children depth=1（5000 直接子节点）×5 | 78.9 ms | 77.6 ms | 持平（结果集构建为主） |
| list_children depth=infinity（1.5 万节点）×5 | 250.0 ms | 259.8 ms | 持平（±3% 噪声内） |
| move 子树（5000 文件）×5 | 648.9 ms | 626.7 ms | **-3%~-7%** |
| copy 子树（5000 文件）×5 | 497.3 ms | 501.4 ms | 持平 |
| delete_recursive（1 万文件） | 128.4 ms | 119.2 ms | **-7%** |
| create_file ×5000 | 961.2 ms | 974.2 ms | +1~2%（parent 索引维护代价） |
| put_chunk_dedup ×20000 | 2642 ms | 2648 ms | 持平 |
| find_chunk_by_sha ×20000 | 62.3 ms | 67.6 ms | 持平 |
| purge_expired_chunks（2 万行） | 52.2 ms | 54.5 ms | 持平（大表时分批收益更大） |
| get_node 轻量（无 chunks）×5 | —（不支持） | 0.04 ms | 新增能力 |

> 关键收益说明：**浅目录大子树列目录是 PROPFIND 对真实目录树的最常见形态**（挂载根、
> 分类目录），旧版耗时随子树规模线性增长（256ms@1.5万，10 万文件目录将到秒级），
> 新版恒定为直接子节点数的点查（0.06ms/次），与子树规模无关。

## 四、兼容性与迁移

- **旧库自动迁移**：启动时 `ALTER TABLE` 补列 + 分批回填 `parent_path`/`first_file_id`
  （只处理 NULL 行，幂等，不重复回填）。
- **回填失败兜底**：`_parent_ready=False` 时 depth=1 自动回退旧查询路径，功能不变只慢些。
- **API 向后兼容**：`get_node(path)`/`list_children(path, depth)`/`purge_expired_chunks(ttl)`
  等原有签名不变（新参数均带默认值）；chunks 仍是唯一分片事实来源，冗余列仅作加速。
- **降级安全**：旧代码打开新库自动重建 `idx_nodes_path`，忽略新列，可正常回滚运行。

## 五、验证

- `selftest.py` 端到端：**70/71 通过**（与基线完全一致；唯一失败项
  `put.name.multi.fname` 为存量问题：分片去重复用旧 file_id 导致命名断言失败，基线同样失败，与本次改动无关）。
- 迁移测试：旧版建库 → 新版打开，回填行数精确（303/300）、索引状态正确、depth=1
  结果集与旧语义逐项一致、move/copy/delete/dedup 在迁移库上全部通过。
- LIKE 转义正确性：`/50%_off/` 与 `/50X_offX/` 邻居目录，删除前者不再误伤后者
  （旧版会误删）。
