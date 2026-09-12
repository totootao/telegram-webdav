"""SQLite 元数据层。

在 Telegram 只存"文件字节"（按 20MB 分片，每片一个 file_id），而把"文件树 / 分片索引"
存在本地 SQLite 里——这正是 otterhub-server 用 Cloudflare KV 干的那件事，这里换成
单机可移植的 SQLite。

nodes 表即一个虚拟文件系统：
  - 目录：is_dir=1，size=0，chunks=NULL
  - 文件：is_dir=0，size=字节数，chunks=JSON 数组
        [{ "file_id": "...", "slot": 0, "size": 20971520, "message_id": 123 }, ...]
    slot 记录上传该分片所用的 bot 序号（file_id 与 bot 绑定，下载时须用同 bot token）。

并发模型（与多连接 WAL 模型对比，刻意改用单连接）：
  HTTP/1.1 多线程服务器下，每条请求在独立线程。早期版本每条请求开/关一个 SQLite 连接
  走 WAL，但在某些环境/SQLite 构建里会出现写事务长时间拿不到写锁、commit 被活锁拖死
  （busy_timeout 也不生效）的现象。这里改为：**整个进程共享一条连接**
  （`check_same_thread=False`），所有读写都包在同一把 `threading.Lock` 下串行化。
  这样既消除了多连接之间的锁互相等待，又把锁的语义收拢到 Python 层，行为确定、可预期。

大数据操作优化（db_bench.py 可复现）：
  1. parent_path 冗余列 + 索引：depth=1 列目录（PROPFIND 最常用深度）从
     「LIKE 前缀拉全子树再 Python 过滤」改为 parent_path 点查，万级目录列一层
     只读直接子节点，不再拖动全部后代；
  2. first_file_id / first_slot 冗余列：PROPFIND 后的 file_path 预热只取首片，
     列表查询彻底甩掉 chunks 大 JSON 字段（SELECT * 会把每个文件的全部分片元数据
     都读出来，万级目录一次 PROPFIND 白读几十 MB JSON）；
  3. move / copy 批量化：逐行 INSERT 改为单事务 executemany，万级子树移动/复制
     的 Python 侧开销大幅下降，同时全局锁持有时间同步缩短（所有请求共享一把锁）；
     并把「先查询后代再重新拿锁写入」的 TOCTOU 窗口合并进同一次持锁；
  4. 删除冗余索引 idx_nodes_path：path 列 UNIQUE 约束自带唯一索引，原先每次写入
     要维护两份完全相同的索引，纯写放大；
  5. LIKE 通配符转义：路径含 % / _ 时前缀匹配会误伤/漏删（正确性修复），
     delete_recursive / move / copy / 子树查询统一走 _like_escape()；
  6. PRAGMA 调优：cache_size=64MB、temp_store=MEMORY、mmap_size=256MB、
     journal_size_limit=64MB（防 WAL 无限膨胀）。
     注：曾试验新库 page_size=8192（chunks JSON 大行理论友好），实测小行高频提交
     （chunk_dedup 逐片落库）反而慢 ~25%，已回退默认 4096——以实测为准；
  7. 过期清理分批提交：chunk_dedup 表百万行级清理时每批 5000 行提交一次，
     避免单条大事务长时间持锁阻塞全部请求；
  8. maintenance() 周期维护：清理过期锁/去重记录 + PRAGMA optimize（刷新查询计划
     统计）+ WAL checkpoint，由 webdav 层的后台线程周期调用。

以上列均带自动迁移：旧库启动时自动 ALTER TABLE 并回填（只处理 NULL 行，一次性），
回填失败则自动回退到旧查询路径（_parent_ready=False），不影响可用性。
"""

import datetime
import json
import os
import sqlite3
import threading
import time
import uuid


def _log(msg):
    """统一的后台日志：带本地时间戳 + [db] 模块前缀。

    聚焦「数据库层本身」的异常：连接失败（权限/磁盘满/文件损坏）、
    schema 初始化失败、写事务被锁死（busy）等，便于把问题从 WebDAV 层分离出来。
    """
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}][db] {msg}", flush=True)


def _parent_of(path):
    """由完整路径求父目录路径：'/a/b/c' -> '/a/b'，'/a' -> '/'，'/' -> ''（根无父）。"""
    if not path or path == "/":
        return ""
    p = path.rsplit("/", 1)[0]
    return p if p else "/"


def _like_escape(s):
    """LIKE 前缀的通配符转义（配合 ESCAPE '\\'）：路径含 % / _ 时不再误匹配。

    例：路径 '/50%_off/x' 若不转义，LIKE '/50%_off/x%' 会把 '/50X_off/...' 也匹配上。
    """
    return s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


# 列清单：轻量版（不含 chunks 大 JSON）。列目录/存在性检查等场景用轻量版，
# 一次 PROPFIND 不再为每个文件读出全部分片元数据。
_NODE_LIGHT_COLS = (
    "id, path, name, is_dir, size, content_type, etag, mtime, ctime, "
    "chunk_size, file_hash, duration, parent_path, first_file_id, first_slot"
)
# 完整版：下载（GET/HEAD 需要分片列表）与 move/copy（需原样复制 chunks）时使用。
_NODE_FULL_COLS = _NODE_LIGHT_COLS + ", chunks"

# 过期清理的分批大小：每批一个事务提交一次，批间让出全局锁给其它请求。
_PURGE_BATCH = 5000


class MetaStore:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        # parent_path 回填是否成功：False 时 depth=1 列目录回退旧查询路径
        self._parent_ready = False
        d = os.path.dirname(os.path.abspath(path))
        if d and not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        try:
            self._conn = self._open()
        except Exception as e:
            _log(f"SQLite 连接失败(致命): path={path} 异常={type(e).__name__}: {e}")
            raise
        self._init_db()

    # ---------- 连接（单例，线程安全由 self._lock 保证） ----------
    def _open(self):
        try:
            conn = sqlite3.connect(self.path, timeout=60, check_same_thread=False)
        except Exception as e:
            _log(f"无法打开 SQLite 文件: path={self.path} 异常={type(e).__name__}: {e}")
            raise
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA busy_timeout=60000")
            conn.execute("PRAGMA synchronous=NORMAL")
            # WAL 文件上限 64MB：海量写入后 checkpoint 回收，防止 WAL 无限膨胀占满磁盘
            conn.execute("PRAGMA journal_size_limit=67108864")
            # 页缓存 64MB（负数=KB）：大数据操作下减少重复读盘
            conn.execute("PRAGMA cache_size=-65536")
            # 临时排序/中间结果放内存：depth=infinity 的 ORDER BY path 不再落盘
            conn.execute("PRAGMA temp_store=MEMORY")
            # mmap 读取 256MB：热数据绕过 read() 系统调用与页缓存拷贝（失败无碍，忽略）
            conn.execute("PRAGMA mmap_size=268435456")
        except Exception as e:
            _log(f"设置 PRAGMA 失败(忽略): {type(e).__name__}: {e}")
        _log(f"SQLite 已连接: path={self.path}")
        return conn

    def close(self):
        try:
            # 关闭前刷新查询计划统计（PRAGMA optimize：SQLite 官方推荐的收尾动作）
            try:
                self._conn.execute("PRAGMA optimize")
            except Exception:
                pass
            self._conn.close()
        except Exception:
            pass

    def _init_db(self):
        with self._lock:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    path        TEXT NOT NULL UNIQUE,
                    name        TEXT NOT NULL,
                    is_dir      INTEGER NOT NULL DEFAULT 0,
                    size        INTEGER NOT NULL DEFAULT 0,
                    content_type TEXT,
                    etag        TEXT,
                    mtime       INTEGER NOT NULL,
                    ctime       INTEGER NOT NULL,
                    chunk_size  INTEGER,
                    chunks      TEXT
                )
                """
            )
            # path 列 UNIQUE 约束自带唯一索引，原先手动建的 idx_nodes_path 与其
            # 完全重复——每次 INSERT/DELETE 都要白维护一份，删除之（旧库也顺带清理）。
            self._conn.execute("DROP INDEX IF EXISTS idx_nodes_path")
            # 迁移：为旧库补列（SQLite 不支持 ADD COLUMN IF NOT EXISTS，先探测）。
            cols = {r[1] for r in self._conn.execute("PRAGMA table_info(nodes)").fetchall()}
            if "file_hash" not in cols:
                self._conn.execute("ALTER TABLE nodes ADD COLUMN file_hash TEXT")
            if "duration" not in cols:
                self._conn.execute("ALTER TABLE nodes ADD COLUMN duration REAL")
            if "parent_path" not in cols:
                self._conn.execute("ALTER TABLE nodes ADD COLUMN parent_path TEXT")
            if "first_file_id" not in cols:
                self._conn.execute("ALTER TABLE nodes ADD COLUMN first_file_id TEXT")
            if "first_slot" not in cols:
                self._conn.execute("ALTER TABLE nodes ADD COLUMN first_slot INTEGER")
            # 父目录复合索引：(parent_path, path)——depth=1 列目录的点查 + ORDER BY path
            # 全部由索引直接满足（点查 + 有序扫描，零临时排序）。单列 parent_path 索引
            # 实测反而比旧 LIKE 查询慢（5000 行结果需 temp b-tree 排序，排序吞掉点查收益）。
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_nodes_parent ON nodes(parent_path, path)"
            )
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS locks (
                    token   TEXT PRIMARY KEY,
                    path    TEXT NOT NULL,
                    owner   TEXT,
                    depth   TEXT,
                    expiry  INTEGER
                )
                """
            )
            # 过期锁清理按 expiry 扫描，小表也顺手建索引（写入成本可忽略）
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_locks_expiry ON locks(expiry)"
            )
            # 分片去重表（内容寻址）：按分片的 SHA-256 记录「已上传到 Telegram 的分片」。
            #
            # 为什么需要它：大文件（如 1.2GB=60 片）上传时，只要有一个分片最终失败，
            # 服务端就返回 502，标准 WebDAV 客户端只能**重传整个文件**——那 59 片已成功
            # 上传的字节就白传了，还会在频道里留下一堆孤儿消息。
            # 有了这张表，客户端重传同一文件时，服务端按分片 SHA 查到「这片传过了」，
            # 直接复用原 file_id 跳过上传，**只真正补传失败的那几片**——客户端行为不变，
            # 实际上传量从 1.2GB 降到几十 MB，且不再产生重复消息。
            #
            # 注意：file_id 与 bot 绑定，故必须连 slot 一起记录；分片内容相同即可复用，
            # 因此这是**跨文件**的去重（不同文件中的相同数据块也能省一次上传）。
            # 仅适用于自有/可信频道场景（复用依赖内容哈希可查）。
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS chunk_dedup (
                    sha        TEXT PRIMARY KEY,
                    file_id    TEXT NOT NULL,
                    slot       INTEGER NOT NULL,
                    message_id INTEGER,
                    size       INTEGER NOT NULL,
                    created    REAL NOT NULL
                )
                """
            )
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_chunk_dedup_created ON chunk_dedup(created)"
            )
            now = int(time.time())
            self._conn.execute(
                "INSERT OR IGNORE INTO nodes(path,name,is_dir,size,mtime,ctime,etag) "
                "VALUES('/','',1,0,?,?,?)",
                (now, now, '"root"'),
            )
            self._conn.commit()
        # 数据回填放锁外（内部自带锁），并决定 depth=1 是否走 parent_path 快路径
        self._parent_ready = self._backfill_derived_cols()

    def _backfill_derived_cols(self):
        """一次性回填 parent_path / first_file_id（只处理 NULL 行，幂等）。

        返回 True 表示回填完成，depth=1 列目录可走 parent_path 点查快路径；
        失败则返回 False，list_children 自动回退旧查询路径（功能不变，只是慢些）。
        """
        try:
            n_pp = n_ff = 0
            while True:  # 分批处理，避免超大库启动时一次性占内存过多
                rows = self._conn.execute(
                    "SELECT id, path FROM nodes WHERE parent_path IS NULL LIMIT ?",
                    (_PURGE_BATCH,),
                ).fetchall()
                if not rows:
                    break
                self._conn.executemany(
                    "UPDATE nodes SET parent_path=? WHERE id=?",
                    [(_parent_of(r[1]), r[0]) for r in rows],
                )
                n_pp += len(rows)
            while True:
                rows = self._conn.execute(
                    "SELECT id, chunks FROM nodes WHERE first_file_id IS NULL "
                    "AND is_dir=0 AND chunks IS NOT NULL LIMIT ?",
                    (_PURGE_BATCH,),
                ).fetchall()
                if not rows:
                    break
                ups = []
                for r in rows:
                    fid, slot = "", 0
                    try:
                        cs = json.loads(r[1]) if r[1] else []
                        if cs and isinstance(cs[0], dict):
                            fid = cs[0].get("file_id") or ""
                            slot = int(cs[0].get("slot", 0) or 0)
                    except (ValueError, TypeError):
                        fid = ""
                    ups.append((fid, slot, r[0]))
                self._conn.executemany(
                    "UPDATE nodes SET first_file_id=?, first_slot=? WHERE id=?", ups
                )
                n_ff += len(rows)
            # 剩余 NULL 全部置 ''（目录行 / 空分片行），标记「已处理」避免每次启动重扫
            self._conn.execute(
                "UPDATE nodes SET first_file_id='' WHERE first_file_id IS NULL"
            )
            self._conn.commit()
            if n_pp or n_ff:
                _log(f"元数据回填完成: parent_path={n_pp} 行, first_file_id={n_ff} 行")
            return True
        except Exception as e:
            _log(f"元数据回填失败(depth=1 回退旧查询路径): {type(e).__name__}: {e}")
            try:
                self._conn.rollback()
            except Exception:
                pass
            return False

    # ---------- 基础读写 ----------
    def get_node(self, path, with_chunks=True):
        """取单节点。下载路径需 chunks（分片列表）；存在性/属性检查用 with_chunks=False
        可避免为大文件读出整个分片 JSON（万级分片文件的单次点查从 MB 级降到字节级）。"""
        cols = _NODE_FULL_COLS if with_chunks else _NODE_LIGHT_COLS
        with self._lock:
            row = self._conn.execute(
                f"SELECT {cols} FROM nodes WHERE path=?", (path,)
            ).fetchone()
            return dict(row) if row else None

    def list_children(self, path, depth="1"):
        """返回 (自身, [子节点...])。depth: 0/1/infinity。

        depth=1（PROPFIND 默认）在 parent_path 回填成功后走点查：
        只读直接子节点，复杂度 O(子节点数)——旧实现是 LIKE 前缀拉全子树
        （含每个文件完整 chunks JSON）再 Python 过滤，万级目录一次列目录
        要白读数十 MB 数据。depth=infinity 保持前缀扫描（本就需要全子树）。
        """
        with self._lock:
            self_node = self._conn.execute(
                f"SELECT {_NODE_LIGHT_COLS} FROM nodes WHERE path=?", (path,)
            ).fetchone()
            if self_node is None:
                return None, []
            self_node = dict(self_node)
            if depth == "0":
                return self_node, []
            if self._parent_ready and depth == "1":
                rows = self._conn.execute(
                    f"SELECT {_NODE_LIGHT_COLS} FROM nodes "
                    "WHERE parent_path=? AND path<>? ORDER BY path",
                    (path, path),
                ).fetchall()
                return self_node, [dict(r) for r in rows]
            # 回退路径（回填失败）与 depth=infinity：前缀扫描
            prefix = "/" if path == "/" else path + "/"
            cols = _NODE_LIGHT_COLS if self._parent_ready else _NODE_FULL_COLS
            rows = self._conn.execute(
                f"SELECT {cols} FROM nodes "
                "WHERE path=? OR path LIKE ? ESCAPE '\\' ORDER BY path",
                (path, _like_escape(prefix) + "%"),
            ).fetchall()
            nodes = [dict(r) for r in rows]
            if depth == "1":
                out = []
                for n in nodes:
                    p = n["path"]
                    if p == path:
                        continue
                    rel = p[len(prefix):]
                    if "/" not in rel:
                        out.append(n)
                return self_node, out
            return self_node, [n for n in nodes if n["path"] != path]

    def descendants(self, path, with_chunks=False):
        """返回 path 及其所有后代（含自身）。默认轻量列；move/copy 内部用 with_chunks=True。"""
        cols = _NODE_FULL_COLS if with_chunks else _NODE_LIGHT_COLS
        with self._lock:
            prefix = "/" if path == "/" else path + "/"
            rows = self._conn.execute(
                f"SELECT {cols} FROM nodes "
                "WHERE path=? OR path LIKE ? ESCAPE '\\' ORDER BY path",
                (path, _like_escape(prefix) + "%"),
            ).fetchall()
            return [dict(r) for r in rows]

    def _fetch_subtree_locked(self, path, with_chunks=True):
        """取子树（假定调用方已持有 self._lock），供 move/copy 在同一事务内使用。"""
        cols = _NODE_FULL_COLS if with_chunks else _NODE_LIGHT_COLS
        prefix = "/" if path == "/" else path + "/"
        rows = self._conn.execute(
            f"SELECT {cols} FROM nodes "
            "WHERE path=? OR path LIKE ? ESCAPE '\\' ORDER BY path",
            (path, _like_escape(prefix) + "%"),
        ).fetchall()
        return [dict(r) for r in rows]

    @staticmethod
    def _delete_subtree_sql():
        return "DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'"

    # ---------- 创建 ----------
    def create_file(self, path, content_type, chunks, size, chunk_size=None, mtime=None,
                    file_hash=None, duration=None):
        now = int(time.time())
        name = path.rstrip("/").split("/")[-1]
        etag = '"' + uuid.uuid4().hex + '"'
        chunks_json = json.dumps(chunks, ensure_ascii=False) if chunks is not None else None
        # 首片冗余列：PROPFIND 预热只取首片，列目录从此不必再读 chunks JSON
        c0 = chunks[0] if chunks else None
        if isinstance(c0, dict):
            first_fid = c0.get("file_id") or ""
            first_slot = int(c0.get("slot", 0) or 0)
        else:
            first_fid, first_slot = "", 0
        with self._lock:
            self._conn.execute(
                "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,"
                "chunk_size,chunks,file_hash,duration,parent_path,first_file_id,first_slot) "
                "VALUES(?,?,0,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET name=excluded.name, size=excluded.size, "
                "content_type=excluded.content_type, etag=excluded.etag, mtime=?, "
                "chunk_size=excluded.chunk_size, chunks=excluded.chunks, "
                "file_hash=excluded.file_hash, duration=excluded.duration, "
                "first_file_id=excluded.first_file_id, first_slot=excluded.first_slot",
                (path, name, size, content_type, etag, mtime or now, now,
                 chunk_size, chunks_json, file_hash, duration,
                 _parent_of(path), first_fid, first_slot, mtime or now),
            )
            self._conn.commit()
        return etag

    def create_dir(self, path, mtime=None):
        now = int(time.time())
        name = path.rstrip("/").split("/")[-1]
        etag = '"' + uuid.uuid4().hex + '"'
        with self._lock:
            self._conn.execute(
                "INSERT INTO nodes(path,name,is_dir,size,etag,mtime,ctime,parent_path) "
                "VALUES(?,?,1,0,?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET mtime=?, etag=excluded.etag",
                (path, name, etag, mtime or now, now, _parent_of(path), mtime or now),
            )
            self._conn.commit()
        return etag

    def set_mtime(self, path, mtime=None):
        with self._lock:
            self._conn.execute(
                "UPDATE nodes SET mtime=? WHERE path=?", (mtime or int(time.time()), path)
            )
            self._conn.commit()

    # ---------- 删除 ----------
    def delete_recursive(self, path):
        with self._lock:
            prefix = "/" if path == "/" else path + "/"
            self._conn.execute(self._delete_subtree_sql(),
                               (path, _like_escape(prefix) + "%"))
            self._conn.commit()

    # ---------- 移动 / 复制 ----------
    def move(self, src, dst):
        """移动子树 src -> dst（覆盖已存在的 dst）。src 不能为 dst 的祖先。

        全程单次持锁：查询后代、删 dst、批量写新路径、删 src 在同一事务内完成——
        既消灭了「先查后写」之间树结构被并发修改的 TOCTOU 窗口，也把万级子树的
        逐行 INSERT 合并为一次 executemany（行数越大收益越明显）。

        移动保留被移动节点原有的 mtime / ctime：文件内容未变，修改时间（getlastmodified）
        与创建时间（creationdate）不应因移动而改变（此前误把 mtime 重置为入库时间）。
        """
        if dst == src or dst.startswith(src + "/"):
            return False
        with self._lock:
            nodes = self._fetch_subtree_locked(src, with_chunks=True)
            if not nodes:
                return False
            dprefix = "/" if dst == "/" else dst + "/"
            self._conn.execute(self._delete_subtree_sql(),
                               (dst, _like_escape(dprefix) + "%"))
            self._conn.executemany(
                "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,"
                "chunk_size,chunks,file_hash,duration,parent_path,first_file_id,first_slot) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET name=excluded.name, is_dir=excluded.is_dir, "
                "size=excluded.size, content_type=excluded.content_type, etag=excluded.etag, "
                "mtime=?, chunk_size=excluded.chunk_size, chunks=excluded.chunks, "
                "file_hash=excluded.file_hash, duration=excluded.duration, "
                "parent_path=excluded.parent_path, "
                "first_file_id=excluded.first_file_id, first_slot=excluded.first_slot",
                [
                    (dst if n["path"] == src else dst + n["path"][len(src):],
                     n["name"], n["is_dir"], n["size"], n["content_type"], n["etag"],
                     n["mtime"], n["ctime"], n["chunk_size"], n["chunks"],
                     n.get("file_hash"), n.get("duration"),
                     _parent_of(dst if n["path"] == src else dst + n["path"][len(src):]),
                     n.get("first_file_id"), n.get("first_slot"),
                     n["mtime"])
                    for n in nodes
                ],
            )
            sprefix = "/" if src == "/" else src + "/"
            self._conn.execute(self._delete_subtree_sql(),
                               (src, _like_escape(sprefix) + "%"))
            self._conn.commit()
        return True

    def copy(self, src, dst):
        """复制子树 src -> dst（共享同一批 Telegram file_id，物理量不重复上传）。

        与 move 同样单次持锁 + executemany；区别仅是 mtime/ctime 原样保留。
        """
        if dst == src or dst.startswith(src + "/"):
            return False
        with self._lock:
            nodes = self._fetch_subtree_locked(src, with_chunks=True)
            if not nodes:
                return False
            dprefix = "/" if dst == "/" else dst + "/"
            self._conn.execute(self._delete_subtree_sql(),
                               (dst, _like_escape(dprefix) + "%"))
            self._conn.executemany(
                "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,"
                "chunk_size,chunks,file_hash,duration,parent_path,first_file_id,first_slot) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET name=excluded.name, is_dir=excluded.is_dir, "
                "size=excluded.size, content_type=excluded.content_type, etag=excluded.etag, "
                "mtime=?, chunk_size=excluded.chunk_size, chunks=excluded.chunks, "
                "file_hash=excluded.file_hash, duration=excluded.duration, "
                "parent_path=excluded.parent_path, "
                "first_file_id=excluded.first_file_id, first_slot=excluded.first_slot",
                [
                    (dst if n["path"] == src else dst + n["path"][len(src):],
                     n["name"], n["is_dir"], n["size"], n["content_type"], n["etag"],
                     n["mtime"], n["ctime"], n["chunk_size"], n["chunks"],
                     n.get("file_hash"), n.get("duration"),
                     _parent_of(dst if n["path"] == src else dst + n["path"][len(src):]),
                     n.get("first_file_id"), n.get("first_slot"),
                     n["mtime"])
                    for n in nodes
                ],
            )
            self._conn.commit()
        return True

    # ---------- 锁 ----------
    def add_lock(self, token, path, owner, depth, ttl):
        expiry = int(time.time()) + ttl
        with self._lock:
            self._conn.execute(
                "INSERT INTO locks(token,path,owner,depth,expiry) VALUES(?,?,?,?,?) "
                "ON CONFLICT(token) DO UPDATE SET path=excluded.path, owner=excluded.owner, "
                "depth=excluded.depth, expiry=excluded.expiry",
                (token, path, owner, depth, expiry),
            )
            self._conn.commit()

    def get_lock(self, token):
        with self._lock:
            row = self._conn.execute("SELECT * FROM locks WHERE token=?", (token,)).fetchone()
            return dict(row) if row else None

    def remove_lock(self, token):
        with self._lock:
            self._conn.execute("DELETE FROM locks WHERE token=?", (token,))
            self._conn.commit()

    def purge_expired_locks(self):
        with self._lock:
            cur = self._conn.execute(
                "DELETE FROM locks WHERE expiry < ?", (int(time.time()),)
            )
            n = cur.rowcount
            self._conn.commit()
            return n

    # ---------- 分片去重（大文件重传时复用已上传分片）----------
    def find_chunk_by_sha(self, sha, size=None):
        """按分片 SHA-256 查已上传过的分片，命中返回 (file_id, slot, message_id)，否则 None。

        命中即意味着：这个分片的内容已经在 Telegram 里了，可以直接复用 file_id，
        不必重新上传——大文件重传时靠它做到「只补传失败的那几片」。
        """
        with self._lock:
            if size is None:
                row = self._conn.execute(
                    "SELECT file_id, slot, message_id FROM chunk_dedup WHERE sha=?",
                    (sha,),
                ).fetchone()
            else:
                row = self._conn.execute(
                    "SELECT file_id, slot, message_id FROM chunk_dedup WHERE sha=? AND size=?",
                    (sha, size),
                ).fetchone()
            return (row[0], row[1], row[2]) if row else None

    def put_chunk_dedup(self, sha, file_id, slot, message_id, size):
        """记录一个已上传分片的 file_id（同一 sha 重复写入直接忽略，保留首次记录）。"""
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO chunk_dedup(sha,file_id,slot,message_id,size,created) "
                "VALUES(?,?,?,?,?,?)",
                (sha, file_id, slot, message_id, size, time.time()),
            )
            self._conn.commit()

    def put_chunk_dedup_many(self, rows):
        """批量登记已上传分片（executemany 单事务）。

        rows: [(sha, file_id, slot, message_id, size), ...]，供批量/补传场景一次落库，
        避免逐条 INSERT+commit 的锁往返。
        """
        if not rows:
            return 0
        now = time.time()
        with self._lock:
            self._conn.executemany(
                "INSERT OR IGNORE INTO chunk_dedup(sha,file_id,slot,message_id,size,created) "
                "VALUES(?,?,?,?,?,?)",
                [(sha, fid, slot, mid, size, now) for sha, fid, slot, mid, size in rows],
            )
            self._conn.commit()
        return len(rows)

    def purge_expired_chunks(self, ttl_seconds, batch_size=_PURGE_BATCH):
        """清掉超过 TTL 的分片去重记录，避免表无限增长（默认 30 天）。

        大表（百万行级）清理改为分批事务：每批 batch_size 行提交一次，
        批间让出全局锁给在线请求，避免「一次 DELETE 数百万行把所有请求卡死几秒」。
        """
        cutoff = time.time() - ttl_seconds
        total = 0
        with self._lock:
            while True:
                cur = self._conn.execute(
                    "DELETE FROM chunk_dedup WHERE rowid IN "
                    "(SELECT rowid FROM chunk_dedup WHERE created < ? LIMIT ?)",
                    (cutoff, batch_size),
                )
                n = cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
                total += n
                self._conn.commit()  # 每批一提交：WAL 下小事务代价极低，换在线请求低延迟
                if n < batch_size:
                    break
        return total

    # ---------- 周期维护 ----------
    def maintenance(self, chunk_ttl_seconds=None):
        """周期维护：清过期锁 + 清过期去重记录 + 刷新查询计划统计 + WAL checkpoint。

        由 webdav 层后台线程周期调用（默认 6h）。全部步骤尽力而为，单项失败不中断。
        返回统计 dict 供日志打印。
        """
        stats = {}
        try:
            stats["locks_purged"] = self.purge_expired_locks()
        except Exception as e:
            stats["locks_purged"] = f"失败({type(e).__name__})"
        if chunk_ttl_seconds:
            try:
                stats["chunks_purged"] = self.purge_expired_chunks(chunk_ttl_seconds)
            except Exception as e:
                stats["chunks_purged"] = f"失败({type(e).__name__})"
        with self._lock:
            # PRAGMA optimize：根据近期查询负载刷新 planner 统计（官方推荐长期运行库定期执行）
            for pragma in ("PRAGMA optimize", "PRAGMA wal_checkpoint(PASSIVE)"):
                try:
                    self._conn.execute(pragma).fetchall()
                except Exception:
                    pass
        return stats
