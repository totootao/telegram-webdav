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
"""

import json
import os
import sqlite3
import threading
import time
import uuid


class MetaStore:
    def __init__(self, path):
        self.path = path
        self._lock = threading.Lock()
        d = os.path.dirname(os.path.abspath(path))
        if d and not os.path.isdir(d):
            os.makedirs(d, exist_ok=True)
        self._conn = self._open()
        self._init_db()

    # ---------- 连接（单例，线程安全由 self._lock 保证） ----------
    def _open(self):
        conn = sqlite3.connect(self.path, timeout=60, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=60000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def close(self):
        try:
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
            self._conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_path ON nodes(path)")
            # 迁移：为旧库补 file_hash 列（SQLite 不支持 ADD COLUMN IF NOT EXISTS，先探测）。
            cols = {r[1] for r in self._conn.execute("PRAGMA table_info(nodes)").fetchall()}
            if "file_hash" not in cols:
                self._conn.execute("ALTER TABLE nodes ADD COLUMN file_hash TEXT")
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
            now = int(time.time())
            self._conn.execute(
                "INSERT OR IGNORE INTO nodes(path,name,is_dir,size,mtime,ctime,etag) "
                "VALUES('/','',1,0,?,?,?)",
                (now, now, '"root"'),
            )
            self._conn.commit()

    # ---------- 基础读写 ----------
    def get_node(self, path):
        with self._lock:
            cur = self._conn.execute("SELECT * FROM nodes WHERE path=?", (path,))
            row = cur.fetchone()
            return dict(row) if row else None

    def list_children(self, path, depth="1"):
        """返回 (自身, [子节点...])。depth: 0/1/infinity。"""
        with self._lock:
            self_node = self._conn.execute(
                "SELECT * FROM nodes WHERE path=?", (path,)
            ).fetchone()
            if self_node is None:
                return None, []
            self_node = dict(self_node)
            if depth == "0":
                return self_node, []
            prefix = "/" if path == "/" else path + "/"
            rows = self._conn.execute(
                "SELECT * FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\' ORDER BY path",
                (path, prefix + "%"),
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

    def descendants(self, path):
        """返回 path 及其所有后代（含自身）。"""
        with self._lock:
            prefix = "/" if path == "/" else path + "/"
            rows = self._conn.execute(
                "SELECT * FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\' ORDER BY path",
                (path, prefix + "%"),
            ).fetchall()
            return [dict(r) for r in rows]

    # ---------- 创建 ----------
    def create_file(self, path, content_type, chunks, size, chunk_size=None, mtime=None, file_hash=None):
        now = int(time.time())
        name = path.rstrip("/").split("/")[-1]
        etag = '"' + uuid.uuid4().hex + '"'
        chunks_json = json.dumps(chunks, ensure_ascii=False) if chunks is not None else None
        with self._lock:
            self._conn.execute(
                "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,chunk_size,chunks,file_hash) "
                "VALUES(?,?,0,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET name=excluded.name, size=excluded.size, "
                "content_type=excluded.content_type, etag=excluded.etag, mtime=?, "
                "chunk_size=excluded.chunk_size, chunks=excluded.chunks, file_hash=excluded.file_hash",
                (path, name, size, content_type, etag, mtime or now, now,
                 chunk_size, chunks_json, file_hash, mtime or now),
            )
            self._conn.commit()
        return etag

    def create_dir(self, path, mtime=None):
        now = int(time.time())
        name = path.rstrip("/").split("/")[-1]
        etag = '"' + uuid.uuid4().hex + '"'
        with self._lock:
            self._conn.execute(
                "INSERT INTO nodes(path,name,is_dir,size,etag,mtime,ctime) "
                "VALUES(?,?,1,0,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET mtime=?, etag=excluded.etag",
                (path, name, etag, mtime or now, now, mtime or now),
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
            self._conn.execute("DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'",
                               (path, prefix + "%"))
            self._conn.commit()

    # ---------- 移动 / 复制 ----------
    def move(self, src, dst):
        """移动子树 src -> dst（覆盖已存在的 dst）。src 不能为 dst 的祖先。"""
        if dst == src or dst.startswith(src + "/"):
            return False
        nodes = self.descendants(src)
        if not nodes:
            return False
        with self._lock:
            dprefix = "/" if dst == "/" else dst + "/"
            self._conn.execute("DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'",
                              (dst, dprefix + "%"))
            for n in nodes:
                old = n["path"]
                new = dst if old == src else dst + old[len(src):]
                self._conn.execute(
                    "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,chunk_size,chunks,file_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(path) DO UPDATE SET name=excluded.name, is_dir=excluded.is_dir, "
                    "size=excluded.size, content_type=excluded.content_type, etag=excluded.etag, "
                    "mtime=?, chunk_size=excluded.chunk_size, chunks=excluded.chunks, file_hash=excluded.file_hash",
                    (new, n["name"], n["is_dir"], n["size"], n["content_type"], n["etag"],
                     int(time.time()), n["ctime"], n["chunk_size"], n["chunks"], n.get("file_hash"), int(time.time())),
                )
            sprefix = "/" if src == "/" else src + "/"
            self._conn.execute("DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'",
                               (src, sprefix + "%"))
            self._conn.commit()
        return True

    def copy(self, src, dst):
        """复制子树 src -> dst（共享同一批 Telegram file_id，物理量不重复上传）。"""
        if dst == src or dst.startswith(src + "/"):
            return False
        nodes = self.descendants(src)
        if not nodes:
            return False
        with self._lock:
            dprefix = "/" if dst == "/" else dst + "/"
            self._conn.execute("DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'",
                               (dst, dprefix + "%"))
            for n in nodes:
                old = n["path"]
                new = dst if old == src else dst + old[len(src):]
                self._conn.execute(
                    "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,chunk_size,chunks,file_hash) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(path) DO UPDATE SET name=excluded.name, is_dir=excluded.is_dir, "
                    "size=excluded.size, content_type=excluded.content_type, etag=excluded.etag, "
                    "mtime=?, chunk_size=excluded.chunk_size, chunks=excluded.chunks, file_hash=excluded.file_hash",
                    (new, n["name"], n["is_dir"], n["size"], n["content_type"], n["etag"],
                     n["mtime"], n["ctime"], n["chunk_size"], n["chunks"], n.get("file_hash"), n["mtime"]),
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
            self._conn.execute("DELETE FROM locks WHERE expiry < ?", (int(time.time()),))
            self._conn.commit()
