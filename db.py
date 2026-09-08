"""SQLite 元数据层。

在 Telegram 只存"文件字节"（按 20MB 分片，每片一个 file_id），而把"文件树 / 分片索引"
存在本地 SQLite 里——这正是 otterhub-server 用 Cloudflare KV 干的那件事，这里换成
单机可移植的 SQLite。

nodes 表即一个虚拟文件系统：
  - 目录：is_dir=1，size=0，chunks=NULL
  - 文件：is_dir=0，size=字节数，chunks=JSON 数组
        [{ "file_id": "...", "slot": 0, "size": 20971520, "message_id": 123 },
         { "file_id": "...", "slot": 1, "size": 12345,    "message_id": 124 }]
    slot 记录上传该分片所用的 bot 序号（file_id 与 bot 绑定，下载时须用同 bot token）。

并发：HTTP/1.1 多线程服务器下，每条请求独立连接 + 全局写锁 + WAL，避免 "database is locked"。
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
        self._init_db()

    # ---------- 连接 ----------
    def _conn(self):
        conn = sqlite3.connect(self.path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=30000")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self):
        with self._lock:
            conn = self._conn()
            conn.execute(
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
            conn.execute("CREATE INDEX IF NOT EXISTS idx_nodes_path ON nodes(path)")
            conn.execute(
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
            conn.execute(
                "INSERT OR IGNORE INTO nodes(path,name,is_dir,size,mtime,ctime,etag) "
                "VALUES('/','',1,0,?,?,?)",
                (now, now, '"root"'),
            )
            conn.commit()
            conn.close()

    # ---------- 基础读写 ----------
    def get_node(self, path):
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM nodes WHERE path=?", (path,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def _row_to_dict(self, row):
        return dict(row)

    def list_children(self, path, depth="1"):
        """返回 (自身, [子节点...])。depth: 0/1/infinity。"""
        conn = self._conn()
        try:
            self_node = conn.execute(
                "SELECT * FROM nodes WHERE path=?", (path,)
            ).fetchone()
            if self_node is None:
                return None, []
            self_node = dict(self_node)
            if depth == "0":
                return self_node, []
            prefix = "/" if path == "/" else path + "/"
            rows = conn.execute(
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
            # infinity
            return self_node, [n for n in nodes if n["path"] != path]
        finally:
            conn.close()

    def descendants(self, path):
        """返回 path 及其所有后代（含自身）。"""
        conn = self._conn()
        try:
            prefix = "/" if path == "/" else path + "/"
            rows = conn.execute(
                "SELECT * FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\' ORDER BY path",
                (path, prefix + "%"),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ---------- 创建 ----------
    def create_file(self, path, content_type, chunks, size, chunk_size=None, mtime=None):
        now = int(time.time())
        name = path.rstrip("/").split("/")[-1]
        etag = '"' + uuid.uuid4().hex + '"'
        chunks_json = json.dumps(chunks, ensure_ascii=False) if chunks is not None else None
        with self._lock:
            conn = self._conn()
            conn.execute(
                "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,chunk_size,chunks) "
                "VALUES(?,?,0,?,?,?,?,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET name=excluded.name, size=excluded.size, "
                "content_type=excluded.content_type, etag=excluded.etag, mtime=?, "
                "chunk_size=excluded.chunk_size, chunks=excluded.chunks",
                (path, name, size, content_type, etag, mtime or now, now,
                 chunk_size, chunks_json, mtime or now),
            )
            conn.commit()
            conn.close()
        return etag

    def create_dir(self, path, mtime=None):
        now = int(time.time())
        name = path.rstrip("/").split("/")[-1]
        etag = '"' + uuid.uuid4().hex + '"'
        with self._lock:
            conn = self._conn()
            conn.execute(
                "INSERT INTO nodes(path,name,is_dir,size,etag,mtime,ctime) "
                "VALUES(?,?,1,0,?,?,?) "
                "ON CONFLICT(path) DO UPDATE SET mtime=?, etag=excluded.etag",
                (path, name, etag, mtime or now, now, mtime or now),
            )
            conn.commit()
            conn.close()
        return etag

    def set_mtime(self, path, mtime=None):
        with self._lock:
            conn = self._conn()
            conn.execute(
                "UPDATE nodes SET mtime=? WHERE path=?", (mtime or int(time.time()), path)
            )
            conn.commit()
            conn.close()

    # ---------- 删除 ----------
    def delete_recursive(self, path):
        with self._lock:
            conn = self._conn()
            prefix = "/" if path == "/" else path + "/"
            conn.execute("DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'",
                         (path, prefix + "%"))
            conn.commit()
            conn.close()

    # ---------- 移动 / 复制 ----------
    def _canonical(self, p):
        return p

    def move(self, src, dst):
        """移动子树 src -> dst（覆盖已存在的 dst）。src 不能为 dst 的祖先。"""
        if dst == src or dst.startswith(src + "/"):
            return False
        nodes = self.descendants(src)
        if not nodes:
            return False
        with self._lock:
            conn = self._conn()
            # 先删目标（含后代）
            dprefix = "/" if dst == "/" else dst + "/"
            conn.execute("DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'",
                         (dst, dprefix + "%"))
            for n in nodes:
                old = n["path"]
                new = dst if old == src else dst + old[len(src):]
                conn.execute(
                    "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,chunk_size,chunks) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(path) DO UPDATE SET name=excluded.name, is_dir=excluded.is_dir, "
                    "size=excluded.size, content_type=excluded.content_type, etag=excluded.etag, "
                    "mtime=?, chunk_size=excluded.chunk_size, chunks=excluded.chunks",
                    (new, n["name"], n["is_dir"], n["size"], n["content_type"], n["etag"],
                     int(time.time()), n["ctime"], n["chunk_size"], n["chunks"], int(time.time())),
                )
            # 删除源子树（移动 = 复制 + 删除源）
            sprefix = "/" if src == "/" else src + "/"
            conn.execute("DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'",
                         (src, sprefix + "%"))
            conn.commit()
            conn.close()
        return True

    def copy(self, src, dst):
        """复制子树 src -> dst（共享同一批 Telegram file_id，物理量不重复上传）。"""
        if dst == src or dst.startswith(src + "/"):
            return False
        nodes = self.descendants(src)
        if not nodes:
            return False
        with self._lock:
            conn = self._conn()
            dprefix = "/" if dst == "/" else dst + "/"
            conn.execute("DELETE FROM nodes WHERE path=? OR path LIKE ? ESCAPE '\\'",
                         (dst, dprefix + "%"))
            for n in nodes:
                old = n["path"]
                new = dst if old == src else dst + old[len(src):]
                conn.execute(
                    "INSERT INTO nodes(path,name,is_dir,size,content_type,etag,mtime,ctime,chunk_size,chunks) "
                    "VALUES(?,?,?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(path) DO UPDATE SET name=excluded.name, is_dir=excluded.is_dir, "
                    "size=excluded.size, content_type=excluded.content_type, etag=excluded.etag, "
                    "mtime=?, chunk_size=excluded.chunk_size, chunks=excluded.chunks",
                    (new, n["name"], n["is_dir"], n["size"], n["content_type"], n["etag"],
                     n["mtime"], n["ctime"], n["chunk_size"], n["chunks"], n["mtime"]),
                )
            conn.commit()
            conn.close()
        return True

    # ---------- 锁 ----------
    def add_lock(self, token, path, owner, depth, ttl):
        expiry = int(time.time()) + ttl
        with self._lock:
            conn = self._conn()
            conn.execute(
                "INSERT INTO locks(token,path,owner,depth,expiry) VALUES(?,?,?,?,?) "
                "ON CONFLICT(token) DO UPDATE SET path=excluded.path, owner=excluded.owner, "
                "depth=excluded.depth, expiry=excluded.expiry",
                (token, path, owner, depth, expiry),
            )
            conn.commit()
            conn.close()

    def get_lock(self, token):
        conn = self._conn()
        try:
            row = conn.execute("SELECT * FROM locks WHERE token=?", (token,)).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def remove_lock(self, token):
        with self._lock:
            conn = self._conn()
            conn.execute("DELETE FROM locks WHERE token=?", (token,))
            conn.commit()
            conn.close()

    def purge_expired_locks(self):
        with self._lock:
            conn = self._conn()
            conn.execute("DELETE FROM locks WHERE expiry < ?", (int(time.time()),))
            conn.commit()
            conn.close()
