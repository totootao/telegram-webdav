"""配置层：从环境变量读取 Telegram 存储与 WebDAV 服务配置。

参考 otterhub-server 的 .env.example：
  - TG_BOT_TOKEN / TG_CHAT_ID          单 bot 模式（频道/群组 chat_id，可为 @channel 或 -100xxxx）
  - TG_BOT_POOLS                       多 bot 池（JSON 数组：[{"token","chatId"}]），分摊 1 msg/s 流控
  - TG_API_BASE                        自建 Telegram API 代理基址（国内/被墙环境），可选
  - CHUNK_SIZE_MB                      分片大小，默认 20（Telegram Bot API 官方上传上限 20MB / 50MB）
  - DAV_USER / DAV_PASSWORD            WebDAV Basic 认证
  - DB_PATH                            SQLite 数据库路径
"""
import os
import json


def _load_dotenv(path=".env"):
    """零依赖地加载 .env（KEY=VALUE，忽略 # 注释与空行）。已存在的环境变量不覆盖。"""
    if not os.path.exists(path):
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                k, v = k.strip(), v.strip()
                if not k:
                    continue
                v = v.strip("'\"")  # 去掉可能的引号
                os.environ.setdefault(k, v)
    except Exception:
        pass


_load_dotenv()


def _load_pools():
    """解析 TG_BOT_POOLS（JSON 数组）或回退到单 bot（TG_BOT_TOKEN / TG_CHAT_ID）。"""
    raw = os.environ.get("TG_BOT_POOLS")
    pools = []
    if raw and raw.strip():
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                for it in parsed:
                    token = str(it.get("token", "")).strip()
                    chat_id = str(it.get("chatId") or it.get("chat_id") or "").strip()
                    if token and chat_id:
                        pools.append({"token": token, "chat_id": chat_id})
        except Exception:
            # 简化格式：token|chatId,token|chatId
            for part in raw.split(","):
                seg = part.strip()
                if not seg:
                    continue
                a, _, b = seg.partition("|")
                if a.strip() and b.strip():
                    pools.append({"token": a.strip(), "chat_id": b.strip()})
    if not pools:
        token = os.environ.get("TG_BOT_TOKEN")
        chat_id = os.environ.get("TG_CHAT_ID")
        if token and chat_id:
            pools.append({"token": token.strip(), "chat_id": chat_id.strip()})
    return pools


class Config:
    def __init__(self):
        self.db_path = os.environ.get("DB_PATH", "./telegram_webdav.db")
        self.chunk_size = int(os.environ.get("CHUNK_SIZE_MB", "20")) * 1024 * 1024
        self.api_base = os.environ.get(
            "TG_API_BASE", "https://api.telegram.org"
        ).rstrip("/")
        self.slots = _load_pools()
        self.auth_user = os.environ.get("DAV_USER")
        self.auth_password = os.environ.get("DAV_PASSWORD")
        self.host = os.environ.get("HOST", "0.0.0.0")
        self.port = int(os.environ.get("PORT", "8080"))
        self.webhook_secret = os.environ.get("TG_WEBHOOK_SECRET")
        self.import_dir = (os.environ.get("WEBDAV_IMPORT_DIR", "/telegram-import") or "/telegram-import").rstrip("/") or "/telegram-import"
        self.rate_limit = float(os.environ.get("TG_RATE_LIMIT", "1.0"))
        self.root_path = (os.environ.get("DAV_ROOT", "/") or "/").rstrip("/") or "/"

    @property
    def auth_enabled(self):
        return bool(self.auth_user)

    def summary(self):
        return {
            "db_path": self.db_path,
            "chunk_size_mb": self.chunk_size // (1024 * 1024),
            "api_base": self.api_base,
            "bot_slots": len(self.slots),
            "auth": "on" if self.auth_enabled else "off",
            "import_dir": self.import_dir,
            "rate_limit_s": self.rate_limit,
        }


config = Config()
