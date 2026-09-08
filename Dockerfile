# 用 Telegram 频道/群组当存储后端的纯 WebDAV 服务
# 仅依赖 Python 标准库，无需 pip 安装任何第三方包。
FROM python:3.11-slim

WORKDIR /app

# 仅拷贝源码（扁平布局，同级 import）
COPY config.py db.py tg.py webdav.py server.py run.py fake_telegram.py selftest.py /app/

# 若有需求可放 README（可选）
COPY README.md /app/README.md

EXPOSE 8080

# 数据（SQLite + 运行时配置）落在外挂卷，便于持久化
ENV HOST=0.0.0.0 \
    PORT=8080 \
    DB_PATH=/data/telegram_webdav.db

VOLUME ["/data"]

# 配置全部走环境变量，参见 .env.example
CMD ["python", "run.py"]
