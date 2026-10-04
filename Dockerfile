# 冷链温区异常归因 API
# 所有依赖随仓库 vendor/ 目录提供，构建过程无需访问外网（本地依赖）。
FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    COLDCHAIN_DB=/data/coldchain.db

WORKDIR /app

# 仅依赖清单与本地 wheel 目录变化时才重装依赖
COPY requirements.txt ./requirements.txt
COPY vendor/ ./vendor/
RUN pip install --no-index --find-links ./vendor -r requirements.txt

COPY app/ ./app/

# SQLite 数据持久化卷
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=3).status == 200 else 1)"

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
