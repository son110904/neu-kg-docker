# ══════════════════════════════════════════════════════════════
#  NEU Knowledge Graph API — Dockerfile
#  Base: Python 3.11-slim  |  Server: Uvicorn
# ══════════════════════════════════════════════════════════════

FROM python:3.11-slim

# Tránh Python tạo .pyc và buffer stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

# ── System deps (libgomp cần cho một số thư viện ML) ──────────
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        curl \
    && rm -rf /var/lib/apt/lists/*

# ── Python deps ───────────────────────────────────────────────
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# ── Application code ──────────────────────────────────────────
COPY index.py      .
COPY script1.py    .
COPY script2.py    .

# Tạo thư mục cache (mount volume ở runtime sẽ override)
RUN mkdir -p /app/cache/output /app/cache

# ── Expose & run ──────────────────────────────────────────────
EXPOSE 8000

# Uvicorn với 2 workers — tăng nếu server nhiều CPU
CMD ["uvicorn", "index:app", "--host", "0.0.0.0", "--port", "8000", \
     "--workers", "2", "--timeout-keep-alive", "120"]
