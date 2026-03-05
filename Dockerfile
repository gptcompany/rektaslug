FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_LINK_MODE=copy \
    PATH="/app/.venv/bin:${PATH}"

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock README.md /app/
COPY src /app/src
COPY scripts /app/scripts
COPY frontend /app/frontend

RUN uv sync --frozen --no-dev

EXPOSE 8002

ENV HEATMAP_PROJECT_ROOT=/app \
    HEATMAP_HOST=0.0.0.0 \
    HEATMAP_PORT=8002

CMD ["uv", "run", "uvicorn", "src.liquidationheatmap.api.main:app", "--host", "0.0.0.0", "--port", "8002"]
