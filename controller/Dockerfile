# ---------------------------------------------------------------------------
# Controller image (same structure as the worker image; see worker/Dockerfile
# for line-by-line reasoning).
# ---------------------------------------------------------------------------
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

COPY requirements.txt .
# --default-timeout/--retries: tolerate slow or flaky networks during the build.
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 -r requirements.txt

COPY app ./app

RUN useradd --create-home --uid 1000 appuser \
 && mkdir -p /data && chown appuser:appuser /data
USER appuser
# /data holds the SQLite metadata database -> must survive container restarts.
VOLUME ["/data"]

EXPOSE 8000
# ONE uvicorn process on purpose: the controller keeps state (SQLite connection,
# background loops) that must not be duplicated across processes. See D-05.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
