# ---------------------------------------------------------------------------
# Storage worker image.
# ---------------------------------------------------------------------------
# python:3.12-slim: small Debian-based image that still has coreutils (dd, sh),
# which the corruption demo uses via `docker exec`.
FROM python:3.12-slim

# Don't write .pyc files and don't buffer stdout: logs show up in
# `docker logs` immediately and the image stays a little smaller.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /srv

# Install dependencies BEFORE copying the source. Docker caches each step, so
# editing code later does not re-download packages.
COPY requirements.txt .
# --default-timeout/--retries: tolerate slow or flaky networks during the build.
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 -r requirements.txt

COPY app ./app

# Run as an unprivileged user (good practice: a bug in the worker cannot
# act as root inside the container). /data is created and owned by that user
# BEFORE the volume is mounted, so the named volume inherits the right owner.
RUN useradd --create-home --uid 1000 appuser \
 && mkdir -p /data && chown appuser:appuser /data
USER appuser
VOLUME ["/data"]

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
