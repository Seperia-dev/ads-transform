# ── Build stage ───────────────────────────────────────────────────────────────
ARG PYTHON_VERSION=3.12
FROM python:3.12-slim AS builder

WORKDIR /app

# System deps needed to compile some wheels (bingads / suds)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --upgrade pip \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy installed packages from builder-(This is the bridge between the two stages — it takes only the installed packages from the builder and copies them into the clean runtime image, leaving all the heavy build tools behind)
COPY --from=builder /install /usr/local

# Copy all source — exclusions are handled by .dockerignore
COPY . .

# Non-root user for security
RUN useradd -m appuser && chown -R appuser /app
USER appuser

#the code with explanation:
# RUN useradd -m appuser        # create a normal user called appuser
# RUN chown -R appuser /app     # give that user ownership of your code
# USER appuser                  # switch to that user — everything runs as appuser from here

EXPOSE 8000

CMD ["python", "main.py"]