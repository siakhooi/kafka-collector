FROM python:3.14-slim

WORKDIR /app

COPY dist/*.whl ./

RUN apt-get update && apt-get install -y curl && \
    pip install --no-cache-dir *.whl && rm -f *.whl && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD curl -f http://localhost:8080/files || exit 1

ENTRYPOINT ["kafka-collector"]
