FROM python:3.14-slim

WORKDIR /app

COPY dist/*.whl ./

RUN pip install --no-cache-dir *.whl && rm -f *.whl

ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["kafka-collector"]
