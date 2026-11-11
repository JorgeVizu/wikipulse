FROM python:3.13-slim
RUN pip install --no-cache-dir kafka-python==2.2.15
WORKDIR /app
COPY ingest.py dashboard.py index.html ./
CMD ["python", "-u", "dashboard.py"]
