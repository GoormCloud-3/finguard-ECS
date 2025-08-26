
FROM python:3.13.1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

COPY service-account-key.json ./service-account-key.json

COPY worker.py ./worker.py

ENV AWS_REGION=ap-northeast-2
ENV XRAY_SERVICE_NAME=fcm-worker
ENV PYTHONUNBUFFERED=1



CMD ["python", "worker.py"]