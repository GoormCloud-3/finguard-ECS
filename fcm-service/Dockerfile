FROM python:3.13.1

WORKDIR /app

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*



ENV AWS_REGION=ap-northeast-2
ENV AWS_DEFAULT_REGION=ap-northeast-2
ENV XRAY_SERVICE_NAME=fcm-worker
ENV PYTHONUNBUFFERED=1
ENV SSM_PARAM_ARN=arn:aws:ssm:ap-northeast-2:381492026475:parameter/prod/firebase-service-account-json

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY . .


CMD ["opentelemetry-instrument", "--traces_exporter", "otlp", "--metrics_exporter", "none", "--service_name", "fcm-service", "python", "worker.py"]
