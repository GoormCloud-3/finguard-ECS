# app.py (맨 위 가장 먼저 위치)
try:
    import whatap
    whatap.agent()
    logging.info("✅Whatap agent initialized")
except Exception as e:
    # whatap 미설치/실패 시 앱이 죽지 않도록만 보호 (원치 않으면 이 블록도 지워도 됨)
    logging.error(f"✅Whatap init failed: {e}")
# app.py
import os
import json
import logging
import time
import signal
import io
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import boto3
import pandas as pd
from botocore.config import Config

# ───────────────────────── OpenTelemetry: SDK + Exporter ─────────────────────────
from opentelemetry import trace
from opentelemetry.propagate import extract, set_global_textmap
from opentelemetry.propagators.textmap import Getter

from opentelemetry.sdk.resources import Resource
from opentelemetry.trace import Status, StatusCode
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

# (있으면 사용) AWS X-Ray 전용 ID/전파자
try:
    from opentelemetry.sdk.extension.aws.trace import AwsXRayIdGenerator  # type: ignore
    from opentelemetry.propagators.aws import AwsXRayPropagator           # type: ignore
    _HAS_XRAY_HELPERS = True
except Exception:
    _HAS_XRAY_HELPERS = False

# ───────────────────────── Prometheus ─────────────────────────
from prometheus_client import Counter, Histogram
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# ───────────────────────── 기본 설정 ─────────────────────────
region = os.getenv("AWS_REGION", "ap-northeast-2")

_boto_cfg = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    connect_timeout=5,
    read_timeout=65,
)

ssm_client = boto3.client("ssm",               region_name=region, config=_boto_cfg)
ddb_client = boto3.client("dynamodb",          region_name=region, config=_boto_cfg)
sagemaker_client = boto3.client("sagemaker-runtime", region_name=region, config=_boto_cfg)
sns_client = boto3.client("sns",               region_name=region, config=_boto_cfg)
sqs_client = boto3.client("sqs",               region_name=region, config=_boto_cfg)
s3_client  = boto3.client("s3",                region_name=region, config=_boto_cfg)

sageMakerEndpoint = None
topicArn = None
tableName = None
queue_url = None
s3_bucket = None
s3_key = None
initialized = False

# ───────────────────────── Prometheus Metrics ─────────────────────────
SQS_POLL_SIZE = Histogram("sqs_poll_batch_size", "Messages received per poll", buckets=(0, 1, 2, 5, 10))
SQS_ERRORS_TOTAL = Counter("sqs_errors_total", "Unhandled errors")
MSG_PROCESSED_TOTAL = Counter("sqs_messages_processed_total", "Messages processed", ["result"])  # success|skipped|error
SM_INVOKE_SECONDS = Histogram("sagemaker_invoke_seconds", "SageMaker invoke latency (s)")
S3_APPEND_SECONDS = Histogram("s3_append_seconds", "S3 append latency (s)")
SNS_PUBLISH_TOTAL = Counter("sns_publish_total", "SNS publishes (alerts)")

# ───────────────────────── 종료 신호 ─────────────────────────
RUNNING = True
def _handle_sigterm(signum, frame):
    global RUNNING
    logging.info("🛑 SIGTERM received, shutting down gracefully...")
    RUNNING = False
signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)

# ───────────────────────── OTel Tracer (init 전 임시) ─────────────────────────
tracer = trace.get_tracer("bootstrap")

def _init_tracing():
    """
    OTLP gRPC Exporter(Collector → X-Ray) 초기화.
    - endpoint: OTEL_EXPORTER_OTLP_ENDPOINT (없으면 http://localhost:4317)
    - X-Ray ID/Propagator 사용(설치돼 있으면)
    - service.name / namespace 는 태스크 env(OTEL_RESOURCE_ATTRIBUTES)와 합쳐짐
    """
    global tracer

    endpoint = os.getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "http://localhost:4317")
    svc_name = "sqs-service"
    svc_ns   = os.getenv("SERVICE_NAMESPACE", "finguard")

    # 리소스 구성 (env의 OTEL_RESOURCE_ATTRIBUTES와 merge됨)
    resource = Resource.create({
        "service.name": svc_name,
        "service.namespace": svc_ns,
    })

    # X-Ray ID 생성기(가능하면) 사용
    provider_kwargs = {"resource": resource}
    if _HAS_XRAY_HELPERS:
        provider_kwargs["id_generator"] = AwsXRayIdGenerator()

    provider = TracerProvider(**provider_kwargs)
    span_exporter = OTLPSpanExporter(endpoint=endpoint, insecure=True)
    provider.add_span_processor(BatchSpanProcessor(span_exporter))
    trace.set_tracer_provider(provider)

    # 전파자: 가능하면 X-Ray 사용 (없으면 기본값 사용)
    if _HAS_XRAY_HELPERS:
        set_global_textmap(AwsXRayPropagator())

    tracer = trace.get_tracer(svc_name)
    logging.info(f"✅ OTel tracing initialized (endpoint={endpoint}, xray_helpers={_HAS_XRAY_HELPERS})")

# ─────────────────────── 헬스/메트릭 서버 ───────────────────────
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "8000"))

class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
        elif self.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.send_header("Content-Length", str(len(output)))
            self.end_headers()
            self.wfile.write(output)
        else:
            self.send_response(404); self.end_headers()

def _start_health_server():
    srv = HTTPServer(("0.0.0.0", HEALTH_PORT), _Health)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

# ─────────────────────── 컨텍스트 추출 Getter ───────────────────────
class _AttrGetter(Getter):
    def get(self, carrier, key):
        if not carrier or not key:
            return []
        for k in (key, key.lower(), key.upper(), key.title()):
            if k in carrier:
                v = carrier[k]
                if v is None:
                    return []
                return [v] if isinstance(v, str) else [str(v)]
        return []
    def keys(self, carrier):
        return list(carrier.keys()) if carrier else []

def _extract_ctx_from_sqs(msg: dict):
    carrier = {}
    attrs = msg.get("MessageAttributes") or {}
    for k, v in attrs.items():
        if isinstance(v, dict) and "StringValue" in v:
            carrier[k] = v["StringValue"]

    try:
        body = json.loads(msg.get("Body") or "{}")
        if isinstance(body, dict):
            sns_attrs = body.get("MessageAttributes") or {}
            for k, v in sns_attrs.items():
                if isinstance(v, dict):
                    if "Value" in v:
                        carrier.setdefault(k, v["Value"])
                    elif "StringValue" in v:
                        carrier.setdefault(k, v["StringValue"])
    except Exception:
        pass

    norm = {}
    for k, v in carrier.items():
        lk = k.lower()
        if lk in ("x-amzn-trace-id", "traceparent", "baggage"):
            norm[k] = v
    if not norm and carrier:
        norm = carrier

    return extract(norm, getter=_AttrGetter())

# ─────────────────────── 헬퍼 ───────────────────────
def get_param(name, with_decryption=False):
    with tracer.start_as_current_span("ssm.get_parameter") as span:
        span.set_attribute("param.name", name)
        span.set_attribute("param.decrypt", with_decryption)
        resp = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp["Parameter"]["Value"]

def init():
    logging.info("🔧 Initializing SQS worker...")
    global sageMakerEndpoint, topicArn, tableName, queue_url, s3_bucket, s3_key, initialized
    if initialized:
        return
    try:
        with tracer.start_as_current_span("bootstrap.load_params"):
            sageMakerEndpoint = get_param("/finguard/dev/finance/fraud_sage_maker_endpoint_name")
            topicArn          = get_param("/finguard/dev/finance/alert_sns_topic")
            tableName         = get_param("/finguard/dev/finance/notification_table_name")
            queue_url         = get_param("/finguard/dev/finance/trade_queue_host")
            s3_bucket         = get_param("/finguard/dev/finance/model_s3_bucket")
            s3_key            = get_param("/finguard/dev/finance/model_s3_key")

            if not all([sageMakerEndpoint, topicArn, tableName, queue_url]):
                raise ValueError("One or more SSM parameters are empty.")
            if not queue_url.startswith("https://"):
                logging.warning("⚠️ queue_url doesn't look like full URL: %s", queue_url)

        initialized = True
        logging.info("✅ Initialized: endpoint=%s, topic=%s, table=%s, queue=%s, s3_bucket=%s, s3_key=%s",
                     sageMakerEndpoint, topicArn, tableName, queue_url, s3_bucket, s3_key)
    except Exception:
        logging.exception("❌ Initialization failed")
        time.sleep(10)

def get_fcm_tokens(user_sub: str):
    with tracer.start_as_current_span("ddb.get_fcm_tokens") as span:
        span.set_attribute("user.sub", user_sub)
        resp = ddb_client.get_item(
            TableName=tableName,
            Key={"user_id": {"S": user_sub}},
            ProjectionExpression="fcmTokens",
        )
        tokens = []
        if "Item" in resp and "fcmTokens" in resp["Item"]:
            tokens = [t["S"] for t in resp["Item"]["fcmTokens"]["L"]]
        return tokens

def invoke_sagemaker(endpoint: str, features):
    with tracer.start_as_current_span("sagemaker.invoke_endpoint") as span, SM_INVOKE_SECONDS.time():
        span.set_attribute("sm.endpoint", endpoint)
        response = sagemaker_client.invoke_endpoint(
            EndpointName=endpoint,
            ContentType="application/json",
            Body=json.dumps({"features": features}),
        )
        result_str = response["Body"].read().decode()
        try:
            result = json.loads(result_str)
        except json.JSONDecodeError as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            raise

    with tracer.start_as_current_span("s3.append_features") as span, S3_APPEND_SECONDS.time():
        span.set_attribute("s3.bucket", s3_bucket or "")
        span.set_attribute("s3.key", s3_key or "")
        try:
            try:
                obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                df_existing = pd.read_csv(io.BytesIO(obj["Body"].read()), header=None)
            except s3_client.exceptions.NoSuchKey:
                df_existing = pd.DataFrame()

            prediction = [result.get("prediction")]
            
            features = features + prediction
            features[:3] = [float(x) for x in features[:3]]
            logging.info(f"✅ Features + Prediction: {features}")
            
            df_new = pd.DataFrame([features])
            df_total = pd.concat([df_existing, df_new], ignore_index=True)

            csv_buffer = io.StringIO()
            df_total.to_csv(csv_buffer, header=False, index=False)
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
    return result

def publish_sns(fcm_tokens):
    with tracer.start_as_current_span("sns.publish") as span:
        span.set_attribute("sns.topic", topicArn or "")
        if not fcm_tokens:
            return
        sns_client.publish(TopicArn=topicArn, Message=json.dumps({"fcmTokens": fcm_tokens}))
        SNS_PUBLISH_TOTAL.inc()

def receive_messages():
    with tracer.start_as_current_span("sqs.receive_messages") as span:
        if not queue_url:
            raise RuntimeError("queue_url is not set")
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageAttributeNames=["All"],
            AttributeNames=["All"],
            VisibilityTimeout=60,
        )
        msgs = response.get("Messages", [])
        SQS_POLL_SIZE.observe(len(msgs))
        span.set_attribute("sqs.messages", len(msgs))
        return msgs

def delete_message(receipt_handle: str):
    with tracer.start_as_current_span("sqs.delete_message"):
        try:
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except Exception:
            logging.exception("❌ Failed to delete message")

def process_message(record: dict):
    with tracer.start_as_current_span("biz.process_message") as span:
        try:
            body = json.loads(record["Body"]) if isinstance(record.get("Body"), str) else record["Body"]
        except Exception as e:
            logging.exception("❌ Invalid SQS message body")
            MSG_PROCESSED_TOTAL.labels(result="error").inc()
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, "invalid body"))
            return False

        try:
            message = json.loads(body.get("Message", body)) if isinstance(body.get("Message", body), str) else body.get("Message", body)
        except Exception as e:
            logging.exception("❌ Invalid nested message (SNS-style)")
            MSG_PROCESSED_TOTAL.labels(result="error").inc()
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, "invalid nested"))
            return False

        user_sub = (message or {}).get("userSub") or body.get("userSub")
        features = (message or {}).get("features") or body.get("features")
        span.set_attribute("user.sub", user_sub or "")
        if features is None:
            logging.warning("⚠️ Missing 'features' in message. Skipping.")
            MSG_PROCESSED_TOTAL.labels(result="skipped").inc()
            span.set_status(Status(StatusCode.ERROR, "no features"))
            return False

        try:
            result = invoke_sagemaker(sageMakerEndpoint, features)
            span.set_attribute("model.prediction", result.get("prediction"))
            if result.get("prediction") == 1:
                tokens = get_fcm_tokens(user_sub) if user_sub else []
                publish_sns(tokens)
            MSG_PROCESSED_TOTAL.labels(result="success").inc()
            return True
        except Exception as e:
            logging.exception("❌ Error during message processing")
            MSG_PROCESSED_TOTAL.labels(result="error").inc()
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            return False

# ─────────────────────────── 메인 루프 ───────────────────────────
def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

    # 1) 트레이싱 먼저 켠다 (X-Ray로 보내기 위한 필수 단계)
    _init_tracing()

    # 2) /health & /metrics 서버 시작 (Prometheus 스크레이프용)
    _start_health_server()

    logging.info("🚀 SQS receive Worker started")

    global initialized
    while not initialized and RUNNING:
        try:
            with tracer.start_as_current_span("bootstrap.init"):
                init()
        except Exception:
            SQS_ERRORS_TOTAL.inc()
        if not initialized:
            time.sleep(5)

    while RUNNING:
        try:
            with tracer.start_as_current_span("sqs.poll"):
                messages = receive_messages()
                logging.info(f"📥 Received {len(messages)} messages from SQS")

            if not messages:
                logging.info("⏳ No messages, polling again ...")
                time.sleep(1)
                continue

            for msg in messages:
                logging.info(f"➡️ Processing message ID: {msg.get('MessageId','')}")
                ctx = _extract_ctx_from_sqs(msg)
                with tracer.start_as_current_span("sqs.process_message", context=ctx) as span:
                    span.set_attribute("sqs.message_id", msg.get("MessageId", ""))
                    ok = process_message(msg)
                    logging.info(f"✅ Message processed, success={ok}")
                    if ok:
                        logging.info("🗑 Deleting message from SQS ...")
                        delete_message(msg.get("ReceiptHandle", ""))
                logging.info("✅ Completed processing message")

        except Exception:
            logging.exception("🚨Error receiving messages")
            SQS_ERRORS_TOTAL.inc()
            time.sleep(5)

    logging.info("🔚 Exiting main loop.")

if __name__ == "__main__":
    main()
