# app.py
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

# OpenTelemetry
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.propagate import extract
from opentelemetry.propagators.textmap import Getter

# ───────────────────────── 기본 설정 ─────────────────────────
region = "ap-northeast-2"

# 네트워크 안정화: 재시도/타임아웃
_boto_cfg = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    connect_timeout=5,
    read_timeout=65,  # SQS long polling 고려
)

# 클라이언트
ssm_client        = boto3.client("ssm",               region_name=region, config=_boto_cfg)
ddb_client        = boto3.client("dynamodb",          region_name=region, config=_boto_cfg)
sagemaker_client  = boto3.client("sagemaker-runtime", region_name=region, config=_boto_cfg)
sns_client        = boto3.client("sns",               region_name=region, config=_boto_cfg)
sqs_client        = boto3.client("sqs",               region_name=region, config=_boto_cfg)
s3_client         = boto3.client("s3",                region_name=region, config=_boto_cfg)
sts_client        = boto3.client("sts",               region_name=region, config=_boto_cfg)

sageMakerEndpoint = None
topicArn          = None
tableName         = None
queue_url         = None
s3_bucket         = None
s3_key            = None
initialized       = False

# 종료 신호
RUNNING = True
def _handle_sigterm(signum, frame):
    global RUNNING
    logging.info("🛑 SIGTERM received, shutting down gracefully...")
    RUNNING = False

signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT,  _handle_sigterm)

# OpenTelemetry Tracer
tracer = trace.get_tracer("sqs-service")

# ─────────────────────── 헬스 서버 (ECS healthcheck 용) ───────────────────────
class _Health(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200); self.end_headers(); self.wfile.write(b"ok")
        else:
            self.send_response(404); self.end_headers()

def _start_health_server():
    srv = HTTPServer(("0.0.0.0", 9400), _Health)
    threading.Thread(target=srv.serve_forever, daemon=True).start()

# ─────────────────────── 컨텍스트 추출 Getter ───────────────────────
class _AttrGetter(Getter):
    def get(self, carrier, key):
        # 대소문자 혼용 방지
        if not carrier or not key:
            return []
        # 일반화된 조회
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
    """
    SQS 메시지에서 OTel/X-Ray 컨텍스트 복원:
    - SQS MessageAttributes (traceparent, baggage, X-Amzn-Trace-Id 등)
    - SNS -> SQS 인 경우 Body(SNS envelope)의 MessageAttributes 도 검사
    """
    carrier = {}

    # 1) SQS MessageAttributes: { Name : {DataType:String, StringValue: "..."} }
    attrs = (msg.get("MessageAttributes") or {})
    for k, v in attrs.items():
        if isinstance(v, dict) and "StringValue" in v:
            carrier[k] = v["StringValue"]

    # 2) SNS envelope 내부 MessageAttributes: { Name : {Type: "String", Value: "..."} }
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
        # Body가 JSON이 아닐 수도 있음
        pass

    # 흘러온 키들 중 표준 키 우선 확보
    norm = {}
    for k, v in carrier.items():
        lk = k.lower()
        if lk in ("x-amzn-trace-id", "traceparent", "baggage"):
            norm[k] = v
    if not norm and carrier:
        norm = carrier  # 혹시 표준 키 없이 들어온 경우도 최후 시도

    return extract(_AttrGetter(), norm)

# ─────────────────────── 헬퍼 ───────────────────────
def get_param(name, with_decryption=False):
    with tracer.start_as_current_span("ssm.get_parameter") as span:
        span.set_attribute("param.name", name)
        span.set_attribute("param.decrypt", with_decryption)
        resp = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp["Parameter"]["Value"]

def init():
    """필수 파라미터 로딩"""
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
        time.sleep(10)  # 상위에서 재시도

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
    with tracer.start_as_current_span("sagemaker.invoke_endpoint") as span:
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

    # S3에 features 저장(누적 CSV)
    with tracer.start_as_current_span("s3.append_features") as span:
        span.set_attribute("s3.bucket", s3_bucket or "")
        span.set_attribute("s3.key", s3_key or "")
        try:
            try:
                obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
                df_existing = pd.read_csv(io.BytesIO(obj["Body"].read()))
            except s3_client.exceptions.NoSuchKey:
                df_existing = pd.DataFrame()

            df_new = pd.DataFrame([features])
            df_total = pd.concat([df_existing, df_new], ignore_index=True)

            csv_buffer = io.StringIO()
            df_total.to_csv(csv_buffer, index=False)
            s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())
        except Exception as e:
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            # 저장 실패해도 추론 결과는 반환
    return result

def publish_sns(fcm_tokens):
    with tracer.start_as_current_span("sns.publish") as span:
        span.set_attribute("sns.topic", topicArn or "")
        if not fcm_tokens:
            return
        # botocore 계측이 켜져 있으면 traceparent/X-Ray 헤더 자동 주입
        sns_client.publish(
            TopicArn=topicArn,
            Message=json.dumps({"fcmTokens": fcm_tokens}),
        )

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
        span.set_attribute("sqs.messages", len(msgs))
        return msgs

def delete_message(receipt_handle: str):
    with tracer.start_as_current_span("sqs.delete_message"):
        try:
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except Exception:
            logging.exception("❌ Failed to delete message")

def process_message(record: dict):
    """비즈 처리"""
    with tracer.start_as_current_span("biz.process_message") as span:
        try:
            body = json.loads(record["Body"]) if isinstance(record.get("Body"), str) else record["Body"]
        except Exception as e:
            logging.exception("❌ Invalid SQS message body")
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, "invalid body"))
            return

        try:
            message = json.loads(body.get("Message", body)) if isinstance(body.get("Message", body), str) else body.get("Message", body)
        except Exception as e:
            logging.exception("❌ Invalid nested message (SNS-style)")
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, "invalid nested"))
            return

        user_sub = (message or {}).get("userSub") or body.get("userSub")
        features = (message or {}).get("features") or body.get("features")
        span.set_attribute("user.sub", user_sub or "")
        if features is None:
            logging.warning("⚠️ Missing 'features' in message. Skipping.")
            span.set_status(Status(StatusCode.ERROR, "no features"))
            return

        try:
            result = invoke_sagemaker(sageMakerEndpoint, features)
            span.set_attribute("model.prediction", result.get("prediction"))
            if result.get("prediction") == 1:
                tokens = get_fcm_tokens(user_sub) if user_sub else []
                publish_sns(tokens)
            # else: 정상 거래
        except Exception as e:
            logging.exception("❌ Error during message processing")
            span.record_exception(e)
            span.set_status(Status(StatusCode.ERROR, str(e)))
            return

# ─────────────────────────── 메인 루프 ───────────────────────────
def main():
    # 로깅
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    _start_health_server()

    # 누가 실행 중인지(역할 확인)
    try:
        arn = sts_client.get_caller_identity()["Arn"]
        logging.info(f"👤 STS caller identity: {arn}")
    except Exception as e:
        logging.warning(f"STS whoami failed: {e}")

    logging.info("🚀 SQS receive Worker started")

    # --- 초기화 재시도 루프 ---
    global initialized
    while not initialized and RUNNING:
        with tracer.start_as_current_span("bootstrap.init"):
            init()
        if not initialized:
            time.sleep(5)

    # --- 메인 폴링 루프 ---
    while RUNNING:
        try:
            with tracer.start_as_current_span("sqs.poll"):
                messages = receive_messages()

            if not messages:
                time.sleep(1)
                continue

            for msg in messages:
                # 1) 메시지에서 컨텍스트 복원
                ctx = _extract_ctx_from_sqs(msg)

                # 2) 복원 컨텍스트로 "메시지 단위" 루트 스팬 시작
                with tracer.start_as_current_span("sqs.process_message", context=ctx) as span:
                    span.set_attribute("sqs.message_id", msg.get("MessageId", ""))
                    process_message(msg)
                    delete_message(msg.get("ReceiptHandle", ""))

        except Exception:
            logging.exception("Error receiving messages")
            time.sleep(5)

    logging.info("👋 Exiting main loop.")

if __name__ == "__main__":
    main()
