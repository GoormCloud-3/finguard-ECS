# worker.py
import os
import json
import logging
import signal
import time
import base64
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import boto3
from botocore.config import Config
from firebase_admin import credentials, initialize_app, messaging

# ---------- Prometheus Client ----------
from prometheus_client import Counter, Histogram
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

# Prometheus metrics (전역)
FCM_PROCESSED = Counter("fcm_processed_total", "FCM messages processed", ["mode"])  # mode: single|topic|condition
FCM_ERRORS    = Counter("fcm_errors_total",    "Errors while processing")
SQS_POLL_SIZE = Histogram("sqs_poll_batch_size", "Messages received per poll", buckets=(0, 1, 2, 5, 10))
FCM_SEND_SEC  = Histogram("fcm_send_seconds",  "Firebase send latency (s)")

# ---------- Health ----------
HEALTH_PORT = int(os.getenv("HEALTH_PORT", "9400"))

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

# ---------- Logging ----------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True,
)
logger = logging.getLogger("fcm-worker")

# ---------- Graceful shutdown ----------
_SHOULD_STOP = False
def _handle_sigterm(signum, frame):
    global _SHOULD_STOP
    _SHOULD_STOP = True
    logger.info("🛑 SIGTERM 수신: 안전 종료 준비")
signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)

# ---------- AWS clients ----------
REGION = os.getenv("AWS_REGION", "ap-northeast-2")
DEFAULT_QUEUE_URL = "https://sqs.ap-northeast-2.amazonaws.com/381492026475/finguard-dev-fcm-push-trade-queue"
QUEUE_URL = os.getenv("QUEUE_URL", DEFAULT_QUEUE_URL)

_boto_cfg = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    connect_timeout=5,
    read_timeout=65,  # long polling 고려
)
sqs = boto3.client("sqs", region_name=REGION, config=_boto_cfg)
logger.info(f"SQS endpoint in use: {sqs.meta.endpoint_url}")
logger.info(f"Using SQS queue: {QUEUE_URL}")

# ---------- OpenTelemetry ----------
from opentelemetry import trace
from opentelemetry.trace import Status, StatusCode
from opentelemetry.propagate import extract
from opentelemetry.context import attach, detach
from opentelemetry.propagators.textmap import Getter

tracer = trace.get_tracer("fcm-service")  # service.name은 OTEL_RESOURCE_ATTRIBUTES로도 세팅됨

class _SQSAttrGetter(Getter):
    """SNS→SQS MessageAttributes에서 전파 헤더를 꺼내기 위한 Getter"""
    def get(self, carrier, key):
        attrs = carrier.get("MessageAttributes") or {}
        # X-Ray/W3C 둘 다 시도
        for k in (key, key.title(), key.upper(), "X-Amzn-Trace-Id", "traceparent"):
            v = attrs.get(k)
            if isinstance(v, dict) and "StringValue" in v:
                return [v["StringValue"]]
        return []
    def keys(self, carrier):
        attrs = carrier.get("MessageAttributes") or {}
        return list(attrs.keys())

_SQS_GETTER = _SQSAttrGetter()

# ---------- Firebase credentials ----------
def _init_firebase_admin():
    """
    우선순위:
    1) FIREBASE_SA_JSON (raw JSON 또는 base64)
    2) FIREBASE_SA_PARAM (SSM 파라미터 이름)  ← 백업 경로
    3) FIREBASE_CRED_FILE (기본: service-account-key.json)
    """
    sa_env   = os.getenv("FIREBASE_SA_JSON")
    sa_param = os.getenv("FIREBASE_SA_PARAM")  # ex) /prod/firebase-service-account-json
    file_path = os.getenv("FIREBASE_CRED_FILE", "service-account-key.json")

    try:
        if sa_env:
            logger.info("🔐 FIREBASE_SA_JSON detected (len=%d)", len(sa_env))
            try:
                cred_dict = json.loads(sa_env)
            except Exception:
                cred_dict = json.loads(base64.b64decode(sa_env).decode("utf-8"))
            cred = credentials.Certificate(cred_dict)
            initialize_app(cred)
            logger.info("✅ Firebase initialized from FIREBASE_SA_JSON")
            return

        if sa_param:
            logger.info("🔐 Fetching Firebase SA from SSM parameter: %s", sa_param)
            ssm = boto3.client("ssm", region_name=REGION, config=_boto_cfg)
            res = ssm.get_parameter(Name=sa_param, WithDecryption=True)
            val = res["Parameter"]["Value"]
            try:
                cred_dict = json.loads(val)
            except Exception:
                cred_dict = json.loads(base64.b64decode(val).decode("utf-8"))
            cred = credentials.Certificate(cred_dict)
            initialize_app(cred)
            logger.info("✅ Firebase initialized from SSM param")
            return

        logger.warning("⚠️ FIREBASE_SA_JSON & FIREBASE_SA_PARAM both missing; falling back to file: %s", file_path)
        cred = credentials.Certificate(file_path)
        initialize_app(cred)
        logger.info("✅ Firebase initialized from file")

    except Exception as e:
        logger.error("Firebase 초기화 실패: %s", e)
        raise

# ---------- helpers ----------
def mask_token(token: str | None) -> str | None:
    if not token:
        return token
    if len(token) <= 8:
        return "***"
    return f"{token[:4]}***{token[-4:]}"

def parse_payload(body_str: str) -> dict:
    try:
        outer = json.loads(body_str)
        if isinstance(outer, dict) and "Message" in outer:
            inner = json.loads(outer["Message"]) if isinstance(outer["Message"], str) else outer["Message"]
            return inner or {}
        return outer or {}
    except Exception as e:
        logger.error(f"Payload 파싱 실패: {e}")
        FCM_ERRORS.inc()
        return {}

def build_fcm_parts(payload: dict) -> dict:
    data = None
    if isinstance(payload.get("data"), dict):
        data = {str(k): str(v) for k, v in payload["data"].items()}
    return {
        "notification": messaging.Notification(
            title=payload.get("title", "알림"),
            body=payload.get("body", f"환영합니다, {payload.get('userId', '사용자')}님!")
        ),
        "data": data
    }

# ---------- main poll loop helpers ----------
def poll_sqs_once() -> int:
    processed = 0
    with tracer.start_as_current_span("sqs.poll_once"):
        try:
            with tracer.start_as_current_span("sqs.receive") as span:
                res = sqs.receive_message(
                    QueueUrl=QUEUE_URL,
                    MaxNumberOfMessages=10,
                    WaitTimeSeconds=20,
                    VisibilityTimeout=60,
                    MessageAttributeNames=["All"],
                )
                span.set_attribute("sqs.queue", QUEUE_URL.split("/")[-1])
        except Exception as e:
            logger.error(f"SQS 수신 오류: {e}")
            FCM_ERRORS.inc()
            return processed

        msgs = res.get("Messages", [])
        SQS_POLL_SIZE.observe(len(msgs))

        if not msgs:
            logger.info("📭 대기열에 메시지가 없습니다.")
            return processed

        for msg in msgs:
            token_ctx = attach(extract(msg, getter=_SQS_GETTER))  # 부모 트레이스 연결
            try:
                with tracer.start_as_current_span("msg.handle") as span:
                    try:
                        payload = parse_payload(msg.get("Body", "{}"))

                        single_token = payload.get("token") or payload.get("fcmToken")
                        topic = payload.get("topic")
                        condition = payload.get("condition")
                        masked_single = mask_token(single_token)

                        span.set_attribute("fcm.has_token", bool(single_token))
                        span.set_attribute("fcm.has_topic", bool(topic))
                        span.set_attribute("fcm.has_condition", bool(condition))

                        msg_kwargs = build_fcm_parts(payload)

                        if single_token:
                            with tracer.start_as_current_span("fcm.send_single") as sspan, FCM_SEND_SEC.time():
                                sspan.set_attribute("fcm.mode", "single")
                                sspan.set_attribute("fcm.token_masked", masked_single or "")
                                messaging.send(messaging.Message(**msg_kwargs, token=single_token))
                            FCM_PROCESSED.labels(mode="single").inc()

                        elif topic:
                            with tracer.start_as_current_span("fcm.send_topic") as sspan, FCM_SEND_SEC.time():
                                sspan.set_attribute("fcm.mode", "topic")
                                sspan.set_attribute("fcm.topic", topic)
                                messaging.send(messaging.Message(**msg_kwargs, topic=topic))
                            FCM_PROCESSED.labels(mode="topic").inc()

                        elif condition:
                            with tracer.start_as_current_span("fcm.send_condition") as sspan, FCM_SEND_SEC.time():
                                sspan.set_attribute("fcm.mode", "condition")
                                sspan.set_attribute("fcm.condition", condition)
                                messaging.send(messaging.Message(**msg_kwargs, condition=condition))
                            FCM_PROCESSED.labels(mode="condition").inc()

                        else:
                            raise ValueError("Invalid payload: token | topic | condition 중 하나는 필요")

                        with tracer.start_as_current_span("sqs.delete") as dspan:
                            dspan.set_attribute("sqs.queue", QUEUE_URL.split("/")[-1])
                            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])

                        processed += 1

                    except Exception as e:
                        logger.error(f"❌ 메시지 처리 오류 (msgId={msg.get('MessageId')}): {e}")
                        FCM_ERRORS.inc()
                        span.record_exception(e)
                        span.set_status(Status(StatusCode.ERROR, str(e)))
                        # 삭제하지 않음 → 재시도 가능
            finally:
                detach(token_ctx)

    return processed

# ---------- run ----------
BACKOFF_BASE = int(os.getenv("BACKOFF_BASE", "2"))
BACKOFF_MAX  = int(os.getenv("BACKOFF_MAX", "30"))
IDLE_RESET   = int(os.getenv("IDLE_RESET", "5"))

def run_forever():
    logger.info("🚀 FCM 워커 시작 (상시 폴링 모드)")
    
    logging.info("🔐 Firebase Admin SDK 초기화 시도 ...")
    _init_firebase_admin()
    logging.info("✅ Firebase Admin SDK 초기화 완료")

    empty = 0
    while not _SHOULD_STOP:
        logging.info("🔄 SQS 폴링 중...")
        n = poll_sqs_once()
        logging.info(f"📬 이번 폴링에서 {n}개 메시지 처리 완료")
        if n == 0:
            logging.info("⏳ No messages, polling again ...")
            empty = min(empty + 1, IDLE_RESET)
            sleep_s = min(BACKOFF_BASE ** empty, BACKOFF_MAX)
            logger.debug(f"😴 빈 폴링: {empty}회, {sleep_s}s 대기")
            time.sleep(sleep_s)
        else:
            logging.info("✅ 메시지 처리 완료, 즉시 다음 폴링 ...")
            empty = 0
    logger.info("🔚 안전 종료 완료")

if __name__ == "__main__":
    _start_health_server()
    run_forever()
