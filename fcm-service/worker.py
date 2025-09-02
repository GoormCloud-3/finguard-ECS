# worker.py
import os
import json
import logging
import signal
import time

import boto3
from botocore.config import Config
from firebase_admin import credentials, initialize_app, messaging

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
DEFAULT_QUEUE_URL = "https://sqs.ap-northeast-2.amazonaws.com/381492026475/fcm-push-trade-queue"
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
    우선순위: FIREBASE_SA_JSON(raw/base64 JSON) -> FIREBASE_CRED_FILE(경로)
    """
    sa_env = os.getenv("FIREBASE_SA_JSON")
    try:
        if sa_env:
            try:
                sa_dict = json.loads(sa_env)
            except Exception:
                import base64
                sa_dict = json.loads(base64.b64decode(sa_env).decode("utf-8"))
            cred = credentials.Certificate(sa_dict)
            initialize_app(cred)
            logger.info("🔐 Firebase initialized from FIREBASE_SA_JSON")
            return
        path = os.getenv("FIREBASE_CRED_FILE", "service-account-key.json")
        cred = credentials.Certificate(path)
        initialize_app(cred)
        logger.info(f"📄 Firebase initialized from file: {path}")
    except Exception as e:
        logger.error(f"Firebase 초기화 실패: {e}")
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
            return processed

        if "Messages" not in res:
            logger.info("📭 대기열에 메시지가 없습니다.")
            return processed

        for msg in res["Messages"]:
            token_ctx = attach(extract(msg, getter=_SQS_GETTER))  # ★ 부모 트레이스 연결
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
                            with tracer.start_as_current_span("fcm.send_single") as sspan:
                                sspan.set_attribute("fcm.mode", "single")
                                sspan.set_attribute("fcm.token_masked", masked_single or "")
                                messaging.send(messaging.Message(**msg_kwargs, token=single_token))

                        elif topic:
                            with tracer.start_as_current_span("fcm.send_topic") as sspan:
                                sspan.set_attribute("fcm.mode", "topic")
                                sspan.set_attribute("fcm.topic", topic)
                                messaging.send(messaging.Message(**msg_kwargs, topic=topic))

                        elif condition:
                            with tracer.start_as_current_span("fcm.send_condition") as sspan:
                                sspan.set_attribute("fcm.mode", "condition")
                                sspan.set_attribute("fcm.condition", condition)
                                messaging.send(messaging.Message(**msg_kwargs, condition=condition))

                        else:
                            raise ValueError("Invalid payload: token | topic | condition 중 하나는 필요")

                        with tracer.start_as_current_span("sqs.delete") as dspan:
                            dspan.set_attribute("sqs.queue", QUEUE_URL.split("/")[-1])
                            sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])

                        processed += 1

                    except Exception as e:
                        logger.error(f"❌ 메시지 처리 오류 (msgId={msg.get('MessageId')}): {e}")
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
    _init_firebase_admin()

    empty = 0
    while not _SHOULD_STOP:
        n = poll_sqs_once()
        if n == 0:
            empty = min(empty + 1, IDLE_RESET)
            sleep_s = min(BACKOFF_BASE ** empty, BACKOFF_MAX)
            logger.debug(f"😴 빈 폴링: {empty}회, {sleep_s}s 대기")
            time.sleep(sleep_s)
        else:
            empty = 0
    logger.info("👋 안전 종료 완료")

if __name__ == "__main__":
    run_forever()
