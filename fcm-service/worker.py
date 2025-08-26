import os
import json
import logging
import signal
import time
import boto3
from firebase_admin import credentials, initialize_app, messaging
from aws_xray_sdk.core import xray_recorder, patcher

# -------------------------
# Logging
# -------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    force=True,
)
logger = logging.getLogger(__name__)

# -------------------------
# Graceful shutdown
# -------------------------
_SHOULD_STOP = False
def _handle_sigterm(signum, frame):
    global _SHOULD_STOP
    _SHOULD_STOP = True
    logger.info("🛑 SIGTERM 수신: 안전 종료 준비")
signal.signal(signal.SIGTERM, _handle_sigterm)
signal.signal(signal.SIGINT, _handle_sigterm)


SERVICE_NAME = os.environ.get("XRAY_SERVICE_NAME", "fcm-worker")
xray_recorder.configure(
    service=SERVICE_NAME,
    daemon_address=os.getenv("AWS_XRAY_DAEMON_ADDRESS", "127.0.0.1:2000"),
    context_missing="IGNORE_ERROR",  
)
patcher.patch(["boto3"])  


REGION = os.environ.get("AWS_REGION", "ap-northeast-2")
QUEUE_URL = os.environ.get(
    "QUEUE_URL",
    "https://sqs.ap-northeast-2.amazonaws.com/381492026475/fcm-push-trade-queue",
)
sqs = boto3.client("sqs", region_name=REGION)
logger.info(f"SQS endpoint in use: {sqs.meta.endpoint_url}")


sa_env = os.environ.get("FIREBASE_SA_JSON")
try:
    if sa_env:
        
        sa_dict = json.loads(sa_env)
        cred = credentials.Certificate(sa_dict)
        initialize_app(cred)
        logger.info("🔐 Firebase initialized from FIREBASE_SA_JSON env (SSM).")
    else:
        
        FIREBASE_CRED_FILE = os.environ.get("FIREBASE_CRED_FILE", "service-account-key.json")
        cred = credentials.Certificate(FIREBASE_CRED_FILE)
        initialize_app(cred)
        logger.info(f"📄 Firebase initialized from file path: {FIREBASE_CRED_FILE}")
except Exception as e:
    logger.error(f"Firebase 초기화 실패: {e}")
    raise


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

def _xray_add_exception(e: Exception):
    try:
        sub = xray_recorder.current_subsegment()
        if sub is not None:
            sub.add_exception(e, stack=True)
    except Exception:
        pass

def poll_sqs_once() -> int:
    processed = 0
    xray_recorder.begin_segment(name="poll_sqs_once")
    try:
        with xray_recorder.in_subsegment("sqs_receive") as sub:
            res = sqs.receive_message(
                QueueUrl=QUEUE_URL,
                MaxNumberOfMessages=10,
                WaitTimeSeconds=20,
                VisibilityTimeout=60,
                MessageAttributeNames=["All"],
            )
            sub.put_annotation("queue", QUEUE_URL.split("/")[-1])
    except Exception as e:
        logger.error(f"SQS 수신 오류: {e}")
        _xray_add_exception(e)
        xray_recorder.end_segment()
        return processed

    if "Messages" not in res:
        logger.info("📭 대기열에 메시지가 없습니다.")
        xray_recorder.end_segment()
        return processed

    for msg in res["Messages"]:
        payload: dict = {}
        trace_id: str | None = None
        try:
            logger.info(f"📥 SQS 메시지 원본 수신 (msgId={msg.get('MessageId')})")

            with xray_recorder.in_subsegment("parse_payload") as sub:
                payload = parse_payload(msg.get("Body", "{}"))
                trace_id = payload.get("traceId")
                sub.put_annotation("traceId", trace_id or "none")
                meta = {k: v for k, v in payload.items() if k not in ("token", "fcmToken", "fcmTokens")}
                sub.put_metadata("payload_meta", meta)

            single_token = payload.get("token") or payload.get("fcmToken")
            
            masked_single = mask_token(single_token)

            logger.info(
                f"🧾 traceId={trace_id}, token={masked_single}, "
                f"topic={payload.get('topic')}, condition={payload.get('condition')}"
            )

            msg_kwargs = build_fcm_parts(payload)

            if single_token:
                with xray_recorder.in_subsegment("fcm_send_single") as sub:
                    sub.put_annotation("traceId", trace_id or "none")
                    sub.put_annotation("mode", "single")
                    sub.put_metadata("token_masked", masked_single)
                    message = messaging.Message(**msg_kwargs, token=single_token)
                    logger.info(f"➡️ FCM 단일 전송 시작 (traceId={trace_id}, token={masked_single})")
                    messaging.send(message)
                    logger.info(f"✅ 단일 푸시 성공 (traceId={trace_id}, token={masked_single})")

            elif payload.get("topic"):
                with xray_recorder.in_subsegment("fcm_send_topic") as sub:
                    sub.put_annotation("traceId", trace_id or "none")
                    sub.put_annotation("mode", "topic")
                    sub.put_metadata("topic", payload["topic"])
                    logger.info(f"➡️ FCM 토픽 전송 시작 (traceId={trace_id}, topic={payload['topic']})")
                    message = messaging.Message(**msg_kwargs, topic=payload["topic"])
                    messaging.send(message)
                    logger.info(f"✅ 토픽 푸시 성공 (traceId={trace_id}, topic={payload['topic']})")

            elif payload.get("condition"):
                with xray_recorder.in_subsegment("fcm_send_condition") as sub:
                    sub.put_annotation("traceId", trace_id or "none")
                    sub.put_annotation("mode", "condition")
                    sub.put_metadata("condition", payload["condition"])
                    logger.info(f"➡️ FCM 조건 전송 시작 (traceId={trace_id}, condition={payload['condition']})")
                    message = messaging.Message(**msg_kwargs, condition=payload["condition"])
                    messaging.send(message)
                    logger.info(f"✅ 조건 푸시 성공 (traceId={trace_id}, condition={payload['condition']})")

            else:
                raise ValueError("Invalid payload: token | topic | condition 중 하나는 필요")

            with xray_recorder.in_subsegment("sqs_delete") as sub:
                sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=msg["ReceiptHandle"])
                sub.put_annotation("traceId", trace_id or "none")
                logger.info(f"🗑️ SQS 메시지 삭제 완료 (traceId={trace_id}, msgId={msg.get('MessageId')})")

            processed += 1

        except Exception as e:
            logger.error(f"❌ 메시지 처리 오류 (msgId={msg.get('MessageId')}, traceId={trace_id}): {e}")
            _xray_add_exception(e)

    xray_recorder.end_segment()
    return processed


BACKOFF_BASE = int(os.getenv("BACKOFF_BASE", "2"))
BACKOFF_MAX  = int(os.getenv("BACKOFF_MAX", "30"))
IDLE_RESET   = int(os.getenv("IDLE_RESET", "5"))

def run_forever():
    logger.info("🚀 워커 시작 (상시 폴링 모드)")
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