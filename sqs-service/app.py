# app.py
import json
import logging
import time
import boto3
import pandas as pd
import signal
import io
from botocore.config import Config

from aws_xray_sdk.core import xray_recorder, patch
from aws_xray_sdk.core.context import Context
from aws_xray_sdk.core.models.trace_header import TraceHeader

# ───────────────────────── 기본 설정 ─────────────────────────
region = "ap-northeast-2"

# 네트워크 안정화: 재시도/타임아웃
_boto_cfg = Config(
    retries={"max_attempts": 10, "mode": "standard"},
    connect_timeout=5,
    read_timeout=65,  # SQS long polling(WaitTimeSeconds=20) 고려
)

# 클라이언트
ssm_client        = boto3.client("ssm",               region_name=region, config=_boto_cfg)
ddb_client        = boto3.client("dynamodb",          region_name=region, config=_boto_cfg)
sagemaker_client  = boto3.client("sagemaker-runtime", region_name=region, config=_boto_cfg)
sns_client        = boto3.client("sns",               region_name=region, config=_boto_cfg)
sqs_client        = boto3.client("sqs",               region_name=region, config=_boto_cfg)
s3_client        = boto3.client("s3",                region_name=region, config=_boto_cfg)


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

# ─────────────────────── 헬퍼 (데코레이터 제거) ───────────────────────

def get_param(name, with_decryption=False):
    # boto3 패치 덕분에 자동 subsegment 생성됨. 명시적으로도 감쌀 수 있음.
    with xray_recorder.in_subsegment("ssm:get_parameter"):
        logging.info("🔍 Fetching SSM parameter: %s (with_decryption=%s)", name, with_decryption)
        resp = ssm_client.get_parameter(Name=name, WithDecryption=with_decryption)
        return resp['Parameter']['Value']

def init():
    """필수 파라미터 로딩 (부모 세그먼트 내에서 호출 필요)"""
    logging.info("🔧 Initializing SQS service...")
    global sageMakerEndpoint, topicArn, tableName, queue_url, s3_bucket, s3_key, initialized
    if initialized:
        return

    try:
        # 각각 내부에서 ssm subsegment가 자동/명시적으로 열림
        sageMakerEndpoint = get_param("/finguard/dev/finance/fraud_sage_maker_endpoint_name")
        topicArn          = get_param("/finguard/dev/finance/alert_sns_topic")
        tableName         = get_param("/finguard/dev/finance/notification_table_name")
        queue_url         = get_param("/finguard/dev/finance/trade_queue_host")
        s3_bucket         = get_param("/finguard/dev/finance/model_s3_bucket")
        s3_key            = get_param("/finguard/dev/finance/model_s3_key")

        if not all([sageMakerEndpoint, topicArn, tableName, queue_url]):
            logging.error("❌ One or more SSM parameters are empty.")
            raise ValueError("One or more SSM parameters are empty.")
        if not queue_url.startswith("https://"):
            logging.warning("⚠️ queue_url doesn't look like a full URL: %s", queue_url)

        initialized = True
        logging.info("✅ Initialized: endpoint=%s, topic=%s, table=%s, queue=%s, s3_bucket=%s, s3_key=%s",
                     sageMakerEndpoint, topicArn, tableName, queue_url, s3_bucket, s3_key)
    except Exception:
        logging.exception("❌ Initialization failed")
        # 컨테이너 종료 방지: 상위에서 재시도
        time.sleep(10)

def get_fcm_tokens(user_sub: str):
    with xray_recorder.in_subsegment("ddb:get_fcm_tokens"):
        logging.info("🔍 Retrieving FCM tokens for user: %s", user_sub)
        resp = ddb_client.get_item(
            TableName=tableName,
            Key={'user_id': {'S': user_sub}},
            ProjectionExpression='fcmTokens'
        )
        tokens = []
        if 'Item' in resp and 'fcmTokens' in resp['Item']:
            tokens = [t['S'] for t in resp['Item']['fcmTokens']['L']]
        logging.info("✅ FCM tokens: %s", tokens)
        return tokens

def invoke_sagemaker(endpoint: str, features):
    with xray_recorder.in_subsegment("sagemaker:invoke_endpoint"):
        logging.info("📡 Calling SageMaker endpoint: %s", endpoint)
        response = sagemaker_client.invoke_endpoint(
            EndpointName=endpoint,
            ContentType='application/json',
            Body=json.dumps({'features': features})
        )
        result_str = response['Body'].read().decode()
        try:
            result = json.loads(result_str)
        except json.JSONDecodeError:
            logging.error("❌ Invalid JSON from SageMaker: %s", result_str)
            raise
        logging.info("✅ SageMaker response: %s", result)
    # --S3에 features 재학습용 csv 저장--
    # ---- S3에 누적 저장 ----
    global s3_bucket, s3_key

    # 1. 기존 CSV 다운로드
    try:
        obj = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        df_existing = pd.read_csv(io.BytesIO(obj['Body'].read()))
        logging.info("📥 Existing features loaded from s3://%s/%s", s3_bucket, s3_key)
    except s3_client.exceptions.NoSuchKey:
        df_existing = pd.DataFrame()  # 파일 없으면 새로 생성
        logging.info("ℹ️ No existing features file at s3://%s/%s, starting fresh", s3_bucket, s3_key)

    # 2. 새로운 features를 DataFrame으로 변환
    df_new = pd.DataFrame([features])  # features가 dict라 가정

    # 3. 기존 + 새 features 합치기
    df_total = pd.concat([df_existing, df_new], ignore_index=True)

    # 4. 다시 CSV로 저장
    csv_buffer = io.StringIO()
    df_total.to_csv(csv_buffer, index=False)
    s3_client.put_object(Bucket=s3_bucket, Key=s3_key, Body=csv_buffer.getvalue())
    logging.info("📤 Features saved to s3://%s/%s", s3_bucket, s3_key)
    
    return result
    
def _build_trace_header() -> str:
    """
    X-Ray 전파 헤더 형식: "Root=1-...;Parent=...;Sampled=1|0"
    가능하면 현재 subsegment의 id를 Parent로 사용.
    """
    ent = xray_recorder.current_subsegment() or xray_recorder.current_segment()
    if not ent:
        return ""
    root = ent.trace_id          # e.g. 1-6f21f3b1-5c7c3c5e3a9c1c0d2e1f0a9b
    parent = ent.id              # segment or subsegment id
    sampled = "1" if getattr(ent, "sampled", False) else "0"
    return f"Root={root};Parent={parent};Sampled={sampled}"

def publish_sns(fcm_tokens):
    with xray_recorder.in_subsegment("sns:publish"):
        logging.info("🔔 Publishing SNS with tokens: %s", fcm_tokens)
        if not fcm_tokens:
            logging.info("ℹ️ No FCM tokens. Skipping SNS publish.")
            return

        
        trace_header = _build_trace_header()  # ✅ 전파 헤더 생성
        sns_client.publish(
            TopicArn=topicArn,
            Message=json.dumps({'fcmTokens': fcm_tokens}),
            MessageAttributes={
                'X-Amzn-Trace-Id': {
                    'DataType': 'String',
                    'StringValue': trace_header or '',
                }
            }
        )
        logging.info("✅ SNS message published")

def receive_messages():
    with xray_recorder.in_subsegment("sqs:receive_messages"):
        logging.info("📥 Receiving messages from SQS queue: %s", queue_url)
        if not queue_url:
            logging.error("❌ queue_url is not set. Cannot receive messages.")
            raise RuntimeError("queue_url is not set")
        response = sqs_client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=20,
            MessageAttributeNames=['All'],
            AttributeNames=['All'],
            VisibilityTimeout=60
        )
        msgs = response.get('Messages', [])
        logging.info("📥 Received %d messages", len(msgs))
        return msgs

def delete_message(receipt_handle: str):
    with xray_recorder.in_subsegment("sqs:delete_message"):
        try:
            logging.info("🗑️ Deleting message")
            sqs_client.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle)
        except Exception:
            logging.exception("❌ Failed to delete message")

def process_message(record: dict):
    """비즈 처리. 호출하는 쪽에서 세그먼트/서브세그먼트 열어둔다."""
    try:
        body = json.loads(record['Body']) if isinstance(record.get('Body'), str) else record['Body']
    except Exception:
        logging.exception("❌ Invalid SQS message body")
        return
    logging.info("📬 Processing message: %s", body)

    try:
        message = json.loads(body.get('Message', body)) if isinstance(body.get('Message', body), str) else body.get('Message', body)
    except Exception:
        logging.exception("❌ Invalid nested message (SNS-style)")
        return

    user_sub = (message or {}).get('userSub') or body.get('userSub')
    features = (message or {}).get('features') or body.get('features')
    if features is None:
        logging.warning("⚠️ Missing 'features' in message. Skipping.")
        return

    try:
        logging.info("🔍 Processing features for user: %s, features: %s", user_sub, features)
        result = invoke_sagemaker(sageMakerEndpoint, features)
        logging.info("✅ SageMaker result: %s", result)

        if result.get('prediction') == 1:
            tokens = get_fcm_tokens(user_sub) if user_sub else []
            publish_sns(tokens)
            logging.info("✅ Notification sent to user: %s tokens=%s", user_sub, tokens)
        else:
            logging.info("ℹ️ No fraud detected for user: %s", user_sub)
    except Exception:
        logging.exception("❌ Error during message processing")
        return

# ─────────────────────────── 메인 루프 ───────────────────────────

def main():
    # 로깅
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [%(name)s] %(message)s")
    logging.info("🚀 SQS receive Worker started")

    # X-Ray 초기화 (개발 중 100% 추적 권장)
    xray_recorder.configure(
        service="sqs-service",
        context=Context(),
        sampling=False,           # ← 개발에서는 전부 기록
    )
    # boto3만 패치 (requests 미사용 시 에러 방지)
    patch(('boto3',))

    # --- 초기화 재시도 루프: init 호출은 'bootstrap:init' 세그먼트 안에서 ---
    global initialized
    while not initialized and RUNNING:
        seg = xray_recorder.begin_segment("bootstrap:init")
        try:
            init()
        finally:
            xray_recorder.end_segment()
        if not initialized:
            time.sleep(5)

    # --- 메인 폴링 루프 ---
    while RUNNING:
        try:
            # 1) 메시지 수신은 별도 'sqs:poll' 세그먼트에서
            poll_seg = xray_recorder.begin_segment("sqs:poll")
            try:
                logging.info("📥 Polling SQS...")
                messages = receive_messages()
            finally:
                xray_recorder.end_segment()  # ★ 수신만 감싸고 바로 닫는다 (세그먼트 중첩 방지)

            if not messages:
                time.sleep(1)
                continue

            logging.info("📥 Received %d messages to process", len(messages))

            # 2) 각 메시지는 **자기만의 세그먼트**로 처리 (Trace 헤더 있으면 이어붙임)
            for msg in messages:
                attrs   = msg.get("MessageAttributes", {}) or {}
                hdr_val = (
                    (attrs.get("trace_header") or {}).get("StringValue")
                    or (attrs.get("X-Amzn-Trace-Id") or {}).get("StringValue")
                    or ""
                )

                # trace header 파싱
                if hdr_val:
                    th = TraceHeader.from_header_str(hdr_val)
                    # 새 세그먼트 (상위 poll 세그먼트와 **중첩되지 않음**)
                    xray_recorder.begin_segment(
                        name="sqs:process_message",
                        traceid=th.root,
                        parent_id=th.parent
                    )
                    is_subsegment = False
                else:
                    # 헤더 없으면 독립 세그먼트로만 (중첩 방지 위해 subsegment 대신 segment 권장)
                    xray_recorder.begin_segment("sqs:process_message")
                    is_subsegment = False

                try:
                    seg = xray_recorder.current_segment()
                    if seg:
                        seg.put_annotation("queue_url", queue_url or "")
                        seg.put_annotation("message_id", msg.get("MessageId", ""))

                    process_message(msg)
                    delete_message(msg["ReceiptHandle"])
                except Exception:
                    logging.exception("Error processing message")
                finally:
                    # 위에서 항상 begin_segment로 열었으므로 end_segment로 닫는다
                    xray_recorder.end_segment()

        except Exception:
            logging.exception("Error receiving messages")
            time.sleep(5)

    logging.info("👋 Exiting main loop.")

if __name__ == "__main__":
    main()
