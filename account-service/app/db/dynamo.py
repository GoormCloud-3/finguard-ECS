import boto3
from boto3.dynamodb.conditions import Key

REGION = "ap-northeast-2"  # ✅ 원하는 AWS 리전

SSM = boto3.client("ssm", region_name=REGION)
dynamo = boto3.client("dynamodb", region_name=REGION)

table_name = None


def store_fcm_token(sub: str, fcm_tokens: list):
    global table_name
    if table_name is None:
        param = SSM.get_parameter(Name="/finguard/dev/finance/notification_table_name")
        table_name = param["Parameter"]["Value"]

        # DynamoDB에서 현재 아이템 조회
    resp = dynamo.get_item(TableName=table_name, Key={"user_id": {"S": sub}})

    existing_tokens = []
    if "Item" in resp:
        existing_tokens = [
            t["S"] for t in resp["Item"].get("fcmTokens", {}).get("L", [])
        ]

    # 중복 없는 새 토큰만 추가
    new_tokens = [t for t in fcm_tokens if t not in existing_tokens]

    if not existing_tokens and new_tokens:
        # 새 아이템 생성
        dynamo.put_item(
            TableName=table_name,
            Item={
                "user_id": {"S": sub},
                "fcmTokens": {"L": [{"S": t} for t in new_tokens]},
            },
        )
    elif new_tokens:
        # 기존 리스트에 새 토큰 추가
        dynamo.update_item(
            TableName=table_name,
            Key={"user_id": {"S": sub}},
            UpdateExpression="SET fcmTokens = list_append(if_not_exists(fcmTokens, :empty), :new)",
            ExpressionAttributeValues={
                ":new": {"L": [{"S": t} for t in new_tokens]},
                ":empty": {"L": []},
            },
        )
