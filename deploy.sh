#!/bin/bash

set -o allexport
source .env
set +o allexport

echo "🧪 배포 환경을 입력해주세요 (예: dev, stage, prod)"
read -r ENV

# 💡 서비스 리스트
SERVICES=("account-service" "user-service" "transaction-service" "notification-service" "sqs-service")

# 📦 Security Group, Subnet, IAM Role 등 가져오기
echo "🔍 AWS 리소스 정보 가져오는 중..."

export SG_DAO=$(aws ec2 describe-security-groups \
  --filters Name=group-name,Values="finguard-${ENV}-backend" Name=tag:Env,Values="${ENV}" \
  --query 'SecurityGroups[*].GroupId' --output text)

export SG_ALERT=$(aws ec2 describe-security-groups \
  --filters Name=group-name,Values="finguard-${ENV}-fraud-checker" Name=tag:Env,Values="${ENV}" \
  --query 'SecurityGroups[*].GroupId' --output text)

read -a PRIVATE_SUBNETS <<<"$(
  aws ec2 describe-subnets \
  --filters "Name=tag:Name,Values=finguard-${ENV}-lambda-*" \
  --query "Subnets[*].SubnetId" --output text
)"
export PRIVATE_SUBNET_1="${PRIVATE_SUBNETS[0]}"
export PRIVATE_SUBNET_2="${PRIVATE_SUBNETS[1]}"

export API_LAMBDA_ROLE_ARN=$(aws iam get-role --role-name "finguard-${ENV}-backend" \
  --query 'Role.Arn' --output text)

export SQS_LAMBDA_ROLE_ARN=$(aws iam get-role --role-name "finguard-${ENV}-fraud-checker" \
  --query 'Role.Arn' --output text)

export SQS_URL=$(aws sqs list-queues \
  --query "QueueUrls[?contains(@, 'finguard-${ENV}-trade-queue')]" --output text)

export SQS_ARN=$(aws sqs get-queue-attributes \
  --queue-url "$SQS_URL" \
  --attribute-names QueueArn \
  --query "Attributes.QueueArn" --output text)

export HTTP_API_ID=$(
  aws apigatewayv2 get-apis \
  --region ap-northeast-2 \
  --query "Items[?Name=='${ENV}-FinGuard-Backend'].ApiId" \
  --output text
)

# 🧾 출력
echo "✔ SG_DAO:              $SG_DAO"
echo "✔ SG_ALERT:            $SG_ALERT"
echo "✔ PRIVATE_SUBNET_1:    $PRIVATE_SUBNET_1"
echo "✔ PRIVATE_SUBNET_2:    $PRIVATE_SUBNET_2"
echo "✔ API_LAMBDA_ROLE_ARN: $API_LAMBDA_ROLE_ARN"
echo "✔ SQS_LAMBDA_ROLE_ARN: $SQS_LAMBDA_ROLE_ARN"
echo "✔ SQS_ARN:             $SQS_ARN"
echo "✔ HTTP_API_ID:         $HTTP_API_ID"

# 🚫 유효성 검사
if [[ -z "$SG_DAO" || -z "$HTTP_API_ID" || -z "$SG_ALERT" || -z "$PRIVATE_SUBNET_1" || -z "$PRIVATE_SUBNET_2" || -z "$API_LAMBDA_ROLE_ARN" || -z "$SQS_LAMBDA_ROLE_ARN" || -z "$SQS_ARN" ]]; then
  echo "❌ 에러: 필수 환경 변수 중 하나 이상이 비어 있습니다." >&2
  exit 1
fi

echo "🚀 배포할 거면 y를 입력하세요"
read -r confirm

if [[ "$confirm" == "y" ]]; then
  for svc in "${SERVICES[@]}"; do
    echo "🔄 Deploying $svc ..."
    (cd "$svc" && sls deploy --stage "$ENV")
  done
else
  echo "❌ 배포를 취소합니다."
fi
