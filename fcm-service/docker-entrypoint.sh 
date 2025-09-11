#!/usr/bin/env sh
set -eu

# 필수값 체크 (없으면 실패)
: "${WHATAP_LICENSE?WHATAP_LICENSE is required}"
: "${WHATAP_HOSTS?WHATAP_HOSTS is required}"   # 예: "13.124.11.223/13.209.172.35"
: "${WHATAP_APP_NAME:=user-service}"
: "${WHATAP_PROCESS_NAME:=python}"

# 설정 파일 생성 (이미지 레이어에 남지 않음)
cat > "${WHATAP_HOME}/whatap.conf" <<EOF
license=${WHATAP_LICENSE}
whatap.server.host=${WHATAP_HOSTS}
app_name=${WHATAP_APP_NAME}
app_process_name=${WHATAP_PROCESS_NAME}
EOF

# 최종 명령 실행
exec "$@"
