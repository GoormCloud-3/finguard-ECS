import pymysql
import boto3
import threading

REGION = "ap-northeast-2"
SSM = boto3.client("ssm", region_name=REGION)
RDS = boto3.client("rds", region_name=REGION)

_lock = threading.Lock()
_initialized = False
_config = {}


def get_ssm_param(name: str, decrypt: bool = False) -> str:
    response = SSM.get_parameter(Name=name, WithDecryption=decrypt)
    return response["Parameter"]["Value"]


def init_config():
    global _initialized, _config
    with _lock:
        if _initialized:
            return
        host = get_ssm_param("/finguard/dev/finance/rds_proxy_hostname")
        user = get_ssm_param("/finguard/dev/finance/rds_db_username")
        db = get_ssm_param("/finguard/dev/finance/rds_db_name")
        print(host, "host")
        print(user, "user")
        print(db, "db")
        _config = {
            "host": host,
            "user": user,
            "db": db,
        }
        _initialized = True


def get_connection():
    init_config()
    host = _config["host"]
    user = _config["user"]
    dbname = _config["db"]

    token = RDS.generate_db_auth_token(
        DBHostname=host, Port=3306, DBUsername=user, Region=REGION
    )

    connection = pymysql.connect(
        host=host,
        user=user,
        password=token,
        database=dbname,
        port=3306,
        ssl={"ca": "/etc/ssl/certs/ca-certificates.crt"},  # Ubuntu base
        cursorclass=pymysql.cursors.DictCursor,
        client_flag=pymysql.constants.CLIENT.MULTI_STATEMENTS,
    )
    return connection
