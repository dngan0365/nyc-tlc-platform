import os

FEATURE_FLAGS = {
    "ENABLE_TEMPLATE_PROCESSING": True,
    "GLOBAL_ASYNC_QUERIES": False,
}

SECRET_KEY = os.environ.get(
    "SUPERSET_SECRET_KEY",
    "super-secret-key-change-this"
)

SESSION_COOKIE_SAMESITE = None
SESSION_COOKIE_SECURE = False
SESSION_COOKIE_HTTPONLY = True

SQLALCHEMY_DATABASE_URI = "sqlite:////app/superset_home/superset.db"

GLOBAL_ASYNC_QUERIES = False
SQLLAB_ASYNC_TIME_LIMIT_SEC = 0
SUPERSET_SQLLAB_BACKEND_PERSISTENCE = False

SUPERSET_WEBSERVER_TIMEOUT = 600
SQLLAB_TIMEOUT = 600
SQL_MAX_ROW = 100000

from superset.sqllab.sql_json_executer import SynchronousSqlJsonExecutor

SQL_JSON_EXECUTOR = SynchronousSqlJsonExecutor