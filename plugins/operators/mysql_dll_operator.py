import pymysql
from airflow.models import BaseOperator
from config.config_loader import get_mysql_config
from pymysql.constants import CLIENT
from pathlib import Path

class MySqlDDLOperator(BaseOperator):
    template_fields = ("sql",)

    def __init__(
        self,
        sql: str,    
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.sql = sql

    def execute(self, context):
        cfg = get_mysql_config()
        sql_text = (
            Path(self.sql).read_text(encoding="utf-8")
            if str(self.sql).strip().endswith(".sql")
            else self.sql
        )
        conn = pymysql.connect(
            host=cfg["host"],
            port=cfg["port"],
            user=cfg["user"],
            password=cfg["password"],
            charset="utf8mb4",
            autocommit=True,
            client_flag=CLIENT.MULTI_STATEMENTS, # MULTI_STATEMENTS permite varias sentencias en una sola llamada
        )
        with conn.cursor() as cur:
            cur.execute(sql_text)
        conn.close()
