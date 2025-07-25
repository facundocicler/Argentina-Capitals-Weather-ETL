from airflow.models import BaseOperator
from config.config_loader import get_mysql_config
from src.utils import wait_for_mysql

class WaitForMySQLOperator(BaseOperator):
    def execute(self, context):

        mysql_config = get_mysql_config()

        wait_for_mysql(mysql_config)