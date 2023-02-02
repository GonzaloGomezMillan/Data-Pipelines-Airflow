from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    load_sql = """
        {}
        ACCESS_KEY_ID = '{}'
        SECRET_ACCESS_KEY = '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
        """
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 sql_query = "",
                 delimiter = ",",
                 ignore_headers = 1,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.sql_query = sql_query
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Upserting data to Redshift")
        formatted_sql = LoadDimensionOperator.load_sql.format(
            self.sql_query,
            credentials.access_key,
            credentials.secret_access_key,
            self.ignore_headers,
            self.delimiter
        )
        
        redshift.run(formatted_sql)
