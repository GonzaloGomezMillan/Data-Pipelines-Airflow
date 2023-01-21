from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    
    load_sql = """
        {}
        ACCESS_KEY_ID = '{}'
        SECRET_ACCESS_KEY = '{}'
        IGNOREHEADERS = '{}'
        DELIMITER = '{}'
    """
    

    @apply_defaults
    def __init__(self,
                 access_key_id = "",
                 secret_access_key = "",
                 sql_query = "",
                 ignore_headers = 1,
                 delimiter = ",",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.sql_query = sql_query
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Upserting data into the dimension tables...")
        formatted_sql = LoadDimensionOperator.load_sql.format(
            self.sql_query,
            credentials.access_key_id,
            credentials.secret_access_key,
            self.ignore_headers,
            self.delimiter
        )
        
        redshift.run(formatted_sql)

