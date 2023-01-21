from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
#     template_fields = ("s3_key",)
    load_sql = """
        {}
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        IGNOREHEADER {}
        DELIMITER '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials = "",
                 sql_query = "",
                 ignore_headers = 1,
                 delimiter = ",",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials = aws_credentials
        self.sql_query = sql_query
        self.ignore_headers = ignore_headers
        self.delimiter = delimiter

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info("Upserting data into the fact table...")
        formatted_sql = LoadFactOperator.copy_sql.format(
            self.sql_query,
            credentials.access_key,
            credentials.secret_access_key,
            self.ignore_header,
            self.delimiter
        )
        
        redshift.run(sql_formatted)
        