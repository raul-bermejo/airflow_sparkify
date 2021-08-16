from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults



class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
                COPY {}
                FROM '{}'
                ACCESS_KEY_ID '{}'
                SECRET_ACCESS_KEY '{}'
                FORMAT AS JSON '{}'
                ;
               """
    
    @apply_defaults
    def __init__(self,
                 # Operators params (with defaults) defined here
                 redshift_conn_id= "",
                 aws_credentials_id = "",
                 s3_bucket = "",
                 s3_key="",
                 table = "",
                 json_type = "auto",
                 extra_params = "",
                 *args, **kwargs):
        
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.extra_params = extra_params
        self.json_type = json_type
        
    def execute(self, context):
        
        self.log.info('Impelementing StageToRedshiftOperator')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Clearing Data from destination Redshift table
        redshift.run("DELETE FROM {}".format(self.table))
        
        #Staging Data from S3 to Redshift
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        # Format sql insert statement and run redshift_hook
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.json_type
        )
        redshift.run(formatted_sql)
