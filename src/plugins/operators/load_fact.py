from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
         # Create PostreSQL connection and load fact data
        self.log.info('Create PostreSQL connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Execute load sql command for songplay fact table
        self.log.info('Inserting songplay data into fact table')
        sql_fact_load = S3ToRedshiftOperator.songplay_table_insert
        redshift.run(sql_fact_load)
