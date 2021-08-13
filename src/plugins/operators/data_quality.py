from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('Starting Data quality check.')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        error_count = 0
        failed_checks = []
        
        # Logging in case input table is null
        if len(tables) == 0:
            self.log.info('No tables were input for check so there was no data quality checks.')
         
        # Perform data quality check for each input table
        for table in tables:
            sql_query = f'SELECT COUNT(*) FROM {table} WHERE userid IS NULL'
            result = redshift.get_records(sql)[0]
            
            expected_result = 0
            
            if expected_result != int(result):
                error_count += 1
                failed_checks.append(table)
         
        # Reporting of data quality checks
        if error_count > 0:
            self.log.info('Data Quality checks failed:')
            self.log.info(f'There were {error_count} check fails corresponding to these tables', ",".join(failed_checks))
        else:
            self.log.info('The data quality checks were passed succesfully!')
       
      
            
