from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators import StageToRedshiftOperator

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
         # Create PostreSQL connection and load fact data
        self.log.info('Create PostreSQL connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        # Determine what dimension has to be loaded
        if self.table == "users":
            sql_dim_load = StageToRedshiftOperator.users_table_insert 
        elif self.table == "song":
            sql_dim_load = StageToRedshiftOperator.song_table_insert 
        elif self.table == "artist":
            sql_dim_load = StageToRedshiftOperator.artist_table_insert 
        elif self.table == "time":
            sql_dim_load = StageToRedshiftOperator.artist_table_insert
        else:
            self.log.info('''Error: Dimension Table to be loaded was not defined or was inserted incorrectly.
                              Available tables are: users, song, artist or time.''')
            sql_dim_load = None
            
        # Execute load sql command for dim table
        self.log.info('Loading data into select dim table')
        redshift.run(sql_dim_load)
