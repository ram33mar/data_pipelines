from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 truncate_insert='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.truncate_insert = truncate_insert

    def execute(self, context):
        self.log.info(f'Loading Dimension Table {self.table} to Redshift Database')
        redshift_hook = PostgresHook(self.redshift_conn_id)

        if not self.truncate_insert:
            self.log.info(f'Inserting Dimension Table {self.table}')
            insert_sql="INSERT INTO {} {}"
            redshift_hook.run(insert_sql.format(self.table,self.sql))

        elif self.truncate_insert:
            self.log.info(f'Truncating Dimension Table {self.table}')
            truncate_sql="TRUNCATE TABLE {}"
            redshift_hook.run(truncate_sql.format(self.table))
            self.log.info(f'{self.table} truncated successfully')

            self.log.info(f'Inserting Dimension Table {self.table}')
            insert_sql="INSERT INTO {} {}"
            redshift_hook.run(insert_sql.format(self.table,self.sql))
            self.log.info(f'{self.table} loaded successfully in Redshift')

