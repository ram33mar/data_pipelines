from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 table='',
                 sql='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        self.log.info(f'Loading Fact Table {self.table} in Redshift')

        redshift_hook = PostgresHook(self.redshift_conn_id)

        insert_sql="INSERT INTO {} {}"
        redshift_hook.run(insert_sql.format(self.table,self.sql))

        self.log.info(f'{self.table} loaded successfully in Redshift')
