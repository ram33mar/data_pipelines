from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.secrets.metastore import MetastoreBackend

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 aws_credentials_id='',
                 table='',
                 s3_bucket_path='',
                 s3_sub_path='',
                 format_json='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket_path = s3_bucket_path
        self.s3_sub_path = s3_sub_path
        self.format_json = format_json

    def execute(self, context):

        self.log.info(f'StageToRedshiftOperator: Loading {self.s3_bucket_path} to Redshift Database')
        
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift_hook = PostgresHook(self.redshift_conn_id)

        self.log.info(f'Truncating Table {self.table}')
        redshift_hook.run("TRUNCATE TABLE {}".format(self.table))
        
        self.log.info(f'Copying S3 Data to Redshift Table {self.table}')

        if self.s3_sub_path != "":
            data_path=self.s3_sub_path.format(**context)
            s3_bucket_path="{}/{}/".format(self.s3_bucket_path,data_path)
            self.log.info("Derived S3 Path: {}".format(s3_bucket_path))
        else:
            s3_bucket_path=self.s3_bucket_path
            self.log.info("Derived S3 Path: {}".format(s3_bucket_path))
        
        if self.format_json =="":
            format_json="auto"
        else:
            format_json=self.format_json
        
        redshift_hook.run(StageToRedshiftOperator.COPY_SQL.format(
            self.table,
            s3_bucket_path,
            aws_connection.login,
            aws_connection.password,
            format_json
            ))

        self.log.info(f'S3 Data copied into Redshift Table {self.table}')