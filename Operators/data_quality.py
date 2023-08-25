from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 dqChecks='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dqChecks = dqChecks

    def execute(self, context):
        self.log.info('Performing Data Quality Checks on Fact and Dimesnsion Tables')
        redshift_hook = PostgresHook(self.redshift_conn_id)
        failedQualityChecks=[]
        errorCount=0

        for dqCheck in self.dqChecks:
            dqCheckSql = dqCheck.get('checkSql')
            expectedResult= dqCheck.get('expectedResult')
            comparisonOperator = dqCheck.get('operator')

            result = redshift_hook.get_records(dqCheckSql)[0]
            
            comparison=f"{result[0]} {comparisonOperator} {expectedResult}"

            self.log.info(f"Evaluated Data Quality Condition: {comparison}")
            if eval(comparison):    
               self.log.info(f"Condition: {comparison} passed")
            else:
                errorCount += 1
                failedQualityChecks.append(dqCheckSql)

        if errorCount > 0:
            self.log.info('Data Quality Issue Found in the below checks')
            self.log.info(failedQualityChecks)
            raise ValueError('Data Quality Check Failed')

        else:
            self.log.info('All Data Quality Checks Passed')


