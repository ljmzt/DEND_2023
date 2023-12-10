from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):
    '''
      perform quality checks
      run sqls using checks = (sql, expected_result)
      if expected_result is None, the sql outputs will be printed instead
    '''
    ui_color = '#89DA59'

    def __init__(self,
                 checks,
                 conn_id = 'redshift',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.checks = checks
        self.conn_id = conn_id

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id = self.conn_id)
        err_counts = 0
        for check in self.checks:
            sql, expect = check

            self.log.info(f'Running query {sql}')
            records = hook.get_records(sql)

            # if no expected result is given, print out the records
            if expect is None:
                self.log.info(f'Check your outputs {sql}')
                for record in records:
                    self.log.info(record)

            # compare with expected result
            else:
                try:
                    result = records[0][0]
                    if result != expect:
                        self.log.info(f'Query expect {expect}, but gives {result}')
                        err_counts += 1
                except:
                    self.log.info(f'Query failed')
                    err_counts += 1

        if err_counts == 0:
            self.log.info('Quality checks all passed')
        else:
            self.log.info('Some quality checks failed')