from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class DropTableOperator(BaseOperator):
    '''
      drop tables, needs to give conn_id
    '''

    ui_color = '#89DA59'

    def __init__(self,
                 tables,
                 conn_id = 'redshift',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.tables = tables
        
    def execute(self, context):        
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        for table in self.tables:
            self.log.info(f'Dropping table {table}')
            sql = 'DROP TABLE IF EXISTS {}'.format(table)
            hook.run(sql)
