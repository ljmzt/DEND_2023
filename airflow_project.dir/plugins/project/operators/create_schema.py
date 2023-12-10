from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

class CreateSchemaOperator(BaseOperator):
    '''
      create scheme for given sql (can include multiple create table) and conn_id
    '''

    ui_color = '#89DA59'

    def __init__(self,
                 sql,
                 conn_id = 'redshift',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.sql = sql
        
    def execute(self, context):
        self.log.info('Creating schema')
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        hook.run(self.sql)
