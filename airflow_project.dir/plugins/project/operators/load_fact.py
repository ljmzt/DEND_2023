from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadFactOperator(BaseOperator):
    '''
      insert into fact table, usually we do not want truncate here
    '''

    ui_color = '#F98866'
    template_sql = """
      INSERT INTO {}
      {};
    """

    def __init__(self,
                 sql,
                 table,
                 conn_id = 'redshift',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.sql = self.template_sql.format(table, sql)
        self.table = table
        self.conn_id = conn_id

    def execute(self, context):
        self.log.info(f'loading fact table {self.table}')
        hook = PostgresHook(postgres_conn_id = self.conn_id)
        hook.run(self.sql)

