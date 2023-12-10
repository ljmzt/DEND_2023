from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadDimensionOperator(BaseOperator):
    '''
      load dimension table, sometimes you would like to truncate the table to restart
    '''

    ui_color = '#80BD9E'
    template_sql = """
      INSERT INTO {}
      {};
    """

    def __init__(self,
                 sql,
                 table,
                 conn_id = 'redshift',
                 truncate = False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = self.template_sql.format(table, sql)
        self.table = table
        self.conn_id = conn_id
        self.truncate = truncate

    def execute(self, context):
        hook = PostgresHook(postgres_conn_id = self.conn_id)
        if self.truncate:
            self.log.info(f'truncating table {self.table}')
            hook.run(f'TRUNCATE TABLE {self.table}')
        self.log.info(f'loading fact table {self.table}')
        hook.run(self.sql)

