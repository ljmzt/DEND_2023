from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.models import BaseOperator

class StageToRedshiftOperator(BaseOperator):
    '''
      staging a redshift table
      NOTE: the table will be truncated first
    '''
    ui_color = '#358140'

    # be careful about the '' which is needed
    template_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
    """

    def __init__(self,
                 aws_conn_id = 'aws_credentials',
                 redshift_conn_id = 'redshift',
                 table = '',
                 s3_bucket = '',
                 s3_key = '',
                 s3_format = "FORMAT AS JSON 'auto'",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_conn_id = aws_conn_id
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.s3_path = 's3://{}/{}'.format(s3_bucket, s3_key)
        self.s3_format = s3_format

    def execute(self, context):
        credentials = AwsBaseHook(self.aws_conn_id).get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info(f'deleting records from {self.table}')
        redshift.run('TRUNCATE TABLE {}'.format(self.table))
        
        self.log.info(f'copied data to {self.table}')
        sql = self.template_sql.format(self.table, self.s3_path, credentials.access_key, credentials.secret_key, self.s3_format)
        redshift.run(sql)
        





