from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_query="""
                INSERT INTO {} 
                {};
                """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 truncate,
                 table,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.truncate = truncate
        self.table = table

    def execute(self, context):
        self.log.info('Getting Hook')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        dim_insert=LoadDimensionOperator.insert_query.format(
            self.table,
            self.sql
        )
        if self.truncate:
            self.log.info('Truncating Dim Data')
            redshift_hook.run(f'TRUNCATE TABLE {self.table}')
        self.log.info('Inserting Dim Data')
        redshift_hook.run(dim_insert)
