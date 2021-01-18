from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    insert_query="""
                INSERT INTO {} 
                {};
                """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id,
                 sql,
                 table, 
                 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql=sql
        self.table=table

    def execute(self, context):
        self.log.info('Getting Hook')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        fact_insert = LoadFactOperator.insert_query.format(
            self.table,
            self.sql
        )
        self.log.info('Inserting Fact Data')
        redshift_hook.run(fact_insert)
