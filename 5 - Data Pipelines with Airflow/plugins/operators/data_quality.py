from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = '',
                 tables = [],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        self.log.info('Getting Hook')
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            records = redshift_hook.get_records(f'SELECT COUNT(*) FROM {table}')
            if (len(records) < 1):
                self.log.error(f'{table} returned no results')
                raise ValueError(f'Data Quality check failed. {table} has no records')
            num_records = records[0][0]
            if num_records == 0:
                err_message = f'No records present in table {table}'
                self.log.error(err_message)
                raise ValueError(err_message)
            self.log.info(f'Data quality on table {table} check passed with {num_records} records')
        