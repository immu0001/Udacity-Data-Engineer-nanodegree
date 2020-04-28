from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 tables=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):

        redshift_hook = PostgresHook(self.redshift_conn_id)

        for tbl in self.tables:
            self.log.info(f"Checking Data Quality for {tbl} table")
            records = redshift_hook.get_records(f"SELECT COUNT(*) FROM {tbl}")
            if len(records[0]) < 1 or len(records) < 1:
                raise ValueError(f"Data quality check failed: Table {tbl} has no results")
            num_records = records[0][0]
            if num_records < 1:
                raise ValueError(f"Data quality check failed: Table {tbl} contained 0 rows")
            self.log.info(f"Data quality on table {tbl} check passed with {records[0][0]} records")
