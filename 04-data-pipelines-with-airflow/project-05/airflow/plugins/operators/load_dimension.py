from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 sql_stmt="",
                 append=False,
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)

        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql_stmt = sql_stmt
        self.append = append

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"Loading dimension table {self.table}")
        sql = ""

        if self.append:
            sql = """
                    BEGIN;
                    INSERT INTO {}
                    {};
                    COMMIT;""".format(self.table, self.sql_stmt)
        else:
            sql = """
                    BEGIN;
                    TRUNCATE TABLE {}; 
                    INSERT INTO {}
                    {}; 
                    COMMIT;""".format(self.table, self.table, self.sql_stmt)

        redshift.run(sql)
