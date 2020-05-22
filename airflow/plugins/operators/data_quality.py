from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Class:
       - DataQualityOperator = check check the quality of the tables contain the function 
    Arguments :
       - redshift_conn_id  = redshift connection id setup in airflow
       - tables_arr = array for names of fact and dimension table
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 tables_arr=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables_arr = tables_arr

    def execute(self, context):
        """
        Arguments :
             - redshift_conn_id  = redshift connection id setup in airflow
             - tables_arr = array for names of fact and dimension table
        Return: 
             - NONE
        Description :
        take the tables and check for the null values in the records
        """
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        for table in self.tables_arr:
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table}")        
            if len(records) < 1 or len(records[0]) < 1:
                self.log.error(f"{table} returned no results")
                raise ValueError(f"Data quality check failed. {table} returned no results")
            num_records = records[0][0]
            if num_records == 0:
                self.log.error(f"No records present in destination table {table}")
                raise ValueError(f"No records present in destination {table}")
            self.log.info(f"Data quality on table {table} check passed with {num_records} records")