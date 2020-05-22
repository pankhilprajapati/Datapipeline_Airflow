from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    Class:
       - LoadDimensionOperator = Load the data into the dimension table
    Arguments :
    
       - redshift_conn_id  = redshift connection id setup in airflow
       - table = name of dimension table
       - Qsql = sql query to insert the data
    """

    ui_color = '#80BD9E'
    

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 table = "",
                 Qsql = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.Qsql = Qsql
         
    def execute(self, context):
        """
        Arguments :
            - redshift_conn_id  = redshift connection id setup in airflow
            - table = name of dimension table
            - Qsql = sql query to insert the data
        Return: 
             - NONE
        Description :
        Load the data to the dimension table using sql query
        """
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        self.log.info(f"Loading the dimension table {self.table}")
        formattedSQL = f"""
        TRUNCATE TABLE {self.table};
        INSERT INTO {self.table}
        {self.Qsql};
        COMMIT;
        """
        redshift.run(formattedSQL)
