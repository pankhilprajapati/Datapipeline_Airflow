from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    Class:
       - LoadFactOperator = Load the data into the fact table
    Arguments :
    
       - redshift_conn_id  = redshift connection id setup in airflow
       - table = name of dimension table
       - Qsql = sql query to insert the data
    """

    ui_color = '#F98866'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 Qsql= "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
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
        Load the data to the fact table using sql query
        """
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info(f"loading the fact table {self.table}")
        formattedSQL = f"""
        INSERT INTO {self.table}
        {self.Qsql};
        COMMIT;
        """
        redshift.run(formattedSQL)