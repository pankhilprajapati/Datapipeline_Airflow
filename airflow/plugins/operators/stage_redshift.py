from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    Class:
       - StageToRedshiftOperator = Load the data from s3 to redshift
    Arguments :
    
       - redshift_conn_id  = redshift connection id setup in airflow
            - aws_credentials_id = aws key to connect with the s3 bucket
            - table = name of dimension table
            - s3_key = key for the s3 bucket 
            - s3_bucket = bucket name in s3
            - path = json path of the file,
            - ftype = file type csv or json
    """
    ui_color = '#358140'
    
    json_copy = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    COMPUPDATE OFF
    """
    csv_copy = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    IGNOREHEADER {}
    DELIMITER '{}'
    """
    
    @apply_defaults
    def __init__(self,
                
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_key="",
                 s3_bucket="",
                 path = "",
                 ftype="",
                 delimiter=",",
                 ignore_headers=1,               
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_key = s3_key
        self.s3_bucket = s3_bucket
        self.path = path
        self.ftype = ftype
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
    def execute(self, context):
        """
        Arguments :
            - redshift_conn_id  = redshift connection id setup in airflow
            - aws_credentials_id = aws key to connect with the s3 bucket
            - table = name of dimension table
            - s3_key = key for the s3 bucket 
            - s3_bucket = bucket name in s3
            - path = json path of the file,
            - ftype = file type csv or json
        Return: 
             - NONE
        Description :
        Load the data from s3 to redshift
        """
        aws = AwsHook(self.aws_credentials_id)
        aws_creds = aws.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self.log.info("Copying data from S3 to Redshift")
        rend_key = self.s3_key.format(**context)
        s3path = f"s3://{self.s3_bucket}/{rend_key}"
        
        if self.path == "json":
            formattedsql = StageToRedshiftOperator.json_copy.format(
                self.table,
                s3_path,
                aws_creds.access_key,
                aws_creds.secret_key,
                self.path
            )
            redshift.run(formattedsql)
        if self.path == 'csv':
            formattedsql = StageToRedshiftOperator.csv_copy.format(
                self.table,
                s3_path,
                aws_creds.access_key,
                aws_creds.secret_key,
                self.ignore_headers,
                self.delimiter
            )
            redshift.run(formattedsql)


