from airflow.contrib.hooks.postgres_hook import PostgresHook
from airflow.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    template_fields = ('s3_key',)
    
    copy_sql = """ 
                  COPY {table_name} 
                  FROM '{s3_path}' 
                  ACCESS_KEY_ID '{access_key}' 
                  SECRET_ACCESS_KEY '{secret_key}' 
                  REGION 'us-east-2b' 
                  {file_type} 
                  timeformat as 'epochmillisecs'; 
                """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 table="",
                 redshift_conn_id="",
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key = "",
                 file_type = "",
                 *args, **kwargs):

       
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.file_type = file_type

    def execute(self, context):
        
        #connection steps
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id) 
        
        #clear existing raw data
        self.log.info("Clearing data from destination redshift table")
        redshift.run("DELETE FROM {}".format(self.table))
        
        #copy the new raw data
        self.log.info(f"In progrss: copying {self.table} from s3 to redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)



        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table_name= self.table,
            s3_path = s3_path,
            access_key = credentials.access_key,
            secret_key = credentials.secret_key,
            file_type = self.file_type
        )
        
        redshift.run(formatted_sql)
        self.log.info(f"Done: copying {self.table} from s3 to redshift")

