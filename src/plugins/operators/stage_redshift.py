from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3

def create_iam_role(aws_key_id, aws_secret,
                    rolename = "redhisft-airflow"):
    """
    Doc-string function later
    """
    
    iam = boto3.client('iam',
                        region_name='us-west-2',
                        aws_access_key_id=aws_key_id,
                        aws_secret_access_key=aws_secret)
    
    print('Creating new IAM role')
    try:
        print('Creating new IAM role')
        airflow_role = iam.create_role(Path='/',
                                RoleName=rolename,
                                Description="Allows Redshift clusters to call AWS services on your behalf.",
                                AssumeRolePolicyDocument=json.dumps({'Statement': [{'Action': 'sts:AssumeRole',
                                                                                    'Effect': 'Allow',
                                                                                    'Principal': {'Service': 'redshift.amazonaws.com'}}],
                                                                                    'Version': '2012-10-17'}))
        print("IAM policy was created succesfully")
        
    except Exception as e:
        print(e)
        
    print('Attaching Policy:')
    iam.attach_role_policy(RoleName= rolename,
                        PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")['ResponseMetadata']['HTTPStatusCode']
    
    role_arn = iam.get_role(RoleName=rolename)['Role']['Arn']
    
    return role_arn


def create_redshift_cluster(aws_key_id, aws_secret, role_arn,
                            cluster_type=""
                            node_type="", 
                            n_nodes=1,
                            db_name="airflow",
                            cluster_id="",
                            db_user="airflow",
                            db_pwd="airflow"):
    """
    Doc-string function later
    """
    
    redshift = boto3.client('redshift',
                            region_name='us-west-2',
                            aws_access_key_id=aws_key_id,
                            aws_secret_access_key=aws_secret)
    
    print(f"Creating Redshift cluster:")
    try:
        response = redshift.create_cluster(ClusterType=cluster_type,
                                        NodeType = node_type,
                                        NumberOfNodes=int(n_nodes),
                                        # Identifiers and credentials
                                        DBName=db_name,
                                        ClusterIdentifier=cluster_id,
                                        MasterUsername=db_user,
                                        MasterUserPassword=db_pwd,
                                        # Roles (for s3 access)
                                        IamRoles = [role_arn])
        
    except Exception as e:
        print(e)
    print(f"Cluster was created succesfully")
    # Obtaining redshift_conn_id
    ### TODO: Look at dl.cfg and extract CLUSTER values
    
    
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="udacity-dend",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,
                 *args, **kwargs):

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        self.table = table
        self.s3_key = s3_key
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers
        self.aws_credentials_id = aws_credentials_id
        
        # Extract aws key and secret
        #### TODO
        aws_key_id, aws_secret = '####'
        
        # Create aws resources on the fly
        role_arn = create_iam_role(aws_key_id, aws_secret)
        redshift_conn_id = create_redshift_cluster(aws_key_id, aws_secret, role_arn)
        
        # Copy data from s3 to Redshift
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = S3ToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.ignore_headers,
            self.delimiter
        )
        redshift.run(formatted_sql)







