import boto3
import json
import configparser
from create_tables import *

def create_iam_role(aws_key_id, aws_secret,
                    rolename = "redhisft-airflow"):
    """
    Doc-string function later
    """
    
    # Initialize iam client
    iam = boto3.client('iam',
                        region_name='us-west-2',
                        aws_access_key_id=aws_key_id,
                        aws_secret_access_key=aws_secret)
    
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


def create_redshift_cluster(aws_key_id, aws_secret,
                            cluster_type="multi-node",
                            node_type="dc2.large", 
                            n_nodes=2,
                            db_name="airflow-sparkify",
                            cluster_id="airflow-sparkify",
                            db_user="airflow-sparkify",
                            db_pwd="Passw0rd"):
    """
    Doc-string function later
    """

    # Call ARN role:
    # role_arn = create_iam_role(aws_key_id, aws_secret)
    
    # Create redshift admin client
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
                                        IamRoles = ['arn:aws:iam::684401159067:role/redhisft-airflow'])
        

        print(f"Cluster was created succesfully.")
        
    except Exception as e:
        print(e)
    

    return None


# Spin up AWS resources
if __name__ == "__main__":
    # Read AWS credentials from .cfg file
    config = configparser.ConfigParser()
    config.read_file(open('aws.cfg'))
    aws_key_id = config.get('AWS','AWS_KEY_ID')
    aws_secret = config.get('AWS','AWS_SECRET')

    # Define cluster and database parameters
    cluster_type="multi-node"
    node_type="dc2.large"
    n_nodes=2
    db_name="airflow-sparkify"
    cluster_id="airflow-sparkify"
    db_user="airflow-sparkify"
    db_pwd="Passw0rd"

#     DWH_ENDPOINT = dwhcluster.caskbqlv7js2.us-west-2.redshift.amazonaws.com
# DWH_DB=dwh
# DWH_DB_USER=dwhuser
# DWH_DB_PASSWORD=Passw0rd
# DWH_PORT=5439

    


    # Create aws resources on the fly
    create_redshift = True
    if create_redshift:
        create_redshift_cluster(aws_key_id, aws_secret,
                                cluster_type, node_type,
                                n_nodes, db_name, cluster_id,
                                db_user, db_pwd)
    
    
    print(f"Attempt table creation:")
    # try:



    #     for query in create_table_queries:
    #         response = redshift_data.execute_statement(
    #             ClusterIdentifier= cluster_id,
    #             Database= db_name,
    #             DbUser= db_user,
    #             Sql=query
    #         )
    #     print("="*70)
    #     print(f"The SQL tables were created succesfully")
    #     print("="*70)
    # except Exception as e:
    #     print(e)
