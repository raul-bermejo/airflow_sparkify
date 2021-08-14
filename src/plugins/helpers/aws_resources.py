import boto3
import json

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
                            cluster_type="",
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


# Spin up AWS resources
if __name__ == "__main__":
    # Create aws resources on the fly
    
    create_redshift_cluster = False
    if create_redshift_cluster:
        role_arn = create_iam_role(aws_key_id, aws_secret)
        redshift_conn_id = create_redshift_cluster(aws_key_id, aws_secret, role_arn)
        print(f"The endpoint of the created cluster is: {redshift_conn_id}.")