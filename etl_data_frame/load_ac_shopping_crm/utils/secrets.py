import boto3
import json
import base64
from botocore.exceptions import ClientError

AWS_REGION = "ap-southeast-2"


def get_secret(secret_name):
    session = boto3.session.Session()
    client = session.client(
        service_name="secretsmanager",
        region_name=AWS_REGION,
        aws_access_key_id="AKIA2P7EBZVPG5DZWD6B",
        aws_secret_access_key="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",
    )

    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        '''{'ARN': 'arn:aws:secretsmanager:ap-southeast-2:721495903582:secret:postgres_ac_master-5XN3uj', 'Name': 'postgres_ac_master', 'VersionId': 'a5d59332-8a20-4984-8476-995a9fb91eea', 
            'SecretString': '{"username":"ac_master","password":"Datasquad2021","engine":"postgres","host":"ac-shopping-crm.cmxlethtljrk.ap-southeast-2.rds.amazonaws.com","port":5432,"database":"ac_shopping_crm","dbInstanceIdentifier":"ac-shopping-crm"}', 
            'VersionStages': ['AWSCURRENT'], 'CreatedDate': datetime.datetime(2021, 3, 6, 15, 44, 13, 352000, tzinfo=tzlocal()), 'ResponseMetadata': {'RequestId': 'e11aca12-5079-4687-870e-0664493eec0d', 
            'HTTPStatusCode': 200, 'HTTPHeaders': {'date': 'Tue, 23 Mar 2021 23:15:55 GMT', 'content-type': 'application/x-amz-json-1.1', 'content-length': '502', 
            'connection': 'keep-alive', 'x-amzn-requestid': 'e11aca12-5079-4687-870e-0664493eec0d'}, 'RetryAttempts': 0}}'''
       
    except ClientError as e:
        if e.response["Error"]["Code"] == "DecryptionFailureException":
            raise e
        elif e.response["Error"]["Code"] == "InternalServiceErrorException":
            raise e
        elif e.response["Error"]["Code"] == "InvalidParameterException":
            raise e
        elif e.response["Error"]["Code"] == "InvalidRequestException":
            raise e
        elif e.response["Error"]["Code"] == "ResourceNotFoundException":
            raise e
        print(e)
    else:
        if "SecretString" in get_secret_value_response:
            secret = get_secret_value_response["SecretString"]
        else:
            secret = base64.b64decode(get_secret_value_response["SecretBinary"])

    return json.loads(secret)
    

