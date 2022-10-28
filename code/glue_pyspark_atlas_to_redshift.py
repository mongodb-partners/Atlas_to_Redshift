import sys
import json
import logging
import boto3
import pyspark

###################GLUE import##############
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.transforms import Relationalize
from awsglue.utils import getResolvedOptions
from awsglue.job import Job
from awsglue.transforms import *

#### ###creating spark and gluecontext ###############

logger = logging.getLogger()
logger.setLevel(logging.INFO)

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = pyspark.SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

## setup the MongoDB Credentials ###
def get_secret():

    secret_name = "<<SECRET_NAME>>"
    region_name = "<<REGION_NAME>>"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
    # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
    # We rethrow the exception by default.

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'DecryptionFailureException':
            # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InternalServiceErrorException':
            # An error occurred on the server side.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidParameterException':
            # You provided an invalid value for a parameter.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'InvalidRequestException':
            # You provided a parameter value that is not valid for the current state of the resource.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
        elif e.response['Error']['Code'] == 'ResourceNotFoundException':
            # We can't find the resource that you asked for.
            # Deal with the exception here, and/or rethrow at your discretion.
            raise e
    else:
        # Decrypts secret using the associated KMS key.
        # Depending on whether the secret is a string or binary, one of these fields will be populated.
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
            secrets_json = json.loads(secret)
            return (secrets_json['USERNAME'], secrets_json['PASSWORD'], secrets_json['SERVER_ADDR'])
        else:
            decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
            return decoded_binary_secret
            

user_name, password, server_addr = get_secret()

mongo_uri = "mongodb+srv://{}.mongodb.net/?retryWrites=true&w=majority'".format(server_addr) 

## Read from the MongoDB Atlas ###
read_mongo_options = {
    "uri": mongo_uri,
    "database": "<databasename>",   #update the databasename
    "collection": "<collection>", #update the collection
    "username": user_name, #"s3load",  #update the username
    "password": password #"s3load"  #update the password
}

## Read from the MongoDB Atlas ###

mongodb_atlas_ds = glueContext.create_dynamic_frame.from_options(connection_type="mongodb", connection_options= read_mongo_options)

# Script generated for node ApplyMapping
applymapping1 = ApplyMapping.apply(frame = mongodb_atlas_ds, mappings = [("state", "string", "state", "string"), ("account_length", "int", "account_length", "int"), ("area_code", "int", "area_code", "int"), ("phone", "string", "phone", "string"), ("intl_plan", "string", "intl_plan", "string"), ("vmail_plan", "string", "vmail_plan", "string"), ("vmail_message", "int", "vmail_message", "int"), ("day_mins", "double", "day_mins", "double"), ("day_calls", "int", "day_calls", "int"), ("day_charge", "double", "day_charge", "double"), ("total_charge", "double", "total_charge", "double"), ("eve_mins", "double", "eve_mins", "double"), ("eve_calls", "int", "eve_calls", "int"), ("eve_charge", "double", "eve_charge", "double"), ("night_mins", "double", "night_mins", "double"), ("night_calls", "int", "night_calls", "int"), ("night_charge", "double", "night_charge", "double"), ("intl_mins", "double", "intl_mins", "double"), ("intl_calls", "int", "intl_calls", "int"), ("intl_charge", "double", "intl_charge", "double"), ("cust_serv_calls", "int", "cust_serv_calls", "int"), ("churn", "string", "churn", "string"), ("record_date", "string", "record_date", "string")], transformation_ctx = "applymapping1")

## write to Redshift Cluster ###
redshift_datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = applymapping1, catalog_connection = "<Enter the Glue Connection name for Redshift>", connection_options = {"dbtable": "<enter the a tablename>", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "redshift_datasink")
job.commit()
