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

## Read from the MongoDB Atlas ###

mongo_uri = "mongodb+srv://<mongodbservername>/?retryWrites=true&w=majority" ####update the servername

## Read from the MongoDB Atlas ###
read_mongo_options = {
    "uri": mongo_uri,
    "database": "<databasename>",   #update the databasename
    "collection": "<collectionname>", #update the collection
    "username": "<username>",  #update the username
    "password": "<password>"  #update the password
}

mongodb_atlas_ds = glueContext.create_dynamic_frame.from_options(connection_type="mongodb", connection_options= read_mongo_options)

# Script generated for node ApplyMapping
ApplyMapping_node2 = ApplyMapping.apply(
    frame=mongodb_atlas_ds,
    mappings=[
        ("record_date", "string", "record_date", "string"),
        ("vmail_plan", "string", "vmail_plan", "string"),
        ("day_charge", "double", "day_charge", "double"),
        ("area_code", "string", "area_code", "string"),
        ("account_length", "int", "account_length", "string"),
        ("churn", "string", "churn", "string"),
        ("cust_serv_calls", "int", "cust_serv_calls", "long"),
        ("night_mins", "double", "night_mins", "double"),
        ("intl_plan", "string", "intl_plan", "string"),
        ("total_charge", "double", "total_charge", "double"),
        ("day_calls", "int", "day_calls", "string"),
        ("night_charge", "double", "night_charge", "double"),
        ("eve_mins", "double", "eve_mins", "double"),
        ("intl_charge", "double", "intl_charge", "double"),
        ("phone", "string", "phone", "string"),
        ("eve_calls", "int", "eve_calls", "string"),
        ("night_calls", "int", "night_calls", "string"),
        ("_id", "string", "_id", "string"),
        ("state", "string", "state", "string"),
        ("vmail_message", "int", "vmail_message", "string"),
        ("eve_charge", "double", "eve_charge", "double"),
        ("intl_mins", "double", "intl_mins", "double"),
        ("day_mins", "double", "day_mins", "double"),
        ("intl_calls", "int", "intl_calls", "string"),
        ("date", "string", "date", "string"),
        ("hour", "string", "hour", "string"),
        ("mm", "string", "mm", "string"),
    ],
    transformation_ctx="ApplyMapping_node2",
)

# Script generated for node Amazon Redshift
AmazonRedshift_node = glueContext.write_dynamic_frame.from_catalog(
    frame=ApplyMapping_node2,
    database="<database>",  # Update the database 
    table_name="<tablename>",  # update the tablename  dev_public_tablenmae
    redshift_tmp_dir=args["TempDir"],
    transformation_ctx="AmazonRedshift_node",
)

job.commit()
