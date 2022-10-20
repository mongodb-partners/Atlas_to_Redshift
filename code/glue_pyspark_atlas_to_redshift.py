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

mongo_uri = "mongodb+srv://<servernaem>/?retryWrites=true&w=majority" ####update the servername

## Read from the MongoDB Atlas ###
read_mongo_options = {
    "uri": mongo_uri,
    "database": "<databasename>",   #update the databasename
    "collection": "<collection>", #update the collection
    "username": "<username>",  #update the username
    "password": "<password>"  #update the password
}

## Read from the MongoDB Atlas ###

mongodb_atlas_ds = glueContext.create_dynamic_frame.from_options(connection_type="mongodb", connection_options= read_mongo_options)

# Script generated for node ApplyMapping
applymapping1 = ApplyMapping.apply(frame = mongodb_atlas_ds, mappings = [("state", "string", "state", "string"), ("account_length", "int", "account_length", "int"), ("area_code", "int", "area_code", "int"), ("phone", "string", "phone", "string"), ("intl_plan", "string", "intl_plan", "string"), ("vmail_plan", "string", "vmail_plan", "string"), ("vmail_message", "int", "vmail_message", "int"), ("day_mins", "double", "day_mins", "double"), ("day_calls", "int", "day_calls", "int"), ("day_charge", "double", "day_charge", "double"), ("total_charge", "double", "total_charge", "double"), ("eve_mins", "double", "eve_mins", "double"), ("eve_calls", "int", "eve_calls", "int"), ("eve_charge", "double", "eve_charge", "double"), ("night_mins", "double", "night_mins", "double"), ("night_calls", "int", "night_calls", "int"), ("night_charge", "double", "night_charge", "double"), ("intl_mins", "double", "intl_mins", "double"), ("intl_calls", "int", "intl_calls", "int"), ("intl_charge", "double", "intl_charge", "double"), ("cust_serv_calls", "int", "cust_serv_calls", "int"), ("churn", "string", "churn", "string"), ("record_date", "string", "record_date", "string")], transformation_ctx = "applymapping1")

## write to Redshift Cluster ###
redshift_datasink = glueContext.write_dynamic_frame.from_jdbc_conf(frame = applymapping1, catalog_connection = "<Enter the Glue Connection name for Redshift>", connection_options = {"dbtable": "<enter the a tablename>", "database": "dev"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "redshift_datasink")
job.commit()
