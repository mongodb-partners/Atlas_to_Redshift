# Operational Analytics with Atlas and Redshift


## Introduction

The modern business world demands expedited decision-making, improved customer experience, and increased productivity. Gone are those days when business intelligence relied only on past data through batch processing. 
The order of the day is Operational analytics, which relies on measuring the existing or real-time operations of the business along with its past datatrends.

## Why Operational Analytics?
First and foremost there is an exponential increase in data volumes and their varieties. Traditional DW needs to evolve constantly to meet this demand of changing needs.
Most recent data are no more tabular in nature. Databases evolved into JSONs, Social logs, Images, Videos, and Time Series data.

Of late the Legacy relational database models are becoming showstoppers for programming and advanced analytics. With the legacy ways of Datastore, the performance becomes a big bottleneck as the data grows into Terabytes and petabytes.

So the need of the hour is to have a cohesive data model, which takes care of both the day-to-day operational data and its past.

Thus the coexistence of Atlas and Redshift evolves as the perfect fit for the business need.

## Integration Framework

The data from/to MongoDB Atlas can be migrated in two step approach

### Step 1: One-Time Load
MongoDB Atlas has direct connectors with  Apache Spark. Using the spark connectors the data is migrated from MongoDB Atlas to Redshift at one time

### Step2: Real-Time Data Sync
With the help of the MongoDB Atlas triggers, the data changes in the database can be continuously written to S3 bucket.
From the S3 bucket data can be loaded into the Redshift either through schedule AWS Glue jobs or can be accessed as an external tables.

In this demonstration we attempt to provided step by step approach for each of these scenarios.

## Pre-requisite: 
a) Good understanding of AWS Redshift , Glue and S3 services

b) Good understanding of MongoDB Atlas and Application services

c) VPC and Network settings are already setup as per the secuirty standards.

c) Redshift Database 

d) S3 bucket to store the json files

e) MongoDB Atlas cluster [for free cluster creation](https://www.mongodb.com/docs/atlas/tutorial/deploy-free-tier-cluster/)



## One-Time Load

### Architecture diagram

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/01.One-Time%20Data%20Load.png)

### Step by Step Instruction

Create a [MongoDB Atlas cluster](https://www.mongodb.com/docs/atlas/tutorial/deploy-free-tier-cluster)

Configure the MongoDB Atlas cluster [network security](https://www.mongodb.com/docs/atlas/security/add-ip-address-to-list/) and [access](https://www.mongodb.com/docs/atlas/tutorial/create-mongodb-user-for-cluster/) .

Load the sample [customer_activity](https://s3.amazonaws.com/redshift-downloads/redshift-ml/customer_activity/customer_activity.csv) data to a collection

Create a [Amazon Redshift Cluster ](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-launch-sample-cluster.html)

Configure the Amazon Redshift Cluster [network security](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-authorize-cluster-access.html) and [access](https://docs.aws.amazon.com/redshift/latest/gsg/rs-gsg-connect-to-cluster.html) .

Create a table using the script.

Use the AWS Glue Studio jobs feature to load data from MongoDB Atlas to Redshift    

[Pyspark job](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/Scripts/pyspark_atlastoredshift_customeractivity.py)



##  Real-Time Data Sync 


The Change Data Capture feature of MongoDB Atlas is utilized to capture the real-time data. Utilizing the Atlas Application Services and Data Federation's $out feature to write to S3 bucket, we capture the change data to S3 continuously.


### Architecture diagram 


#### With Glue: 


![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/11.AWS%20Glue%20s3tocatalog%20Connections%204.png)


#### with Redshift Spectrum (External Table)


![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/12.AWS%20Glue%20s3tocatalog%20Connections%204.png)


### Step by Step Instruction for setting up Glue Job
1. The data from MongoDB Atlas can be continuously written to S3 bucket using the Data Federation and MongoDB Atlas triggers. 
 Please refer the [link](https://www.mongodb.com/developer/products/atlas/automated-continuous-data-copying-from-mongodb-to-s3/) for the step by step instructions to capture the data to S3.

 For any further reference , please follow the MongoDB documentation [link](https://www.mongodb.com/docs/atlas/data-federation/config/config-aws-s3/)

2. create a AWS Glue job to move the data from S3 bucket to AWS Redshift
      
a. Create the Glue Connections Redshift Database.

Navigate to the AWS Glue console and to the "Data Catalog" menu on the left panel. Select "Connections" and Click on "Add Connection".  Enter the parameters taking  guidance of the screenshots attached. 

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/05.AWS%20Glue%20Redshift%20Connections%201.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/06.AWS%20Glue%20Redshift%20Connections%202.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/07.AWS%20Glue%20Redshift%20Connections%203.png)

b.Create the Glue Connection for S3 bucket.
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/10.AWS%20Glue%20s3tocatalog%20Connections%201.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/10.AWS%20Glue%20s3tocatalog%20Connections%202.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/10.AWS%20Glue%20s3tocatalog%20Connections%203.png)

Test these connections are working fine. 

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/10.AWS%20Glue%20s3tocatalog%20Connections%204.png)

C.Create the Crawler to populate Database and Tables in AWS Glue Catalog from S3.
Navigate to "Crawlers" menu on the left side panel and click "Add Crawler". Add all the required information for the crawler taking guidance from the attached screenshots.

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%201.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%202.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%203.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%204.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%205.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%206.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%207.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%208.png)

Once the crawlers are created successfully , run the crawler and ensure its successful completion.

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%2010.png)

Click the "Tables" menu from the left side and ensure the required tables are created. 

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/08.AWS%20Glue%20Redshift%20Crawler%209.png)

c.Now we are all set to create the Job to map and populate the redshift tables. Click " Jobs (legacy)" from the left side menu and click "Add job". Fill all the information required for creating the job, by taking the reference from screenshots attached.

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2001.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2002.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2003.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2004.png)

Select the option of creating a new table in the destination. Select the Redshift connection and the Database name you already created in the Redshift (as part of the pre-requisite).

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2005.png)

Validate the mapping and alter as required.

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2006.png)

The scripts for the conversions are created automatically and allowed for edit. Change the Redshift database table name to customer_activity.

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2007.png)

Run the job and ensure it's successful completion. Use the logs and Error logs generated for debugging. (if required)

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2008.png)
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2009.png)

Verify the data is populated successfully in the Redshift table.
![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/09.AWS%20Glue%20Job%2010.png)

### Step by Step Instruction for setting up Redshift Spectrum - External Table

Redshift Spectrum host the S3 bucket data as an external table. Provided the reference and steps to create the extenal table in the following link

[Redshift Specturm - external table](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/Scripts/external_tables_in_redshift.sql)

Reference [link](https://docs.aws.amazon.com/redshift/latest/dg/tutorial-query-nested-data.html) for querying JSON structure data in Redshift.


## Analytical Services using Redshift ML.

The data thus populated from MongoDB Atlas either through AWS Glue or as a external tables in Redshift can be utilized to train the models . Redshift ML services enables to directly use the Sagemaker Models to train and infer results.

Please refer the [link](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/Scripts/RedshiftML_for_CustomerActivity.sql) for training a model and infer result from the model.



## Business Use Cases:
Enterprises like Retail, Banking & Finance and Manufacturing are in great demand for the operational analytics for it's various real-time analytics.

A few are captured below

![](https://github.com/Babusrinivasan76/atlastoredshift/blob/main/images/04.Key%20Business%20Use%20Cases.png)

## Summary: 
With the synergy it creates by having Atlas for its operational efficiency and Redshift for its DWH excellence, all the “Operational Analytics” use cases can be delivered in no time. The solution can be extended to integrate the AI/ML needs using the AWS SageMaker.

Hope you are able to setup the integration successfully. For any further reference pls reach out to partners@mongodb.com


