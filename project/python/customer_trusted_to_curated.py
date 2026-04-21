import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1776721079992 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="accelerometer_trusted", transformation_ctx="AmazonS3_node1776721079992")

# Script generated for node Amazon S3
AmazonS3_node1776721078508 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customer_trusted", transformation_ctx="AmazonS3_node1776721078508")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1776721141270 = ApplyMapping.apply(frame=AmazonS3_node1776721079992, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("z", "double", "right_z", "double"), ("birthday", "string", "right_birthday", "string"), ("sharewithpublicasofdate", "long", "right_sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "right_sharewithresearchasofdate", "long"), ("registrationdate", "long", "right_registrationdate", "long"), ("customername", "string", "right_customername", "string"), ("user", "string", "right_user", "string"), ("y", "double", "right_y", "double"), ("sharewithfriendsasofdate", "long", "right_sharewithfriendsasofdate", "long"), ("x", "double", "right_x", "double"), ("timestamp", "long", "right_timestamp", "long"), ("lastupdatedate", "long", "right_lastupdatedate", "long")], transformation_ctx="RenamedkeysforJoin_node1776721141270")

# Script generated for node Join
Join_node1776721106134 = Join.apply(frame1=AmazonS3_node1776721078508, frame2=RenamedkeysforJoin_node1776721141270, keys1=["email"], keys2=["right_user"], transformation_ctx="Join_node1776721106134")

# Script generated for node Drop Fields
DropFields_node1776721179814 = DropFields.apply(frame=Join_node1776721106134, paths=["right_birthday", "right_y", "right_sharewithpublicasofdate", "right_sharewithfriendsasofdate", "right_sharewithresearchasofdate", "right_serialnumber", "right_x", "right_registrationdate", "right_customername", "right_z", "right_lastupdatedate", "right_user", "right_timestamp"], transformation_ctx="DropFields_node1776721179814")

# Script generated for node Drop Duplicates
DropDuplicates_node1776721277026 =  DynamicFrame.fromDF(DropFields_node1776721179814.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1776721277026")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1776721277026, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776719736267", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776721315728 = glueContext.getSink(path="s3://d609-stedi-lake/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776721315728")
AmazonS3_node1776721315728.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="customer_curated")
AmazonS3_node1776721315728.setFormat("json")
AmazonS3_node1776721315728.writeFrame(DropDuplicates_node1776721277026)
job.commit()
