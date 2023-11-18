import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1700188026519 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1700188026519",
)

# Script generated for node customer_trusted
customer_trusted_node1700188011357 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1700188011357",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT *
FROM customer_trusted c
WHERE c.email in (SELECT DISTINCT user FROM accelerometer_trusted);
"""
SQLQuery_node1700188060155 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": customer_trusted_node1700188011357,
        "accelerometer_trusted": accelerometer_trusted_node1700188026519,
    },
    transformation_ctx="SQLQuery_node1700188060155",
)

# Script generated for node Amazon S3
AmazonS3_node1700188177832 = glueContext.getSink(
    path="s3://stedi-ljmzt-lakehouse/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1700188177832",
)
AmazonS3_node1700188177832.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_curated"
)
AmazonS3_node1700188177832.setFormat("json")
AmazonS3_node1700188177832.writeFrame(SQLQuery_node1700188060155)
job.commit()
