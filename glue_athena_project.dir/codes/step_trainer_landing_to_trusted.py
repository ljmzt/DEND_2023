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

# Script generated for node customer_trusted
customer_trusted_node1700191904924 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="customer_trusted_node1700191904924",
)

# Script generated for node step_trainer_landing
step_trainer_landing_node1700191916990 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_landing",
    transformation_ctx="step_trainer_landing_node1700191916990",
)

# Script generated for node SQL Query
SqlQuery0 = """
SELECT s.sensorreadingtime, s.serialnumber, s.distancefromobject
FROM step_trainer_landing s INNER JOIN customer_trusted c
  ON s.serialnumber = c.serialnumber;
"""
SQLQuery_node1700191951030 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "customer_trusted": customer_trusted_node1700191904924,
        "step_trainer_landing": step_trainer_landing_node1700191916990,
    },
    transformation_ctx="SQLQuery_node1700191951030",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1700192072476 = glueContext.getSink(
    path="s3://stedi-ljmzt-lakehouse/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_node1700192072476",
)
step_trainer_trusted_node1700192072476.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="step_trainer_trusted"
)
step_trainer_trusted_node1700192072476.setFormat("json")
step_trainer_trusted_node1700192072476.writeFrame(SQLQuery_node1700191951030)
job.commit()
