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
accelerometer_trusted_node1700298733684 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="accelerometer_trusted",
    transformation_ctx="accelerometer_trusted_node1700298733684",
)

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1700298737820 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="step_trainer_trusted",
    transformation_ctx="step_trainer_trusted_node1700298737820",
)

# Script generated for node SQL Query
SqlQuery0 = """
select s.sensorreadingtime, s.serialnumber, s.distancefromobject, a.user, a.x, a.y, a.z
from accelerometer_trusted a, step_trainer_trusted s
where s.sensorreadingtime = a.timestamp
order by s.serialnumber, s.sensorreadingtime;
"""
SQLQuery_node1700298773441 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={
        "accelerometer_trusted": accelerometer_trusted_node1700298733684,
        "step_trainer_trusted": step_trainer_trusted_node1700298737820,
    },
    transformation_ctx="SQLQuery_node1700298773441",
)

# Script generated for node Amazon S3
AmazonS3_node1700298874970 = glueContext.getSink(
    path="s3://stedi-ljmzt-lakehouse/step_trainer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="snappy",
    enableUpdateCatalog=True,
    transformation_ctx="AmazonS3_node1700298874970",
)
AmazonS3_node1700298874970.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="machine_learning_curated"
)
AmazonS3_node1700298874970.setFormat("glueparquet")
AmazonS3_node1700298874970.writeFrame(SQLQuery_node1700298773441)
job.commit()

