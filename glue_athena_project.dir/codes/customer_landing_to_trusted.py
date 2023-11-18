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

# Script generated for node Amazon S3
AmazonS3_node1700056589072 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_landing",
    transformation_ctx="AmazonS3_node1700056589072",
)

# Script generated for node privacy filter
SqlQuery0 = """
select * from myDataSource
where sharewithresearchasofdate is not null
"""
privacyfilter_node1700106298659 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": AmazonS3_node1700056589072},
    transformation_ctx="privacyfilter_node1700106298659",
)

# Script generated for node customer_trusted
customer_trusted_node1700057211321 = glueContext.getSink(
    path="s3://stedi-ljmzt-lakehouse/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_trusted_node1700057211321",
)
customer_trusted_node1700057211321.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
customer_trusted_node1700057211321.setFormat("json")
customer_trusted_node1700057211321.writeFrame(privacyfilter_node1700106298659)
job.commit()
