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
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1757277329715 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="customer_landing", transformation_ctx="AWSGlueDataCatalog_node1757277329715")

# Script generated for node SQL Query
SqlQuery1165 = '''
select * from myDataSource
where sharewithresearchasofdate IS NOT null
'''
SQLQuery_node1757277381325 = sparkSqlQuery(glueContext, query = SqlQuery1165, mapping = {"myDataSource":AWSGlueDataCatalog_node1757277329715}, transformation_ctx = "SQLQuery_node1757277381325")

# Script generated for node Amazon S3
AmazonS3_node1757277690805 = glueContext.getSink(path="s3://${{ secrets.MY_BUCKET }}/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], compression="snappy", enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1757277690805")
AmazonS3_node1757277690805.setCatalogInfo(catalogDatabase="stedi-db-bj",catalogTableName="customer_trusted")
AmazonS3_node1757277690805.setFormat("json")
AmazonS3_node1757277690805.writeFrame(SQLQuery_node1757277381325)
job.commit()