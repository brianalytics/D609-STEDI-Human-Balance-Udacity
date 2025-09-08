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

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1757294172295 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1757294172295")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1757294171218 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="step_trainer_trusted", transformation_ctx="step_trainer_trusted_node1757294171218")

# Script generated for node SQL Query
SqlQuery867 = '''
SELECT DISTINCT *
FROM st
INNER JOIN at
ON at.timestamp = st.sensorreadingtime;
'''
SQLQuery_node1757294558167 = sparkSqlQuery(glueContext, query = SqlQuery867, mapping = {"at":accelerometer_trusted_node1757294172295, "st":step_trainer_trusted_node1757294171218}, transformation_ctx = "SQLQuery_node1757294558167")

# Script generated for node machine_learning_curated
machine_learning_curated_node1757294620067 = glueContext.getSink(path="s3://${{ secrets.MY_BUCKET }}/machine_learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="machine_learning_curated_node1757294620067")
machine_learning_curated_node1757294620067.setCatalogInfo(catalogDatabase="stedi-db-bj",catalogTableName="machine_learning_curated")
machine_learning_curated_node1757294620067.setFormat("json")
machine_learning_curated_node1757294620067.writeFrame(SQLQuery_node1757294558167)
job.commit()