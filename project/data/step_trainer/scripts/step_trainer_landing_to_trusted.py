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

# Script generated for node step_trainer_landing
step_trainer_landing_node1757293250045 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="step_trainer_landing", transformation_ctx="step_trainer_landing_node1757293250045")

# Script generated for node customer_curated
customer_curated_node1757293247707 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="customer_curated", transformation_ctx="customer_curated_node1757293247707")

# Script generated for node SQL Query
SqlQuery1048 = '''
SELECT DISTINCT st.sensorreadingtime, st.serialnumber, st.distancefromobject
FROM st
INNER JOIN cc
ON cc.serialnumber = st.serialnumber;
'''
SQLQuery_node1757293339445 = sparkSqlQuery(glueContext, query = SqlQuery1048, mapping = {"cc":customer_curated_node1757293247707, "st":step_trainer_landing_node1757293250045}, transformation_ctx = "SQLQuery_node1757293339445")

# Script generated for node step_trainer_trusted
step_trainer_trusted_node1757293539224 = glueContext.getSink(path="s3://${{ secrets.MY_BUCKET }}/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="step_trainer_trusted_node1757293539224")
step_trainer_trusted_node1757293539224.setCatalogInfo(catalogDatabase="stedi-db-bj",catalogTableName="step_trainer_trusted")
step_trainer_trusted_node1757293539224.setFormat("json")
step_trainer_trusted_node1757293539224.writeFrame(SQLQuery_node1757293339445)
job.commit()