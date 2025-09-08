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

# Script generated for node accelerometer_landing
accelerometer_landing_node1757281311017 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="accelerometer_landing", transformation_ctx="accelerometer_landing_node1757281311017")

# Script generated for node customer_trusted
customer_trusted_node1757281134569 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="customer_trusted", transformation_ctx="customer_trusted_node1757281134569")

# Script generated for node SQL Query
SqlQuery1000 = '''
select a.user, a.timestamp, a.x, a.y, a.z
from a_landing a
inner join c_trusted c
on c.email = a.user;
'''
SQLQuery_node1757281361307 = sparkSqlQuery(glueContext, query = SqlQuery1000, mapping = {"a_landing":accelerometer_landing_node1757281311017, "c_trusted":customer_trusted_node1757281134569}, transformation_ctx = "SQLQuery_node1757281361307")

# Script generated for node accelerometer_landing_to_trusted
accelerometer_landing_to_trusted_node1757286255600 = glueContext.getSink(path="s3://${{ secrets.MY_BUCKET }}/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="accelerometer_landing_to_trusted_node1757286255600")
accelerometer_landing_to_trusted_node1757286255600.setCatalogInfo(catalogDatabase="stedi-db-bj",catalogTableName="accelerometer_trusted")
accelerometer_landing_to_trusted_node1757286255600.setFormat("json")
accelerometer_landing_to_trusted_node1757286255600.writeFrame(SQLQuery_node1757281361307)
job.commit()