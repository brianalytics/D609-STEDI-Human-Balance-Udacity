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

# Script generated for node customer_trusted
customer_trusted_node1757290501559 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="customer_trusted", transformation_ctx="customer_trusted_node1757290501559")

# Script generated for node accelerometer_trusted
accelerometer_trusted_node1757290503879 = glueContext.create_dynamic_frame.from_catalog(database="stedi-db-bj", table_name="accelerometer_trusted", transformation_ctx="accelerometer_trusted_node1757290503879")

# Script generated for node SQL Query
SqlQuery926 = '''
SELECT DISTINCT ct.serialnumber, ct.sharewithresearchasofdate, ct.birthday, ct.registrationdate, ct.sharewithpublicasofdate, ct.customername, ct.email, ct.lastupdatedate, ct.phone, ct.sharewithfriendsasofdate
FROM ct
INNER JOIN at
ON at.user = ct.email
WHERE at.x != 0.0 AND at.y != 0.0 AND at.z != 0.0;
'''
SQLQuery_node1757290563275 = sparkSqlQuery(glueContext, query = SqlQuery926, mapping = {"at":accelerometer_trusted_node1757290503879, "ct":customer_trusted_node1757290501559}, transformation_ctx = "SQLQuery_node1757290563275")

# Script generated for node customer_curated
customer_curated_node1757290680669 = glueContext.getSink(path="s3://${{ secrets.MY_BUCKET }}/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="customer_curated_node1757290680669")
customer_curated_node1757290680669.setCatalogInfo(catalogDatabase="stedi-db-bj",catalogTableName="customer_curated")
customer_curated_node1757290680669.setFormat("json")
customer_curated_node1757290680669.writeFrame(SQLQuery_node1757290563275)
job.commit()