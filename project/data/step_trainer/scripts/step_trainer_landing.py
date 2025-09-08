import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer S3
StepTrainerS3_node1757273063466 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://stedi-udacity-bjordan/step_trainer/"], "recurse": True}, transformation_ctx="StepTrainerS3_node1757273063466")

# Script generated for node Amazon S3
AmazonS3_node1757273492459 = glueContext.getSink(path="s3://${{ secrets.MY_BUCKET }}/step_trainer/landing_v2/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1757273492459")
AmazonS3_node1757273492459.setCatalogInfo(catalogDatabase="stedi-db-bj",catalogTableName="step_trainer_landing")
AmazonS3_node1757273492459.setFormat("json")
AmazonS3_node1757273492459.writeFrame(StepTrainerS3_node1757273063466)
job.commit()