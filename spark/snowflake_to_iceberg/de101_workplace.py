from pyspark.sql import SparkSession
from pyspark import SparkContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
import json
import boto3
iceberg_warehouse_location = "s3://de-iceberg-bucket/tables/"
glue_catalog_name = "AwsDataCatalog"
spark = SparkSession.builder.appName("de_workplace") \
            .config(f"spark.sql.catalog.{glue_catalog_name}","org.apache.iceberg.spark.SparkCatalog") \
            .config(f"spark.sql.catalog.{glue_catalog_name}.warehouse",f"{iceberg_warehouse_location}") \
            .config(f"spark.sql.catalog.{glue_catalog_name}.catalog-impl","org.apache.iceberg.aws.glue.GlueCatalog") \
            .config(f"spark.sql.catalog.{glue_catalog_name}.io-impl","org.apache.iceberg.aws.s3.S3FileIO") \
            .config("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .config('spark.driver.extraJavaOptions', '-Duser.timezone=GMT+5:30') \
            .config('spark.executor.extraJavaOptions', '-Duser.timezone=GMT+5:30') \
            .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.avro.int96RebaseModeInRead", "CORRECTED") \
            .config("spark.sql.avro.int96RebaseModeInWrite", "CORRECTED") \
            .config("spark.sql.avro.datetimeRebaseModeInRead", "CORRECTED") \
            .config("spark.sql.avro.datetimeRebaseModeInWrite", "CORRECTED") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.hive.convertMetastoreParquet","false") \
            .getOrCreate()



SNOWFLAKE_SOURCE_NAME = "net.snowflake.spark.snowflake"

secret_name = "snowflake_secret"
region_name = "ap-south-1"
session = boto3.session.Session()
client = session.client(
    service_name='secretsmanager',
    region_name=region_name
)
get_secret_value_response = client.get_secret_value(SecretId=secret_name)
print(get_secret_value_response)
sfOptions = json.loads(get_secret_value_response['SecretString'])
df = spark.read.format(SNOWFLAKE_SOURCE_NAME).options(**sfOptions).option("dbtable", "claims").load()

df.show(20,False)
df.createOrReplaceTempView("df")
db_name = "de101"
schema = "public"
table = "claims"
query = f"""CREATE OR REPLACE TABLE {glue_catalog_name}.{db_name}.{table}
USING iceberg
LOCATION '{iceberg_warehouse_location}{db_name}/{schema}/{table}/'
TBLPROPERTIES ("format-version"="2")
AS SELECT * FROM df"""
spark.sql(query)
