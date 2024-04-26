import argparse
from pyspark.sql import SparkSession

input_path = 's3://wstt-apacheairflow-poc/sampledata.parquet'
output_path = 's3://wstt-apacheairflow-poc/emr-serverless/'

spark = SparkSession.builder.appName("emr-poc-s3-to-s3").getOrCreate()
input_df = spark.read.parquet(input_path)
input_df.write.parquet(output_path)
spark.stop()
