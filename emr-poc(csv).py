import argparse
from pyspark.sql import SparkSession

input_path =  's3://wstt-apacheairflow-poc/emr-serverless/sample_test_data.csv'
output_path = 's3://wstt-apacheairflow-poc/emr-serverless-csv/'

spark = SparkSession.builder.appName("emr-poc(csv)-s3-to-s3").getOrCreate()
input_df = spark.read.csv(input_path)
input_df.write.csv(output_path)
spark.stop()
