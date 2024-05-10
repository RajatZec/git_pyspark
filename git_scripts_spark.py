import boto3
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType,StructField, StringType ,ArrayType

# Create a Spark session
spark = SparkSession.builder.appName("task1").getOrCreate()

def download_json_from_s3(bucket_name, key, local_path, s3_client):
    """
    Download JSON file from S3 to the local filesystem.
    """
    s3_client.download_file(bucket_name, key, local_path)

def read_json_file(file_path):
    """
    Read JSON data from a file and return as a DataFrame, handling corrupt records.
    """

    schema = StructType([
        StructField("url", StringType(), False),
        StructField("additional_data",StructType([StructField("imageUrls", ArrayType(StringType(), True), True)]),True ),
        StructField("searchString", StringType(), False)
    ])
    # df = spark.read.schema(schema).option("multiline", "true").json("/home/ubuntu/Downloads/learn_pyspark/Spark_Learning/test.json")
    df = spark.read.schema(schema).option("multiline", "true").json(file_path)


    df = df.select("url", "additional_data.imageUrls", "searchString")

    # df.show()
    df.printSchema()


    return df

def process_json_files(bucket_name, json_prefix, local_dir, s3_client):
    """
    Process JSON files from S3.
    """
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=json_prefix)
    json_keys = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith(".json")]

    combined_df = None

    for json_key in json_keys:
        if not json_key.endswith('.7fB15390'):  # Ignore temporary files
            local_file_path = os.path.join(local_dir, os.path.basename(json_key))
            # download_json_from_s3(bucket_name, json_key, local_file_path, s3_client)
            df = read_json_file(local_file_path)
            
            df = df.withColumn("imageUrls", F.expr("concat_ws('|', imageUrls)"))
            
            if combined_df is None:
                combined_df = df
            else:
                combined_df = combined_df.union(df)
    
    # Write combined DataFrame to CSV
    csv_path = "/home/ubuntu/Downloads/learn_pyspark/output/combined_data.csv"
    combined_df.write.csv(csv_path, header=True, mode="overwrite")


def main():
    bucket_name = 'websitesgooglescrapper'
    json_prefix = 'google_json_dataset/'
    local_dir = '/tmp/json_files'  # Directory to store downloaded JSON files
    os.makedirs(local_dir, exist_ok=True)

 

    s3_client = boto3.client('s3', aws_access_key_id=access_key_id, aws_secret_access_key=secret_access_key)

    process_json_files(bucket_name, json_prefix, local_dir, s3_client)

if __name__ == "__main__":
    main()
