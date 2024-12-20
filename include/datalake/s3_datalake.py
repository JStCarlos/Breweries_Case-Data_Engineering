import boto3
from pyspark.sql import SparkSession, DataFrame
from include.datalake.base_datalake import BaseDatalake

class S3Datalake(BaseDatalake):
    def __init__(self, bucket_name, aws_access_key, aws_secret_key, region_name):
        self.bucket_name = bucket_name
        self.s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=region_name
        )
        self.spark = SparkSession.builder \
            .appName("S3Datalake") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region_name}.amazonaws.com") \
            .getOrCreate()

    def write_file(self, df: DataFrame, layer: str, execution_date: str, filename: str = None, format: str = "parquet", partitions: list = None):
        if filename:
            filepath = f"s3a://{self.bucket_name}/{layer}/breweries_data_{execution_date}/{filename}"
        else:
            filepath = f"s3a://{self.bucket_name}/{layer}/breweries_data_{execution_date}/"

        writer = df.write.mode("overwrite").format(format)

        if partitions:
            df.write.mode("overwrite").format(format).partitionBy(*partitions).save(filepath)
        else:
            df.write.mode("overwrite").format(format).save(filepath)

        writer.save(filepath)
        print(f"Data written to S3 file: {filepath}")

    def read_files(self, layer: str, execution_date: str, format: str = "parquet", partitioned:bool = False):
        base_path = f"s3a://{self.bucket_name}/{layer}/breweries_data_{execution_date}/"

        try:
            df = self.spark.read.format(format).load(base_path)
            print(f"Read data from {base_path}")
            return df
        except Exception as e:
            raise FileNotFoundError(f"Failed to read files from {base_path}: {e}")

    def list_files(self, layer: str, execution_date: str, format: str = "parquet") -> list:
        prefix = f"{layer}/breweries_data_{execution_date}/"
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith(format)]
        print(f"Files in s3://{self.bucket_name}/{prefix}: {files}")
        return files
