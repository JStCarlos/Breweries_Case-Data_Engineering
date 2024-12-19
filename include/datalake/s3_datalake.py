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

    def write_file(self, df: DataFrame, layer: str, filename: str, execution_date: str, format: str = "parquet"):
        filepath = f"s3a://{self.bucket_name}/{layer}/breweries_data_{execution_date}/{filename}"
        df.write.mode("overwrite").format(format).save(filepath)
        print(f"Data written to S3 file: {filepath}")

    def read_file(self, layer: str, execution_date: str, filename: str, format: str = "parquet") -> DataFrame:
        filepath = f"s3a://{self.bucket_name}/{layer}/breweries_data_{execution_date}/{filename}"
        df = self.spark.read.format(format).load(filepath)
        print(f"Data read from S3 file: {filepath}")
        return df

    def list_files(self, layer: str, execution_date: str, format: str = "parquet") -> list:
        prefix = f"{layer}/breweries_data_{execution_date}/"
        response = self.s3.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        files = [content['Key'] for content in response.get('Contents', []) if content['Key'].endswith(format)]
        print(f"Files in s3://{self.bucket_name}/{prefix}: {files}")
        return files
