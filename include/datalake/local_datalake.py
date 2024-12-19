import os
from pyspark.sql import SparkSession, DataFrame
from include.datalake.base_datalake import BaseDatalake

class LocalDatalake(BaseDatalake):
    def __init__(self, base_dir="/opt/airflow/data"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.spark = SparkSession.builder.appName("LocalDatalake").getOrCreate()

    def write_file(self, df: DataFrame, layer: str, filename: str, execution_date: str, format: str = "parquet"):
        layer_dir = os.path.join(self.base_dir, layer, f"breweries_data_{execution_date}")
        os.makedirs(layer_dir, exist_ok=True)
        filepath = os.path.join(layer_dir, filename)
        df.write.mode("overwrite").format(format).save(filepath)
        print(f"Data written to local file: {filepath}")

    def read_file(self, layer: str, execution_date: str, filename: str, format: str = "parquet") -> DataFrame:
        layer_dir = os.path.join(self.base_dir, layer, f"breweries_data_{execution_date}")
        filepath = os.path.join(layer_dir, filename)
        df = self.spark.read.format(format).load(filepath)
        print(f"Data read from local file: {filepath}")
        return df

    def list_files(self, layer: str, execution_date: str, format: str = "parquet") -> list:
        layer_dir = os.path.join(self.base_dir, layer, f"breweries_data_{execution_date}")
        files = [f for f in os.listdir(layer_dir) if f.endswith(format)]
        print(f"Files in {layer_dir}: {files}")
        return files
