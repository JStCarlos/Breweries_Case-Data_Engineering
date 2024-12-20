import os
from pyspark.sql import SparkSession, DataFrame
from include.datalake.base_datalake import BaseDatalake

class LocalDatalake(BaseDatalake):
    def __init__(self, base_dir="/opt/airflow/data"):
        self.base_dir = base_dir
        os.makedirs(self.base_dir, exist_ok=True)
        self.spark = SparkSession.builder.appName("LocalDatalake").getOrCreate()

    def write_file(self, df: DataFrame, layer: str, execution_date: str, filename: str = None, format: str = "parquet", partitions: list = None):
        layer_dir = os.path.join(self.base_dir, layer, f"breweries_data_{execution_date}")
        os.makedirs(layer_dir, exist_ok=True)

        if filename:
            filepath = os.path.join(layer_dir, filename)
        else:
            filepath = layer_dir 

        if partitions:
            df.write.mode("overwrite").format(format).partitionBy(*partitions).save(filepath)
        else:
            df.write.mode("overwrite").format(format).save(filepath)

        print(f"Data written to local file: {filepath}")

    def read_files(self, layer: str, execution_date: str, format: str = "parquet", partitioned:bool = False):
        base_path = os.path.join(self.base_dir, layer, f"breweries_data_{execution_date}")
        
        if not os.path.exists(base_path):
            raise FileNotFoundError(f"Directory {base_path} does not exist.")
        
        if partitioned:
            df = self.spark.read.format(format).load(base_path)
            return df

        files = []
        for root, _, filenames in os.walk(base_path):
            for filename in filenames:
                if filename.endswith(f".{format}"):
                    files.append(os.path.join(root, filename))
                    temp_df = self.spark.read.parquet(os.path.join(root, filename))
                    temp_df.show()
        
        if not files:
            raise FileNotFoundError(f"No {format} files found in {base_path}")

        df = self.spark.read.format(format).load(files)
        df.show()
        return df

    def list_files(self, layer: str, execution_date: str, format: str = "parquet") -> list:
        layer_dir = os.path.join(self.base_dir, layer, f"breweries_data_{execution_date}")
        files = [f for f in os.listdir(layer_dir) if f.endswith(format)]
        print(f"Files in {layer_dir}: {files}")
        return files
