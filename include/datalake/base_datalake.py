from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseDatalake(ABC):
    @abstractmethod
    def write_file(self, df: DataFrame, layer: str, execution_date: str, filename: str = None, format: str = "parquet", partitions: list = None):
        pass

    @abstractmethod
    def read_files(self, layer: str, execution_date: str, filename: str, format: str = "parquet", partitioned:bool = False) -> DataFrame:
        pass

    @abstractmethod
    def list_files(self, layer: str, execution_date: str, format: str = "parquet") -> list:
        pass
