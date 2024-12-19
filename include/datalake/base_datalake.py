from abc import ABC, abstractmethod
from pyspark.sql import DataFrame

class BaseDatalake(ABC):
    @abstractmethod
    def write_file(self, df: DataFrame, layer: str, filename: str, execution_date: str, format: str = "parquet"):
        pass

    @abstractmethod
    def read_file(self, layer: str, execution_date: str, filename: str, format: str = "parquet") -> DataFrame:
        pass

    @abstractmethod
    def list_files(self, layer: str, execution_date: str, format: str = "parquet") -> list:
        pass
