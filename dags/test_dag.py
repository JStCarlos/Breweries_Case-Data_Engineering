from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
import requests
    
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
)

def my_dag():
    
    # submit_job = SparkSubmitOperator(
    #     task_id="submit_job",
    #     conn_id="my_spark_conn",
    #     application="include/scripts/read.py",
    #     verbose=True,
    # )
    
    # submit_job
    
    @task.pyspark(conn_id="spark_conn")
    def read_data(spark: SparkSession, sc: SparkContext) -> pd.DataFrame:
        url = 'https://api.openbrewerydb.org/v1/breweries?page=3&per_page=200'
        response = requests.get(url)
        breweries_data = response.json()
        print(breweries_data)
        df = spark.createDataFrame(
            [
                (1, "John Doe", 21),
                (2, "Jane Doe", 22),
                (3, "Joe Bloggs", 23),
            ],
            ["id", "name", "age"],
        )
        df.show()

        return df.toPandas()
    
    read_data()

my_dag()