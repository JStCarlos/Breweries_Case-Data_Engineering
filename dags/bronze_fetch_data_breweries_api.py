from airflow.decorators import dag, task
from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SparkSession
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pandas as pd
import requests
from include.bronze_fetch_data_breweries_api.tasks import BonzeFetchDataBreweriesApi


bronze_layer_tasks = BonzeFetchDataBreweriesApi()
    
@dag(
    start_date=datetime(2024, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['bronze']
)

def bronze_layer():
    
    @task
    def fetch_breweries_metadata():
        return bronze_layer_tasks.get_total_breweries_metadata()
    
    @task
    def fetch_breweries_data(pages_to_fetch):
        bronze_layer_tasks.fetch_breweries_data(pages_to_fetch)
    
    fetch_breweries_data(fetch_breweries_metadata())

bronze_layer()