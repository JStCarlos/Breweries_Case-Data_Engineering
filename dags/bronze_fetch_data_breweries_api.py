from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.trigger_rule import TriggerRule
from include.bronze_fetch_data_breweries_api.tasks import BonzeFetchDataBreweriesApi

# DAG to fetch data from the Open Brewery DB API, store it in the bronze layer, and trigger the silver layer DAG for further processing.

bronze_layer_tasks = BonzeFetchDataBreweriesApi()
    
@dag(
    dag_id='bronze_layer_ingestion',
    start_date=datetime.now(),
    schedule='@once',
    catchup=True,
    tags=['bronze', 'ingestion'],
    is_paused_upon_creation=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
)

def bronze_layer():
    
    @task
    def fetch_breweries_metadata():
        return bronze_layer_tasks.get_total_breweries_metadata()
    
    @task.pyspark(conn_id="spark_conn")
    def fetch_and_persist_breweries_data(pages_to_fetch, **kwargs):
        bronze_layer_tasks.fetch_and_persist_breweries_data(pages_to_fetch, layer="bronze", **kwargs)

    trigger_silver_layer = TriggerDagRunOperator(
        task_id='trigger_silver_layer',
        trigger_dag_id="silver_layer_transformations",
        conf={"execution_date": "{{ execution_date }}"},
        trigger_rule=TriggerRule.ALL_SUCCESS
    )
    
    fetch_and_persist_breweries_data(fetch_breweries_metadata()) >> trigger_silver_layer
bronze_layer()