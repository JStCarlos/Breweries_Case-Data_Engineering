from airflow.decorators import dag, task
from datetime import datetime, timedelta
from include.gold_aggregate_data_from_silver.tasks import GoldAggregateDataFromSilver

# DAG to aggregate data from the silver layer, store it in the gold layer, providing data ready for analysis.

gold_layer_tasks = GoldAggregateDataFromSilver()

@dag(
    dag_id='gold_layer_aggregation',
    start_date=datetime.now(),
    schedule_interval=None,  
    catchup=False,
    tags=['gold', 'aggregation'],
    is_paused_upon_creation=False,
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
)
def gold_layer():
    
    @task.pyspark(conn_id="spark_conn")
    def aggregate_and_persist_breweries_data(**kwargs):
        gold_layer_tasks.aggregate_breweries_data(**kwargs)

    aggregate_and_persist_breweries_data()

gold_layer()
