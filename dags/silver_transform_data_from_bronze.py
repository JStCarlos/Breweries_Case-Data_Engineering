from airflow.decorators import dag, task
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from datetime import datetime, timedelta
from include.silver_transform_data_from_bronze.tasks import SilverTransformDataFromBronze

# DAG to transform data from the bronze layer, store it in the silver layer, and trigger the gold layer DAG for aggregation.


silver_layer_tasks = SilverTransformDataFromBronze()
    
@dag(
    dag_id='silver_layer_transformations',
    start_date=datetime.now(),
    schedule_interval=None,
    catchup=False,
    tags=['silver', 'transformation'],
    is_paused_upon_creation=False,   
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=5)
    }
)

def silver_layer():
        
    @task.pyspark(conn_id="spark_conn")
    def transform_and_persist_breweries_data(**kwargs): 
        silver_layer_tasks.transform_and_persist_breweries_data(**kwargs)
    
    trigger_gold_dag = TriggerDagRunOperator(
    task_id='trigger_gold_layer',
    trigger_dag_id='gold_layer_aggregation',
    reset_dag_run=True
    )

    
    transform_and_persist_breweries_data() >> trigger_gold_dag

silver_layer()