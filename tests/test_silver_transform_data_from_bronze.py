import pytest
from unittest.mock import patch, MagicMock
from include.silver_transform_data_from_bronze.tasks import SilverTransformDataFromBronze

@pytest.fixture
def silver_layer_tasks():
    return SilverTransformDataFromBronze()

@patch('include.silver_transform_data_from_bronze.tasks.SilverTransformDataFromBronze.transform_and_persist_breweries_data')
def test_transform_and_persist_breweries_data(mock_transform, silver_layer_tasks):
    mock_transform.return_value = None
    kwargs = {'some_key': 'some_value'}
    
    silver_layer_tasks.transform_and_persist_breweries_data(**kwargs)
    
    mock_transform.assert_called_once_with(**kwargs)

@patch('airflow.providers.apache.spark.operators.spark_submit.SparkSubmitOperator')
def test_spark_submit_operator(mock_spark_submit):
    mock_spark_submit.return_value = MagicMock()
    
    operator = mock_spark_submit(
        task_id='spark_submit_task',
        conn_id='spark_conn',
        application='path/to/your/application.py'
    )
    
    assert operator.task_id == 'spark_submit_task'
    assert operator.conn_id == 'spark_conn'
    assert operator.application == 'path/to/your/application.py'

@patch('airflow.operators.dagrun_operator.TriggerDagRunOperator')
def test_trigger_dag_run_operator(mock_trigger_dag_run):
    mock_trigger_dag_run.return_value = MagicMock()
    
    operator = mock_trigger_dag_run(
        task_id='trigger_gold_layer',
        trigger_dag_id='gold_layer_aggregation',
        reset_dag_run=True
    )
    
    assert operator.task_id == 'trigger_gold_layer'
    assert operator.trigger_dag_id == 'gold_layer_aggregation'
    assert operator.reset_dag_run is True