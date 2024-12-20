import pytest
from airflow.models import DagBag
from airflow.utils.state import State
from airflow.models import TaskInstance
from datetime import datetime

@pytest.fixture(scope="module")
def dagbag():
    return DagBag()

def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id='bronze_layer')
    assert dag is not None
    assert len(dag.tasks) == 3

def test_task_dependencies(dagbag):
    dag = dagbag.get_dag(dag_id='bronze_layer')
    fetch_breweries_metadata = dag.get_task('fetch_breweries_metadata')
    fetch_and_persist_breweries_data = dag.get_task('fetch_and_persist_breweries_data')
    trigger_silver_layer = dag.get_task('trigger_silver_layer')

    assert fetch_breweries_metadata.downstream_task_ids == {'fetch_and_persist_breweries_data'}
    assert fetch_and_persist_breweries_data.downstream_task_ids == {'trigger_silver_layer'}

def test_dag_run(dagbag):
    dag = dagbag.get_dag(dag_id='bronze_layer')
    dagrun = dag.create_dagrun(
        run_id='test_dag_run',
        start_date=datetime.now(),
        execution_date=datetime.now(),
        state=State.RUNNING
    )

    fetch_breweries_metadata = dag.get_task('fetch_breweries_metadata')
    fetch_and_persist_breweries_data = dag.get_task('fetch_and_persist_breweries_data')
    trigger_silver_layer = dag.get_task('trigger_silver_layer')

    ti1 = TaskInstance(task=fetch_breweries_metadata, execution_date=dagrun.execution_date)
    ti2 = TaskInstance(task=fetch_and_persist_breweries_data, execution_date=dagrun.execution_date)
    ti3 = TaskInstance(task=trigger_silver_layer, execution_date=dagrun.execution_date)

    ti1.run(ignore_ti_state=True)
    assert ti1.state == State.SUCCESS

    ti2.run(ignore_ti_state=True)
    assert ti2.state == State.SUCCESS

    ti3.run(ignore_ti_state=True)
    assert ti3.state == State.SUCCESS