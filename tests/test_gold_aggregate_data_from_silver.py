import unittest
from unittest.mock import patch, MagicMock
from airflow.models import DagBag
from airflow.utils.state import State

class TestGoldAggregateDataFromSilver(unittest.TestCase):

    def setUp(self):
        self.dagbag = DagBag()
        self.dag = self.dagbag.get_dag(dag_id='gold_layer_aggregation')

    def test_dag_loaded(self):
        self.assertIsNotNone(self.dag)
        self.assertEqual(self.dag.dag_id, 'gold_layer_aggregation')

    @patch('include.gold_aggregate_data_from_silver.tasks.GoldAggregateDataFromSilver.aggregate_breweries_data')
    def test_aggregate_and_persist_breweries_data(self, mock_aggregate_breweries_data):
        task = self.dag.get_task(task_id='aggregate_and_persist_breweries_data')
        self.assertIsNotNone(task)

        # Mock the task instance
        ti = MagicMock()
        ti.task_id = 'aggregate_and_persist_breweries_data'
        ti.dag_id = 'gold_layer_aggregation'
        ti.state = State.NONE

        # Run the task
        task.execute(ti.get_template_context())

        # Check if the aggregate_breweries_data method was called
        mock_aggregate_breweries_data.assert_called_once()

if __name__ == '__main__':
    unittest.main()