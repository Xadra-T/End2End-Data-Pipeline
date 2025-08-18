from __future__ import annotations

import pytest
from airflow.models.dagbag import DagBag


@pytest.fixture(scope='module')
def dagbag():
    """Load the DAGBag."""
    return DagBag(dag_folder='.')


def test_dag_imports_without_error(dagbag):
    errors = dagbag.import_errors
    assert len(errors) == 0, f'Found error(s) importing dagbag: {errors}'


def test_dag_is_present(dagbag):
    """Verify 'clickHouse_pyspark_dashboard' dag is present."""
    dag_id = 'clickHouse_pyspark_dashboard'
    assert dag_id in dagbag.dags, f"'{dag_id}' NOT found in dagbag. Available dags are: {list(dagbag.dags.keys())}"
    dag = dagbag.dags.get(dag_id)
    assert dag is not None, f"'{dag_id}' NOT found in dagbag."


def test_tasks_are_present(dagbag):
    """Verify 'clickHouse_pyspark_dashboard' tasks are present."""
    dag = dagbag.dags.get('clickHouse_pyspark_dashboard')
    tasks = {t.task_id for t in dag.tasks}
    assert tasks == {
        'stream_from_clickhouse_to_minio',
        'spark_analysis',
        'send_to_dashboard',
    }, f"Found these tasks for 'clickHouse_pyspark_dashboard': [{tasks}]"


def test_upstream_and_downstream_relations(dagbag):
    """Verify the structure of the 'clickHouse_pyspark_dashboard' dag."""
    dag = dagbag.dags.get('clickHouse_pyspark_dashboard')
    stream_task = dag.get_task('stream_from_clickhouse_to_minio')
    spark_task = dag.get_task('spark_analysis')
    dashboard_task = dag.get_task('send_to_dashboard')
    
    assert len(stream_task.upstream_task_ids) == 0
    assert len(dashboard_task.downstream_task_ids) == 0
    assert spark_task.task_id in stream_task.downstream_task_ids, "The spark task is NOT correctly set as the stream's downstream task."
    assert dashboard_task.task_id in spark_task.downstream_task_ids, "The dashboard task is NOT correctly set as the spark's downstream task."
    assert stream_task.task_id in spark_task.upstream_task_ids, "The stream task is NOT correctly set as the spark's upstream task."
    assert spark_task.task_id in dashboard_task.upstream_task_ids, "The spark task is NOT correctly set as the dashboard's upstream task."
