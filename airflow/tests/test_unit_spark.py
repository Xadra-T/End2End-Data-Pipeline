from __future__ import annotations

import os


def test_spark_task_configuration(dag):
    """Test that the Spark task is properly configured."""
    spark_task = dag.get_task('spark_analysis')
    
    assert spark_task._conn_id == os.environ['SPARK_CONN_NAME']
    assert spark_task._deploy_mode == 'client'
    assert spark_task._driver_memory == '512m'
    assert spark_task._executor_memory == '512m'
    assert spark_task._executor_cores == 2
    assert spark_task._num_executors == 2
    
    expected_conf_keys = {
        'spark.hadoop.fs.s3a.endpoint',
        'spark.hadoop.fs.s3a.access.key',
        'spark.hadoop.fs.s3a.secret.key',
        'spark.hadoop.fs.s3a.path.style.access',
        'spark.hadoop.fs.s3a.impl',
        'spark.hadoop.fs.s3a.connection.ssl.enabled',
        'spark.eventLog.enabled',
        'spark.eventLog.dir'
    }
    
    actual_conf_keys = set(spark_task.conf.keys())
    assert expected_conf_keys.issubset(actual_conf_keys)
