from __future__ import annotations

from unittest.mock import Mock, patch

from pipeline import on_success_callback_func, on_failure_callback_func


def test_on_success_callback_func() -> None:
    """Test the success callback logs correctly."""
    mock_dag_run = Mock()
    dag_id = 'test_dag'
    run_id = 'test_run_123'
    task_id = 'test_task'
    mock_dag_run.dag_id = dag_id
    mock_dag_run.run_id = run_id
    
    mock_task_instance = Mock()
    mock_task_instance.task_id = task_id
    
    context = {
        'dag_run': mock_dag_run,
        'task_instance': mock_task_instance
    }
    
    with patch('pipeline.logger', autospec=True) as mock_logger:
        on_success_callback_func(context)
        
        mock_logger.info.assert_called_once_with(
            "DAG '%s' - Task '%s' succeeded. Run ID: %s",
            dag_id,
            task_id,
            run_id,
        )


def test_on_failure_callback_func() -> None:
    """Test the failure callback logs correctly."""
    mock_dag_run = Mock()
    dag_id = 'test_dag'
    run_id = 'test_run_123'
    task_id = 'test_task'
    mock_dag_run.dag_id = dag_id
    mock_dag_run.run_id = run_id
    
    mock_task_instance = Mock()
    mock_task_instance.task_id = task_id
    
    test_exception = ValueError('Test error')
    
    context = {
        'dag_run': mock_dag_run,
        'task_instance': mock_task_instance,
        'exception': test_exception
    }
    
    with patch('pipeline.logger', autospec=True) as mock_logger:
        on_failure_callback_func(context)
        
        mock_logger.error.assert_called_once_with(
            "DAG '%s' - Task '%s' failed. Run ID: %s. Exception: %s",
            dag_id,
            task_id,
            run_id,
            test_exception
        )
