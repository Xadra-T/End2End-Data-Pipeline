# ruff: noqa: D205, D208, D400, D415, S608

from __future__ import annotations
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Any, TYPE_CHECKING
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # For test on spark which has python 3.8

import clickhouse_connect
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

if TYPE_CHECKING:
    from uuid import UUID
    row_type = tuple[UUID, UUID, UUID, str, datetime, int, str, int | None, int | None]


load_dotenv()

schema = pa.schema([
    pa.field('event_id', pa.string()),
    pa.field('user_id', pa.string()),
    pa.field('session_id', pa.string()),
    pa.field('event_type', pa.string()),
    pa.field('event_timestamp', pa.timestamp('ms', tz='Asia/Tehran')),
    pa.field('request_latency_ms', pa.int32()),
    pa.field('status', pa.string()),
    pa.field('error_code', pa.int32(), nullable=True),
    pa.field('product_id', pa.int32(), nullable=True),
])

logger = logging.getLogger(__name__)

CLICKHOUSE_CONN_NAME = os.environ['CLICKHOUSE_CONN_NAME']
MINIO_CONN_NAME = os.environ['MINIO_CONN_NAME']
SPARK_CONN_NAME = os.environ['SPARK_CONN_NAME']
SPARK_APPLICATION_PATH = os.environ['SPARK_APPLICATION_PATH']
MINIO_BUCKET_NAME = os.environ['MINIO_BUCKET_NAME']


def get_minio_client() -> Minio:
    logger.info("Retrieving MinIO client.")
    minio_conn = BaseHook.get_connection(MINIO_CONN_NAME)
    minio_client = Minio(
        endpoint=minio_conn.extra_dejson.get('host').replace('http://', ''),
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=False
    )
    return minio_client


def on_success_callback_func(context: dict[str, Any]) -> None:
    """Log successful task completion."""
    dag_run = context['dag_run']
    task_instance = context['task_instance']
    logger.info(
        "DAG '%s' - Task '%s' succeeded. Run ID: %s",
        dag_run.dag_id,
        task_instance.task_id,
        dag_run.run_id
    )


def on_failure_callback_func(context: dict[str, Any]) -> None:
    """Log failed task  and exception."""
    dag_run = context['dag_run']
    task_instance = context['task_instance']
    exception = context.get('exception')
    logger.error(
        "DAG '%s' - Task '%s' failed. Run ID: %s. Exception: %s",
        dag_run.dag_id,
        task_instance.task_id,
        dag_run.run_id,
        exception
    )


@dag(
    dag_id='clickHouse_pyspark_dashboard',
    description='Extract data from ClickHouse, stream to minio, run spark analysis, report to dashboard.',
    schedule='* * * * *',
    start_date=datetime(2025, 8, 9, tzinfo=ZoneInfo('UTC')),
    default_args={
        'retries': 1,
        'retry_delay': timedelta(seconds=10),
        'on_success_callback': on_success_callback_func,
        'on_failure_callback': on_failure_callback_func,
    },
    max_active_runs=2,
    catchup=False,
    doc_md="""
    ### ETAR Pipeline
    1. Extract data from ClickHouse for the previous minute and stream into MinIO.
    2. Run Spark analysis.
    3. Send the analysis result to the dashboard API.
    """,
    is_paused_upon_creation=False,
    fail_fast=True,
)
def etar_pipeline() -> None:
    """Extract-Transform-Analysis-Report Pipeline:
        1- Extract data from ClickHouse for the previous minute and stream into MinIO as a Parquet file
        2- Trigger Spark analysis
        3- Report the result back to the dashboard.
    """
    
    @task
    def stream_from_clickhouse_to_minio(data_interval_start: datetime) -> str:
        """Stream data from ClickHouse to MinIO, return the s3a file path if there were any data stored, otherwise return the file name.
        
        Args:
            data_interval_start: Task start time. Comes from Airflow.
        
        Returns:
            MinIO path of the file or the timestamp converted to string.
        
        """
        logger.info("Retrieving ClickHouse connection from Airflow's metadata database.")
        ch_conn = BaseHook.get_connection(CLICKHOUSE_CONN_NAME)
        clickhouse_client = clickhouse_connect.get_client(
            host=ch_conn.host,
            port=ch_conn.port,
            user=ch_conn.login,
            password=ch_conn.password,
            database=ch_conn.schema,
        )
        
        timestamp = data_interval_start.astimezone(ZoneInfo('Asia/Tehran')).replace(second=0, microsecond=0) - timedelta(minutes=1)
        logger.info('Timestamp after tz: %s', timestamp)
        
        timestamp_str = timestamp.strftime('%Y-%m-%d_%H-%M')
        logger.info('Processing %s', timestamp_str)
        s3_path = f's3a://{MINIO_BUCKET_NAME}/{timestamp_str}'
        
        table = os.environ['CLICKHOUSE_TABLE']
        query = 'SELECT * FROM %(table)s WHERE event_minute = %(timestamp)s;'
        total_rows = 0
        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_file:
            writer = None
            try:
                with clickhouse_client.query_df_stream(query=query, parameters={'table': table, 'timestamp': timestamp}, settings={'max_block_size': 100000}) as stream:  # 100k rows; query_df_stream uses Native (bytes) or Arrow
                    for df_chunk in stream:
                        n = len(df_chunk)
                        if n == 0:
                            break
                        total_rows += n
                        
                        uuid_columns = ['event_id', 'user_id', 'session_id']
                        for col in uuid_columns:
                            df_chunk[col] = df_chunk[col].astype(str)
                        
                        df_chunk['event_timestamp'] = df_chunk['event_timestamp'].dt.floor('s')
                        
                        if writer is None:
                            writer = pq.ParquetWriter(tmp_file.name, schema=schema)
                        
                        table = pa.Table.from_pandas(df_chunk, schema=schema)
                        writer.write_table(table)
                if writer:
                    writer.close()
            except Exception:
                logger.exception('Unexpected error occured. Failed to fetch data from ClickHouse or write to temp file.')
                raise
            
            if total_rows == 0:
                logger.warning('No data found for minute: %s.', timestamp_str)
                return s3_path
            
            logger.info('Number of rows written: %d', total_rows)
            
            minio_client = get_minio_client()
            obj_name = f'{timestamp_str}.parquet'
            minio_client.fput_object(
                bucket_name=MINIO_BUCKET_NAME,
                object_name=obj_name,
                file_path=tmp_file.name,
            )
            full_path = s3_path + '.parquet'
            logger.info('Successfully uploaded Parquet file to %s', full_path)
        
        return full_path
    
    file_path = stream_from_clickhouse_to_minio()
    
    spark_analysis = SparkSubmitOperator(
        task_id='spark_analysis',
        conn_id=SPARK_CONN_NAME,
        application=SPARK_APPLICATION_PATH,
        application_args=[file_path],
        deploy_mode='client',
        conf={
            'spark.hadoop.fs.s3a.endpoint': 'http://minio:9000',
            'spark.hadoop.fs.s3a.access.key': f'{{{{ conn.{MINIO_CONN_NAME}.login }}}}',
            'spark.hadoop.fs.s3a.secret.key': f'{{{{ conn.{MINIO_CONN_NAME}.password }}}}',
            'spark.hadoop.fs.s3a.path.style.access': 'true',
            'spark.hadoop.fs.s3a.impl': 'org.apache.hadoop.fs.s3a.S3AFileSystem',
            'spark.hadoop.fs.s3a.connection.ssl.enabled': 'false',
            'spark.eventLog.enabled': os.environ['SPARK_EVENT_LOG_ENABLED'],
            'spark.eventLog.dir': '/opt/airflow/logs/spark',
            
        },
        driver_memory='512m',
        executor_memory='512m',
        executor_cores=2,
        num_executors=2,
        verbose=False
    )
    
    @task
    def send_to_dashboard(file_path: str) -> None:
        """Send analysis result to the dashboard api.
        
        Args:
            file_path: MinIO path for the analysis report.
        
        Raises:
            S3Error: If the file cannot be fetched from MinIO.
            JSONDecodeError: If the file contains invalid JSON.
            RequestException: If the dashboard API request fails.
        """
        logger.info(' log Dashboard task- file_path: %s', file_path)
        if 'parquet' in file_path:
            file_path = file_path.replace('parquet', 'json')
        else:
            file_path += '.json'
        
        file_name = file_path.split(os.sep)[-1]
        minio_client = get_minio_client()
        minio_response = None
        try:
            minio_response = minio_client.get_object(bucket_name=MINIO_BUCKET_NAME, object_name=file_name)
            logger.info('Dashboard task try - minio_response: %s', minio_response)
            result = json.loads(minio_response.read())
            logger.info('Dashboard task try - result: %s', result)
            dashboard_response = requests.post(url='http://dashboard-api:8080/report', json=result)
            logger.info('Dashboard task try - dashboard_response: %s', dashboard_response)
            dashboard_response.raise_for_status()
        except S3Error:
            logger.exception('Failed to fetch %s from MinIO', file_name)
            raise
        except json.JSONDecodeError:
            logger.exception('Invalid JSON payload in %s', file_name)
            raise
        except requests.RequestException:
            logger.exception('Dashboard API request failed for %s', file_name)
            raise
        except Exception:
            logger.exception('An unexpected in send_to_dashboard')
            raise
        finally:
            if minio_response:
                minio_response.close()
                minio_response.release_conn()
        
    file_path >> spark_analysis >> send_to_dashboard(file_path=file_path)


etar_pipeline()
