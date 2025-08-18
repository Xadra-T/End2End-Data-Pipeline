import io
import json
import os
import time
import sys
import logging

from minio import Minio
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


def analyze_events(spark: SparkSession, file_path: str):
    """Read a Parquet file from S3, perform analysis, write the result to MinIO and return results."""
    logger.debug('Starting analysis for %s', file_path)
    result = dict()
    df = spark.read.parquet(file_path).cache()
    result['total_events'] = df.count()
    
    logger.debug('Read %d rows from %s', result['total_events'], file_path)
    
    status_counts_df = (
        df.groupBy('event_type')
        .pivot('status', ['ERROR', 'SUCCESS'])
        .count()
        .fillna(0)
    ).orderBy('event_type')
    error_count = status_counts_df.select(F.sum('ERROR')).first()[0]
    result['total_errors'] = int(error_count) if error_count is not None else 0
    
    event_type_stats = {}
    for row in status_counts_df.collect():
        event_type = row.asDict()['event_type']
        event_type_stats[event_type] = {
            'SUCCESS': row.asDict().get('SUCCESS', 0),
            'ERROR': row.asDict().get('ERROR', 0),
        }
    result['by_event_type'] = event_type_stats
    
    df.unpersist()
    logger.debug("Analysis finished successfully.")
    return result


def main() -> None:
    spark = SparkSession.builder.appName('EventAnalysis').getOrCreate()
    
    if len(sys.argv) != 2:
        logger.error('Usage: spark_analysis.py <s3a_file_path>')
        spark.stop()
        sys.exit(-1)
    
    bucket_name = os.environ['MINIO_BUCKET_NAME']
    minio_client = Minio(
        endpoint='minio:9000',
        access_key=os.environ['MINIO_ROOT_USER'],
        secret_key=os.environ['MINIO_ROOT_PASSWORD'],
        secure=False
    )
    
    file_path = sys.argv[1]
    file_name = file_path.split(os.sep)[-1]
    
    if 'parquet' not in file_name:
        logger.debug('Empty file for spark: %s', file_name)
        analysis_result = json.dumps({'report': f'No data for {file_name}.'})
        file_name += '.json'
        spark.stop()
    else:
        start_time = time.time()
        analysis_result = {}
        try:
            analysis_result.update(analyze_events(spark=spark, file_path=file_path))
        except Exception as e:
            logger.exception('Analysis failed for %s', file_name)
            analysis_result['error'] = str(e)
            raise
        finally:
            spark.stop()
        
        file_name = file_name.replace('parquet', 'json')
        analysis_result['process_time'] = time.time() - start_time
        analysis_result['file_name'] = file_name
        analysis_result = {'report': analysis_result}
        analysis_result = json.dumps(analysis_result)
        logger.debug('Final result: %s', analysis_result)
    
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=file_name,
        data=io.BytesIO(analysis_result.encode('utf-8')),
        length=len(analysis_result)
    )
    sys.exit(0)


if __name__ == "__main__":
    main()
