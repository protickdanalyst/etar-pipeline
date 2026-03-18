import io
import json
import logging
import os
import time
import sys
from typing import Any, Dict

from minio import Minio
from minio.commonconfig import CopySource  # available in minio >= 7.1.0
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


logger = logging.getLogger(__name__)


def get_minio_client() -> Minio:
    """Build a MinIO client from environment variables."""
    return Minio(
        endpoint=os.environ['MINIO_ENDPOINT'],
        access_key=os.environ['MINIO_ROOT_USER'],
        secret_key=os.environ['MINIO_ROOT_PASSWORD'],
        secure=False,
    )


def analyze_events(*, spark: SparkSession, file_path: str) -> Dict[str, Any]:
    """Read a Parquet file from S3, perform analysis and return results.

    Returns:
        Analysis result dictionary.
    """
    result: Dict[str, Any] = {}
    df = spark.read.parquet(file_path).cache()
    result['total_events'] = df.count()

    status_counts_df = (
        df.groupBy('event_type')
        .pivot('status', ['ERROR', 'SUCCESS'])
        .count()
        .fillna(0)
    ).orderBy('event_type')

    error_count = status_counts_df.select(F.sum('ERROR')).first()[0]
    result['total_errors'] = int(error_count) if error_count else 0

    event_type_stats: Dict[str, Dict[str, int]] = {}
    for row in status_counts_df.collect():
        row_dict = row.asDict()
        event_type = row_dict['event_type']
        event_type_stats[event_type] = {
            'SUCCESS': int(row_dict.get('SUCCESS', 0)),  # cast to int — Spark returns LongType
            'ERROR': int(row_dict.get('ERROR', 0)),      # which is not JSON-serialisable by default
        }
    result['by_event_type'] = event_type_stats

    df.unpersist()
    return result


def main() -> None:
    """Run the analysis on the given Parquet file path and save the result to MinIO."""
    spark = SparkSession.builder.appName('EventAnalysis').getOrCreate()

    if len(sys.argv) != 2:
        logger.error('Error in calling spark.py. Usage: spark.py <s3a_file_path>')
        spark.stop()
        sys.exit(-1)

    bucket_name = os.environ['MINIO_BUCKET_NAME']
    minio_client = get_minio_client()

    file_path = sys.argv[1]
    # S3 paths always use '/' not os.sep.
    file_name = file_path.split('/')[-1]

    if 'parquet' not in file_name:
        logger.info('Empty file for spark: %s', file_name)
        analysis_result = json.dumps({'report': f'No data for {file_name}.'})
        file_name += '.json'
        spark.stop()
    else:
        start_time = time.time()
        analysis_result_dict: Dict[str, Any] = {}
        try:
            analysis_result_dict.update(analyze_events(spark=spark, file_path=file_path))
        except Exception:
            logger.exception('Analysis failed for %s', file_name)
            # Re-raise immediately — do NOT write analysis_result_dict to MinIO
            # after a failure. Storing a partial/error result and then continuing
            # to put_object would publish corrupt data downstream.
            raise
        finally:
            spark.stop()

        file_name = file_name.replace('parquet', 'json')
        analysis_result_dict['process_time'] = time.time() - start_time
        analysis_result_dict['file_name'] = file_name
        analysis_result = json.dumps({'report': analysis_result_dict})

    encoded = analysis_result.encode('utf-8')

    # Atomic write: upload to a staging key first, then copy to the final key
    # and delete staging. This prevents concurrent readers from observing a
    # partially-written or zero-byte object during the upload window.
    staging_name = file_name + '.tmp'
    minio_client.put_object(
        bucket_name=bucket_name,
        object_name=staging_name,
        data=io.BytesIO(encoded),
        length=len(encoded),    # len(bytes) not len(str) — avoids mismatch for non-ASCII content
        content_type='application/json',
    )
    minio_client.copy_object(
        bucket_name,
        file_name,
        CopySource(bucket_name, staging_name),
    )
    minio_client.remove_object(bucket_name, staging_name)

    logger.info('Successfully wrote result to MinIO: %s/%s', bucket_name, file_name)
    sys.exit(0)


if __name__ == '__main__':
    main()
