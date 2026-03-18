from __future__ import annotations
import json
import logging
import os
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID
try:
    from zoneinfo import ZoneInfo  # Python 3.9+
except ImportError:
    from backports.zoneinfo import ZoneInfo  # For test on spark which has python 3.8

import clickhouse_connect
import pyarrow as pa
import pyarrow.fs as fs
import pyarrow.parquet as pq
import requests
from airflow.hooks.base import BaseHook
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag, task
from clickhouse_connect.driver.exceptions import ClickHouseError
from dotenv import load_dotenv
from minio import Minio
from minio.error import S3Error

row_type = tuple[UUID, UUID, UUID, str, datetime, int, str, int | None, int | None]

load_dotenv()

schema = pa.schema([
    pa.field('event_type', pa.string()),
    pa.field('status', pa.string()),
])

logger = logging.getLogger(__name__)

CLICKHOUSE_CONN_NAME = os.environ['CLICKHOUSE_CONN_NAME']
MINIO_CONN_NAME = os.environ['MINIO_CONN_NAME']
SPARK_CONN_NAME = os.environ['SPARK_CONN_NAME']
SPARK_APPLICATION_PATH = os.environ['SPARK_APPLICATION_PATH']
MINIO_BUCKET_NAME = os.environ['MINIO_BUCKET_NAME']
# MinIO endpoint read from env so it can be used in SparkSubmitOperator conf
# without relying on conn.extra_dejson which is not accessible in Jinja templates.
MINIO_ENDPOINT = os.environ['MINIO_ENDPOINT']
ALERT_WEBHOOK_URL = os.environ.get('ALERT_WEBHOOK_URL', '')


def get_minio_client() -> Minio:
    minio_conn = BaseHook.get_connection(MINIO_CONN_NAME)
    # Connection created with --conn-host so endpoint is in minio_conn.host.
    # Previously stored in extra_dejson which is not Jinja-accessible for
    # SparkSubmitOperator conf values — that caused a silent empty endpoint.
    endpoint = minio_conn.host.replace('http://', '').replace('https://', '')
    minio_client = Minio(
        endpoint=endpoint,
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
    """Log failed task, exception, and send a webhook alert if configured."""
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
    if ALERT_WEBHOOK_URL:
        payload = {
            'dag_id': dag_run.dag_id,
            'task_id': task_instance.task_id,
            'run_id': dag_run.run_id,
            'exception': str(exception),
        }
        try:
            resp = requests.post(ALERT_WEBHOOK_URL, json=payload, timeout=5)
            resp.raise_for_status()
        except requests.RequestException:
            logger.exception('Failed to send failure alert to webhook %s', ALERT_WEBHOOK_URL)


@dag(
    dag_id='clickHouse_pyspark_dashboard',
    description='Extract data from ClickHouse, stream to minio, run spark analysis, report to dashboard.',
    schedule='* * * * *',
    start_date=datetime(2025, 8, 9, tzinfo=ZoneInfo('UTC')),
    default_args={
        'retries': 1,
        # 3 seconds is far too short for infra-level failures (ClickHouse reconnect,
        # MinIO timeout). A retry fires while the root cause still holds, wastes the
        # slot, and marks the DAG failed before any recovery can occur.
        'retry_delay': timedelta(seconds=30),
        'on_success_callback': on_success_callback_func,
        'on_failure_callback': on_failure_callback_func,
    },
    max_active_runs=1,
    catchup=False,
    doc_md="""
    ### ETAR Pipeline
    1. Extract the previous minute data from ClickHouse and stream it into MinIO.
    2. Analyze the data with Spark.
    3. Send the analysis result to the dashboard API.
    """,
    is_paused_upon_creation=False,
    fail_fast=True,
)
def etar_pipeline() -> None:
    """Extract-Transform-Analyze-Report Pipeline:
        1- Stream the previous minute data from ClickHouse into MinIO as a Parquet file.
        2- Trigger Spark analysis.
        3- Report the result back to the dashboard.
    """

    @task
    def stream_from_clickhouse_to_minio(data_interval_start: datetime) -> str:
        """Stream data from ClickHouse to MinIO, return the s3a file path.

        Args:
            data_interval_start: Task start time. Comes from Airflow.

        Returns:
            MinIO s3a path of the Parquet file, or a path without .parquet suffix when no data.

        Raises:
            ClickHouseError: If ClickHouse error happens.
            S3Error: If MinIO error happens.
        """
        ch_conn = BaseHook.get_connection(CLICKHOUSE_CONN_NAME)
        clickhouse_client = clickhouse_connect.get_client(
            host=ch_conn.host,
            port=ch_conn.port,
            user=ch_conn.login,
            password=ch_conn.password,
            database=ch_conn.schema,
        )

        minio_conn = BaseHook.get_connection(MINIO_CONN_NAME)
        s3_fs = fs.S3FileSystem(
            access_key=minio_conn.login,
            secret_key=minio_conn.password,
            endpoint_override=minio_conn.host
        )

        timestamp = data_interval_start.astimezone(ZoneInfo('UTC')).replace(second=0, microsecond=0) - timedelta(minutes=1)
        timestamp_str = timestamp.strftime('%Y-%m-%d_%H-%M')
        parquet_path = f'{MINIO_BUCKET_NAME}/{timestamp_str}.parquet'

        ch_table = os.environ['CLICKHOUSE_TABLE']

        try:
            # Guard: check row count before opening any writers.
            # The original code opened a ParquetWriter eagerly and then deleted the
            # zero-byte file on no-data — creating a race window where concurrent
            # readers could observe a stale or partially-written object.
            # Checking count first avoids writing (and deleting) anything on empty minutes.
            count_query = 'SELECT count() FROM %(table)s WHERE event_minute = %(timestamp)s'
            row_count = clickhouse_client.command(
                count_query,
                parameters={'table': ch_table, 'timestamp': timestamp},
            )
            if row_count == 0:
                logger.warning('No data found for minute: %s.', timestamp_str)
                return 's3a://' + parquet_path.replace('.parquet', '')

            query = 'SELECT event_type, status FROM %(table)s WHERE event_minute = %(timestamp)s;'
            total_rows = 0
            with (
                s3_fs.open_output_stream(path=parquet_path) as s3_stream,
                pq.ParquetWriter(where=s3_stream, schema=schema) as writer,
                clickhouse_client.query_df_stream(
                    query=query,
                    parameters={'table': ch_table, 'timestamp': timestamp},
                    settings={'max_block_size': 100000}
                ) as ch_stream
            ):
                for df_chunk in ch_stream:
                    if df_chunk.empty:
                        break
                    total_rows += len(df_chunk)
                    arrow_table = pa.Table.from_pandas(df=df_chunk, schema=schema, preserve_index=False)
                    writer.write_table(table=arrow_table)
        except ClickHouseError:
            logger.exception('ClickHouse error occurred while streaming from ClickHouse to MinIO.')
            raise
        except S3Error:
            logger.exception('MinIO error occurred while streaming from ClickHouse to MinIO.')
            raise
        except Exception:
            logger.exception('Unexpected error occurred while streaming from ClickHouse to MinIO.')
            raise
        finally:
            clickhouse_client.close()

        if total_rows == 0:
            # COUNT returned > 0 but all streamed chunks were empty.
            # Clean up the empty file and return the no-data path.
            logger.warning('All chunks were empty for minute: %s. Deleting empty file.', timestamp_str)
            s3_fs.delete_file(parquet_path)
            return 's3a://' + parquet_path.replace('.parquet', '')

        logger.info('Successfully uploaded Parquet file to %s. Number of rows written: %d', parquet_path, total_rows)

        return 's3a://' + parquet_path

    file_path = stream_from_clickhouse_to_minio()

    spark_analysis = SparkSubmitOperator(
        task_id='spark_analysis',
        conn_id=SPARK_CONN_NAME,
        application=SPARK_APPLICATION_PATH,
        application_args=[file_path],
        deploy_mode='client',
        conf={
            # Read MinIO credentials from Airflow connection via Jinja templates.
            # conn.{id}.login and .password are valid Jinja-accessible attributes.
            # conn.{id}.extra_dejson is NOT accessible in Jinja — using it caused
            # the endpoint to render as an empty string, silently breaking Spark's
            # S3A access. The endpoint is now sourced from the MINIO_ENDPOINT env var
            # which is reliably available at DAG-parse time.
            'spark.hadoop.fs.s3a.endpoint': f'http://{MINIO_ENDPOINT}',
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
            file_path: MinIO s3a path for the analysis report (Parquet or no-data path).

        Raises:
            S3Error: If the file cannot be fetched from MinIO.
            JSONDecodeError: If the file contains invalid JSON.
            RequestException: If the dashboard API request fails.
        """
        if 'parquet' in file_path:
            file_path = file_path.replace('parquet', 'json')
        else:
            file_path += '.json'

        # S3 paths always use forward slashes regardless of host OS.
        file_name = file_path.split('/')[-1]

        minio_client = get_minio_client()
        minio_response = None
        try:
            minio_response = minio_client.get_object(bucket_name=MINIO_BUCKET_NAME, object_name=file_name)
            result = json.loads(minio_response.read())

            dashboard_response = requests.post(
                url=os.environ['DASHBOARD_API_URL'],
                json=result,
                timeout=10,
            )
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
            logger.exception('An unexpected error in send_to_dashboard')
            raise
        finally:
            if minio_response:
                minio_response.close()
                minio_response.release_conn()

    file_path >> spark_analysis >> send_to_dashboard(file_path=file_path)


etar_pipeline()
