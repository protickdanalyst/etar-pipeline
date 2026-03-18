from __future__ import annotations

import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from common import MINIO_BUCKET_NAME, insert_test_data


def test_e2e_with_data(dag, clickhouse_client, minio_client, delete_all_data):
    """Test end-to-end with data."""
    num_rows = 5
    test_timestamp = datetime.now(tz=ZoneInfo('UTC')) - timedelta(minutes=1)
    insert_test_data(clickhouse_client, test_timestamp, num_rows=num_rows)

    dag.test(logical_date=test_timestamp + timedelta(minutes=1))

    # FIX T1: Pipeline now uses UTC for file naming (was Asia/Tehran).
    timestamp_str = test_timestamp.strftime('%Y-%m-%d_%H-%M')
    file_name = f'{timestamp_str}.json'
    minio_response = minio_client.get_object(bucket_name=MINIO_BUCKET_NAME, object_name=file_name)
    report = json.loads(minio_response.read())
    assert isinstance(report['report'], dict)
    assert report['report']['total_events'] == num_rows


def test_e2e_without_data(dag, minio_client, delete_all_data):
    """Test end-to-end without data — expects a no-data JSON report in MinIO."""
    test_timestamp = datetime.now(tz=ZoneInfo('UTC')) - timedelta(minutes=1)

    dag.test(logical_date=test_timestamp + timedelta(minutes=1))

    # FIX T1: Use UTC for filename.
    timestamp_str = test_timestamp.strftime('%Y-%m-%d_%H-%M')
    file_name = f'{timestamp_str}.json'
    minio_response = minio_client.get_object(bucket_name=MINIO_BUCKET_NAME, object_name=file_name)
    report = json.loads(minio_response.read())
    assert report['report'] == f'No data for {timestamp_str}.'
