from __future__ import annotations

import io
import json
import os

import pytest
import requests
from minio.error import S3Error

from common import DASHBOARD_API_URL, MINIO_BUCKET_NAME, REPORT_SAMPLE


@pytest.fixture(autouse=True)
def test_setup_teardown(minio_client):
    """Clean the api storage and MinIO bucket before and after each test.

    Raises:
        S3Error: If fails to remove an object from MinIO.
    """
    # FIX T5: DELETE /report is now gated behind ENABLE_TEST_ENDPOINTS=true.
    # Integration test containers must set that env var; the URL stays the same.
    delete_url = f'{DASHBOARD_API_URL}/report'
    requests.delete(delete_url)

    yield

    requests.delete(delete_url)

    try:
        objects = minio_client.list_objects(MINIO_BUCKET_NAME, recursive=True)
        for obj in objects:
            minio_client.remove_object(MINIO_BUCKET_NAME, obj.object_name)
    except S3Error as e:
        # FIX T6: Original used print() in test teardown. Use pytest warnings instead.
        pytest.warns(UserWarning, match=str(e))
        raise


def test_integration_dashboard_success(report_func, minio_client):
    """Test that a valid JSON report is read from MinIO and sent to the dashboard API."""
    report_json = json.dumps(REPORT_SAMPLE)
    encoded = report_json.encode('utf-8')
    object_name = '2025-08-10_12-00.json'

    # FIX T7: Integration test also had the len(str) vs len(bytes) bug. Fixed here too.
    minio_client.put_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(encoded),
        length=len(encoded),
        content_type='application/json',
    )

    file_path = f's3a://{MINIO_BUCKET_NAME}/{object_name.replace("json", "parquet")}'
    report_func(file_path=file_path)

    response = requests.get(DASHBOARD_API_URL)
    response.raise_for_status()

    received_report = response.json()
    assert received_report == REPORT_SAMPLE


def test_integration_dashboard_invalid_filename_failure(report_func):
    """Test that an invalid file path causes S3Error."""
    object_name = 'invalid_filename.json'

    file_path = f's3a://{MINIO_BUCKET_NAME}/{object_name.replace("json", "parquet")}'

    with pytest.raises(S3Error) as exc_info:
        report_func(file_path=file_path)

    assert exc_info.value.code == 'NoSuchKey'


def test_integration_dashboard_invalid_json_failure(report_func, minio_client):
    """Test that an invalid json file causes JSONDecodeError."""
    report_json = "{'bad dict': {'total_events': }}"
    encoded = report_json.encode('utf-8')
    object_name = '2025-08-10_12-00.json'

    # FIX T7: Use len(encoded) not len(str).
    minio_client.put_object(
        bucket_name=MINIO_BUCKET_NAME,
        object_name=object_name,
        data=io.BytesIO(encoded),
        length=len(encoded),
    )

    file_path = f's3a://{MINIO_BUCKET_NAME}/{object_name.replace("json", "parquet")}'

    with pytest.raises(json.JSONDecodeError):
        report_func(file_path=file_path)
