from __future__ import annotations
import logging
import os
from collections import deque
from datetime import datetime, timezone
from typing import Any

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel


logger = logging.getLogger(__name__)

app = FastAPI(title='Dashboard API')
storage: deque = deque(maxlen=1)
last_received_at: datetime | None = None
NO_REPORT_STORED = 'No report stored.'


class AnalysisReport(BaseModel):
    """Incoming analysis report from Airflow."""
    report: dict[str, Any] | str


@app.post('/report', status_code=status.HTTP_204_NO_CONTENT)
async def receive_report(report: AnalysisReport) -> None:
    """Endpoint for Airflow to push analysis reports.

    Cases of a report:
        Case 1: Data:
            {'report': {
                    'total_events': 5805,
                    'total_errors': 1398,
                    'by_event_type': {
                        'ADD_TO_CART': {'SUCCESS': 876, 'ERROR': 292},
                        ...
                    },
                    'process_time': 22.15,
                    'file_name': '2025-08-04_19-04.json'
                }
            }

        Case 2: No Data:
            {'report': 'No data for 2025-08-04_19-04.json.'}

    Args:
        report: Analysis report.
    """
    global last_received_at
    logger.info('Received report: %s', report)
    storage.append(report)
    last_received_at = datetime.now(timezone.utc)
    logger.info('Reports in storage: %d', len(storage))


@app.get(
    path='/report',
    response_model=AnalysisReport,
    summary='Get the most recent report.',
    responses={status.HTTP_404_NOT_FOUND: {'description': NO_REPORT_STORED}}
)
async def get_report() -> AnalysisReport:
    """Return the most recent report.

    Returns:
        The most recent report.

    Raises:
        HTTPException: If no valid reports exist in storage. Status code is HTTP_404_NOT_FOUND.
    """
    logger.info('GET /report requested')
    if storage:
        report = storage[0]
        logger.info('Returning report: %s', report)
        return report
    logger.info('No report in storage.')
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=NO_REPORT_STORED)


@app.get('/health')
async def health_check() -> dict[str, Any]:
    """Health check endpoint.

    Returns:
        Status, number of reports in storage, and age of the last report in seconds.
        A None last_report_age_seconds indicates no report has been received yet —
        consumers can use this as a staleness signal.
    """
    age_seconds: float | None = None
    if last_received_at is not None:
        age_seconds = (datetime.now(timezone.utc) - last_received_at).total_seconds()
    return {
        'status': 'healthy',
        'reports_count': len(storage),
        'last_report_age_seconds': age_seconds,
    }


# DELETE /report is gated behind ENABLE_TEST_ENDPOINTS=true.
# Never set this variable in production.
if os.environ.get('ENABLE_TEST_ENDPOINTS', '').lower() == 'true':
    @app.delete('/report', status_code=status.HTTP_204_NO_CONTENT)
    def clear_storage() -> None:
        """Clear storage between tests. Only active when ENABLE_TEST_ENDPOINTS=true."""
        global last_received_at
        storage.clear()
        last_received_at = None
        logger.warning('Storage cleared via DELETE /report (test endpoint).')
