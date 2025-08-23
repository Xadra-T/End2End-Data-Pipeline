from __future__ import annotations
import logging
from collections import deque
from typing import Any

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel


logger = logging.getLogger(__name__)

app = FastAPI(title='Dashboard API')
storage = deque(maxlen=1)
NO_REPORT_STORED = 'No report stored.'


class AnalysisReport(BaseModel):
    """Incoming analysis report from Airflow."""
    
    report: dict[str, Any] | str


@app.post('/report')
async def receive_report(report: AnalysisReport) -> None:
    """Enpoint for Airflow to push analysis reports.
    
    Cases of a report:
        Case 1: Data:
            {'report': {
                    'total_events': 5805,
                    'total_errors': 1398,
                    'by_event_type': {
                        'ADD_TO_CART': {'SUCCESS': 876, 'ERROR': 292},
                        'CHECKOUT': {'SUCCESS': 846, 'ERROR': 289},
                        'PAYMENT': {'SUCCESS': 884, 'ERROR': 281},
                        'SEARCH': {'SUCCESS': 933, 'ERROR': 261},
                        'VIEW_PRODUCT': {'SUCCESS': 868, 'ERROR': 275}
                    },
                        
                    'process_time': 22.15983009338379,
                    'file_name': '2025-08-04_19-04.json'
                }
            }
        
        Case 2: No Data:
            {'report': 'No data for 2025-08-04_19-04.json.'}
    
    Args:
        report: Analysis report.
    """
    print('Received data:', report)
    storage.append(report)
    print(f'number of reports in storage: {len(storage)}')
    logger.info('log report: %s', report)


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
        HTTPException: If no valid reports exist in storage. The status code is HTTP_404_NOT_FOUND.
    """
    print('Got request to send the most recent report')
    if storage:
        print('Responding with', storage[0])
        return storage[0]
    print('No data to send back.')
    raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=NO_REPORT_STORED)


@app.get('/health')
async def health_check() -> dict[str, Any]:
    """Health check endpoint.
    
    Returns:
        Status and number of reports in the storage.
    """
    return {'status': 'healthy', 'reports_count': len(storage)}


# This is for testability (airflow test_integration_dashboard.py)
@app.delete('/report')
def clear_storage() -> dict[str, str]:
    """Endpoint to clear storage between tests.
    
    Returns:
        Status of the storage.
    """
    storage.clear()
    return {'status': 'cleared'}
