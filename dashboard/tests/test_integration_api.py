from __future__ import annotations

from http import HTTPStatus

import pytest
import requests

from dashboard_api import NO_REPORT_STORED


API_BASE_URL = 'http://dashboard-api:8080'
HEALTH_URL = f'{API_BASE_URL}/health'
REPORT_URL = f'{API_BASE_URL}/report'


@pytest.fixture
def api_client():
    """Provide a requests session for making API calls."""
    with requests.Session() as session:
        yield session


def test_health_check(api_client: requests.Session) -> None:
    """Verify that the API health check endpoint is working."""
    response = api_client.get(HEALTH_URL)
    
    assert response.status_code == HTTPStatus.OK
    assert response.json()['status'] == 'healthy'
    assert 'reports_count' in response.json()


def test_get_report_when_storage_is_empty(api_client: requests.Session) -> None:
    """Verify the behavior when the storage is empty.
    
    Scenario: The UI starts before any report has been sent.
    Behavior: The API should return a 404 Not Found.
    """
    response = api_client.get(REPORT_URL)
    
    assert response.status_code == HTTPStatus.NOT_FOUND
    assert response.json()['detail'] == NO_REPORT_STORED


def test_post_and_get_valid_data_report(api_client: requests.Session) -> None:
    """Verify the behavior when posting a valid report and getting it.
    
    Scenario: Airflow posts a valid analysis report. The UI then fetches it.
    Behavior: The API should store the report and return it on a subsequent GET request.
    """
    report_data = {
        "report": {
            "total_events": 5,
            "total_errors": 1,
            "by_event_type": {
                "ADD_TO_CART": {"SUCCESS": 4, "ERROR": 1}
            },
            "process_time": 22.16,
            "file_name": "2025-08-04_19-04.json"
        }
    }
    
    post_response = api_client.post(REPORT_URL, json=report_data)
    assert post_response.status_code == HTTPStatus.OK
    
    get_response = api_client.get(REPORT_URL)
    assert get_response.status_code == HTTPStatus.OK
    assert get_response.json() == report_data


def test_post_and_get_no_data_report(api_client: requests.Session) -> None:
    """Verify the behavior when posting an empty report and getting it.
    
    Scenario: Airflow reports that there was no data to process. The UI fetches this status.
    Behavior: The API should store the string-based report and return it.
    """
    report_data = {'report': 'No data for 2025-08-04_19-04.json.'}
    
    post_response = api_client.post(REPORT_URL, json=report_data)
    assert post_response.status_code == HTTPStatus.OK
    
    get_response = api_client.get(REPORT_URL)
    assert get_response.status_code == HTTPStatus.OK
    assert get_response.json() == report_data


def test_storage_holds_only_the_latest_report(api_client: requests.Session) -> None:
    """Verify the storage only keeps the last report.
    
    Scenario: Airflow sends two reports in quick succession.
    Behavior: The API should only store and return the most recent report.
    """
    first_report = {'report': {'total_events': 100, 'file_name': 'first.json'}}
    second_report = {'report': {'total_events': 200, 'file_name': 'second.json'}}
    
    post_one_response = api_client.post(REPORT_URL, json=first_report)
    assert post_one_response.status_code == HTTPStatus.OK
    
    post_two_response = api_client.post(REPORT_URL, json=second_report)
    assert post_two_response.status_code == HTTPStatus.OK
    
    get_response = api_client.get(REPORT_URL)
    assert get_response.status_code == HTTPStatus.OK
    assert get_response.json() == second_report
    
    health_response = api_client.get(HEALTH_URL)
    assert health_response.json()['reports_count'] == 1
