import os

import pytest
from fastapi import status
from fastapi.testclient import TestClient

from common import SAMPLE_REPORT_WITH_DATA
from dashboard_api import app, NO_REPORT_STORED


@pytest.fixture
def client() -> TestClient:
    """Fixture to provide a FastAPI test client."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def clear_storage(client: TestClient) -> None:
    """Clear the storage before each test to ensure isolation."""
    client.delete(os.environ['REPORT_URL'])
    health_response = client.get(os.environ['HEALTH_URL'])
    assert health_response.json()['reports_count'] == 0, 'Storage is not cleared!'


def test_receive_report(client: TestClient) -> None:
    """Test posting a report stores it correctly and returns 200."""
    report_data = SAMPLE_REPORT_WITH_DATA
    
    response = client.post('/report', json=report_data)
    
    assert response.status_code == status.HTTP_200_OK
    assert response.json() is None


def test_get_report(client: TestClient) -> None:
    """Test getting a report returns the stored report correctly."""
    report_data = SAMPLE_REPORT_WITH_DATA
    response = client.post('/report', json=report_data)
    
    response = client.get('/report')
    
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == report_data


def test_get_report_no_data(client: TestClient) -> None:
    """Test getting a report with no data returns 404 with the correct detail."""
    response = client.get('/report')
    
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()['detail'] == NO_REPORT_STORED


def test_health_check(client: TestClient) -> None:
    """Test the health check endpoint returns the expected status and metrics count."""
    response = client.get('/health')
    
    assert response.status_code == 200
    assert response.json() == {'status': 'healthy', 'reports_count': 0}
