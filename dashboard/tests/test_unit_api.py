import pytest
from fastapi import status
from fastapi.testclient import TestClient

from dashboard_api import app, NO_REPORT_STORED, storage


@pytest.fixture
def client() -> TestClient:
    """Fixture to provide a FastAPI test client."""
    return TestClient(app)


@pytest.fixture(autouse=True)
def clear_storage() -> None:
    """Clear the storage before each test to ensure isolation."""
    storage.clear()


def test_receive_report(client: TestClient) -> None:
    """Test that posting a report stores it correctly and returns 200."""
    report_data = {'report': {'total_events': 100}}
    
    response = client.post('/report', json=report_data)
    
    assert response.status_code == status.HTTP_200_OK
    assert response.json() is None


def test_get_report(client: TestClient) -> None:
    """Test that getting a report returns the stored data correctly."""
    report_data = {'report': {'total_events': 100}}
    
    storage.append(report_data)
    
    response = client.get('/report')
    
    assert response.status_code == status.HTTP_200_OK
    assert response.json() == report_data


def test_get_report_no_data(client: TestClient) -> None:
    """Test that getting a report with no data returns 404 with the correct detail."""
    response = client.get('/report')
    
    assert response.status_code == status.HTTP_404_NOT_FOUND
    assert response.json()['detail'] == NO_REPORT_STORED


def test_health_check(client: TestClient) -> None:
    """Test the health check endpoint returns the expected status and metrics count."""
    response = client.get('/health')
    
    assert response.status_code == 200
    assert response.json() == {'status': 'healthy', 'reports_count': 0}
