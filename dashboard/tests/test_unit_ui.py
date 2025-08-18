from http import HTTPStatus
from unittest.mock import Mock

import requests

from dashboard_ui import prepare_timestamp, fetch_report, prepare_no_data


TIMEOUT = 1


def test_prepare_timestamp() -> None:
    """Test that prepare_timestamp correctly formats the file path into a timestamp string."""
    file_path = 's3/some_bucket/2025-08-04_19-04.json'
    expected = '2025/08/04 19:04'
    
    result = prepare_timestamp(file_path=file_path)
    
    assert result == expected


def test_prepare_no_data() -> None:
    """Test that prepare_no_data correctly formats a 'no data' message into a user-friendly string."""
    report   = 'No data for 2025-08-04_19-04.json'
    expected = 'No data for 2025/08/04 19:04'
    
    result = prepare_no_data(report=report)
    
    assert result == expected


def test_fetch_report_success(mocker) -> None:
    """Test that fetch_report handles a successful API response and returns the JSON data."""
    report = {'report': {'total_events': 100}}
    
    mock_response = Mock()
    mock_response.status_code = HTTPStatus.OK
    mock_response.json.return_value = report
    mock_get = mocker.patch('requests.get', return_value=mock_response)
    
    result = fetch_report(url='http://test-url', timeout=TIMEOUT)
    
    assert result == report
    mock_get.assert_called_once_with('http://test-url', timeout=TIMEOUT)


def test_fetch_report_timeout(mocker) -> None:
    """Test that fetch_report handles a timeout exception and returns None."""
    mock_get = mocker.patch('requests.get', side_effect=requests.Timeout)
    
    result = fetch_report(url='http://test-url', timeout=TIMEOUT)
    
    assert result is None
    mock_get.assert_called_once_with('http://test-url', timeout=TIMEOUT)


def test_fetch_report_404(mocker) -> None:
    """Test that fetch_report handles a 404 HTTP error and returns None."""
    mock_response = Mock()
    mock_response.status_code = HTTPStatus.NOT_FOUND
    mock_response.json.return_value = {'detail': 'No report stored.'}
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
    mock_get = mocker.patch('requests.get', return_value=mock_response)
    
    result = fetch_report(url='http://test-url', timeout=TIMEOUT)
    
    assert result is None
    mock_get.assert_called_once_with('http://test-url', timeout=TIMEOUT)


def test_fetch_report_unexpected_http_error(mocker) -> None:
    """Test that fetch_report handles an unexpected HTTP error (non-404) and returns None."""
    mock_response = Mock()
    mock_response.status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError(response=mock_response)
    mock_get = mocker.patch('requests.get', return_value=mock_response)
    
    result = fetch_report(url='http://test-url', timeout=TIMEOUT)
    
    assert result is None
    mock_get.assert_called_once_with('http://test-url', timeout=TIMEOUT)


def test_fetch_report_network_error(mocker) -> None:
    """Test that fetch_report handles a general network error and returns None."""
    mock_get = mocker.patch('requests.get', side_effect=requests.RequestException('Network error'))
    
    result = fetch_report(url='http://test-url', timeout=TIMEOUT)
    
    assert result is None
    mock_get.assert_called_once_with('http://test-url', timeout=TIMEOUT)
