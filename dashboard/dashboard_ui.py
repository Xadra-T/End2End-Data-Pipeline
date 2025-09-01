from __future__ import annotations

import logging
import os
import time
from http import HTTPStatus
from typing import Any

import requests
import streamlit as st
import matplotlib.pyplot as plt
from requests.exceptions import HTTPError


def prepare_timestamp(*, file_path: str) -> str:
    """Convert file path to a proper timestamp format.
    
    Example: 's3/some_bucket/2025-08-04_19-04.json' -> '2025/08/04 19:04'
    Args:
        file_path: Path of the file.
    
    Returns:
        Desired timestamp format.
    """
    return (
        file_path
        .rsplit('/', maxsplit=1)[-1]
        .replace('.json', '')
        .replace('_', ' ')
        .replace('-', '/', 2)
        .replace('-', ':')
    )


def fetch_report(*, url: str, timeout: int) -> dict[str, Any] | None:
    """Get the most recent report via REST API.
    
    Args:
        url: API url.
        timeout: Request timeout.
    
    Returns:
        Report.
    """
    try:
        response = requests.get(url, timeout=timeout)
        logger.info('fetch_reports - response: %s', response)
        response.raise_for_status()
        logger.info('fetch_reports - response.json: %s', response.json())
        return response.json()
    except requests.Timeout:
        logger.info('Timeout occured.')
        return None
    except HTTPError as e:
        if e.response.status_code == HTTPStatus.NOT_FOUND:
            error_detail = e.response.json().get('detail')
            logger.info("Request successful but no data. Detail: %s", error_detail)
        else:
            logger.exception('Unexpected error.')
        return None
    except requests.RequestException:
        logger.exception('Network error connecting to API')
        return None


def show_report(*, report: dict[str, Any]) -> None:
    """Display a bar chart and summary stats for an analysis report.
    
    Sample report:
    {
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
    
    Args:
        report: User event analysis report.
    """
    event_types = []
    successes = []
    errors = []
    for key, value in report['by_event_type'].items():
        event_types.append(key)
        successes.append(value['SUCCESS'])
        errors.append(value['ERROR'])
    
    plt.style.use('dark_background')
    fig, ax = plt.subplots(figsize=(5, 2))
    bar_width = 0.35
    n = list(range(len(event_types)))
    
    bars1 = ax.bar([i - bar_width / 2 for i in n], successes, width=bar_width, label='Success', color='#4CAF50')
    
    bars2 = ax.bar([i + bar_width / 2 for i in n], errors, width=bar_width, label='Error', color='#FF5252')
    
    for bar in bars1:
        height = bar.get_height()
        ax.annotate(
            f'{height:,}', xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=7, color='#4CAF50'
        )
    for bar in bars2:
        height = bar.get_height()
        ax.annotate(
            f'{height:,}', xy=(bar.get_x() + bar.get_width() / 2, height),
            xytext=(0, 3), textcoords="offset points", ha='center', va='bottom', fontsize=7, color='#FF5252'
        )
    
    ax.set_xticks(n)
    ax.set_xticklabels(event_types, rotation=0, color='white', fontsize=7)
    ax.set_ylabel('Count', color='white')
    ax.tick_params(axis='y', colors='white')
    ax.legend(facecolor='#222', edgecolor='white', labelcolor='white', bbox_to_anchor=(1.1, 1))
    
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['bottom'].set_color('white')
    ax.spines['left'].set_color('white')
    
    total_events = f"Total Events: {report.get('total_events', 'N/A'):,}"
    total_errors = f"Total Errors: {report.get('total_errors', 'N/A'):,}"
    timestamp = prepare_timestamp(file_path=report['file_name'])
    summary_text = f'{total_events}    |    {total_errors}    |    Timestamp: {timestamp}'
    ax.text(0.5, -0.18, summary_text, ha='center', va='top', fontsize=14, color='#FFD600', transform=ax.transAxes)
    ax.text(
        0.5, -0.38,
        f'Spark process took {report["process_time"]:.2f} seconds.', ha='center',
        va='top', fontsize=14, color='#FFD600', transform=ax.transAxes
    )
    
    st.pyplot(fig)


def prepare_no_data(*, report: str) -> str:
    timestamp = prepare_timestamp(file_path=report.replace('No data for ', ''))
    return f'No data for {timestamp}'


def prepare_report(*, report: str | dict[str, Any]) -> None:
    """Prepare output based on incoming report.
    
    Args:
        report: Analysis report.
    """
    report = report['report']
    if isinstance(report, str):
        st.subheader(prepare_no_data(report=report))
    else:
        show_report(report=report)


logger = logging.getLogger(__name__)

if __name__ == '__main__':
    st.set_page_config(
        page_icon='📊',
        page_title='Dashboard',
        layout='wide',
        initial_sidebar_state='collapsed'
    )
    
    st.title('Event Analysis Dashboard')
    timeout = 5
    placeholder = st.empty()
    logger.info('Starting streamlit.')
    while True:
        with placeholder.container():
            logger.info('Fetching reports ...')
            report = fetch_report(url=os.environ['REPORTS_URL'], timeout=timeout)
            logger.info('Report: %s', report)
            if not report:
                st.subheader('No analysis report yet...')
            else:
                prepare_report(report=report)
        time.sleep(45)
