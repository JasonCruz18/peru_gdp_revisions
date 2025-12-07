"""Scraper utility functions for Peru GDP RTD pipeline.

This module provides utilities for web scraping, including:
- HTTP session management with retry logic
- Rate limiting to mimic human behavior
"""

import random
import time
from typing import Tuple

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def get_http_session(
    total: int = 3,
    backoff: float = 0.5,
    statuses: Tuple[int, ...] = (429, 500, 502, 503, 504),
) -> requests.Session:
    """Create a persistent HTTP session with retries and exponential backoff.

    This session automatically retries on transient HTTP errors (e.g., 429/5xx),
    making downloads more resilient to network issues and server errors.

    Args:
        total: Max retries for connect/read/status failures (default: 3)
        backoff: Backoff factor - sleep grows as 0.5, 1.0, 2.0, ... (default: 0.5)
        statuses: HTTP status codes that should trigger a retry (default: 429, 500, 502, 503, 504)

    Returns:
        A requests.Session with mounted retry-enabled adapters

    Example:
        >>> session = get_http_session()
        >>> response = session.get("https://example.com/file.pdf")
        >>> if response.status_code == 200:
        ...     with open("file.pdf", "wb") as f:
        ...         f.write(response.content)
    """
    retry = Retry(
        total=total,
        read=total,
        connect=total,
        backoff_factor=backoff,  # Controls exponential sleep between retries
        status_forcelist=statuses,  # Retry only on these HTTP status codes
        allowed_methods=frozenset(["GET", "HEAD"]),  # Retry idempotent methods only
        raise_on_status=False,  # Do not raise; let caller inspect status_code
    )

    sess = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)

    # Apply retry policy to both HTTP and HTTPS
    sess.mount("https://", adapter)
    sess.mount("http://", adapter)

    return sess


def random_wait(min_time: float, max_time: float) -> None:
    """Pause execution for a random duration to mimic human behavior.

    This helps avoid detection as a bot and reduces server load by spacing
    out requests naturally.

    Args:
        min_time: Lower bound for waiting time in seconds
        max_time: Upper bound for waiting time in seconds

    Example:
        >>> random_wait(5.0, 10.0)  # Wait between 5 and 10 seconds
        ⏳ Waiting 7.34 seconds...
    """
    wait_time = random.uniform(min_time, max_time)
    print(f"⏳ Waiting {wait_time:.2f} seconds...")
    time.sleep(wait_time)
