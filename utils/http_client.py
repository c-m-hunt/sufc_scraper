"""Rate-limited HTTP client for web scraping."""

import logging
import random
import time
from typing import Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class HttpClient:
    """HTTP client with rate limiting and retry logic."""

    # Common browser user agents
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    ]

    def __init__(
        self,
        rate_limit: float = 2.0,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
    ):
        """
        Initialize the HTTP client.

        Args:
            rate_limit: Minimum seconds between requests
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            backoff_factor: Factor for exponential backoff between retries
        """
        self.rate_limit = rate_limit
        self.timeout = timeout
        self.last_request_time: float = 0

        self.session = requests.Session()

        # Configure retries
        retry_strategy = Retry(
            total=max_retries,
            backoff_factor=backoff_factor,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def _get_headers(self) -> dict:
        """Get headers with a random user agent."""
        return {
            "User-Agent": random.choice(self.USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-GB,en;q=0.9",
            "Accept-Encoding": "identity",  # Some older sites have compression issues
            "Connection": "keep-alive",
        }

    def _wait_for_rate_limit(self) -> None:
        """Wait if necessary to respect rate limit."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit:
            wait_time = self.rate_limit - elapsed
            # Add some jitter to appear more human
            wait_time += random.uniform(0, 0.5)
            logger.debug(f"Rate limiting: waiting {wait_time:.2f}s")
            time.sleep(wait_time)

    def get(self, url: str, **kwargs) -> requests.Response:
        """
        Make a GET request with rate limiting.

        Args:
            url: The URL to fetch
            **kwargs: Additional arguments passed to requests.get

        Returns:
            The response object

        Raises:
            requests.RequestException: If the request fails after retries
        """
        self._wait_for_rate_limit()

        headers = self._get_headers()
        if "headers" in kwargs:
            headers.update(kwargs.pop("headers"))

        logger.debug(f"Fetching: {url}")
        self.last_request_time = time.time()

        response = self.session.get(
            url,
            headers=headers,
            timeout=kwargs.pop("timeout", self.timeout),
            **kwargs,
        )
        response.raise_for_status()

        return response

    def get_text(self, url: str, **kwargs) -> str:
        """Fetch URL and return response text."""
        return self.get(url, **kwargs).text

    def get_json(self, url: str, **kwargs) -> dict:
        """Fetch URL and return parsed JSON."""
        return self.get(url, **kwargs).json()

    def download(self, url: str, path: str, **kwargs) -> None:
        """Download a file to the specified path."""
        response = self.get(url, stream=True, **kwargs)
        with open(path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
