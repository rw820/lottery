import abc
import logging
import random
import time

import requests

from .config import MAX_RETRIES, REQUEST_DELAY, RETRY_BACKOFF_FACTOR, USER_AGENTS

logger = logging.getLogger(__name__)


class BaseScraper(abc.ABC):
    def __init__(self, delay: float = REQUEST_DELAY, max_retries: int = MAX_RETRIES):
        self.session = requests.Session()
        self.delay = delay
        self.max_retries = max_retries

    def _rotate_ua(self, headers: dict) -> dict:
        headers = dict(headers)
        headers["User-Agent"] = random.choice(USER_AGENTS)
        return headers

    def fetch_with_retry(self, url: str, params: dict, headers: dict) -> requests.Response:
        for attempt in range(self.max_retries):
            try:
                hdrs = self._rotate_ua(headers)
                resp = self.session.get(url, params=params, headers=hdrs, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                wait = self.delay * (RETRY_BACKOFF_FACTOR ** attempt) + random.uniform(0, 1)
                logger.warning("Attempt %d/%d failed: %s. Retrying in %.1fs",
                               attempt + 1, self.max_retries, e, wait)
                if attempt < self.max_retries - 1:
                    time.sleep(wait)
        raise RuntimeError(f"Failed after {self.max_retries} retries: {url}")

    @abc.abstractmethod
    def parse_response(self, data: dict) -> list:
        ...

    @abc.abstractmethod
    def crawl_full(self) -> list:
        ...

    @abc.abstractmethod
    def crawl_incremental(self) -> list:
        ...
