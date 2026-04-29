import json
import logging
import re
import time

from .base_scraper import BaseScraper
from .config import DEFAULT_PAGE_SIZE, SSQ_API_URL, SSQ_HEADERS
from .db import get_latest_code, insert_ssq_draws
from .models import SSQDraw

logger = logging.getLogger(__name__)

SSQ_FIRST_YEAR = 2003


def _parse_date(raw: str) -> str:
    return raw.split("(")[0] if "(" in raw else raw


class SSQScraper(BaseScraper):
    def parse_response(self, data: dict) -> list:
        results = []
        for item in data.get("result", []):
            try:
                draw = SSQDraw(
                    code=item["code"],
                    draw_date=_parse_date(item.get("date", "")),
                    week_day=item.get("week", ""),
                    red=item.get("red", ""),
                    blue=item.get("blue", ""),
                    sales=int(item.get("sales", "0") or "0"),
                    pool_money=int(item.get("poolmoney", "0") or "0"),
                    content=item.get("content", ""),
                    prize_grades=json.dumps(item.get("prizegrades", []), ensure_ascii=False),
                )
                results.append(draw)
            except (KeyError, ValueError) as e:
                logger.warning("Failed to parse SSQ item %s: %s", item.get("code"), e)
        return results

    def _fetch_page(self, page_no: int, issue_start: str = "", issue_end: str = "",
                    page_size: int = DEFAULT_PAGE_SIZE) -> dict:
        params = {
            "name": "ssq",
            "issueCount": "",
            "issueStart": issue_start,
            "issueEnd": issue_end,
            "dayStart": "",
            "dayEnd": "",
            "pageNo": page_no,
            "pageSize": page_size,
            "week": "",
            "systemType": "PC",
        }
        resp = self.fetch_with_retry(SSQ_API_URL, params, SSQ_HEADERS)
        return resp.json()

    def crawl_full(self) -> list:
        all_draws = []
        current_year = SSQ_FIRST_YEAR
        import datetime
        end_year = datetime.datetime.now().year

        while current_year <= end_year:
            issue_start = f"{current_year}001"
            issue_end = f"{current_year}999"
            page_no = 1

            while True:
                logger.info("SSQ: fetching year %s, page %d", current_year, page_no)
                try:
                    data = self._fetch_page(page_no, issue_start, issue_end)
                except Exception as e:
                    logger.error("SSQ: failed year %s page %d: %s", current_year, page_no, e)
                    break

                if data.get("state") != 0:
                    logger.warning("SSQ: API error: %s", data.get("message"))
                    break

                draws = self.parse_response(data)
                if not draws:
                    break

                all_draws.extend(draws)
                page_count = data.get("pageCount", 1)
                if page_no >= page_count:
                    break

                page_no += 1
                time.sleep(self.delay)

            current_year += 1

        if all_draws:
            inserted = insert_ssq_draws(all_draws)
            logger.info("SSQ: %d draws fetched, %d new inserted", len(all_draws), inserted)

        return all_draws

    def crawl_incremental(self) -> list:
        latest = get_latest_code("ssq")
        if not latest:
            logger.info("SSQ: no existing data, falling back to full crawl")
            return self.crawl_full()

        all_draws = []
        page_no = 1

        while True:
            logger.info("SSQ: incremental fetch page %d (since %s)", page_no, latest)
            try:
                data = self._fetch_page(page_no, page_size=DEFAULT_PAGE_SIZE)
            except Exception as e:
                logger.error("SSQ: incremental fetch failed: %s", e)
                break

            if data.get("state") != 0:
                break

            draws = self.parse_response(data)
            if not draws:
                break

            new_draws = [d for d in draws if d.code > latest]
            all_draws.extend(new_draws)

            if len(new_draws) < len(draws):
                break

            page_count = data.get("pageCount", 1)
            if page_no >= page_count:
                break

            page_no += 1
            time.sleep(self.delay)

        if all_draws:
            inserted = insert_ssq_draws(all_draws)
            logger.info("SSQ: %d new draws inserted", inserted)
        else:
            logger.info("SSQ: already up to date")

        return all_draws
