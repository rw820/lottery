import json
import logging
import time

from .base_scraper import BaseScraper
from .config import DEFAULT_PAGE_SIZE, DLT_API_URL, DLT_HEADERS
from .db import get_latest_code, insert_dlt_draws
from .models import DLTDraw

logger = logging.getLogger(__name__)


def _parse_draw_result(raw: str):
    nums = raw.strip().split()
    front = ",".join(nums[:5])
    back = ",".join(nums[5:])
    return front, back


def _parse_prizes(item: dict) -> str:
    prizes = item.get("prizeLevelList", [])
    result = []
    for p in prizes:
        result.append({
            "level": p.get("prizeLevel", ""),
            "count": p.get("stakeCount", ""),
            "amount": p.get("stakeAmountFormat", "0"),
            "total": p.get("totalPrizeamount", "0"),
        })
    return json.dumps(result, ensure_ascii=False)


class DLTScraper(BaseScraper):
    def parse_response(self, data: dict) -> list:
        results = []
        items = data.get("value", {}).get("list", [])
        for item in items:
            try:
                front, back = _parse_draw_result(item["lotteryDrawResult"])
                pool = item.get("poolBalanceAfterdraw", "0").replace(",", "")
                draw = DLTDraw(
                    code=item["lotteryDrawNum"],
                    draw_date=item.get("lotteryDrawTime", ""),
                    front=front,
                    back=back,
                    sales=int(item.get("totalSaleAmount", "0").replace(",", "") or "0"),
                    pool_money=int(float(pool or "0")),
                    prize_grades=_parse_prizes(item),
                )
                results.append(draw)
            except (KeyError, ValueError) as e:
                logger.warning("Failed to parse DLT item %s: %s", item.get("lotteryDrawNum"), e)
        return results

    def _fetch_page(self, page_no: int = 1, page_size: int = DEFAULT_PAGE_SIZE) -> dict:
        params = {
            "gameNo": "85",
            "provinceId": "0",
            "pageSize": str(page_size),
            "is498": "true",
            "pageNo": str(page_no),
        }
        resp = self.fetch_with_retry(DLT_API_URL, params, DLT_HEADERS)
        return resp.json()

    def crawl_full(self) -> list:
        all_draws = []
        page_no = 1

        while True:
            logger.info("DLT: fetching page %d", page_no)
            try:
                data = self._fetch_page(page_no)
            except Exception as e:
                logger.error("DLT: failed page %d: %s", page_no, e)
                break

            draws = self.parse_response(data)
            if not draws:
                break

            all_draws.extend(draws)

            total_pages = data.get("value", {}).get("pages", 1)
            if page_no >= total_pages:
                break

            page_no += 1
            time.sleep(self.delay)

        if all_draws:
            inserted = insert_dlt_draws(all_draws)
            logger.info("DLT: %d draws fetched, %d new inserted", len(all_draws), inserted)

        return all_draws

    def crawl_incremental(self) -> list:
        latest = get_latest_code("dlt")
        if not latest:
            logger.info("DLT: no existing data, falling back to full crawl")
            return self.crawl_full()

        all_draws = []
        page_no = 1

        while True:
            logger.info("DLT: incremental fetch page %d (since %s)", page_no, latest)
            try:
                data = self._fetch_page(page_no)
            except Exception as e:
                logger.error("DLT: incremental fetch failed: %s", e)
                break

            draws = self.parse_response(data)
            if not draws:
                break

            new_draws = [d for d in draws if d.code > latest]
            all_draws.extend(new_draws)

            if len(new_draws) < len(draws):
                break

            total_pages = data.get("value", {}).get("pages", 1)
            if page_no >= total_pages:
                break

            page_no += 1
            time.sleep(self.delay)

        if all_draws:
            inserted = insert_dlt_draws(all_draws)
            logger.info("DLT: %d new draws inserted", inserted)
        else:
            logger.info("DLT: already up to date")

        return all_draws
