import json
import logging
import re
import time

import requests

from .db import insert_ssq_draws
from .models import SSQDraw

logger = logging.getLogger(__name__)

BASE_URL = "https://datachart.500.com/ssq/history/newinc/history.php"
DETAIL_URL = "https://kaijiang.500.com/shtml/ssq/{}.shtml"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Referer": "https://datachart.500.com/ssq/history/",
}

PRIZE_ORDER = {"一等奖": 1, "二等奖": 2, "三等奖": 3, "四等奖": 4, "五等奖": 5, "六等奖": 6}


def _parse_amount(s: str) -> int:
    return int(s.replace(",", "").replace(chr(160), "").strip()) if s.strip() else 0


def fetch_detail_prizes(code: str) -> str:
    """Fetch full 6-level prize details from 500.com detail page."""
    try:
        url = DETAIL_URL.format(code)
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.encoding = "utf-8"
        text = resp.text

        rows = re.findall(r"<tr[^>]*>(.*?)</tr>", text, re.DOTALL)
        prizes = []
        for row in rows:
            cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
            if not cells:
                continue
            vals = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
            if len(vals) >= 3 and vals[0] in PRIZE_ORDER and vals[1].strip().isdigit():
                prizes.append({
                    "type": PRIZE_ORDER[vals[0]],
                    "typenum": vals[1].strip(),
                    "typemoney": vals[2].replace(",", "").strip(),
                })

        prizes.sort(key=lambda x: x["type"])
        return json.dumps(prizes, ensure_ascii=False) if prizes else ""
    except Exception as e:
        logger.warning("Failed to fetch detail for %s: %s", code, e)
        return ""


def fetch_500com_range(start: str, end: str) -> list:
    """Fetch SSQ draws from 500.com. Issue format: '03001'-'03100'."""
    resp = requests.get(BASE_URL, params={"start": start, "end": end}, headers=HEADERS, timeout=30)
    resp.encoding = "gb2312"
    text = resp.text

    rows = re.findall(r'<tr[^>]*class="t_tr1"[^>]*>(.*?)</tr>', text, re.DOTALL)
    draws = []
    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        vals = [re.sub(r"<[^>]+>", "", c).strip().replace(chr(160), "") for c in cells]
        if len(vals) < 17:
            continue
        try:
            code = vals[1]
            red = ",".join(vals[2:8])
            blue = vals[8]
            pool_money = _parse_amount(vals[10])
            sales = _parse_amount(vals[15])
            draw_date = vals[16]

            draws.append(SSQDraw(
                code=code,
                draw_date=draw_date,
                week_day="",
                red=red,
                blue=blue,
                sales=sales,
                pool_money=pool_money,
                content="",
                prize_grades="",
            ))
        except (ValueError, IndexError) as e:
            logger.warning("Failed to parse 500.com row: %s", e)
    return draws


def crawl_500com_history() -> int:
    """Fetch SSQ 2003-2012 data from 500.com with full 6-level prize details."""
    total_inserted = 0
    for year in range(2003, 2013):
        yy = str(year)[2:]
        start = f"{yy}001"
        end = f"{yy}999"
        logger.info("500.com: fetching year %s", year)
        try:
            draws = fetch_500com_range(start, end)
        except Exception as e:
            logger.error("500.com: failed year %s: %s", year, e)
            continue

        for i, d in enumerate(draws):
            prizes = fetch_detail_prizes(d.code)
            if prizes:
                d.prize_grades = prizes
            if (i + 1) % 10 == 0:
                time.sleep(1.0)

        if draws:
            inserted = insert_ssq_draws(draws)
            total_inserted += inserted
            logger.info("500.com: year %s - %d fetched, %d inserted", year, len(draws), inserted)
        time.sleep(1.5)
    return total_inserted
