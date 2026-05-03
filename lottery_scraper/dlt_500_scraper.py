"""Scrape DLT 七等奖/八等奖 data from 500.com detail pages for pre-2014 draws.

sporttery.cn API only returns 6 prize tiers (一等奖-六等奖). The 8-tier period
(code < 14052, years 2007-2014) has 七等奖 and 八等奖 that we must scrape from
500.com detail pages which carry the full prize table.
"""

import json
import logging
import re
import sqlite3
import time
import os

import requests

logger = logging.getLogger(__name__)

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "data", "lottery.db")
DETAIL_URL = "https://kaijiang.500.com/shtml/dlt/{}.shtml"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}
BATCH_SLEEP = 1.5  # seconds between requests
BATCH_SIZE = 20     # save progress every N draws


def parse_prize_table(html_text: str) -> list:
    """Extract all prize rows from the 500.com DLT detail page."""
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html_text, re.DOTALL)
    prize_table = None
    for t in tables:
        if "注数" in t and ("七等奖" in t or "八等奖" in t):
            prize_table = t
            break
    if not prize_table:
        return []

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", prize_table, re.DOTALL)
    results = []
    current_level = ""

    for row in rows:
        cells = re.findall(r"<td[^>]*>(.*?)</td>", row, re.DOTALL)
        clean = [re.sub(r"<[^>]+>", "", c).strip().replace(",", "") for c in cells]
        if not clean:
            continue

        if len(clean) == 5:
            level = clean[0]
            subtype = clean[1]
            count = clean[2]
            amount = clean[3]
            total = clean[4]
            if level in ("合计", "---"):
                continue
            current_level = level
            results.append({
                "level": f"{level}({subtype})" if subtype != "基本" else level,
                "count": count,
                "amount": amount,
                "total": total,
            })
        elif len(clean) == 4 and clean[0] == "追加":
            results.append({
                "level": f"{current_level}(追加)",
                "count": clean[1],
                "amount": clean[2],
                "total": clean[3],
            })

    return results


def fetch_detail(code: str) -> list:
    """Fetch prize data from 500.com DLT detail page."""
    url = DETAIL_URL.format(code)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.encoding = "utf-8"
    return parse_prize_table(resp.text)


def update_missing_dlt_prizes():
    """Update DLT draws (<14052) with 七等奖/八等奖 from 500.com."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    # Find draws that need updating: code < 14052 AND missing 七等奖/八等奖
    rows = conn.execute(
        "SELECT code, prize_grades FROM dlt_draws WHERE code < ? ORDER BY code ASC",
        ("14052",)
    ).fetchall()

    codes_to_fix = []
    for r in rows:
        try:
            pg = json.loads(r["prize_grades"]) if r["prize_grades"] else []
        except (json.JSONDecodeError, TypeError):
            pg = []
        levels = set()
        for g in pg:
            lv = str(g.get("level", "")).replace("(追加)", "")
            levels.add(lv)
        if "七等奖" not in levels or "八等奖" not in levels:
            codes_to_fix.append(r["code"])

    logger.info("DLT draws <14052: %d total, %d need 七/八等奖", len(rows), len(codes_to_fix))
    if not codes_to_fix:
        logger.info("All draws already have complete data. Nothing to do.")
        conn.close()
        return 0

    updated = 0
    for i, code in enumerate(codes_to_fix):
        try:
            prizes = fetch_detail(code)
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", code, e)
            continue

        # Extract only 七等奖 and 八等奖 from 500.com result
        new_entries = []
        for p in prizes:
            lv = p["level"].replace("(追加)", "")
            if lv in ("七等奖", "八等奖"):
                new_entries.append(p)

        if not new_entries:
            logger.warning("%s: no 七/八等奖 found in 500.com data", code)
            continue

        # Read existing prize_grades and append
        row = conn.execute("SELECT prize_grades FROM dlt_draws WHERE code=?", (code,)).fetchone()
        try:
            existing = json.loads(row["prize_grades"]) if row["prize_grades"] else []
        except (json.JSONDecodeError, TypeError):
            existing = []

        # Remove any existing 七等奖/八等奖 entries (avoid duplicates)
        existing = [g for g in existing if str(g.get("level", "")).replace("(追加)", "") not in ("七等奖", "八等奖")]
        existing.extend(new_entries)

        for attempt in range(5):
            try:
                conn.execute(
                    "UPDATE dlt_draws SET prize_grades=? WHERE code=?",
                    (json.dumps(existing, ensure_ascii=False), code)
                )
                break
            except sqlite3.OperationalError:
                if attempt == 4:
                    raise
                time.sleep(1)
        updated += 1

        if (i + 1) % BATCH_SIZE == 0:
            for attempt in range(5):
                try:
                    conn.commit()
                    break
                except sqlite3.OperationalError:
                    if attempt == 4:
                        raise
                    time.sleep(1)
            logger.info("Progress: %d/%d updated (%.1f%%)", updated, len(codes_to_fix),
                        updated / len(codes_to_fix) * 100)

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(BATCH_SLEEP)
        else:
            time.sleep(0.3)  # polite delay between requests

    conn.commit()
    conn.close()
    logger.info("Done: updated %d draws with 七等奖/八等奖 data", updated)
    return updated


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    update_missing_dlt_prizes()
