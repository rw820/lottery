"""Scrape DLT 八等奖 data from sporttery.cn for draws 10008-14051.

sporttery.cn individual draw pages have the full prize table including
八等奖 for the 8-tier period. This fills the gap left by 500.com which
shows 八等奖=0 for all pre-2014 draws.
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
DETAIL_URL = "https://www.sporttery.cn/kj/lskj/3{}.html"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "zh-CN,zh;q=0.9",
}
BATCH_SLEEP = 1.5
BATCH_SIZE = 20


def parse_prize_table(html_text: str) -> list:
    """Extract prize rows from sporttery.cn DLT detail page."""
    tables = re.findall(r"<table[^>]*>(.*?)</table>", html_text, re.DOTALL)
    prize_table = None
    for t in tables:
        if "奖等级别" in t or ("等奖" in t and "中奖注数" in t):
            prize_table = t
            break
    if not prize_table:
        return []

    rows = re.findall(r"<tr[^>]*>(.*?)</tr>", prize_table, re.DOTALL)
    results = []
    current_level = ""

    for row in rows:
        cells = re.findall(r"<(?:th|td)[^>]*>(.*?)</(?:th|td)>", row, re.DOTALL)
        clean = [re.sub(r"<[^>]+>", "", c).strip() for c in cells]
        if not clean:
            continue

        # Skip header row
        if clean[0] in ("奖等级别", "奖项"):
            continue

        # Clean numbers: remove spaces, commas, "注", "元"
        def clean_num(s: str) -> str:
            return re.sub(r"[,\s注元]", "", s) if s and s != "---" else "0"

        if len(clean) >= 3:
            first = clean[0]

            if first == "追加":
                # 追加 row (4 cells std, 5 cells bonus-period):
                #   [追加, count, amount, total] or [追加, count, amount, bonus, total]
                results.append({
                    "level": f"{current_level}(追加)",
                    "count": clean_num(clean[1]),
                    "amount": clean_num(clean[2]),
                    "total": clean_num(clean[-1]),
                })
            elif first in ("合计", "---"):
                continue
            elif len(clean) >= 5 and clean[1] == "基本":
                # 基本 row (5 cells std, 6 cells bonus-period):
                #   [level, 基本, count, amount, total] or [... amount, bonus, total]
                current_level = first
                results.append({
                    "level": first,
                    "count": clean_num(clean[2]),
                    "amount": clean_num(clean[3]),
                    "total": clean_num(clean[-1]),
                })
            elif len(clean) >= 4:
                # Tier without 基本/追加 split (e.g. 八等奖)
                #   4 cells std: [level, count, amount, total]
                #   5 cells bonus: [level, count, amount, bonus, total]
                current_level = first
                results.append({
                    "level": first,
                    "count": clean_num(clean[1]),
                    "amount": clean_num(clean[2]),
                    "total": clean_num(clean[-1]),
                })

    return results


def fetch_detail(code: str) -> list:
    """Fetch prize data from sporttery.cn DLT detail page."""
    url = DETAIL_URL.format(code)
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.encoding = "utf-8"
    if resp.status_code != 200:
        logger.warning("%s: HTTP %d", code, resp.status_code)
        return []
    return parse_prize_table(resp.text)


def update_dlt_badeng_from_sporttery():
    """Update DLT draws (10008-14051) with 八等奖 from sporttery.cn."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    rows = conn.execute(
        "SELECT code, prize_grades FROM dlt_draws WHERE code >= ? AND code < ? ORDER BY code ASC",
        ("10008", "14052")
    ).fetchall()

    logger.info("DLT draws 10008-14051: %d total", len(rows))
    if not rows:
        logger.info("No draws in range. Nothing to do.")
        conn.close()
        return 0

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

        # Check if 八等奖 has non-zero count
        has_valid_8 = False
        for g in pg:
            lv = str(g.get("level", "")).replace("(追加)", "")
            if lv == "八等奖":
                cnt = str(g.get("count", "0")).replace(",", "")
                if cnt.isdigit() and int(cnt) > 0:
                    has_valid_8 = True

        if not has_valid_8:
            codes_to_fix.append(r["code"])

    logger.info("Draws needing 八等奖 update: %d", len(codes_to_fix))
    if not codes_to_fix:
        logger.info("All draws already have valid 八等奖 data.")
        conn.close()
        return 0

    updated = 0
    for i, code in enumerate(codes_to_fix):
        try:
            prizes = fetch_detail(code)
        except Exception as e:
            logger.warning("Failed to fetch %s: %s", code, e)
            time.sleep(2)
            continue

        if not prizes:
            logger.warning("%s: no prize data found", code)
            continue

        # Extract only 八等奖 entries
        new_entries = []
        for p in prizes:
            lv = p["level"].replace("(追加)", "")
            if lv == "八等奖":
                new_entries.append(p)

        if not new_entries:
            logger.warning("%s: no 八等奖 in sporttery data", code)
            continue

        # Read existing prize_grades and update
        row = conn.execute(
            "SELECT prize_grades FROM dlt_draws WHERE code=?", (code,)
        ).fetchone()
        try:
            existing = json.loads(row["prize_grades"]) if row["prize_grades"] else []
        except (json.JSONDecodeError, TypeError):
            existing = []

        # Remove old 八等奖 entries (from 500.com with count=0)
        existing = [
            g for g in existing
            if str(g.get("level", "")).replace("(追加)", "") != "八等奖"
        ]
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
            logger.info("Progress: %d/%d updated (%.1f%%)",
                        updated, len(codes_to_fix),
                        updated / len(codes_to_fix) * 100)

        if (i + 1) % BATCH_SIZE == 0:
            time.sleep(BATCH_SLEEP)
        else:
            time.sleep(0.3)

    conn.commit()
    conn.close()
    logger.info("Done: updated %d draws with 八等奖 data from sporttery.cn", updated)
    return updated


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
    )
    update_dlt_badeng_from_sporttery()
