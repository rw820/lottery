"""Quick update script: fetch latest SSQ and DLT draws and insert into DB."""
import json
import re
import sqlite3
import os
import sys

# Add parent to path so we can import from lottery_scraper
sys.path.insert(0, os.path.dirname(__file__))
from lottery_scraper.nuxt_payload_parser import fetch_and_parse_dlt
import requests

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "lottery.db")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
}


def clean_num(s):
    """Clean Chinese-format numbers to integers."""
    if isinstance(s, (int, float)):
        return int(s)
    if not s or s == "---" or s == "":
        return 0
    s = str(s).replace(",", "").replace("注", "").replace("元", "").strip()
    # Handle "x.xx亿" format
    yi_match = re.match(r"([\d.]+)亿", s)
    if yi_match:
        return int(float(yi_match.group(1)) * 100_000_000)
    try:
        return int(float(s))
    except ValueError:
        return 0


def update_dlt(code_str):
    """Fetch DLT draw and insert into DB."""
    data = fetch_and_parse_dlt(code_str)
    if not data or "current" not in data:
        print(f"FAILED: DLT {code_str} - no data")
        return False

    c = data["current"]
    code = c.get("periodicalnum", "")
    resulttime = c.get("resulttime", "")
    result = c.get("result", "")
    resultspecial = c.get("resultspecial", "")
    totalmoney = c.get("totalmoney", "0")
    ccmoney = c.get("ccmoney", "0")
    week_day = c.get("week_day", "")

    # Parse date
    draw_date = resulttime.split(" ")[0] if resulttime else ""

    print(f"DLT {code}: {result} + {resultspecial} on {draw_date}")

    # Build prize grades list (current 7-tier period: levels 1-7, with 追加 for 1-3)
    prizes = []
    for lv in range(1, 8):
        count = clean_num(c.get(f"num{lv}", 0))
        amount = clean_num(c.get(f"money{lv}", 0))
        total = clean_num(c.get(f"pmoney{lv}", 0))
        if count > 0 or amount > 0:
            prizes.append({
                "level": f"{lv}等奖",
                "count": str(count),
                "amount": str(amount),
                "total": str(total),
            })
        # Check for 追加 (additional prize, only for levels 1-3)
        if lv <= 3:
            add_count = clean_num(c.get(f"additionnum{lv}", 0))
            add_amount = clean_num(c.get(f"additionmoney{lv}", 0))
            add_total = clean_num(c.get(f"padditionmoney{lv}", 0))
            if add_count > 0 or add_amount > 0:
                prizes.append({
                    "level": f"{lv}等奖(追加)",
                    "count": str(add_count),
                    "amount": str(add_amount),
                    "total": str(add_total),
                })

    sales = clean_num(totalmoney)
    pool = clean_num(ccmoney)

    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT OR REPLACE INTO dlt_draws (code, draw_date, front, back, sales, pool_money, prize_grades) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (code, draw_date, result, resultspecial, sales, pool, json.dumps(prizes, ensure_ascii=False))
    )
    conn.commit()
    conn.close()
    print(f"  Inserted: sales={sales}, pool={pool}, prizes={len(prizes)} tiers")
    return True


def update_ssq(code_str):
    """Fetch SSQ draw from 500.com datachart and insert into DB."""
    url = f"https://datachart.500.com/ssq/history/newinc/history.php?start={code_str}&end={code_str}"
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.encoding = "utf-8"

    # Parse table row
    rows = re.findall(r'<tr class="t_tr1">(.*?)</tr>', resp.text, re.DOTALL)
    if not rows:
        print(f"FAILED: SSQ {code_str} - no rows in datachart")
        return False

    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row)
        clean = [re.sub(r'<[^>]+>', '', c).strip() for c in cells]
        if len(clean) < 7:
            continue
        # Format: [row_num, code, r1, r2, r3, r4, r5, r6, b1, b2_empty, sales_or_pool, ..., date]
        code = clean[1]
        # Convert short year to 4-digit year for SSQ consistency
        # 500.com uses 2-digit code (26048), DB uses 4-digit (2026048)
        if len(code) == 5 and code.startswith("2"):
            code = "20" + code
        red_balls = ",".join(clean[2:8])
        blue_ball = clean[8] if clean[8] != "&nbsp;" else ""
        draw_date = clean[-1]

        print(f"SSQ {code}: {red_balls} + {blue_ball} on {draw_date}")

        # Try to get more data from other columns
        # The columns after blue ball may contain sales, pool, etc.
        # But they're all 0 for now — check later columns
        sales = 0
        pool = 0
        prizes = []

        # Check if any prize data is available
        # Column layout (from datachart header):
        #   ..., sales, pool, 1st_count, 1st_amount, 2nd_count, 2nd_amount, date
        if len(clean) >= 13:
            sales_str = clean[9] if clean[9] != "&nbsp;" else "0"
            pool_str = clean[10] if clean[10] != "&nbsp;" else "0"
            sales = clean_num(sales_str)
            pool = clean_num(pool_str)

        # Build basic prize list if data exists
        if len(clean) >= 15:
            for lv, (cnt_idx, amt_idx) in enumerate([(11, 12), (13, 14)], 1):
                cnt = clean_num(clean[cnt_idx])
                amt = clean_num(clean[amt_idx])
                if cnt > 0 or amt > 0:
                    prizes.append({
                        "type": lv,
                        "typenum": str(cnt),
                        "typemoney": str(amt),
                    })

        conn = sqlite3.connect(DB_PATH)
        conn.execute(
            "INSERT OR REPLACE INTO ssq_draws (code, draw_date, week_day, red, blue, sales, pool_money, content, prize_grades) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (code, draw_date, "", red_balls, blue_ball, sales, pool, "", json.dumps(prizes, ensure_ascii=False))
        )
        conn.commit()
        conn.close()
        print(f"  Inserted: sales={sales}, pool={pool}, prizes={len(prizes)} tiers")
        return True

    print(f"FAILED: SSQ {code_str} - no data row found")
    return False


def main():
    print("=== Updating DLT 26047 ===")
    update_dlt("26047")

    print("\n=== Updating SSQ 26048 ===")
    update_ssq("26048")

    # Verify
    conn = sqlite3.connect(DB_PATH)
    for table, col in [("ssq_draws", "code"), ("dlt_draws", "code")]:
        row = conn.execute(f"SELECT {col}, draw_date FROM {table} ORDER BY {col} DESC LIMIT 3").fetchall()
        print(f"\nLatest {table}:")
        for r in row:
            print(f"  {r[0]} {r[1]}")
    conn.close()


if __name__ == "__main__":
    main()
