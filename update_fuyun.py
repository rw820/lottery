"""Fetch SSQ prize data from 500.com for draws >= 2026014 to get 福运奖."""
import json
import re
import sqlite3
import time

import requests

DB_PATH = "data/lottery.db"
JSON_PATH = "docs/data/lottery_data.json"
DETAIL_URL = "https://kaijiang.500.com/shtml/ssq/{}.shtml"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://datachart.500.com/ssq/history/",
}

# 500.com prize name -> type mapping, includes 福运奖
PRIZE_ORDER = {
    "一等奖": 1, "二等奖": 2, "三等奖": 3,
    "四等奖": 4, "五等奖": 5, "六等奖": 6,
    "福运奖": 7, "幸运奖": 7,
}


def code_to_500(code: str) -> str:
    """Convert full code like 2026047 to 500.com format like 26047."""
    return code[2:]  # drop the '20' prefix


def fetch_prizes(code: str) -> list:
    """Fetch prize grades from 500.com detail page."""
    url = DETAIL_URL.format(code_to_500(code))
    try:
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
            # Results row: [prize_name, count_number, amount]
            if len(vals) >= 3 and vals[0] in PRIZE_ORDER and vals[1].replace(",", "").strip().isdigit():
                prizes.append({
                    "type": PRIZE_ORDER[vals[0]],
                    "typenum": vals[1].replace(",", "").strip(),
                    "typemoney": vals[2].replace(",", "").strip(),
                })

        prizes.sort(key=lambda x: x["type"])
        return prizes
    except Exception as e:
        print(f"  ERROR fetching {code}: {e}")
        return []


def main():
    # 1. Read existing data
    print("Loading lottery_data.json...")
    with open(JSON_PATH, "r", encoding="utf-8") as f:
        data = json.load(f)

    ssq_draws = data["ssq"]
    print(f"  {len(ssq_draws)} SSQ draws total")

    # 2. Find draws that need updating (>= 2026014)
    to_update = [d for d in ssq_draws if int(d["code"]) >= 2026014]
    print(f"  {len(to_update)} draws from 2026014+")

    # 3. Fetch 福运奖 data from 500.com
    updated = 0
    for i, d in enumerate(to_update):
        code = d["code"]
        # Skip if already has valid type 7 data
        existing = d.get("prize_grades_parsed", [])
        has_fuyun = any(
            p.get("type") == 7 and p.get("typenum", "") and p.get("typemoney", "")
            for p in existing
        )
        if has_fuyun:
            print(f"  [{i+1}/{len(to_update)}] {code}: already has 福运奖 data, skip")
            continue

        print(f"  [{i+1}/{len(to_update)}] {code}: fetching...", end=" ", flush=True)
        prizes = fetch_prizes(code)
        if prizes and len(prizes) >= 6:
            d["prize_grades_parsed"] = prizes
            d["prize_grades"] = json.dumps(prizes, ensure_ascii=False)
            updated += 1
            has_fuyun = any(p["type"] == 7 for p in prizes)
            fuyun_info = ""
            if has_fuyun:
                f7 = next(p for p in prizes if p["type"] == 7)
                fuyun_info = f" (福运奖: {f7['typenum']}注)"
            print(f"OK{', 7 tiers' if len(prizes) == 7 else ', ' + str(len(prizes)) + ' tiers'}{fuyun_info}")
        else:
            print(f"FAILED ({len(prizes)} prizes parsed)")

        time.sleep(0.5)  # be polite

    # 4. Save updated JSON
    print(f"\nSaving lottery_data.json ({updated} draws updated)...")
    with open(JSON_PATH, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    print(f"  Done! ({len(json.dumps(data, ensure_ascii=False)):,} bytes)")

    # 5. Update SQLite database
    print("\nUpdating database...")
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    db_updated = 0
    for d in to_update:
        code = d["code"]
        prizes = d.get("prize_grades_parsed", [])
        if prizes and len(prizes) == 7:  # only update if we have 7 tiers
            pg_json = json.dumps(prizes, ensure_ascii=False)
            conn.execute(
                "UPDATE ssq_draws SET prize_grades = ? WHERE code = ?",
                (pg_json, code),
            )
            if conn.execute("SELECT changes()").fetchone()[0] > 0:
                db_updated += 1

    conn.commit()
    conn.close()
    print(f"  Database: {db_updated} rows updated")

    # 6. Summary
    print(f"\n=== Summary ===")
    print(f"Updated {updated} draws with full prize data from 500.com")
    # Show a sample
    if to_update:
        latest = to_update[0]
        pg = latest.get("prize_grades_parsed", [])
        print(f"\nLatest draw {latest['code']} prize data:")
        for p in pg:
            name = {1: "一等奖", 2: "二等奖", 3: "三等奖", 4: "四等奖",
                    5: "五等奖", 6: "六等奖", 7: "福运奖"}.get(p["type"], f"奖{p['type']}")
            print(f"  {name}: {p['typenum']}注, {p['typemoney']}元")


if __name__ == "__main__":
    main()
