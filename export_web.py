"""Export lottery database to single JSON file for GitHub Pages."""

import json
import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "lottery.db")
OUT_FILE = os.path.join(os.path.dirname(__file__), "docs", "data", "lottery_data.json")


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    os.makedirs(os.path.dirname(OUT_FILE), exist_ok=True)

    result = {}
    for lt in ("ssq", "dlt"):
        table = "ssq_draws" if lt == "ssq" else "dlt_draws"
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY code DESC").fetchall()
        items = []
        for row in rows:
            d = dict(row)
            grades = d.get("prize_grades", "")
            try:
                d["prize_grades_parsed"] = json.loads(grades) if grades else []
            except (json.JSONDecodeError, TypeError):
                d["prize_grades_parsed"] = []
            items.append(d)
        result[lt] = items
        print(f"  {lt}: {len(items)} draws")

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(result, f, ensure_ascii=False)

    size_mb = os.path.getsize(OUT_FILE) / 1024 / 1024
    print(f"  -> {OUT_FILE} ({size_mb:.1f} MB)")
    conn.close()


if __name__ == "__main__":
    main()
