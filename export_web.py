"""Export lottery database to static JSON files for GitHub Pages."""

import json
import math
import os
import sqlite3
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "data", "lottery.db")
OUT_DIR = os.path.join(os.path.dirname(__file__), "docs", "data")
PAGE_SIZE = 50


def _get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def export_draws(lottery: str):
    table = "ssq_draws" if lottery == "ssq" else "dlt_draws"
    out_dir = os.path.join(OUT_DIR, lottery)
    os.makedirs(out_dir, exist_ok=True)

    conn = _get_conn()
    try:
        total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        rows = conn.execute(f"SELECT * FROM {table} ORDER BY code DESC").fetchall()

        all_items = []
        for row in rows:
            d = dict(row)
            # Parse prize_grades
            grades_str = d.get("prize_grades", "")
            if grades_str:
                try:
                    d["prize_grades_parsed"] = json.loads(grades_str)
                except (json.JSONDecodeError, TypeError):
                    d["prize_grades_parsed"] = []
            else:
                d["prize_grades_parsed"] = []
            all_items.append(d)

        # Write paginated files: page_1.json, page_2.json, ...
        total_pages = math.ceil(total / PAGE_SIZE)
        for page in range(1, total_pages + 1):
            start = (page - 1) * PAGE_SIZE
            end = start + PAGE_SIZE
            page_data = {
                "items": all_items[start:end],
                "total": total,
                "page": page,
                "page_size": PAGE_SIZE,
                "total_pages": total_pages,
            }
            path = os.path.join(out_dir, f"page_{page}.json")
            with open(path, "w", encoding="utf-8") as f:
                json.dump(page_data, f, ensure_ascii=False)

        # Write summary (latest 10 + stats)
        latest = conn.execute(
            f"SELECT code, draw_date FROM {table} ORDER BY code DESC LIMIT 10"
        ).fetchall()
        summary = {
            "total": total,
            "total_pages": total_pages,
            "page_size": PAGE_SIZE,
            "latest": [dict(r) for r in latest],
        }
        with open(os.path.join(out_dir, "summary.json"), "w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False)

        print(f"  {lottery}: {total} draws -> {total_pages} pages")
    finally:
        conn.close()


def export_stats():
    conn = _get_conn()
    try:
        result = {}
        for lt in ("ssq", "dlt"):
            table = "ssq_draws" if lt == "ssq" else "dlt_draws"
            total = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            latest = conn.execute(f"SELECT MAX(code) FROM {table}").fetchone()[0]
            result[lt] = {"total": total, "latest_code": latest}
        with open(os.path.join(OUT_DIR, "stats.json"), "w", encoding="utf-8") as f:
            json.dump(result, f, ensure_ascii=False)
        print(f"  stats: {result}")
    finally:
        conn.close()


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    print("Exporting lottery data to static JSON...")
    export_stats()
    export_draws("ssq")
    export_draws("dlt")
    print("Done!")


if __name__ == "__main__":
    main()
