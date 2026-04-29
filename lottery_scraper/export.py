import csv
import json
import logging
import os
from datetime import datetime
from typing import List, Optional

from .config import CSV_DIR
from .db import query_draws

logger = logging.getLogger(__name__)

SSQ_PRIZE_NAMES = {
    1: "一等奖", 2: "二等奖", 3: "三等奖",
    4: "四等奖", 5: "五等奖", 6: "六等奖",
}

DLT_PRIZE_NAMES = {
    "一等奖": "一等奖", "一等奖(追加)": "一等奖追加",
    "二等奖": "二等奖", "二等奖(追加)": "二等奖追加",
    "三等奖": "三等奖", "三等奖(追加)": "三等奖追加",
    "四等奖": "四等奖", "四等奖(追加)": "四等奖追加",
    "五等奖": "五等奖", "六等奖": "六等奖",
    "七等奖": "七等奖", "八等奖": "八等奖",
    "九等奖": "九等奖",
}


def _expand_ssq_prizes(prize_grades_str: str) -> dict:
    result = {}
    try:
        grades = json.loads(prize_grades_str) if prize_grades_str else []
        for g in grades:
            t = g.get("type", 0)
            count = g.get("typenum", "")
            money = g.get("typemoney", "")
            name = SSQ_PRIZE_NAMES.get(t, f"奖{t}")
            result[f"{name}_注数"] = count
            result[f"{name}_金额"] = money
    except (json.JSONDecodeError, TypeError):
        pass
    return result


def _expand_dlt_prizes(prize_grades_str: str) -> dict:
    result = {}
    try:
        grades = json.loads(prize_grades_str) if prize_grades_str else []
        for g in grades:
            level = g.get("level", "")
            count = g.get("count", "")
            amount = g.get("amount", "")
            total = g.get("total", "")
            short = DLT_PRIZE_NAMES.get(level, level)
            result[f"{short}_注数"] = count
            result[f"{short}_单注金额"] = amount
            result[f"{short}_总金额"] = total
    except (json.JSONDecodeError, TypeError):
        pass
    return result


def _ssq_rows_to_csv(rows: List[dict], path: str):
    base_fields = ["code", "draw_date", "week_day",
                   "red1", "red2", "red3", "red4", "red5", "red6", "blue",
                   "sales", "pool_money", "content"]

    all_prize_keys = set()
    expanded = []
    for row in rows:
        ex = _expand_ssq_prizes(row.get("prize_grades", ""))
        all_prize_keys.update(ex.keys())
        expanded.append(ex)
    prize_fields = sorted(all_prize_keys)

    fieldnames = base_fields + prize_fields
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row, ex in zip(rows, expanded):
            reds = row["red"].split(",")
            out = {
                "code": row["code"],
                "draw_date": row["draw_date"],
                "week_day": row.get("week_day", ""),
                "red1": reds[0] if len(reds) > 0 else "",
                "red2": reds[1] if len(reds) > 1 else "",
                "red3": reds[2] if len(reds) > 2 else "",
                "red4": reds[3] if len(reds) > 3 else "",
                "red5": reds[4] if len(reds) > 4 else "",
                "red6": reds[5] if len(reds) > 5 else "",
                "blue": row["blue"],
                "sales": row.get("sales", ""),
                "pool_money": row.get("pool_money", ""),
                "content": row.get("content", ""),
            }
            out.update(ex)
            writer.writerow(out)


def _dlt_rows_to_csv(rows: List[dict], path: str):
    base_fields = ["code", "draw_date",
                   "front1", "front2", "front3", "front4", "front5",
                   "back1", "back2", "sales", "pool_money"]

    all_prize_keys = set()
    expanded = []
    for row in rows:
        ex = _expand_dlt_prizes(row.get("prize_grades", ""))
        all_prize_keys.update(ex.keys())
        expanded.append(ex)
    prize_fields = sorted(all_prize_keys)

    fieldnames = base_fields + prize_fields
    with open(path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        for row, ex in zip(rows, expanded):
            fronts = row["front"].split(",")
            backs = row["back"].split(",")
            out = {
                "code": row["code"],
                "draw_date": row["draw_date"],
                "front1": fronts[0] if len(fronts) > 0 else "",
                "front2": fronts[1] if len(fronts) > 1 else "",
                "front3": fronts[2] if len(fronts) > 2 else "",
                "front4": fronts[3] if len(fronts) > 3 else "",
                "front5": fronts[4] if len(fronts) > 4 else "",
                "back1": backs[0] if len(backs) > 0 else "",
                "back2": backs[1] if len(backs) > 1 else "",
                "sales": row.get("sales", ""),
                "pool_money": row.get("pool_money", ""),
            }
            out.update(ex)
            writer.writerow(out)


def export_to_csv(
    lottery: str,
    output_path: Optional[str] = None,
    code: Optional[str] = None,
    date_start: Optional[str] = None,
    date_end: Optional[str] = None,
    latest: Optional[int] = None,
) -> str:
    rows = query_draws(lottery, code=code, date_start=date_start,
                       date_end=date_end, limit=latest, order="DESC")
    if not rows:
        logger.warning("No data to export for %s", lottery)
        return ""

    if not output_path:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(CSV_DIR, f"{lottery}_{ts}.csv")

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    if lottery == "ssq":
        _ssq_rows_to_csv(rows, output_path)
    else:
        _dlt_rows_to_csv(rows, output_path)

    logger.info("Exported %d rows to %s", len(rows), output_path)
    return output_path
