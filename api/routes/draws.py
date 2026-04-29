import math

from flask import Blueprint, request, jsonify

from lottery_scraper.db import count_draws, get_draw_count, get_latest_code, query_draws

draws_bp = Blueprint("draws", __name__)


@draws_bp.route("/api/draws")
def list_draws():
    lottery = request.args.get("lottery", "ssq")
    if lottery not in ("ssq", "dlt"):
        return jsonify({"error": "lottery must be ssq or dlt"}), 400

    page = max(1, int(request.args.get("page", 1)))
    page_size = min(100, max(1, int(request.args.get("page_size", 20))))
    code = request.args.get("code")
    date_start = request.args.get("date_start")
    date_end = request.args.get("date_end")
    order = request.args.get("order", "DESC").upper()
    if order not in ("ASC", "DESC"):
        order = "DESC"

    offset = (page - 1) * page_size
    total = count_draws(lottery, code=code, date_start=date_start, date_end=date_end)
    rows = query_draws(
        lottery, code=code, date_start=date_start, date_end=date_end,
        limit=page_size, order=order, offset=offset,
    )

    return jsonify({
        "items": rows,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": math.ceil(total / page_size) if total > 0 else 0,
    })


@draws_bp.route("/api/draws/<lottery>/<code>")
def get_draw(lottery, code):
    if lottery not in ("ssq", "dlt"):
        return jsonify({"error": "lottery must be ssq or dlt"}), 400

    rows = query_draws(lottery, code=code, limit=1)
    if not rows:
        return jsonify({"error": "not found"}), 404

    draw = rows[0]
    # Parse prize_grades for frontend display
    import json
    grades = draw.get("prize_grades", "")
    if grades:
        try:
            draw["prize_grades_parsed"] = json.loads(grades)
        except (json.JSONDecodeError, TypeError):
            draw["prize_grades_parsed"] = []

    return jsonify(draw)


@draws_bp.route("/api/stats")
def stats():
    result = {}
    for lt in ("ssq", "dlt"):
        result[lt] = {
            "total": get_draw_count(lt),
            "latest_code": get_latest_code(lt),
        }
    return jsonify(result)
