import json
import logging

from flask import Blueprint, request, jsonify

from api.checker import check_ticket, get_prize_detail, parse_ticket_line
from lottery_scraper.db import query_draws

logger = logging.getLogger(__name__)

check_bp = Blueprint("check", __name__)


@check_bp.route("/api/check", methods=["POST"])
def check_tickets():
    data = request.get_json()
    if not data:
        return jsonify({"error": "invalid JSON body"}), 400

    lottery = data.get("lottery")
    code = data.get("code")
    tickets = data.get("tickets", [])

    if lottery not in ("ssq", "dlt"):
        return jsonify({"error": "lottery must be ssq or dlt"}), 400
    if not code:
        return jsonify({"error": "code is required"}), 400
    if not tickets:
        return jsonify({"error": "tickets is required"}), 400

    # Fetch draw
    rows = query_draws(lottery, code=code, limit=1)
    if not rows:
        return jsonify({"error": f"draw {code} not found"}), 404

    draw = rows[0]

    results = []
    for t in tickets:
        prize = check_ticket(lottery, t, draw)
        if prize:
            prize = get_prize_detail(lottery, prize, draw)
            results.append({
                "ticket": t,
                "prize": prize["prize"],
                "name": prize["name"],
                "amount": prize["amount"],
            })
        else:
            results.append({
                "ticket": t,
                "prize": None,
                "name": "未中奖",
                "amount": "0",
            })

    # Strip large fields from draw response
    draw_summary = {
        "code": draw["code"],
        "draw_date": draw.get("draw_date", ""),
    }
    if lottery == "ssq":
        draw_summary["red"] = draw["red"]
        draw_summary["blue"] = draw["blue"]
    else:
        draw_summary["front"] = draw["front"]
        draw_summary["back"] = draw["back"]

    return jsonify({"draw": draw_summary, "results": results})


@check_bp.route("/api/check/upload", methods=["POST"])
def check_upload():
    lottery = request.form.get("lottery", "ssq")
    code = request.form.get("code")

    if lottery not in ("ssq", "dlt"):
        return jsonify({"error": "lottery must be ssq or dlt"}), 400
    if not code:
        return jsonify({"error": "code is required"}), 400

    file = request.files.get("file")
    if not file:
        return jsonify({"error": "file is required"}), 400

    content = file.read().decode("utf-8")
    lines = content.strip().split("\n")

    tickets = []
    errors = []
    for i, line in enumerate(lines, 1):
        line = line.strip()
        if not line:
            continue
        parsed = parse_ticket_line(line, lottery)
        if parsed:
            tickets.append(parsed)
        else:
            errors.append({"line": i, "text": line, "error": "invalid format"})

    if not tickets:
        return jsonify({"error": "no valid tickets found", "parse_errors": errors}), 400

    # Fetch draw
    rows = query_draws(lottery, code=code, limit=1)
    if not rows:
        return jsonify({"error": f"draw {code} not found"}), 404

    draw = rows[0]

    results = []
    for t in tickets:
        prize = check_ticket(lottery, t, draw)
        if prize:
            prize = get_prize_detail(lottery, lottery, prize, draw)
            results.append({
                "ticket": t,
                "prize": prize["prize"],
                "name": prize["name"],
                "amount": prize["amount"],
            })
        else:
            results.append({
                "ticket": t,
                "prize": None,
                "name": "未中奖",
                "amount": "0",
            })

    draw_summary = {
        "code": draw["code"],
        "draw_date": draw.get("draw_date", ""),
    }
    if lottery == "ssq":
        draw_summary["red"] = draw["red"]
        draw_summary["blue"] = draw["blue"]
    else:
        draw_summary["front"] = draw["front"]
        draw_summary["back"] = draw["back"]

    return jsonify({
        "draw": draw_summary,
        "results": results,
        "parse_errors": errors,
        "total": len(tickets),
        "winning": sum(1 for r in results if r["prize"] is not None),
    })
