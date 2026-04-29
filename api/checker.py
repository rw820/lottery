import json
from typing import Optional


SSQ_PRIZE_NAMES = {
    1: "一等奖", 2: "二等奖", 3: "三等奖",
    4: "四等奖", 5: "五等奖", 6: "六等奖",
}

DLT_PRIZE_NAMES = {
    1: "一等奖", 2: "二等奖", 3: "三等奖",
    4: "四等奖", 5: "五等奖", 6: "六等奖",
    7: "七等奖", 8: "八等奖", 9: "九等奖",
}


def _match_ssq(ticket_red: set, ticket_blue: str, draw_red: set, draw_blue: str) -> Optional[dict]:
    red_match = len(ticket_red & draw_red)
    blue_match = 1 if ticket_blue == draw_blue else 0

    if red_match == 6 and blue_match == 1:
        prize = 1
    elif red_match == 6 and blue_match == 0:
        prize = 2
    elif red_match == 5 and blue_match == 1:
        prize = 3
    elif (red_match == 5 and blue_match == 0) or (red_match == 4 and blue_match == 1):
        prize = 4
    elif (red_match == 4 and blue_match == 0) or (red_match == 3 and blue_match == 1):
        prize = 5
    elif blue_match == 1:
        prize = 6
    else:
        return None

    amount = _get_ssq_prize_amount(prize, draw_red, draw_blue)
    return {"prize": prize, "name": SSQ_PRIZE_NAMES[prize], "amount": amount}


def _get_ssq_prize_amount(prize: int, draw_red: set, draw_blue: str) -> str:
    # Fixed amounts for lower tiers
    fixed = {5: "200", 6: "5"}
    if prize in fixed:
        return fixed[prize]
    return "浮动"


def _match_dlt(ticket_front: set, ticket_back: set, draw_front: set, draw_back: set) -> Optional[dict]:
    front_match = len(ticket_front & draw_front)
    back_match = len(ticket_back & draw_back)

    if front_match == 5 and back_match == 2:
        prize = 1
    elif front_match == 5 and back_match == 1:
        prize = 2
    elif front_match == 5 and back_match == 0:
        prize = 3
    elif front_match == 4 and back_match == 2:
        prize = 4
    elif front_match == 4 and back_match == 1:
        prize = 5
    elif front_match == 3 and back_match == 2:
        prize = 6
    elif (front_match == 4 and back_match == 0) or (front_match == 3 and back_match == 1) or (front_match == 2 and back_match == 2):
        prize = 7
    elif (front_match == 3 and back_match == 0) or (front_match == 2 and back_match == 1) or (front_match == 1 and back_match == 2) or (front_match == 0 and back_match == 2):
        prize = 8
    elif (front_match == 2 and back_match == 0) or (front_match == 1 and back_match == 1) or (front_match == 0 and back_match == 1):
        prize = 9
    else:
        return None

    amount = _get_dlt_prize_amount(prize)
    return {"prize": prize, "name": DLT_PRIZE_NAMES[prize], "amount": amount}


def _get_dlt_prize_amount(prize: int) -> str:
    fixed = {8: "200", 9: "5"}
    if prize in fixed:
        return fixed[prize]
    return "浮动"


def check_ticket(lottery: str, ticket_nums: dict, draw: dict) -> Optional[dict]:
    if lottery == "ssq":
        ticket_red = set(ticket_nums.get("red", "").split(","))
        ticket_blue = ticket_nums.get("blue", "").strip()
        draw_red = set(draw.get("red", "").split(","))
        draw_blue = draw.get("blue", "").strip()
        return _match_ssq(ticket_red, ticket_blue, draw_red, draw_blue)
    else:
        ticket_front = set(ticket_nums.get("front", "").split(","))
        ticket_back = set(ticket_nums.get("back", "").split(","))
        draw_front = set(draw.get("front", "").split(","))
        draw_back = set(draw.get("back", "").split(","))
        return _match_dlt(ticket_front, ticket_back, draw_front, draw_back)


def get_prize_detail(lottery: str, prize_result: dict, draw: dict) -> dict:
    if not prize_result or lottery != "ssq":
        return prize_result

    grades_str = draw.get("prize_grades", "")
    if not grades_str:
        return prize_result

    try:
        grades = json.loads(grades_str)
    except (json.JSONDecodeError, TypeError):
        return prize_result

    for g in grades:
        if g.get("type") == prize_result["prize"]:
            prize_result["amount"] = g.get("typemoney", "浮动")
            break

    return prize_result


def parse_ticket_line(line: str, lottery: str) -> Optional[dict]:
    line = line.strip()
    if not line:
        return None

    # Normalize separators: space/comma/+ -> comma for main split
    # Support formats: "01 04 19 22 24 25 + 15", "01,04,19,22,24,25|15", "01 04 19 22 24 25 15"
    line = line.replace(" + ", "|").replace("+", "|")
    parts = line.split("|")

    if lottery == "ssq":
        if len(parts) == 2:
            red_part = parts[0].replace(" ", ",").replace(",,", ",")
            blue_part = parts[1].strip().replace(" ", "")
        else:
            nums = line.replace(" ", ",").replace(",,", ",").split(",")
            nums = [n for n in nums if n]
            if len(nums) != 7:
                return None
            red_part = ",".join(nums[:6])
            blue_part = nums[6]

        red_nums = [n.strip() for n in red_part.split(",") if n.strip()]
        if len(red_nums) != 6 or not blue_part:
            return None
        return {"red": ",".join(n.zfill(2) for n in red_nums), "blue": blue_part.zfill(2)}

    else:  # dlt
        if len(parts) == 2:
            front_part = parts[0].replace(" ", ",").replace(",,", ",")
            back_part = parts[1].strip().replace(" ", ",")
        else:
            nums = line.replace(" ", ",").replace(",,", ",").split(",")
            nums = [n for n in nums if n]
            if len(nums) != 7:
                return None
            front_part = ",".join(nums[:5])
            back_part = nums[5] + "," + nums[6] if len(nums) >= 7 else ""

        front_nums = [n.strip() for n in front_part.split(",") if n.strip()]
        back_nums = [n.strip() for n in back_part.split(",") if n.strip()]
        if len(front_nums) != 5 or len(back_nums) != 2:
            return None
        return {"front": ",".join(n.zfill(2) for n in front_nums), "back": ",".join(n.zfill(2) for n in back_nums)}
