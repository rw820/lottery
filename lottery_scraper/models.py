from dataclasses import dataclass
from typing import List


@dataclass
class SSQDraw:
    code: str
    draw_date: str
    week_day: str
    red: str
    blue: str
    sales: int
    pool_money: int
    content: str
    prize_grades: str


@dataclass
class DLTDraw:
    code: str
    draw_date: str
    front: str
    back: str
    sales: int
    pool_money: int
    prize_grades: str
