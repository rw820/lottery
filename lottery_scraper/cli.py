import argparse
import json
import logging
import sys

from tabulate import tabulate

from .db import get_draw_count, init_db, query_draws
from .export import export_to_csv
from .ssq_scraper import SSQScraper
from .ssq_500_scraper import crawl_500com_history
from .dlt_scraper import DLTScraper

logger = logging.getLogger(__name__)


def cmd_init(args):
    init_db()
    print("Database initialized.")


def cmd_crawl(args):
    lotteries = ["ssq", "dlt"] if args.lottery == "all" else [args.lottery]
    mode = "full" if args.full else "incremental"

    for lt in lotteries:
        scraper = SSQScraper(delay=args.delay) if lt == "ssq" else DLTScraper(delay=args.delay)
        if mode == "full":
            print(f"Crawling {lt.upper()} full history...")
            draws = scraper.crawl_full()
        else:
            print(f"Crawling {lt.upper()} incremental...")
            draws = scraper.crawl_incremental()

        count = get_draw_count(lt)
        print(f"{lt.upper()}: {len(draws)} fetched, {count} total in DB")


def cmd_backfill(args):
    print("Backfilling SSQ 2003-2012 from 500.com...")
    total = crawl_500com_history()
    count = get_draw_count("ssq")
    print(f"SSQ: {total} new draws inserted, {count} total in DB")


def cmd_query(args):
    rows = query_draws(
        args.lottery,
        code=args.code,
        date_start=args.date_start,
        date_end=args.date_end,
        limit=args.latest,
        order="DESC" if not args.sort or args.sort.upper() == "DESC" else "ASC",
    )

    if not rows:
        print("No results found.")
        return

    if args.format == "json":
        print(json.dumps(rows, ensure_ascii=False, indent=2))
    else:
        if args.lottery == "ssq":
            headers = ["code", "draw_date", "red", "blue", "sales", "pool_money"]
        else:
            headers = ["code", "draw_date", "front", "back", "sales", "pool_money"]
        table = [[r.get(h, "") for h in headers] for r in rows]
        print(tabulate(table, headers=headers, tablefmt="github"))

        if args.prizes and len(rows) == 1:
            _print_prizes(args.lottery, rows[0])


def _print_prizes(lottery: str, row: dict):
    grades = row.get("prize_grades", "")
    if not grades:
        return
    try:
        prizes = json.loads(grades)
    except json.JSONDecodeError:
        return
    print(f"\n--- Prize Details ({row['code']}) ---")
    for p in prizes:
        if lottery == "ssq":
            names = {1: "一等奖", 2: "二等奖", 3: "三等奖", 4: "四等奖", 5: "五等奖", 6: "六等奖"}
            name = names.get(p.get("type"), f"奖{p.get('type')}")
            print(f"  {name}: {p.get('typenum', '?')}注 x {p.get('typemoney', '?')}元")
        else:
            print(f"  {p.get('level', '?')}: {p.get('count', '?')}注 x {p.get('amount', '?')}元 (总{p.get('total', '?')})")


def cmd_export(args):
    lotteries = ["ssq", "dlt"] if args.lottery == "all" else [args.lottery]
    for lt in lotteries:
        path = export_to_csv(
            lt,
            output_path=args.output,
            code=args.code,
            date_start=args.date_start,
            date_end=args.date_end,
            latest=args.latest,
        )
        if path:
            print(f"Exported {lt.upper()} to {path}")
        else:
            print(f"No data to export for {lt.upper()}")


def build_parser():
    parser = argparse.ArgumentParser(
        prog="lottery_scraper",
        description="China lottery (SSQ + DLT) draw results scraper",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    sub = parser.add_subparsers(dest="command", help="Available commands")

    # init
    p_init = sub.add_parser("init", help="Initialize the database")
    p_init.set_defaults(func=cmd_init)

    # crawl
    p_crawl = sub.add_parser("crawl", help="Fetch draw results from API")
    p_crawl.add_argument("lottery", choices=["ssq", "dlt", "all"], help="Lottery type")
    p_crawl.add_argument("--full", action="store_true", help="Fetch entire history")
    p_crawl.add_argument("--incremental", action="store_true", help="Fetch only new draws (default)")
    p_crawl.add_argument("--delay", type=float, default=2.5, help="Delay between requests (seconds)")
    p_crawl.set_defaults(func=cmd_crawl)

    # backfill
    p_back = sub.add_parser("backfill", help="Backfill SSQ 2003-2012 from 500.com")
    p_back.set_defaults(func=cmd_backfill)

    # query
    p_query = sub.add_parser("query", help="Query stored results")
    p_query.add_argument("lottery", choices=["ssq", "dlt"], help="Lottery type")
    p_query.add_argument("--code", help="Specific issue number")
    p_query.add_argument("--date-start", help="Start date (YYYY-MM-DD)")
    p_query.add_argument("--date-end", help="End date (YYYY-MM-DD)")
    p_query.add_argument("--latest", type=int, help="Show latest N draws")
    p_query.add_argument("--sort", choices=["ASC", "DESC"], default="DESC", help="Sort order")
    p_query.add_argument("--format", choices=["table", "json"], default="table", help="Output format")
    p_query.add_argument("--prizes", action="store_true", help="Show prize details (single draw)")
    p_query.set_defaults(func=cmd_query)

    # export
    p_export = sub.add_parser("export", help="Export results to CSV")
    p_export.add_argument("lottery", choices=["ssq", "dlt", "all"], help="Lottery type")
    p_export.add_argument("--code", help="Filter by issue number")
    p_export.add_argument("--date-start", help="Filter start date")
    p_export.add_argument("--date-end", help="Filter end date")
    p_export.add_argument("--output", "-o", help="Output file path")
    p_export.add_argument("--latest", type=int, help="Export latest N draws")
    p_export.set_defaults(func=cmd_export)

    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    init_db()

    if not args.command:
        parser.print_help()
        return

    args.func(args)
