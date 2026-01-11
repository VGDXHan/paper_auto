from __future__ import annotations

import argparse
import asyncio
import os

import db
from exporter import export_rows
from crawler import CrawlConfig, crawl
from translator import build_async_client, translated_at, translate_abstract_async
from utils import RateLimiter, clean_text, sha256_text


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="paper_auto")
    sub = p.add_subparsers(dest="cmd", required=True)

    p_crawl = sub.add_parser("crawl")
    p_crawl.add_argument("--search-url", required=True)
    p_crawl.add_argument("--db", default="nature.sqlite")
    p_crawl.add_argument("--max-pages", type=int, default=0)
    p_crawl.add_argument("--limit-articles", type=int, default=0)
    p_crawl.add_argument("--concurrency", type=int, default=3)
    p_crawl.add_argument("--rate", type=float, default=1.5)
    p_crawl.add_argument(
        "--no-resume",
        action="store_true",
        help="disable resume behavior (re-fetch even if abstract_en exists)",
    )
    p_crawl.add_argument(
        "--export-format",
        choices=["csv", "jsonl", "txt"],
        default=None,
        help="export a user-visible file after crawl finishes",
    )
    p_crawl.add_argument(
        "--export-path",
        default=None,
        help="export file path (default: export.<fmt> in current folder)",
    )

    p_tr = sub.add_parser("translate")
    p_tr.add_argument("--db", default="nature.sqlite")
    p_tr.add_argument("--model", required=True)
    p_tr.add_argument("--base-url", default=None)
    p_tr.add_argument("--api-key", default=None)
    p_tr.add_argument("--batch-size", type=int, default=20)
    p_tr.add_argument("--max-items", type=int, default=0)
    p_tr.add_argument("--concurrency", type=int, default=3)
    p_tr.add_argument("--rate", type=float, default=1.5)

    p_ex = sub.add_parser("export")
    p_ex.add_argument("--db", default="nature.sqlite")
    p_ex.add_argument("--format", choices=["csv", "jsonl", "txt"], required=True)
    p_ex.add_argument("--out", required=True)
    p_ex.add_argument(
        "--search-url",
        default=None,
        help="optional: export only records from a specific search_url",
    )

    return p


async def run_crawl(args: argparse.Namespace) -> None:
    cfg = CrawlConfig(
        search_url=args.search_url,
        db_path=args.db,
        max_pages=args.max_pages,
        limit_articles=args.limit_articles,
        concurrency=args.concurrency,
        rate=args.rate,
        resume=not args.no_resume,
    )
    await crawl(cfg)

    if args.export_format:
        out_path = args.export_path or f"export.{args.export_format}"
        conn = db.connect(args.db)
        rows = db.iter_articles_for_export(conn, args.search_url)
        export_rows(rows, out_path, args.export_format)
        conn.close()


async def run_translate(args: argparse.Namespace) -> None:
    conn = db.connect(args.db)
    db.init_db(conn)

    client = build_async_client(args.base_url, args.api_key)
    limiter = RateLimiter(args.rate)
    sem = asyncio.Semaphore(max(1, args.concurrency))
    db_lock = asyncio.Lock()

    rows = db.get_pending_translations(conn, args.max_items if args.max_items > 0 else 0)
    total = len(rows)
    if args.max_items > 0:
        total = min(total, args.max_items)
    print(f"待翻译摘要数量：{total}", flush=True)

    progress = {"done": 0}

    async def process_row(r) -> None:
        abstract_en = clean_text(r["abstract_en"]) or ""
        if not abstract_en:
            return

        ident = clean_text(r["title"]) or r["article_url"]
        h = r["abstract_en_hash"] or sha256_text(abstract_en)

        async with db_lock:
            cached = db.get_cached_translation(conn, h)

        if cached:
            async with db_lock:
                idx = progress["done"] + 1
                print(f"[{idx}/{total}] 命中缓存：{ident}", flush=True)
                db.update_translation(conn, r["article_url"], cached, translated_at())
                progress["done"] += 1
            return

        async with sem:
            await limiter.wait()
            zh = await translate_abstract_async(client, args.model, abstract_en)
            zh = clean_text(zh) or zh

        async with db_lock:
            idx = progress["done"] + 1
            print(f"[{idx}/{total}] 已完成：{ident}", flush=True)
            db.update_translation(conn, r["article_url"], zh, translated_at())
            progress["done"] += 1

    tasks = [asyncio.create_task(process_row(r)) for r in rows[:total]]
    if tasks:
        await asyncio.gather(*tasks)

    print(f"翻译完成：{progress['done']}/{total}", flush=True)
    conn.close()


def run_export(args: argparse.Namespace) -> None:
    conn = db.connect(args.db)
    db.init_db(conn)
    rows = db.iter_articles_for_export(conn, args.search_url)
    export_rows(rows, args.out, args.format)
    conn.close()


def main() -> None:
    args = build_parser().parse_args()
    if args.cmd == "crawl":
        asyncio.run(run_crawl(args))
    elif args.cmd == "translate":
        asyncio.run(run_translate(args))
    elif args.cmd == "export":
        run_export(args)


if __name__ == "__main__":
    main()
