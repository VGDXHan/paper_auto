from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from typing import Iterable
from urllib.parse import parse_qs, urlparse

import httpx
from bs4 import BeautifulSoup
from tqdm import tqdm

import db
from extractors import extract_fields, extract_fields_cvf
from utils import RateLimiter, clean_text, normalize_url, now_iso, sha256_text


NATURE_BASE = "https://www.nature.com"
CVF_BASE = "https://openaccess.thecvf.com"


def _is_cvf_openaccess(url: str) -> bool:
    try:
        return urlparse(url).netloc.lower() == "openaccess.thecvf.com"
    except Exception:
        return False


def _cvf_defaults_from_search_url(search_url: str) -> tuple[str | None, str | None]:
    p = urlparse(search_url)
    parts = [x for x in p.path.split("/") if x]
    conf = parts[0] if parts else None
    journal = conf.replace("_", " ") if conf else None
    q = parse_qs(p.query or "")
    day = (q.get("day") or [None])[0]
    return journal, day


@dataclass
class CrawlConfig:
    search_url: str
    db_path: str
    max_pages: int
    limit_articles: int
    concurrency: int
    rate: float
    resume: bool


def _default_headers() -> dict[str, str]:
    return {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
    }


async def _fetch(client: httpx.AsyncClient, url: str, limiter: RateLimiter) -> str:
    await limiter.wait()
    r = await client.get(url, timeout=30)
    r.raise_for_status()
    return r.text


def _parse_article_links(html: str, *, search_url: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []

    if _is_cvf_openaccess(search_url):
        for a in soup.find_all("a", href=True):
            href = a.get("href")
            if not href:
                continue
            u = normalize_url(href, CVF_BASE)
            if "/content/" in u and u.endswith("_paper.html"):
                urls.append(u)
        return sorted(set(urls))

    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue
        u = normalize_url(href, NATURE_BASE)
        if "/articles/" in u:
            urls.append(u)
    return sorted(set(urls))


def _find_next_page_url(html: str, current_url: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    base = CVF_BASE if _is_cvf_openaccess(current_url) else NATURE_BASE
    link = soup.find("link", attrs={"rel": "next"})
    if link and link.get("href"):
        return normalize_url(link.get("href"), base)
    a = soup.find("a", attrs={"rel": "next"}, href=True)
    if a and a.get("href"):
        return normalize_url(a.get("href"), base)
    a2 = soup.find(lambda t: t.name == "a" and t.get_text(" ", strip=True).lower() in {"next", "next page"} and t.get("href"))
    if a2 and a2.get("href"):
        return normalize_url(a2.get("href"), base)
    return None


async def crawl(cfg: CrawlConfig) -> None:
    conn = db.connect(cfg.db_path)
    db.init_db(conn)

    limiter = RateLimiter(cfg.rate)
    sem = asyncio.Semaphore(max(1, cfg.concurrency))

    site = "CVF OpenAccess" if _is_cvf_openaccess(cfg.search_url) else "Nature"
    print(f"开始抓取：{site} | url={cfg.search_url}", flush=True)
    print(
        f"配置：db={cfg.db_path} concurrency={cfg.concurrency} rate={cfg.rate} resume={cfg.resume} max_pages={cfg.max_pages} limit_articles={cfg.limit_articles}",
        flush=True,
    )
    t0 = time.monotonic()

    async with httpx.AsyncClient(headers=_default_headers(), follow_redirects=True) as client:
        seen_pages: set[str] = set()
        page_url: str | None = cfg.search_url
        page_count = 0
        total_articles = 0

        cvf_default_journal, cvf_default_published_date = _cvf_defaults_from_search_url(cfg.search_url)

        async def handle_article(url: str, *, pbar: tqdm, pbar_lock: asyncio.Lock) -> None:
            nonlocal total_articles
            if cfg.resume and db.has_abstract_en(conn, url):
                async with pbar_lock:
                    pbar.update(1)
                return
            async with sem:
                html = await _fetch(client, url, limiter)

            if _is_cvf_openaccess(url):
                fields = extract_fields_cvf(
                    html,
                    default_journal=cvf_default_journal,
                    default_published_date=cvf_default_published_date,
                )
            else:
                fields = extract_fields(html)
            abstract_en = clean_text(fields.get("abstract_en"))
            item = {
                "article_url": url,
                "search_url": cfg.search_url,
                "title": clean_text(fields.get("title")),
                "journal": clean_text(fields.get("journal")),
                "published_date": clean_text(fields.get("published_date")),
                "abstract_en": abstract_en,
                "abstract_en_hash": sha256_text(abstract_en) if abstract_en else None,
                "crawled_at": now_iso(),
            }
            db.upsert_article(conn, item)
            total_articles += 1
            async with pbar_lock:
                pbar.update(1)

        while page_url:
            if page_url in seen_pages:
                print(f"检测到重复分页链接，停止：{page_url}", flush=True)
                break
            seen_pages.add(page_url)

            page_count += 1
            if cfg.max_pages > 0 and page_count > cfg.max_pages:
                break

            html = await _fetch(client, page_url, limiter)
            article_urls = _parse_article_links(html, search_url=cfg.search_url)

            if cfg.limit_articles > 0:
                remain = cfg.limit_articles - total_articles
                if remain <= 0:
                    break
                article_urls = article_urls[:remain]

            print(f"第{page_count}页：候选文章 {len(article_urls)}", flush=True)
            before_total = total_articles

            pbar_lock = asyncio.Lock()
            pbar = tqdm(
                total=len(article_urls),
                desc=f"page {page_count}",
                leave=False,
                unit="article",
            )

            try:
                tasks = [asyncio.create_task(handle_article(u, pbar=pbar, pbar_lock=pbar_lock)) for u in article_urls]
                if tasks:
                    await asyncio.gather(*tasks)
            finally:
                pbar.close()

            added = total_articles - before_total
            print(f"第{page_count}页完成：新增 {added}，累计新增 {total_articles}", flush=True)

            next_url = _find_next_page_url(html, page_url)
            page_url = next_url

    elapsed = time.monotonic() - t0
    print(f"抓取结束：页数 {page_count}，累计新增 {total_articles}，耗时 {elapsed:.1f}s", flush=True)
    conn.close()
