from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Iterable

import httpx
from bs4 import BeautifulSoup

import db
from extractors import extract_fields
from utils import RateLimiter, clean_text, normalize_url, now_iso, sha256_text


BASE = "https://www.nature.com"


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


def _parse_article_links(html: str) -> list[str]:
    soup = BeautifulSoup(html, "lxml")
    urls: list[str] = []
    for a in soup.find_all("a", href=True):
        href = a.get("href")
        if not href:
            continue
        u = normalize_url(href, BASE)
        if "/articles/" in u:
            urls.append(u)
    return sorted(set(urls))


def _find_next_page_url(html: str, current_url: str) -> str | None:
    soup = BeautifulSoup(html, "lxml")
    link = soup.find("link", attrs={"rel": "next"})
    if link and link.get("href"):
        return normalize_url(link.get("href"), BASE)
    a = soup.find("a", attrs={"rel": "next"}, href=True)
    if a and a.get("href"):
        return normalize_url(a.get("href"), BASE)
    a2 = soup.find(lambda t: t.name == "a" and t.get_text(" ", strip=True).lower() in {"next", "next page"} and t.get("href"))
    if a2 and a2.get("href"):
        return normalize_url(a2.get("href"), BASE)
    return None


async def crawl(cfg: CrawlConfig) -> None:
    conn = db.connect(cfg.db_path)
    db.init_db(conn)

    limiter = RateLimiter(cfg.rate)
    sem = asyncio.Semaphore(max(1, cfg.concurrency))

    async with httpx.AsyncClient(headers=_default_headers(), follow_redirects=True) as client:
        seen_pages: set[str] = set()
        page_url: str | None = cfg.search_url
        page_count = 0
        total_articles = 0

        async def handle_article(url: str) -> None:
            nonlocal total_articles
            if cfg.resume and db.has_abstract_en(conn, url):
                return
            async with sem:
                html = await _fetch(client, url, limiter)
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

        while page_url:
            if page_url in seen_pages:
                break
            seen_pages.add(page_url)

            page_count += 1
            if cfg.max_pages > 0 and page_count > cfg.max_pages:
                break

            html = await _fetch(client, page_url, limiter)
            article_urls = _parse_article_links(html)

            if cfg.limit_articles > 0:
                remain = cfg.limit_articles - total_articles
                if remain <= 0:
                    break
                article_urls = article_urls[:remain]

            tasks = [asyncio.create_task(handle_article(u)) for u in article_urls]
            if tasks:
                await asyncio.gather(*tasks)

            next_url = _find_next_page_url(html, page_url)
            page_url = next_url

    conn.close()
