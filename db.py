from __future__ import annotations

import sqlite3
from typing import Any


def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS articles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            article_url TEXT NOT NULL UNIQUE,
            search_url TEXT,
            title TEXT,
            journal TEXT,
            published_date TEXT,
            abstract_en TEXT,
            abstract_zh TEXT,
            abstract_en_hash TEXT,
            crawled_at TEXT,
            translated_at TEXT
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_hash ON articles(abstract_en_hash)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_articles_zh ON articles(abstract_zh)")
    conn.commit()


def has_abstract_en(conn: sqlite3.Connection, article_url: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM articles WHERE article_url=? AND abstract_en IS NOT NULL AND abstract_en!='' LIMIT 1",
        (article_url,),
    ).fetchone()
    return row is not None


def upsert_article(conn: sqlite3.Connection, item: dict[str, Any]) -> None:
    cols = [
        "article_url",
        "search_url",
        "title",
        "journal",
        "published_date",
        "abstract_en",
        "abstract_zh",
        "abstract_en_hash",
        "crawled_at",
        "translated_at",
    ]
    data = {c: item.get(c) for c in cols}
    conn.execute(
        """
        INSERT INTO articles (
            article_url, search_url, title, journal, published_date,
            abstract_en, abstract_zh, abstract_en_hash, crawled_at, translated_at
        ) VALUES (
            :article_url, :search_url, :title, :journal, :published_date,
            :abstract_en, :abstract_zh, :abstract_en_hash, :crawled_at, :translated_at
        )
        ON CONFLICT(article_url) DO UPDATE SET
            search_url=COALESCE(excluded.search_url, articles.search_url),
            title=COALESCE(excluded.title, articles.title),
            journal=COALESCE(excluded.journal, articles.journal),
            published_date=COALESCE(excluded.published_date, articles.published_date),
            abstract_en=COALESCE(excluded.abstract_en, articles.abstract_en),
            abstract_zh=COALESCE(excluded.abstract_zh, articles.abstract_zh),
            abstract_en_hash=COALESCE(excluded.abstract_en_hash, articles.abstract_en_hash),
            crawled_at=COALESCE(excluded.crawled_at, articles.crawled_at),
            translated_at=COALESCE(excluded.translated_at, articles.translated_at)
        """,
        data,
    )
    conn.commit()


def get_pending_translations(conn: sqlite3.Connection, limit: int) -> list[sqlite3.Row]:
    q = (
        "SELECT * FROM articles WHERE abstract_en IS NOT NULL AND abstract_en!='' "
        "AND (abstract_zh IS NULL OR abstract_zh='') "
        "ORDER BY id ASC"
    )
    if limit > 0:
        q += " LIMIT ?"
        rows = conn.execute(q, (limit,)).fetchall()
    else:
        rows = conn.execute(q).fetchall()
    return rows


def get_cached_translation(conn: sqlite3.Connection, abstract_en_hash: str) -> str | None:
    row = conn.execute(
        "SELECT abstract_zh FROM articles WHERE abstract_en_hash=? AND abstract_zh IS NOT NULL AND abstract_zh!='' LIMIT 1",
        (abstract_en_hash,),
    ).fetchone()
    return None if row is None else row[0]


def update_translation(conn: sqlite3.Connection, article_url: str, abstract_zh: str, translated_at: str) -> None:
    conn.execute(
        "UPDATE articles SET abstract_zh=?, translated_at=? WHERE article_url=?",
        (abstract_zh, translated_at, article_url),
    )
    conn.commit()


def iter_articles_for_export(conn: sqlite3.Connection, search_url: str | None) -> list[sqlite3.Row]:
    if search_url:
        return conn.execute(
            "SELECT article_url, title, journal, published_date, abstract_en, abstract_zh FROM articles WHERE search_url=? ORDER BY id ASC",
            (search_url,),
        ).fetchall()
    return conn.execute(
        "SELECT article_url, title, journal, published_date, abstract_en, abstract_zh FROM articles ORDER BY id ASC"
    ).fetchall()
