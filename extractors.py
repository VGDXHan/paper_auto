from __future__ import annotations

import json
from typing import Any

from bs4 import BeautifulSoup

from utils import clean_text


def _iter_jsonld_objects(soup: BeautifulSoup) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for s in soup.find_all("script", attrs={"type": "application/ld+json"}):
        raw = s.get_text(strip=True)
        if not raw:
            continue
        try:
            obj = json.loads(raw)
        except Exception:
            continue
        _collect_jsonld(out, obj)
    return out


def _collect_jsonld(out: list[dict[str, Any]], obj: Any) -> None:
    if isinstance(obj, dict):
        if "@graph" in obj:
            _collect_jsonld(out, obj.get("@graph"))
            return
        out.append(obj)
        for k in ("mainEntity", "mainEntityOfPage"):
            if k in obj:
                _collect_jsonld(out, obj.get(k))
    elif isinstance(obj, list):
        for it in obj:
            _collect_jsonld(out, it)


def _pick_article_jsonld(objs: list[dict[str, Any]]) -> dict[str, Any] | None:
    for o in objs:
        t = o.get("@type")
        if isinstance(t, list):
            if any("Article" in str(x) for x in t):
                return o
        if isinstance(t, str) and "Article" in t:
            return o
    return objs[0] if objs else None


def extract_fields(html: str) -> dict[str, str | None]:
    soup = BeautifulSoup(html, "lxml")
    objs = _iter_jsonld_objects(soup)
    o = _pick_article_jsonld(objs) or {}

    title = clean_text(
        o.get("headline")
        or o.get("name")
        or (soup.title.get_text(strip=True) if soup.title else None)
    )

    journal = None
    is_part_of = o.get("isPartOf")
    if isinstance(is_part_of, dict):
        journal = clean_text(is_part_of.get("name"))
    if not journal:
        m = soup.find("meta", attrs={"name": "citation_journal_title"})
        journal = clean_text(m.get("content") if m else None)

    published_date = clean_text(o.get("datePublished") or o.get("dateCreated"))
    if not published_date:
        m = soup.find("meta", attrs={"property": "article:published_time"})
        published_date = clean_text(m.get("content") if m else None)

    abstract = clean_text(o.get("abstract") or o.get("description"))
    if not abstract:
        abstract = _extract_meta_abstract(soup)
    if not abstract:
        abstract = _extract_dom_abstract(soup)

    return {
        "title": title,
        "journal": journal,
        "published_date": published_date,
        "abstract_en": abstract,
    }


def _extract_meta_abstract(soup: BeautifulSoup) -> str | None:
    for attrs in (
        {"name": "dc.description"},
        {"property": "og:description"},
        {"name": "description"},
    ):
        m = soup.find("meta", attrs=attrs)
        if m and m.get("content"):
            v = clean_text(m.get("content"))
            if v:
                return v
    return None


def _extract_dom_abstract(soup: BeautifulSoup) -> str | None:
    header = soup.find(
        lambda t: t.name in {"h1", "h2", "h3", "h4"}
        and "abstract" in t.get_text(" ", strip=True).lower()
    )
    if not header:
        return None

    parts: list[str] = []
    parent = header.parent
    for sib in parent.find_all(recursive=False):
        if sib == header:
            continue
    for el in header.next_elements:
        if getattr(el, "name", None) in {"h1", "h2", "h3", "h4"} and el is not header:
            break
        if getattr(el, "name", None) == "p":
            txt = clean_text(el.get_text(" ", strip=True))
            if txt:
                parts.append(txt)
    return clean_text(" ".join(parts))
