from __future__ import annotations

import csv
import json
from pathlib import Path


def export_rows(rows, out_path: str, fmt: str) -> None:
    p = Path(out_path)
    p.parent.mkdir(parents=True, exist_ok=True)

    if fmt == "csv":
        with p.open("w", encoding="utf-8-sig", newline="") as f:
            w = csv.DictWriter(
                f,
                fieldnames=[
                    "article_url",
                    "title",
                    "journal",
                    "published_date",
                    "abstract_en",
                    "abstract_zh",
                ],
            )
            w.writeheader()
            for r in rows:
                w.writerow(dict(r))
        return

    if fmt == "txt":
        with p.open("w", encoding="utf-8") as f:
            first = True
            idx = 0
            for r in rows:
                if not first:
                    f.write("----\n\n")
                first = False
                idx += 1

                d = dict(r)
                article_url = d.get("article_url")
                title = d.get("title")
                abstract_en = d.get("abstract_en")
                abstract_zh = d.get("abstract_zh")

                article_url = article_url if article_url else "空字段"
                title = title if title else "空字段"
                abstract_en = abstract_en if abstract_en else "空字段"
                abstract_zh = abstract_zh if abstract_zh else "空字段"

                f.write(f"{idx}. {title}\n\n")
                f.write("Abstract (EN):\n")
                f.write(f"{abstract_en}\n")
                f.write("Abstract (ZH):\n")
                f.write(f"{abstract_zh}\n\n")
                f.write("URL:\n")
                f.write(f"{article_url}\n")
            if not first:
                f.write("----\n\n")
        return

    if fmt == "jsonl":
        with p.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(dict(r), ensure_ascii=False) + "\n")
        return

    raise ValueError(f"unsupported export format: {fmt}")
