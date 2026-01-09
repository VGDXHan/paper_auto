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

    if fmt == "jsonl":
        with p.open("w", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(dict(r), ensure_ascii=False) + "\n")
        return

    raise ValueError(f"unsupported export format: {fmt}")
