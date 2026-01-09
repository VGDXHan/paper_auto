from __future__ import annotations

import os

from openai import OpenAI

from utils import now_iso


def build_client(base_url: str | None, api_key: str | None) -> OpenAI:
    return OpenAI(base_url=base_url or os.getenv("DEEPSEEK_BASE_URL"), api_key=api_key or os.getenv("DEEPSEEK_API_KEY"))


def translate_abstract(client: OpenAI, model: str, abstract_en: str) -> str:
    system = "你是学术翻译助手，输出简体中文，忠实准确，风格正式。"
    user = (
        "请将下面英文摘要翻译为简体中文。\n"
        "规则：\n"
        "1) 关键术语首次出现采用：英文术语（中文翻译）；后续只保留英文术语，不再重复括号中文。\n"
        "2) 模型名/方法名/数据集名/缩写：保留英文；必要时首次出现给出中文解释。\n"
        "3) 不要添加原文没有的信息，不要扩写。\n\n"
        f"英文摘要：\n{abstract_en}"
    )

    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        temperature=0.2,
    )
    return (resp.choices[0].message.content or "").strip()


def translated_at() -> str:
    return now_iso()
