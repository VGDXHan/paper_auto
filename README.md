# paper_auto (Nature Abstract Crawler + DeepSeek Translator)

输入任意 Nature Search URL，自动遍历所有分页，抓取每篇文章的 Abstract（英文），写入 SQLite，并可调用 DeepSeek（OpenAI 兼容接口）翻译成中文（术语首次出现：英文（中文），后续只保留英文）。

## 安装

```bat
python -m pip install -r requirements.txt
```

## 抓取（写入 SQLite，可选导出）

```bat
python main.py crawl --search-url "<nature search url>" --db nature.sqlite --concurrency 3 --rate 1.5
```

抓取后导出 CSV（用户可直接看/Excel 打开）：

```bat
python main.py crawl --search-url "<nature search url>" --db nature.sqlite --export-format csv --export-path articles.csv
```

调试用（限制页数/文章数）：

```bat
python main.py crawl --search-url "<nature search url>" --db nature.sqlite --max-pages 2 --limit-articles 10
```

## 翻译（DeepSeek OpenAI 兼容）

先配置环境变量：

```bat
set DEEPSEEK_API_KEY=your_key
set DEEPSEEK_BASE_URL=https://api.deepseek.com
```

然后翻译（`--model` 运行时指定，例如 `deepseek-chat`）：

```bat
python main.py translate --db nature.sqlite --model deepseek-chat
```

并行翻译（通过 `--concurrency` 控制并发，请求过快可降低 `--rate` 避免 429）：

```bat
python main.py translate --db nature.sqlite --model deepseek-chat --concurrency 20 --rate 2
```

只翻译少量用于验证：

```bat
python main.py translate --db nature.sqlite --model deepseek-chat --max-items 5
```

## 从 SQLite 导出（随时可重新导出）

```bat
python main.py export --db nature.sqlite --format csv --out articles.csv
```

导出 JSONL：

```bat
python main.py export --db nature.sqlite --format jsonl --out articles.jsonl
```

## 输出字段

- `article_url`
- `title`
- `journal`
- `published_date`
- `abstract_en`
- `abstract_zh`
