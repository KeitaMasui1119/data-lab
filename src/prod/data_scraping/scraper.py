import re
import time
from pathlib import Path
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

# ==== 設定ここから ====
BASE_PAGE = (
    "https://www.tepco.co.jp/forecast/html/download-j.html"  # 年リンクが並ぶページ
)
BASE_HOST = "https://www.tepco.co.jp"
OUT_DIR = "/workspace/src/data_engineering/data_lake"

# ==== リクエスト設定 ====
sess = requests.Session()
sess.headers.update({"User-Agent": "denki-pipeline/1.0 (+you@example.com)"})

# ==== 1. 年ごとのCSVリンクを収集 ====
r = sess.get(BASE_PAGE, timeout=30)
r.raise_for_status()
soup = BeautifulSoup(r.text)

# /forecast/html/images/juyo-YYYY.csv の形式を探す
pat = re.compile(r"^/forecast/html/images/juyo-(\d{4})\.csv$", re.IGNORECASE)
links = []
for a in soup.select("a[href]"):
    # hrefを安全に文字列化
    href = a.get("href")
    if not isinstance(href, str):
        continue  # hrefがNoneやリストの場合はスキップ

    m = pat.match(href)
    if m:
        year = m.group(1)
        abs_url = urljoin(BASE_HOST, href)
        links.append((year, abs_url))

# ==== 2. ダウンロードして保存 ====
for year, url in sorted(links):
    csv_path = Path(f"{OUT_DIR}\tepco_juyo_{year}.csv")
    if Path.exists(csv_path):
        continue

    time.sleep(1.2)  # 丁寧に1秒以上間隔を空ける
    resp = sess.get(url, timeout=30)
    resp.raise_for_status()
    with Path.open(csv_path, "wb") as f:
        f.write(resp.content)
