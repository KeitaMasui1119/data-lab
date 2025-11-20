import datetime
import time

import functions_framework
import requests
from flask import jsonify
from google.cloud import storage
from google.cloud.exceptions import NotFound

BUCKET_NAME = "nf-nwa9-igvwik-l9y7"


@functions_framework.http
def main(request):
    try:
        # Cloud Storageのクライアントを作成する
        cs_client = storage.Client()
    except Exception as e:
        error_message = f"Clientの初期化中にエラーが発生しました: {e}"
        print(error_message)
        return {"error": error_message}, 500

    try:
        current_timestamp = get_timestamp()
        response = scrape_jepx(current_timestamp)
        file_name = f"spot_summary_{get_fiscal_year(current_timestamp)}.csv"
        bucket = cs_client.get_bucket(BUCKET_NAME)
        blob = bucket.blob(f"datalake/jepx_spot_summary/data/{file_name}")
        blob.upload_from_string(response.content, "text/csv")
        print("処理が完了しました")
        output = {
            "bucket_name": BUCKET_NAME,
            "file_name": f"datalake/jepx_spot_summary/data/{file_name}",
            "content_type": "text/csv",
        }
        return jsonify(output)

    except NotFound as e:
        error_message = f"指定されたバケットまたはファイルが見つかりませんでした: {e}"
        print(error_message)
        return {"error": error_message}, 404


# スクレイピングする関数
def scrape_jepx(time_stamp):
    url = f"https://www.jepx.jp/_download.php?timestamp={time_stamp!s}"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": "https://www.jepx.jp/electricpower/market-data/spot/",
    }

    year = get_fiscal_year(time_stamp)
    data = {"dir": "spot_summary", "file": f"spot_summary_{year}.csv"}

    response = requests.post(url, headers=headers, data=data)

    return response


# 実行時点でのタイムスタンプを取得する関数
def get_timestamp():
    ts = int(time.time() * 1000)
    return ts


# タイムスタンプ値から会計年度を算出する関数
def get_fiscal_year(timestamp_ms):
    # UNIXタイムスタンプ（ミリ秒）から datetime オブジェクトを作成
    dt = datetime.datetime.fromtimestamp(timestamp_ms / 1000)
    year = dt.year
    month = dt.month

    # 4月以前なら前年度を会計年度とする
    fiscal_year = year if month >= 3 else year - 1

    return fiscal_year
