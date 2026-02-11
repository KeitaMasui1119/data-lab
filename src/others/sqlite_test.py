import os
import sqlite3
from datetime import datetime

# 1. 保存先ディレクトリの作成
db_dir = "/workspace/src/stg/pydev"
os.makedirs(db_dir, exist_ok=True)
db_path = os.path.join(db_dir, "jobs.db")


def test_sqlite():
    # 2. データベース接続（なければ作成される）
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 3. テーブル作成（ジョブログ用）
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS job_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            scraper_name TEXT NOT NULL,
            target_date TEXT,
            status TEXT,
            rows_count INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # 4. テストデータの挿入（JEPXのスクレイピングが成功した想定）
    now_date = datetime.now().strftime("%Y-%m-%d")
    cursor.execute(
        """
        INSERT INTO job_history (scraper_name, target_date, status, rows_count)
        VALUES (?, ?, ?, ?)
    """,
        ("jepx_spot", now_date, "SUCCESS", 48),
    )  # 1日48コマ想定

    conn.commit()

    # 5. データの読み出し確認
    print(f"--- SQLite Test Output ({db_path}) ---")
    cursor.execute("SELECT * FROM job_history ORDER BY created_at DESC LIMIT 1")
    row = cursor.fetchone()
    if row:
        print(row)
    conn.close()


if __name__ == "__main__":
    test_sqlite()
