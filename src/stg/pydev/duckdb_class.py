"""DuckDBデータローダーモジュール."""

import io
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd


@dataclass
class LoaderConfig:
    """ローダー設定クラス."""

    db_path: Path = Path("data/database/jepx.db")
    read_only: bool = False
    auto_checkpoint: bool = True


class DuckDBLoader:
    """DuckDBにデータをロードするクラス."""

    def __init__(self, config: LoaderConfig | None = None) -> None:
        """初期化.

        Args:
            config: ローダー設定。Noneの場合はデフォルト設定を使用
        """
        self.config = config or LoaderConfig()
        self.conn: duckdb.DuckDBPyConnection | None = None
        self._connect()

    def _connect(self) -> None:
        """データベースに接続."""
        # ディレクトリを作成
        self.config.db_path.parent.mkdir(parents=True, exist_ok=True)

        # 接続
        self.conn = duckdb.connect(
            str(self.config.db_path),
            read_only=self.config.read_only,
        )

        if self.config.auto_checkpoint:
            self.conn.execute("PRAGMA enable_checkpoint_on_shutdown=true")

    def execute(self, query: str, params: dict[str, Any] | None = None) -> duckdb.DuckDBPyConnection:
        """SQLを実行.

        Args:
            query: 実行するSQL
            params: パラメータ

        Returns:
            実行結果

        Raises:
            RuntimeError: 接続が確立されていない場合
        """
        if self.conn is None:
            msg = "Database connection is not established"
            raise RuntimeError(msg)

        if params:
            return self.conn.execute(query, params)
        return self.conn.execute(query)

    def load_csv_from_bytes(
        self,
        csv_bytes: bytes,
        table_name: str,
        *,
        if_exists: str = "append",
        encoding: str = "utf-8",
    ) -> int:
        """バイトデータからCSVを読み込んでテーブルに格納.

        Args:
            csv_bytes: CSVファイルのバイトデータ
            table_name: 格納先テーブル名
            if_exists: 既存テーブルの処理方法 ("append", "replace", "fail")
            encoding: CSVのエンコーディング

        Returns:
            挿入した行数

        Raises:
            ValueError: if_existsが不正な値の場合
        """
        if if_exists not in {"append", "replace", "fail"}:
            msg = f"Invalid if_exists value: {if_exists}"
            raise ValueError(msg)

        # バイトデータをDataFrameに変換
        csv_string = csv_bytes.decode(encoding)
        df = pd.read_csv(io.StringIO(csv_string))

        return self.load_dataframe(df, table_name, if_exists=if_exists)

    def load_csv_from_file(
        self,
        filepath: Path,
        table_name: str,
        *,
        if_exists: str = "append",
        encoding: str = "utf-8",
    ) -> int:
        """ファイルからCSVを読み込んでテーブルに格納.

        Args:
            filepath: CSVファイルのパス
            table_name: 格納先テーブル名
            if_exists: 既存テーブルの処理方法 ("append", "replace", "fail")
            encoding: CSVのエンコーディング

        Returns:
            挿入した行数
        """
        csv_bytes = filepath.read_bytes()
        return self.load_csv_from_bytes(
            csv_bytes,
            table_name,
            if_exists=if_exists,
            encoding=encoding,
        )

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        *,
        if_exists: str = "append",
    ) -> int:
        """DataFrameをテーブルに格納.

        Args:
            df: 格納するDataFrame
            table_name: 格納先テーブル名
            if_exists: 既存テーブルの処理方法 ("append", "replace", "fail")

        Returns:
            挿入した行数

        Raises:
            ValueError: if_existsが不正な値の場合
            RuntimeError: テーブルが既に存在する場合（if_exists="fail"の時）
        """
        if if_exists not in {"append", "replace", "fail"}:
            msg = f"Invalid if_exists value: {if_exists}"
            raise ValueError(msg)

        if self.conn is None:
            msg = "Database connection is not established"
            raise RuntimeError(msg)

        # テーブルの存在確認
        table_exists = self._table_exists(table_name)

        if table_exists and if_exists == "fail":
            msg = f"Table '{table_name}' already exists"
            raise RuntimeError(msg)

        if if_exists == "replace" and table_exists:
            self.conn.execute(f"DROP TABLE IF EXISTS {table_name}")

        # DataFrameを登録して挿入
        self.conn.register("temp_df", df)

        if not table_exists or if_exists == "replace":
            # テーブルを作成
            self.conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM temp_df")
        else:
            # 既存テーブルに追加
            self.conn.execute(f"INSERT INTO {table_name} SELECT * FROM temp_df")

        self.conn.unregister("temp_df")

        return len(df)

    def _table_exists(self, table_name: str) -> bool:
        """テーブルの存在確認.

        Args:
            table_name: 確認するテーブル名

        Returns:
            存在する場合True
        """
        if self.conn is None:
            return False

        result = self.conn.execute(
            """
            SELECT COUNT(*) as cnt
            FROM information_schema.tables
            WHERE table_name = ?
            """,
            [table_name],
        ).fetchone()

        return result[0] > 0 if result else False

    def query_to_df(self, query: str) -> pd.DataFrame:
        """SQLクエリを実行してDataFrameを取得.

        Args:
            query: 実行するSQL

        Returns:
            クエリ結果のDataFrame
        """
        if self.conn is None:
            msg = "Database connection is not established"
            raise RuntimeError(msg)

        return self.conn.execute(query).df()

    def get_table_info(self, table_name: str) -> pd.DataFrame:
        """テーブルの情報を取得.

        Args:
            table_name: テーブル名

        Returns:
            テーブル情報のDataFrame
        """
        query = f"DESCRIBE {table_name}"
        return self.query_to_df(query)

    def get_row_count(self, table_name: str) -> int:
        """テーブルの行数を取得.

        Args:
            table_name: テーブル名

        Returns:
            行数
        """
        if self.conn is None:
            msg = "Database connection is not established"
            raise RuntimeError(msg)

        result = self.conn.execute(
            f"SELECT COUNT(*) as cnt FROM {table_name}"
        ).fetchone()

        return result[0] if result else 0

    def close(self) -> None:
        """データベース接続をクローズ."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __enter__(self) -> "DuckDBLoader":
        """コンテキストマネージャー: enter."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """コンテキストマネージャー: exit."""
        self.close()


# 使用例
if __name__ == "__main__":
    from datetime import datetime

    # JEPXScraperと組み合わせて使う例
    # from jepx_scraper import JEPXScraper

    # 基本的な使い方
    with DuckDBLoader() as loader:
        # CSVファイルから読み込み
        filepath = Path("data/raw/spot_summary_2024.csv")
        if filepath.exists():
            row_count = loader.load_csv_from_file(
                filepath,
                "spot_summary",
                if_exists="replace",
            )
            print(f"Loaded {row_count} rows")

            # テーブル情報確認
            print("\nTable Info:")
            print(loader.get_table_info("spot_summary"))

            # データ確認
            print(f"\nTotal rows: {loader.get_row_count('spot_summary')}")

            # クエリ実行
            df = loader.query_to_df("SELECT * FROM spot_summary LIMIT 5")
            print("\nSample data:")
            print(df)

    # スクレイパーと組み合わせた完全な例
    # timestamp = datetime(2024, 4, 1)
    #
    # with JEPXScraper() as scraper, DuckDBLoader() as loader:
    #     # データ取得
    #     response = scraper.scrape(timestamp)
    #
    #     # DuckDBに格納
    #     row_count = loader.load_csv_from_bytes(
    #         response.content,
    #         "spot_summary",
    #         if_exists="append",
    #     )
    #     print(f"Loaded {row_count} rows to DuckDB")
