"""JEPXデータスクレイピングモジュール."""

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

import requests


@dataclass
class ScraperConfig:
    """スクレイパー設定クラス."""

    base_url: str = "https://www.jepx.jp/_download.php"
    referer: str = "https://www.jepx.jp/electricpower/market-data/spot/"
    timeout: int = 30
    save_dir: Path = Path("data/raw")


class JEPXScraper:
    """JEPXスポット市場データをスクレイピングするクラス."""

    def __init__(self, config: ScraperConfig | None = None) -> None:
        """初期化.

        Args:
            config: スクレイパー設定。Noneの場合はデフォルト設定を使用
        """
        self.config = config or ScraperConfig()
        self.session = requests.Session()
        self._setup_session()

    def _setup_session(self) -> None:
        """セッションの初期設定."""
        self.session.headers.update(
            {
                "Content-Type": "application/x-www-form-urlencoded",
                "Referer": self.config.referer,
            }
        )

    @staticmethod
    def get_fiscal_year(timestamp: datetime) -> int:
        """タイムスタンプから年度を取得.

        Args:
            timestamp: 対象のタイムスタンプ

        Returns:
            年度（4月始まり）
        """
        if timestamp.month >= 4:
            return timestamp.year
        return timestamp.year - 1

    def _build_request_data(self, timestamp: datetime) -> dict[str, Any]:
        """リクエストデータを構築.

        Args:
            timestamp: 対象のタイムスタンプ

        Returns:
            リクエストボディ
        """
        year = self.get_fiscal_year(timestamp)
        return {
            "dir": "spot_summary",
            "file": f"spot_summary_{year}.csv",
        }

    def scrape(self, timestamp: datetime) -> requests.Response:
        """データをスクレイピング.

        Args:
            timestamp: 取得対象のタイムスタンプ

        Returns:
            HTTPレスポンス

        Raises:
            requests.RequestException: リクエスト失敗時
        """
        url = f"{self.config.base_url}?timestamp={timestamp!s}"
        data = self._build_request_data(timestamp)

        response = self.session.post(
            url,
            data=data,
            timeout=self.config.timeout,
        )
        response.raise_for_status()

        return response

    def scrape_and_save(self, timestamp: datetime) -> Path:
        """データをスクレイピングして保存.

        Args:
            timestamp: 取得対象のタイムスタンプ

        Returns:
            保存したファイルのパス

        Raises:
            requests.RequestException: リクエスト失敗時
            IOError: ファイル保存失敗時
        """
        response = self.scrape(timestamp)

        # 保存先ディレクトリを作成
        self.config.save_dir.mkdir(parents=True, exist_ok=True)

        # ファイル名を生成
        year = self.get_fiscal_year(timestamp)
        filename = f"spot_summary_{year}_{timestamp:%Y%m%d}.csv"
        filepath = self.config.save_dir / filename

        # ファイルに保存
        filepath.write_bytes(response.content)

        return filepath

    def close(self) -> None:
        """セッションをクローズ."""
        self.session.close()

    def __enter__(self) -> "JEPXScraper":
        """コンテキストマネージャー: enter."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """コンテキストマネージャー: exit."""
        self.close()


# 使用例
if __name__ == "__main__":
    # 基本的な使い方
    timestamp = datetime(2024, 4, 1)
    scraper = JEPXScraper()

    try:
        response = scraper.scrape(timestamp)
        print(f"Status: {response.status_code}")
        print(f"Content length: {len(response.content)}")
    finally:
        scraper.close()

    # コンテキストマネージャーを使う方法（推奨）
    with JEPXScraper() as scraper:
        filepath = scraper.scrape_and_save(timestamp)
        print(f"Saved to: {filepath}")

    # カスタム設定を使う
    custom_config = ScraperConfig(
        timeout=60,
        save_dir=Path("data/jepx"),
    )
    with JEPXScraper(config=custom_config) as scraper:
        filepath = scraper.scrape_and_save(timestamp)
        print(f"Saved to: {filepath}")
