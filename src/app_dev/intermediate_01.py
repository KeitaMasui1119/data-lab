# 1:クラス設計と特殊メソッド
# **問題**
# 以下の要件を満たす`Book`クラスを作成せよ。
# - 属性:title, author, price
# - `__str__`メソッドで"『タイトル』 by 著者 - ￥価格"の形式で出力
# - `__eq__`メソッドで、タイトルと著者が同じなら同一とみなす
# - `discount(rate:float)`メソッドで価格を割引する(例:rate=0.1 → 10%割引)


class Book:
    """_summary_."""

    def __init__(self, title: str, author: str, price: int) -> None:
        """_summary_.

        Args:
            title (str): _description_
            author (str): _description_
            price (int): _description_

        """
        self.title = title
        self.author = author
        self.price = price

    # def __str__(self, title, author, price):
    #     msg =
