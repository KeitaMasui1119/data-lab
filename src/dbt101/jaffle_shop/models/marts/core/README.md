# core marts モデル

このディレクトリには、Jaffle Shop の主要なビジネスプロセスに関連するモデルが含まれています。

## モデル一覧

- `fact_orders`: 注文と支払い情報を結合したファクトテーブル
- `dim_customers`: 顧客情報と注文サマリーを結合したディメンションテーブル

## 使用例

```sql
-- 顧客ごとの平均注文金額を計算
select
    customer_id,
    first_name,
    last_name,
    lifetime_value / nullif(number_of_orders, 0) as average_order_value
from {{ ref('dim_customers') }}
order by average_order_value desc
```
