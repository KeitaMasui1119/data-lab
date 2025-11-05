-- DuckDBで実行するモックデータセットアップスクリプト
-- CSVファイルからデータをインポートします

-- jaffle_shop_rawスキーマを作成
CREATE SCHEMA IF NOT EXISTS jaffle_shop_raw;

-- 顧客テーブルの作成とデータインポート
CREATE OR REPLACE TABLE jaffle_shop_raw.customers AS
SELECT * FROM read_csv_auto('../data/jaffle-data/raw_customers.csv');

-- 店舗テーブルの作成とデータインポート
CREATE OR REPLACE TABLE jaffle_shop_raw.stores AS
SELECT * FROM read_csv_auto('../data/jaffle-data/raw_stores.csv');

-- 注文テーブルの作成とデータインポート
CREATE OR REPLACE TABLE jaffle_shop_raw.orders AS
SELECT * FROM read_csv_auto('../data/jaffle-data/raw_orders.csv');

-- 製品テーブルの作成とデータインポート
CREATE OR REPLACE TABLE jaffle_shop_raw.products AS
SELECT * FROM read_csv_auto('../data/jaffle-data/raw_products.csv');

-- アイテムテーブルの作成とデータインポート（注文と製品を紐付ける）
CREATE OR REPLACE TABLE jaffle_shop_raw.items AS
SELECT * FROM read_csv_auto('../data/jaffle-data/raw_items.csv');

-- サプライテーブルの作成とデータインポート（材料・資材）
CREATE OR REPLACE TABLE jaffle_shop_raw.supplies AS
SELECT * FROM read_csv_auto('../data/jaffle-data/raw_supplies.csv');

-- データ確認用のクエリ
SELECT 'jaffle_shop_raw.customers' as table_name,
    COUNT(*) as row_count
FROM jaffle_shop_raw.customers
UNION ALL
SELECT 'jaffle_shop_raw.stores',
    COUNT(*)
FROM jaffle_shop_raw.stores
UNION ALL
SELECT 'jaffle_shop_raw.orders',
    COUNT(*)
FROM jaffle_shop_raw.orders
UNION ALL
SELECT 'jaffle_shop_raw.products',
    COUNT(*)
FROM jaffle_shop_raw.products
UNION ALL
SELECT 'jaffle_shop_raw.items',
    COUNT(*)
FROM jaffle_shop_raw.items
UNION ALL
SELECT 'jaffle_shop_raw.supplies',
    COUNT(*)
FROM jaffle_shop_raw.supplies;
