from pyiceberg.catalog import load_catalog

# 1. カタログの読み込み
catalog = load_catalog("dlh_dev")
table_name = "bronze.jepx_spot_price"

try:
    # 2. テーブルの削除
    catalog.drop_table(table_name)
    print(f"✅ 成功: テーブル '{table_name}' をカタログとストレージから削除しました。")
except Exception as e:
    print(f"❌ 失敗: テーブルの削除中にエラーが発生しました。\n詳細: {e}")
