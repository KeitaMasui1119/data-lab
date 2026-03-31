from pyiceberg.catalog import load_catalog

catalog = load_catalog("dlh_dev")
print("Catalog loaded successfully:", catalog)

# 既存の名前空間を確認する
existing_namespaces = catalog.list_namespaces()
print("既存の名前空間:")
for namespace in existing_namespaces:
    print(f"- {'.'.join(namespace)}")

# 新しい名前空間の作成
namespace = "bronze.jepx"
if not catalog.namespace_exists(namespace):
    catalog.create_namespace(namespace)
