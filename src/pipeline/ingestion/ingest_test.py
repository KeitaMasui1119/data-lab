from pyiceberg.catalog import load_catalog

catalog = load_catalog("default")
print("Catalog loaded successfully:", catalog)
