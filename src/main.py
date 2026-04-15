from core.storage_client import RustFSClient


def main():
    rustfs = RustFSClient()
    rustfs.delete_folder("jp-power-grid-dev", "bronze/jepx_spot_price/")


if __name__ == "__main__":
    main()
