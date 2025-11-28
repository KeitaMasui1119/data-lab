# scripts/hourly_ping.py
import datetime
from pathlib import Path

def main():
    now = datetime.datetime.now()
    ts = now.strftime("%Y-%m-%d %H:%M:%S")

    # ログ保存先
    log_dir = Path("/workspace/src/stg/data_lake")
    log_dir.mkdir(exist_ok=True)
    log_file = log_dir / "scheduler.log"

    message = f"[{ts}] Scheduler executed.\n"

    # 追記
    with log_file.open("a", encoding="utf-8") as f:
        f.write(message)

    print(message, end="")  # GitHub Actionsログにも出す

if __name__ == "__main__":
    main()
