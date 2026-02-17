import datetime
import os
import platform


def main():
    print("=" * 40)
    print("   Data Pipeline Container Test")
    print("=" * 40)

    # 1. 現在時刻 (Timezoneの確認になります。Dockerは通常UTCです)
    now = datetime.datetime.now()
    print(f"[-] Current Time : {now}")

    # 2. Pythonバージョン (Dockerfileで指定したバージョンか確認)
    print(f"[-] Python Ver   : {platform.python_version()}")

    # 3. 実行ユーザーと権限 (appuserで実行されているか、rootじゃないか確認)
    # useradd -m appuser した結果が反映されているか
    try:
        user = os.getlogin()
    except OSError:
        # Docker環境等でgetloginが失敗する場合のフォールバック
        import getpass

        user = getpass.getuser()

    uid = os.getuid()
    print(f"[-] User (UID)   : {user} ({uid})")

    # 4. カレントディレクトリ (WORKDIR /app が効いているか確認)
    print(f"[-] Working Dir  : {os.getcwd()}")

    # 5. ファイルシステムの確認 (COPY src ... が成功しているか確認)
    print("[-] Directory Contents:")
    for item in os.listdir("."):
        print(f"    - {item}")

    print("=" * 40)
    print("SUCCESS: Pipeline is ready to accept commands.")
    print("=" * 40)


if __name__ == "__main__":
    main()
