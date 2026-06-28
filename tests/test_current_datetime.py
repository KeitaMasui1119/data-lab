from datetime import datetime


def test_display_current_datetime():
    now = datetime.now()
    print(f"\nCurrent date and time: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    assert isinstance(now, datetime)


def main():
    test_display_current_datetime()
    return "Success"


if __name__ == "__main__":
    main()
