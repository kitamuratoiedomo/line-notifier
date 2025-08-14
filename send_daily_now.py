# send_daily_now.py
from notifier import summarize_today_and_notify, load_user_ids_from_simple_col

if __name__ == "__main__":
    targets = load_user_ids_from_simple_col()
    summarize_today_and_notify(targets)