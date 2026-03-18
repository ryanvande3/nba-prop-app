# ─────────────────────────────────────────────
#  scheduler.py  ·  NBA Prop Research Tool
#  Uses the `schedule` library to run the
#  daily pipeline automatically.
#
#  Run: python scheduler.py
#  Keep-alive process (e.g. via screen/tmux/systemd)
# ─────────────────────────────────────────────

import time
import schedule
from datetime import datetime


def _run():
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M}] ▶ Running daily pipeline...")
    try:
        from main import run_daily_system
        run_daily_system()
    except Exception as e:
        print(f"  [ERROR] Pipeline failed: {e}")


def _refresh_lines():
    """Mid-day line refresh to catch sharp movement."""
    print(f"\n[{datetime.now():%Y-%m-%d %H:%M}] ▶ Refreshing lines...")
    try:
        from data_feed import fetch_sportsbook_lines
        from sharp_tracker import detect_sharp_movement, load_line_snapshot, format_movement_report
        from edge_detector import _pivot_lines

        current = fetch_sportsbook_lines()
        if current.empty:
            print("  No lines returned.")
            return

        snapshot = load_line_snapshot()
        if snapshot.empty:
            print("  No snapshot to compare.")
            return

        wide     = _pivot_lines(current.rename(columns={"player_name": "player"})
                                        if "player_name" in current.columns else current)
        movement = detect_sharp_movement(wide, snapshot)
        print(format_movement_report(movement))
    except Exception as e:
        print(f"  [ERROR] Line refresh failed: {e}")


# ── Schedule ──────────────────────────────────
#  09:00 AM — full daily run (after morning lines post)
#  12:00 PM — midday line movement check
#  04:00 PM — afternoon refresh before tip-offs
schedule.every().day.at("09:00").do(_run)
schedule.every().day.at("12:00").do(_refresh_lines)
schedule.every().day.at("16:00").do(_refresh_lines)


if __name__ == "__main__":
    print("🏀 NBA Prop Scheduler started.")
    print("   Jobs: 09:00 full run | 12:00 line refresh | 16:00 line refresh")
    print("   Press Ctrl+C to stop.\n")

    # Run immediately on startup so you don't wait until 9am
    _run()

    while True:
        schedule.run_pending()
        time.sleep(30)
