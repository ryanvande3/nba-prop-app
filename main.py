# ─────────────────────────────────────────────
#  main.py  ·  NBA Prop Research Tool
#
#  Usage:
#    python main.py                   → full daily run
#    python main.py --settle          → settle pending bets
#    python main.py --performance     → season ROI summary
#    python main.py --season 2024-25  → specify season
# ─────────────────────────────────────────────

import argparse
from datetime import date

import pandas as pd

from data_feed import fetch_nba_stats, fetch_sportsbook_lines, get_todays_games, get_team_pace_stats
from projection_engine import calculate_projections, build_all_projections
from edge_detector import calculate_edge
from tracker_dashboard import generate_daily_edge_board, log_bets, update_result, print_edge_board, get_performance_summary
from sharp_tracker import detect_sharp_movement, detect_sgp_correlations, save_line_snapshot, load_line_snapshot, format_movement_report, format_sgp_report


# ─────────────────────────────────────────────
#  run_daily_system  (your interface)
# ─────────────────────────────────────────────

def run_daily_system(previous_lines=None):
    """
    Full daily pipeline. Keeps your exact function call order:
      fetch → project → edge → sharp → sgp → board

    previous_lines: pass a lines DataFrame to compare for movement,
                    or leave None to auto-load today's snapshot.
    """
    print(f"\n{'='*55}")
    print(f"  🏀  NBA Prop System — {date.today().isoformat()}")
    print(f"{'='*55}\n")

    # ── 1. Fetch ──────────────────────────────
    print("▶ Fetching stats...")
    stats_df = fetch_nba_stats()
    if stats_df.empty:
        print("  ⚠️  No stats returned. Check API keys or try again later.")
        return pd.DataFrame(), pd.DataFrame()

    print("▶ Fetching lines...")
    lines_df = fetch_sportsbook_lines()
    if lines_df.empty:
        print("  ⚠️  No lines returned. Check API keys or try again later.")
        return pd.DataFrame(), pd.DataFrame()

    # Save opening snapshot (only on first run of the day)
    _maybe_save_snapshot(lines_df)

    # ── 2. Project ────────────────────────────
    print("▶ Calculating projections...")

    # Enrich stats_df with pace + matchup context if available
    stats_df = _enrich_with_matchup(stats_df)
    projections_df = calculate_projections(stats_df)

    # ── 3. Edge ───────────────────────────────
    print("▶ Calculating edges...")
    edge_df = calculate_edge(projections_df, lines_df)

    if edge_df.empty:
        print("  No edges found today.")
        return pd.DataFrame(), pd.DataFrame()

    # ── 4. Sharp movement ─────────────────────
    if previous_lines is not None:
        print("▶ Detecting sharp movement (provided lines)...")
        edge_df = detect_sharp_movement(edge_df, previous_lines)
    else:
        snapshot = load_line_snapshot()
        if not snapshot.empty:
            print("▶ Detecting sharp movement (snapshot)...")
            edge_df = detect_sharp_movement(edge_df, snapshot)
        else:
            print("▶ No previous lines — skipping movement detection.")

    # ── 5. SGP ────────────────────────────────
    print("▶ Detecting SGP correlations...")
    sgp_candidates = detect_sgp_correlations(edge_df)

    # ── 6. Board ──────────────────────────────
    daily_board = generate_daily_edge_board(edge_df)

    # ── Output ────────────────────────────────
    print("\n=== DAILY EDGE BOARD ===")
    print_edge_board(daily_board)

    print("\n=== SGP CANDIDATES ===")
    if sgp_candidates.empty:
        print("  None identified today.")
    else:
        print(format_sgp_report(sgp_candidates))

    # Log bets to CSV
    log_bets(daily_board)

    return daily_board, sgp_candidates


# ─────────────────────────────────────────────
#  Post-game: settle results
# ─────────────────────────────────────────────

def settle_results():
    """Interactive prompt to settle pending bets after games finish."""
    from tracker_dashboard import load_bet_log
    log = load_bet_log()
    pending = log[log["result"] == "PENDING"]

    if pending.empty:
        print("\n  No pending bets to settle.\n")
        return

    print(f"\n  {len(pending)} pending bet(s):\n")
    for _, row in pending.iterrows():
        print(f"  {row['player_name']} | {row['prop']} {row['line']}  ({row['date']})")
        raw = input("  → Actual stat (Enter to skip): ").strip()
        if not raw:
            continue
        try:
            update_result(row["player_name"], row["prop"],
                          float(raw), bet_date=row["date"])
        except ValueError:
            print("  Invalid input, skipping.")

    print()
    _print_season_summary()


# ─────────────────────────────────────────────
#  Helpers
# ─────────────────────────────────────────────

def _maybe_save_snapshot(lines_df: pd.DataFrame) -> None:
    """Save opening lines snapshot once per day."""
    from sharp_tracker import SNAPSHOT_FILE
    if not SNAPSHOT_FILE.exists():
        save_line_snapshot(lines_df)
    else:
        print("  [Snapshot] Already saved today.")


def _enrich_with_matchup(stats_df: pd.DataFrame,
                          season: str = "2024-25") -> pd.DataFrame:
    """
    Add pace_adj and opponent columns to stats_df using today's schedule.
    Silently returns unmodified df if schedule fetch fails.
    """
    try:
        games      = get_todays_games()
        pace_stats = get_team_pace_stats(season)
        if not games or pace_stats.empty:
            return stats_df

        # Build team → opponent map
        opp_map = {}
        for g in games:
            opp_map[g["home_team"]] = {"opponent": g["away_team"], "is_home": True}
            opp_map[g["away_team"]] = {"opponent": g["home_team"], "is_home": False}

        league_avg_pace = float(pace_stats["PACE"].mean())

        def _pace_adj(team):
            matchup = opp_map.get(team, {})
            opp     = matchup.get("opponent", "")
            row     = pace_stats[pace_stats["TEAM_ABBREVIATION"] == opp]
            if row.empty:
                return 1.0
            return round(float(row["PACE"].iloc[0]) / league_avg_pace, 4)

        stats_df = stats_df.copy()
        stats_df["pace_adj"] = stats_df["team"].apply(_pace_adj)
        stats_df["opponent"] = stats_df["team"].apply(
            lambda t: opp_map.get(t, {}).get("opponent", ""))
        stats_df["is_home"]  = stats_df["team"].apply(
            lambda t: opp_map.get(t, {}).get("is_home", False))

    except Exception as e:
        print(f"  [WARN] Matchup enrichment failed: {e}")

    return stats_df


def _print_season_summary() -> None:
    perf = get_performance_summary()
    if "message" in perf:
        print(f"  {perf['message']}")
        return
    print(f"\n  Season: {perf['record']}  ({perf['win_pct']} win rate)")
    print(f"  Profit: {perf['profit']:+.2f}u  |  ROI: {perf['roi']}\n")


# ─────────────────────────────────────────────
#  Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="NBA Prop Research Tool")
    parser.add_argument("--settle",      action="store_true",
                        help="Settle pending bets after games finish")
    parser.add_argument("--performance", action="store_true",
                        help="Print season performance summary")
    parser.add_argument("--season",      type=str, default="2024-25",
                        help="NBA season string (default: 2024-25)")
    args = parser.parse_args()

    if args.settle:
        settle_results()
    elif args.performance:
        _print_season_summary()
    else:
        run_daily_system()
