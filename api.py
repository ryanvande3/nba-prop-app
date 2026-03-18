from flask import Flask, jsonify, request, abort
import config
import os
import threading
import traceback

app = Flask(__name__)
app.secret_key = config.FLASK_SECRET_KEY

run_status = {"status": "idle", "last_run": None, "bets": 0, "error": None, "log": []}


def _run_pipeline():
    global run_status
    run_status["status"] = "running"
    run_status["error"]  = None
    run_status["log"]    = []

    def log(msg):
        print(msg)
        run_status["log"].append(msg)

    try:
        log("Step 1: importing modules...")
        from data_feed import fetch_nba_stats, fetch_sportsbook_lines

        log("Step 2: fetching stats...")
        stats_df = fetch_nba_stats()
        log(f"Step 3: got {len(stats_df)} players.")

        if stats_df.empty:
            log("ERROR: No player stats returned. Check BallDontLie API.")
            run_status["status"] = "error"
            run_status["error"]  = "No player stats returned from BallDontLie API."
            return

        log("Step 4: fetching lines...")
        lines_df = fetch_sportsbook_lines()
        log(f"Step 5: got {len(lines_df)} lines.")

        if lines_df.empty:
            log("ERROR: No lines returned. Check ODDS_API_KEY.")
            run_status["status"] = "error"
            run_status["error"]  = "No lines returned."
            return

        log("Step 6: enriching with matchup data...")
        stats_df = _enrich(stats_df)

        log("Step 7: calculating projections...")
        from projection_engine import calculate_projections
        proj_df = calculate_projections(stats_df)
        log(f"Step 8: projections done for {len(proj_df)} players.")

        log("Step 9: detecting edges...")
        from edge_detector import calculate_edge
        edge_df = calculate_edge(proj_df, lines_df)
        bet_cols = [c for c in edge_df.columns if c.startswith("bet_")]
        flagged  = edge_df[edge_df[bet_cols].any(axis=1)] if bet_cols else edge_df
        log(f"Step 10: found {len(flagged)} edges.")

        log("Step 11: generating board and logging bets...")
        from tracker_dashboard import generate_daily_edge_board, log_bets
        board = generate_daily_edge_board(edge_df)
        log_bets(board)

        run_status["status"]   = "complete"
        run_status["bets"]     = len(board)
        run_status["last_run"] = str(__import__("datetime").datetime.now())
        log(f"✅ Done. {len(board)} bets logged.")

    except Exception as e:
        run_status["status"] = "error"
        run_status["error"]  = str(e)
        tb = traceback.format_exc()
        run_status["log"].append(tb)
        print(tb)


def _enrich(stats_df, season=2024):
    try:
        from data_feed import get_todays_games
        games = get_todays_games()
        if not games:
            return stats_df
        opp_map = {}
        for g in games:
            opp_map[g["home_team"]] = {"opponent": g["away_team"], "is_home": True}
            opp_map[g["away_team"]] = {"opponent": g["home_team"], "is_home": False}
        df = stats_df.copy()
        df["pace_adj"] = 1.0
        df["opponent"] = df["team"].apply(lambda t: opp_map.get(t, {}).get("opponent", ""))
        df["is_home"]  = df["team"].apply(lambda t: opp_map.get(t, {}).get("is_home", False))
        return df
    except Exception as e:
        print(f"  [WARN] Enrichment failed: {e}")
        return stats_df


@app.get("/")
def index():
    return jsonify({"status": "ok", "service": "NBA Prop Research API"})


@app.get("/run")
def trigger_run():
    if run_status["status"] == "running":
        return jsonify({"status": "already_running"})
    thread = threading.Thread(target=_run_pipeline, daemon=True)
    thread.start()
    return jsonify({"status": "started", "message": "Check /run/status for updates"})


@app.get("/run/status")
def run_status_check():
    return jsonify(run_status)


@app.get("/bets")
def get_bets():
    from tracker_dashboard import load_bet_log
    df = load_bet_log()
    if df.empty:
        return jsonify([])
    if date_filter := request.args.get("date"):
        df = df[df["date"] == date_filter]
    if result_filter := request.args.get("result"):
        df = df[df["result"] == result_filter.upper()]
    return jsonify(df.to_dict(orient="records"))


@app.get("/bets/today")
def get_todays_bets():
    from datetime import date
    from tracker_dashboard import load_bet_log
    df = load_bet_log()
    if df.empty:
        return jsonify([])
    return jsonify(df[df["date"] == date.today().isoformat()].to_dict(orient="records"))


@app.get("/performance")
def get_performance():
    from tracker_dashboard import get_performance_summary
    return jsonify(get_performance_summary())


@app.post("/bets/settle")
def settle_bet():
    from tracker_dashboard import update_result
    data = request.get_json(silent=True)
    if not data:
        abort(400, "JSON body required.")
    missing = [k for k in ["player_name","prop","actual_stat"] if k not in data]
    if missing:
        abort(400, f"Missing: {missing}")
    try:
        update_result(data["player_name"], data["prop"],
                      float(data["actual_stat"]), data.get("date"))
        return jsonify({"status": "settled", **data})
    except Exception as e:
        abort(500, str(e))


if __name__ == "__main__":
    port = int(os.getenv("PORT", config.FLASK_PORT))
    app.run(host="0.0.0.0", port=port)
