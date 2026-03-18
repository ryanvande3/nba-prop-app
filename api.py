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
        log(f"Step 3: got {len(stats_df)} players. Fetching lines...")
        lines_df = fetch_sportsbook_lines()
        log(f"Step 4: got {len(lines_df)} lines. Calculating projections...")
        from projection_engine import calculate_projections
        stats_df = _enrich(stats_df)
        proj_df  = calculate_projections(stats_df)
        log("Step 5: detecting edges...")
        from edge_detector import calculate_edge
        edge_df  = calculate_edge(proj_df, lines_df)
        log("Step 6: generating board...")
        from tracker_dashboard import generate_daily_edge_board, log_bets
        board = generate_daily_edge_board(edge_df)
        log_bets(board)
        run_status["status"]   = "complete"
        run_status["bets"]     = len(board)
        run_status["last_run"] = str(__import__("datetime").datetime.now())
        log(f"Done. {len(board)} bets logged.")
    except Exception as e:
        run_status["status"] = "error"
        run_status["error"]  = str(e)
        run_status["log"].append(traceback.format_exc())
        print(traceback.format_exc())


def _enrich(stats_df, season="2024-25"):
    try:
        from data_feed import get_todays_games, get_team_pace_stats
        games      = get_todays_games()
        pace_stats = get_team_pace_stats(season)
        if not games or pace_stats.empty:
            return stats_df
        opp_map = {}
        for g in games:
            opp_map[g["home_team"]] = {"opponent": g["away_team"], "is_home": True}
            opp_map[g["away_team"]] = {"opponent": g["home_team"], "is_home": False}
        league_avg = float(pace_stats["PACE"].mean())
        df = stats_df.copy()
        def _pace(team):
            m   = opp_map.get(team, {})
            opp = m.get("opponent", "")
            row = pace_stats[pace_stats["TEAM_ABBREVIATION"] == opp]
            return round(float(row["PACE"].iloc[0]) / league_avg, 4) if not row.empty else 1.0
        df["pace_adj"] = df["team"].apply(_pace)
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
