from flask import Flask, jsonify, request, abort
from tracker_dashboard import load_bet_log, get_performance_summary, update_result
import config
import os
import threading

app = Flask(__name__)
app.secret_key = config.FLASK_SECRET_KEY

# Track run status
run_status = {"status": "idle", "last_run": None, "bets": 0, "error": None}


def _run_pipeline():
    global run_status
    run_status["status"] = "running"
    run_status["error"]  = None
    try:
        from main import run_daily_system
        board, sgp = run_daily_system()
        run_status["status"]   = "complete"
        run_status["bets"]     = len(board)
        run_status["last_run"] = str(__import__("datetime").datetime.now())
    except Exception as e:
        run_status["status"] = "error"
        run_status["error"]  = str(e)


@app.get("/")
def index():
    return jsonify({"status": "ok", "service": "NBA Prop Research API"})


@app.get("/bets")
def get_bets():
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
    df = load_bet_log()
    if df.empty:
        return jsonify([])
    return jsonify(df[df["date"] == date.today().isoformat()].to_dict(orient="records"))


@app.get("/performance")
def get_performance():
    return jsonify(get_performance_summary())


@app.get("/run")
def trigger_run():
    """
    Starts the pipeline in the background and returns immediately.
    Check /run/status to see when it completes.
    """
    if run_status["status"] == "running":
        return jsonify({"status": "already_running",
                        "message": "Pipeline is already running. Check /run/status"})
    thread = threading.Thread(target=_run_pipeline, daemon=True)
    thread.start()
    return jsonify({"status": "started",
                    "message": "Pipeline started. Check /run/status for updates."})


@app.get("/run/status")
def run_status_check():
    """Check the status of the last pipeline run."""
    return jsonify(run_status)


@app.post("/bets/settle")
def settle_bet():
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
