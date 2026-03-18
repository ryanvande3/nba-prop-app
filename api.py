from flask import Flask, jsonify, request, abort
from tracker_dashboard import load_bet_log, get_performance_summary, update_result
import config
import os

app = Flask(__name__)
app.secret_key = config.FLASK_SECRET_KEY


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


@app.get("/run")
def trigger_run():
    try:
        from main import run_daily_system
        board, sgp = run_daily_system()
        return jsonify({"status": "ok", "bets": len(board), "sgp": len(sgp),
                        "board": board.to_dict(orient="records") if not board.empty else []})
    except Exception as e:
        abort(500, str(e))


if __name__ == "__main__":
    port = int(os.getenv("PORT", config.FLASK_PORT))
    app.run(host="0.0.0.0", port=port)
