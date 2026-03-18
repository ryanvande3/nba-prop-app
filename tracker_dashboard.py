import csv
from datetime import date
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from config import EDGE_THRESHOLDS, UNITS
import config

_engine = None

def _get_engine():
    global _engine
    if _engine is None:
        _engine = create_engine(config.DATABASE_URL, pool_pre_ping=True)
        _init_db(_engine)
    return _engine

def _init_db(engine):
    is_sqlite = "sqlite" in config.DATABASE_URL
    pk_type   = "INTEGER" if is_sqlite else "SERIAL"
    with engine.begin() as conn:
        conn.execute(text(f"""
            CREATE TABLE IF NOT EXISTS bets (
                id          {pk_type} PRIMARY KEY,
                date        TEXT NOT NULL,
                player_name TEXT NOT NULL,
                prop        TEXT NOT NULL,
                line        REAL, projection REAL, edge REAL, units REAL,
                floor       REAL, ceiling REAL, opponent TEXT, home_away TEXT,
                result      TEXT DEFAULT 'PENDING',
                actual_stat REAL, units_won REAL, notes TEXT,
                UNIQUE(date, player_name, prop)
            )
        """))

ALL_MARKETS = [
    ("points",   "Points",   "line_points",   "proj_points",   "edge_points",   "units_points"),
    ("assists",  "Assists",  "line_assists",  "proj_assists",  "edge_assists",  "units_assists"),
    ("rebounds", "Rebounds", "line_rebounds", "proj_rebounds", "edge_rebounds", "units_rebounds"),
    ("pra",      "PRA",      "line_pra",      "proj_pra",      "edge_pra",      "units_pra"),
    ("steals",   "Steals",   "line_steals",   "proj_steals",   "edge_steals",   "units_steals"),
    ("blocks",   "Blocks",   "line_blocks",   "proj_blocks",   "edge_blocks",   "units_blocks"),
    ("threes",   "Threes",   "line_threes",   "proj_threes",   "edge_threes",   "units_threes"),
]

_CSV_COLS = ["date","player_name","prop","line","projection","edge","units",
             "floor","ceiling","opponent","home_away","result","actual_stat",
             "units_won","notes"]


def generate_daily_edge_board(df):
    bets = []
    for _, row in df.iterrows():
        for market, label, line_col, proj_col, edge_col, units_col in ALL_MARKETS:
            if not row.get(f"bet_{market}", False):
                continue
            bets.append({
                "Player":     row["player_name"],
                "Prop":       label,
                "Line":       row.get(line_col),
                "Projection": row.get(proj_col),
                "Edge":       row.get(edge_col),
                "Units":      row.get(units_col),
                "Floor":      row.get(f"floor_{market}"),
                "Ceiling":    row.get(f"ceiling_{market}"),
                "Opponent":   row.get("opponent", ""),
                "Home":       "H" if row.get("is_home") else "A",
                "Rating":     row.get("rating", ""),
            })
    if not bets:
        return pd.DataFrame(columns=["Player","Prop","Line","Projection",
                                     "Edge","Units","Floor","Ceiling",
                                     "Opponent","Home","Rating"])
    return pd.DataFrame(bets).sort_values(
        ["Units","Edge"], ascending=[False,False]).reset_index(drop=True)


def log_bets(edge_board):
    if edge_board.empty:
        print("  [Log] No bets to log.")
        return
    today = date.today().isoformat()
    try:
        engine = _get_engine()
        rows   = [{"date": today, "player_name": r["Player"], "prop": r["Prop"],
                   "line": _f(r.get("Line")), "projection": _f(r.get("Projection")),
                   "edge": _f(r.get("Edge")), "units": _f(r.get("Units")),
                   "floor": _f(r.get("Floor")), "ceiling": _f(r.get("Ceiling")),
                   "opponent": r.get("Opponent",""), "home_away": r.get("Home","")}
                  for _, r in edge_board.iterrows()]
        inserted = 0
        with engine.begin() as conn:
            for row in rows:
                res = conn.execute(text("""
                    INSERT INTO bets (date,player_name,prop,line,projection,edge,
                                     units,floor,ceiling,opponent,home_away)
                    VALUES (:date,:player_name,:prop,:line,:projection,:edge,
                            :units,:floor,:ceiling,:opponent,:home_away)
                    ON CONFLICT (date,player_name,prop) DO NOTHING
                """), row)
                inserted += res.rowcount
        print(f"  [DB] {inserted} bet(s) logged.")
    except Exception as e:
        print(f"  [WARN] DB unavailable ({e}). Using CSV fallback.")
        _log_csv(edge_board, today)


def update_result(player_name, prop, actual_stat, bet_date=None):
    if bet_date is None:
        bet_date = date.today().isoformat()
    try:
        engine = _get_engine()
        with engine.begin() as conn:
            row = conn.execute(text("""
                SELECT id, line, units FROM bets
                WHERE date=:date AND player_name=:player AND prop=:prop
                  AND result='PENDING' LIMIT 1
            """), {"date": bet_date, "player": player_name, "prop": prop}).fetchone()
            if not row:
                print(f"  [Warn] No pending bet: {player_name} {prop}")
                return
            bet_id, line, units = row
            result    = "WIN" if actual_stat > line else ("PUSH" if actual_stat == line else "LOSS")
            units_won = units if result == "WIN" else (0.0 if result == "PUSH" else -units)
            conn.execute(text("""
                UPDATE bets SET result=:result, actual_stat=:actual, units_won=:won
                WHERE id=:id
            """), {"result": result, "actual": actual_stat, "won": units_won, "id": bet_id})
        print(f"  [Settled] {player_name} {prop}: {actual_stat} vs {line} → {result} ({units_won:+.2f}u)")
    except Exception as e:
        print(f"  [ERROR] update_result: {e}")


def load_bet_log():
    try:
        return pd.read_sql("SELECT * FROM bets", _get_engine())
    except Exception:
        path = Path(config.BET_LOG_FILE)
        return pd.read_csv(path) if path.exists() else pd.DataFrame(columns=_CSV_COLS)


def get_performance_summary():
    df      = load_bet_log()
    settled = df[df["result"].isin(["WIN","LOSS","PUSH"])].copy()
    if settled.empty:
        return {"message": "No settled bets yet."}
    settled["units_won"] = pd.to_numeric(settled["units_won"], errors="coerce").fillna(0)
    settled["units"]     = pd.to_numeric(settled["units"],     errors="coerce").fillna(1)
    wins, losses, pushes = ((settled["result"]=="WIN").sum(),
                            (settled["result"]=="LOSS").sum(),
                            (settled["result"]=="PUSH").sum())
    total   = wins + losses
    wagered = settled["units"].sum()
    profit  = settled["units_won"].sum()
    by_prop = (settled.groupby("prop")
               .agg(bets=("result","count"),
                    wins=("result", lambda x: (x=="WIN").sum()),
                    profit=("units_won","sum"))
               .reset_index())
    by_prop["win_pct"] = (by_prop["wins"] / by_prop["bets"] * 100).round(1)
    return {
        "record":  f"{wins}W-{losses}L-{pushes}P",
        "win_pct": f"{round(wins/total*100,1)}%" if total else "—",
        "wagered": round(wagered, 2),
        "profit":  round(profit, 2),
        "roi":     f"{round(profit/wagered*100,1)}%" if wagered else "—",
        "by_prop": by_prop.to_dict(orient="records"),
    }


def push_to_google_sheets(board):
    if not config.GOOGLE_SHEETS_CREDS_JSON or not config.EDGE_BOARD_SHEET_ID:
        print("  [Sheets] Credentials not set — skipping.")
        return
    try:
        import gspread
        from oauth2client.service_account import ServiceAccountCredentials
        scope  = ["https://spreadsheets.google.com/feeds",
                  "https://www.googleapis.com/auth/drive"]
        creds  = ServiceAccountCredentials.from_json_keyfile_name(
                     config.GOOGLE_SHEETS_CREDS_JSON, scope)
        client = gspread.authorize(creds)
        ws     = client.open_by_key(config.EDGE_BOARD_SHEET_ID).get_worksheet(0)
        ws.clear()
        ws.update([board.columns.tolist()] + board.values.tolist())
        print(f"  [Sheets] Edge board pushed.")
    except Exception as e:
        print(f"  [WARN] Sheets push failed: {e}")


def print_edge_board(board):
    print(f"\n{'='*65}")
    print(f"  🏀  NBA PROP EDGE BOARD — {date.today().isoformat()}")
    print(f"{'='*65}")
    if board.empty:
        print("  No edges found today.\n")
        return
    print(f"\n  {'Player':<26} {'Prop':<9} {'Line':>5} {'Proj':>6} "
          f"{'Edge':>5} {'U':>2}  {'Range':<14}  Rating")
    print("  " + "─"*80)
    for _, row in board.iterrows():
        f_, c_ = row.get("Floor"), row.get("Ceiling")
        rng = f"{f_:.1f}–{c_:.1f}" if f_ is not None else "—"
        print(f"  {str(row['Player']):<26} {str(row['Prop']):<9} "
              f"{row['Line']:>5.1f} {row['Projection']:>6.1f} "
              f"{row['Edge']:>+5.1f} {int(row['Units']):>2}u  "
              f"{rng:<14}  {row.get('Rating','')}")
    print()
    perf = get_performance_summary()
    if "message" not in perf:
        print(f"  Season: {perf['record']}  ({perf['win_pct']} win)  "
              f"Profit: {perf['profit']:+.2f}u  ROI: {perf['roi']}\n")


def _log_csv(board, today):
    Path(config.LOG_DIR).mkdir(parents=True, exist_ok=True)
    path   = Path(config.BET_LOG_FILE)
    is_new = not path.exists()
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=_CSV_COLS)
        if is_new:
            w.writeheader()
        for _, row in board.iterrows():
            w.writerow({"date": today, "player_name": row["Player"],
                        "prop": row["Prop"], "line": row.get("Line",""),
                        "projection": row.get("Projection",""),
                        "edge": row.get("Edge",""), "units": row.get("Units",""),
                        "floor": row.get("Floor",""), "ceiling": row.get("Ceiling",""),
                        "opponent": row.get("Opponent",""), "home_away": row.get("Home",""),
                        "result": "PENDING", "actual_stat":"", "units_won":"", "notes":""})


def _f(val):
    try: return float(val)
    except (TypeError, ValueError): return None
