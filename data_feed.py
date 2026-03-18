import requests
import pandas as pd
from datetime import date
from config import API_KEYS, TOP_20_PLAYERS
import config

TOP_PLAYERS = TOP_20_PLAYERS
BASE_URL     = "https://api.balldontlie.io/v1"
HEADERS      = {"Authorization": API_KEYS.get("balldontlie", "")} \
               if API_KEYS.get("balldontlie") else {}


def _get(path, params=None):
    """Safe GET with timeout and error handling."""
    try:
        r = requests.get(f"{BASE_URL}{path}", params=params,
                         headers=HEADERS, timeout=15)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"  [ERROR] GET {path}: {e}")
        return {}


def get_player_id(full_name):
    data = _get("/players", {"search": full_name, "per_page": 5}).get("data", [])
    if not data:
        print(f"  [WARN] Not found: {full_name}")
        return None, "N/A"
    for p in data:
        if f"{p['first_name']} {p['last_name']}".lower() == full_name.lower():
            return p["id"], p.get("team", {}).get("abbreviation", "N/A")
    return data[0]["id"], data[0].get("team", {}).get("abbreviation", "N/A")


def get_season_averages(player_id, season=2024):
    data = _get("/season_averages",
                {"season": season, "player_ids[]": player_id}).get("data", [])
    return data[0] if data else {}


def get_recent_games(player_id, last_n=15, season=2024):
    data = _get("/stats", {"player_ids[]": player_id,
                           "seasons[]": season,
                           "per_page": last_n}).get("data", [])
    if not data:
        return pd.DataFrame()
    rows = []
    for g in data:
        rows.append({
            "PTS":  float(g.get("pts",  0) or 0),
            "REB":  float(g.get("reb",  0) or 0),
            "AST":  float(g.get("ast",  0) or 0),
            "STL":  float(g.get("stl",  0) or 0),
            "BLK":  float(g.get("blk",  0) or 0),
            "FG3M": float(g.get("fg3m", 0) or 0),
            "MIN":  _parse_min(g.get("min", "0")),
        })
    df = pd.DataFrame(rows)
    df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
    return df


def _parse_min(val):
    try:
        s = str(val)
        if ":" in s:
            p = s.split(":")
            return float(p[0]) + float(p[1]) / 60
        return float(s or 0)
    except Exception:
        return 0.0


def fetch_nba_stats(season=2024):
    print("▶ Fetching NBA stats via BallDontLie...")
    rows = []
    for name in TOP_20_PLAYERS:
        print(f"  {name}...")
        pid, team = get_player_id(name)
        if not pid:
            continue

        avgs     = get_season_averages(pid, season)
        game_log = get_recent_games(pid, last_n=15, season=season)

        if not avgs and game_log.empty:
            print(f"  [SKIP] {name} — no data")
            continue

        if avgs:
            pts  = float(avgs.get("pts",  0) or 0)
            reb  = float(avgs.get("reb",  0) or 0)
            ast  = float(avgs.get("ast",  0) or 0)
            stl  = float(avgs.get("stl",  0) or 0)
            blk  = float(avgs.get("blk",  0) or 0)
            fg3m = float(avgs.get("fg3m", 0) or 0)
            mins = _parse_min(avgs.get("min", 0))
        else:
            pts  = game_log["PTS"].mean()
            reb  = game_log["REB"].mean()
            ast  = game_log["AST"].mean()
            stl  = game_log["STL"].mean()
            blk  = game_log["BLK"].mean()
            fg3m = game_log["FG3M"].mean()
            mins = game_log["MIN"].mean()

        if mins == 0:
            print(f"  [SKIP] {name} — 0 minutes")
            continue

        rows.append({
            "player_name": name,
            "team":        team,
            "MIN":         round(mins, 2),
            "PTS":         round(pts,  2),
            "REB":         round(reb,  2),
            "AST":         round(ast,  2),
            "STL":         round(stl,  2),
            "BLK":         round(blk,  2),
            "FG3M":        round(fg3m, 2),
            "PRA":         round(pts + reb + ast, 2),
            "_game_log":   game_log,
        })
        print(f"  ✓ {name} ({team})")

    df = pd.DataFrame(rows)
    print(f"  Loaded {len(df)} players.")
    return df


def fetch_sportsbook_lines():
    key = API_KEYS.get("odds_api", config.THE_ODDS_API_KEY)
    if not key:
        print("  [WARN] No Odds API key set.")
        return pd.DataFrame()
    print("  [INFO] Fetching lines from The Odds API...")
    lines = get_all_prop_lines()
    if lines.empty:
        return lines
    if "player" in lines.columns and "player_name" not in lines.columns:
        lines = lines.rename(columns={"player": "player_name"})
    return lines.reset_index(drop=True)


def get_prop_lines(market):
    key = API_KEYS.get("odds_api", config.THE_ODDS_API_KEY)
    if not key:
        return []
    try:
        r = requests.get(
            f"{config.ODDS_BASE_URL}/sports/{config.ODDS_SPORT}/events",
            params={"apiKey": key}, timeout=15)
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        print(f"  [ERROR] events: {e}")
        return []
    results = []
    for event in events:
        try:
            r = requests.get(
                f"{config.ODDS_BASE_URL}/sports/{config.ODDS_SPORT}"
                f"/events/{event['id']}/odds",
                params={"apiKey": key, "regions": config.ODDS_REGION,
                        "markets": market, "oddsFormat": "american"},
                timeout=15)
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue
        for bk in data.get("bookmakers", []):
            if bk["key"] not in config.ODDS_BOOKMAKERS:
                continue
            for mkt in bk.get("markets", []):
                if mkt["key"] != market:
                    continue
                pl = {}
                for o in mkt.get("outcomes", []):
                    player = o.get("description", o.get("name", ""))
                    if player not in pl:
                        pl[player] = {"player": player, "line": o.get("point"),
                                      "over_odds": None, "under_odds": None,
                                      "bookmaker": bk["key"], "market": market}
                    if o["name"] == "Over":
                        pl[player]["over_odds"] = o["price"]
                    else:
                        pl[player]["under_odds"] = o["price"]
                results.extend(pl.values())
    return results


def get_all_prop_lines():
    all_lines = []
    for market in config.ODDS_MARKETS:
        print(f"  Fetching: {market}")
        all_lines.extend(get_prop_lines(market))
    if not all_lines:
        return pd.DataFrame()
    df = pd.DataFrame(all_lines)
    df["player"] = df["player"].str.strip()
    return df


def get_todays_games():
    try:
        today = date.today().isoformat()
        data  = _get("/games", {"dates[]": today, "per_page": 15}).get("data", [])
        games = [{"game_id":   g["id"],
                  "home_team": g["home_team"]["abbreviation"],
                  "away_team": g["visitor_team"]["abbreviation"],
                  "date":      g["date"]} for g in data]
        print(f"  Found {len(games)} game(s) today.")
        return games
    except Exception as e:
        print(f"  [ERROR] todays games: {e}")
        return []


def get_team_pace_stats(season=2024):
    return pd.DataFrame()
