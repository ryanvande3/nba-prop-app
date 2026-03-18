# ─────────────────────────────────────────────
#  data_feed.py  ·  uses BallDontLie API
#  Free, no API key, works from cloud servers
#  https://www.balldontlie.io
# ─────────────────────────────────────────────

import requests
import pandas as pd
from datetime import date
from config import API_KEYS, TOP_20_PLAYERS
import config

TOP_PLAYERS = TOP_20_PLAYERS
BASE_URL    = "https://api.balldontlie.io/v1"


# ─────────────────────────────────────────────
#  Player lookup
# ─────────────────────────────────────────────

def _get_headers():
    key = API_KEYS.get("balldontlie", "")
    return {"Authorization": key} if key else {}


def get_player_id(full_name):
    try:
        r = requests.get(f"{BASE_URL}/players",
                         params={"search": full_name, "per_page": 5},
                         headers=_get_headers(), timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            print(f"  [WARN] Not found: {full_name}")
            return None
        # Prefer exact match
        for p in data:
            if f"{p['first_name']} {p['last_name']}".lower() == full_name.lower():
                return p["id"]
        return data[0]["id"]
    except Exception as e:
        print(f"  [ERROR] get_player_id {full_name}: {e}")
        return None


def get_player_team(full_name):
    try:
        r = requests.get(f"{BASE_URL}/players",
                         params={"search": full_name, "per_page": 5},
                         headers=_get_headers(), timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            return "N/A"
        for p in data:
            if f"{p['first_name']} {p['last_name']}".lower() == full_name.lower():
                return p.get("team", {}).get("abbreviation", "N/A")
        return data[0].get("team", {}).get("abbreviation", "N/A")
    except Exception as e:
        print(f"  [ERROR] get_player_team {full_name}: {e}")
        return "N/A"


# ─────────────────────────────────────────────
#  Season averages
# ─────────────────────────────────────────────

def get_season_averages(player_id, season=2024):
    try:
        r = requests.get(f"{BASE_URL}/season_averages",
                         params={"season": season, "player_ids[]": player_id},
                         headers=_get_headers(), timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        return data[0] if data else {}
    except Exception as e:
        print(f"  [ERROR] season_averages {player_id}: {e}")
        return {}


def get_recent_games(player_id, last_n=15, season=2024):
    try:
        r = requests.get(f"{BASE_URL}/stats",
                         params={"player_ids[]": player_id,
                                 "seasons[]": season,
                                 "per_page": last_n},
                         headers=_get_headers(), timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        if not data:
            return pd.DataFrame()
        rows = []
        for g in data:
            rows.append({
                "PTS":  g.get("pts", 0) or 0,
                "REB":  g.get("reb", 0) or 0,
                "AST":  g.get("ast", 0) or 0,
                "STL":  g.get("stl", 0) or 0,
                "BLK":  g.get("blk", 0) or 0,
                "FG3M": g.get("fg3m", 0) or 0,
                "MIN":  _parse_min(g.get("min", "0")),
            })
        df = pd.DataFrame(rows)
        df["PRA"] = df["PTS"] + df["REB"] + df["AST"]
        return df
    except Exception as e:
        print(f"  [ERROR] recent_games {player_id}: {e}")
        return pd.DataFrame()


def _parse_min(min_str):
    try:
        if ":" in str(min_str):
            parts = str(min_str).split(":")
            return float(parts[0]) + float(parts[1]) / 60
        return float(min_str or 0)
    except Exception:
        return 0.0


# ─────────────────────────────────────────────
#  fetch_nba_stats  (main interface)
# ─────────────────────────────────────────────

def fetch_nba_stats(season=2024):
    print("▶ Fetching NBA player stats via BallDontLie...")
    rows = []
    for name in TOP_20_PLAYERS:
        print(f"  {name}...")
        pid = get_player_id(name)
        if not pid:
            continue

        avgs     = get_season_averages(pid, season)
        game_log = get_recent_games(pid, last_n=15, season=season)
        team     = get_player_team(name)

        if not avgs and game_log.empty:
            print(f"  [SKIP] {name} — no data")
            continue

        # Use season averages if available, else compute from game log
        if avgs:
            pts  = avgs.get("pts",  0) or 0
            reb  = avgs.get("reb",  0) or 0
            ast  = avgs.get("ast",  0) or 0
            stl  = avgs.get("stl",  0) or 0
            blk  = avgs.get("blk",  0) or 0
            fg3m = avgs.get("fg3m", 0) or 0
            mins = _parse_min(avgs.get("min", 0))
        elif not game_log.empty:
            pts  = game_log["PTS"].mean()
            reb  = game_log["REB"].mean()
            ast  = game_log["AST"].mean()
            stl  = game_log["STL"].mean()
            blk  = game_log["BLK"].mean()
            fg3m = game_log["FG3M"].mean()
            mins = game_log["MIN"].mean()
        else:
            continue

        rows.append({
            "player_name": name,
            "team":        team,
            "position":    avgs.get("position", ""),
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
        print(f"  ✓ {name} ({team}) — {pts:.1f}pts {reb:.1f}reb {ast:.1f}ast")

    df = pd.DataFrame(rows)
    print(f"  Loaded {len(df)} players.")
    return df


# ─────────────────────────────────────────────
#  fetch_sportsbook_lines
# ─────────────────────────────────────────────

def fetch_sportsbook_lines():
    fanatics_key = API_KEYS.get("sportsbook", "")
    if fanatics_key and fanatics_key != "YOUR_FANATICS_API_KEY":
        lines = _fetch_fanatics_lines(fanatics_key)
    else:
        print("  [INFO] Using The Odds API for lines.")
        lines = get_all_prop_lines()
    if lines.empty:
        return lines
    if "player" in lines.columns and "player_name" not in lines.columns:
        lines = lines.rename(columns={"player": "player_name"})
    lines = lines[lines["player_name"].isin(TOP_20_PLAYERS)]
    return lines.reset_index(drop=True)


def _fetch_fanatics_lines(api_key):
    try:
        r = requests.get("https://api.fanatics.com/sportsbook/lines/nba",
                         headers={"X-API-KEY": api_key}, timeout=15)
        r.raise_for_status()
        return pd.DataFrame(r.json())
    except Exception as e:
        print(f"  [WARN] Fanatics unavailable ({e}). Falling back.")
        return pd.DataFrame()


def get_prop_lines(market):
    key = API_KEYS.get("odds_api", config.THE_ODDS_API_KEY)
    if not key:
        print("  [WARN] Odds API key not set.")
        return []
    events_url = f"{config.ODDS_BASE_URL}/sports/{config.ODDS_SPORT}/events"
    try:
        r = requests.get(events_url, params={"apiKey": key}, timeout=15)
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        print(f"  [ERROR] events: {e}")
        return []
    results = []
    for event in events:
        event_id = event["id"]
        try:
            r = requests.get(
                f"{config.ODDS_BASE_URL}/sports/{config.ODDS_SPORT}/events/{event_id}/odds",
                params={"apiKey": key, "regions": config.ODDS_REGION,
                        "markets": market, "oddsFormat": "american"},
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception:
            continue
        for bookmaker in data.get("bookmakers", []):
            bk_name = bookmaker["key"]
            if bk_name not in config.ODDS_BOOKMAKERS:
                continue
            for mkt in bookmaker.get("markets", []):
                if mkt["key"] != market:
                    continue
                player_lines = {}
                for outcome in mkt.get("outcomes", []):
                    player = outcome.get("description", outcome.get("name", ""))
                    side   = outcome["name"]
                    price  = outcome["price"]
                    point  = outcome.get("point")
                    if player not in player_lines:
                        player_lines[player] = {
                            "player": player, "line": point,
                            "over_odds": None, "under_odds": None,
                            "bookmaker": bk_name, "market": market,
                        }
                    if side == "Over":
                        player_lines[player]["over_odds"] = price
                    else:
                        player_lines[player]["under_odds"] = price
                results.extend(player_lines.values())
    return results


def get_all_prop_lines():
    all_lines = []
    for market in config.ODDS_MARKETS:
        print(f"  Fetching lines: {market}")
        all_lines.extend(get_prop_lines(market))
    if not all_lines:
        return pd.DataFrame()
    df = pd.DataFrame(all_lines)
    df["player"] = df["player"].str.strip()
    return df


def get_todays_games():
    try:
        today = date.today().isoformat()
        r = requests.get(f"{BASE_URL}/games",
                         params={"dates[]": today, "per_page": 15},
                         headers=_get_headers(), timeout=10)
        r.raise_for_status()
        data = r.json().get("data", [])
        games = []
        for g in data:
            games.append({
                "game_id":   g["id"],
                "home_team": g["home_team"]["abbreviation"],
                "away_team": g["visitor_team"]["abbreviation"],
                "date":      g["date"],
            })
        print(f"  Found {len(games)} game(s) today.")
        return games
    except Exception as e:
        print(f"  [ERROR] todays games: {e}")
        return []


def get_team_pace_stats(season=2024):
    # BallDontLie doesn't have pace stats
    # Return empty — projection engine defaults to 1.0 multiplier
    return pd.DataFrame()
