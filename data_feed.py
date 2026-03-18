import time
import requests
import pandas as pd
from datetime import date
from nba_api.stats.endpoints import (
    playergamelog,
    leaguegamefinder,
    leaguedashteamstats,
    commonplayerinfo,
)
from nba_api.stats.static import players
from config import API_KEYS, TOP_20_PLAYERS
import config

TOP_PLAYERS = TOP_20_PLAYERS
NBA_REQUEST_DELAY = 1.0   # increased delay for cloud servers
NBA_TIMEOUT       = 10    # seconds before giving up on a request


def fetch_nba_stats(season="2024-25"):
    print("▶ Fetching NBA player stats...")
    rows = []
    for name in TOP_20_PLAYERS:
        print(f"  Fetching: {name}")
        bundle = get_player_stats_bundle(name, season=season)
        if not bundle:
            print(f"  [SKIP] {name}")
            continue
        info     = bundle["info"]
        game_log = bundle["game_log"]
        if game_log.empty:
            print(f"  [SKIP] {name} — empty log")
            continue
        avg = game_log[["MIN","PTS","REB","AST","STL","BLK","FG3M","PRA"]].mean()
        rows.append({
            "player_name": name,
            "team":        info.get("team", ""),
            "position":    info.get("position", ""),
            "_game_log":   game_log,
            **{col: round(avg[col], 2) for col in avg.index},
        })
        print(f"  ✓ {name}")

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["player_name"].isin(TOP_20_PLAYERS)]
    print(f"  Loaded {len(df)} players.")
    return df


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
    url = "https://api.fanatics.com/sportsbook/lines/nba"
    try:
        r = requests.get(url, headers={"X-API-KEY": api_key}, timeout=15)
        r.raise_for_status()
        return pd.DataFrame(r.json())
    except Exception as e:
        print(f"  [WARN] Fanatics unavailable ({e}). Falling back to Odds API.")
        return pd.DataFrame()


def get_player_id(full_name):
    try:
        matches = players.find_players_by_full_name(full_name)
        if not matches:
            print(f"  [WARN] Player not found: {full_name}")
            return None
        active = [p for p in matches if p["is_active"]]
        return (active or matches)[0]["id"]
    except Exception as e:
        print(f"  [ERROR] get_player_id {full_name}: {e}")
        return None


def get_player_game_log(player_id, season="2024-25", last_n=25):
    time.sleep(NBA_REQUEST_DELAY)
    try:
        log = playergamelog.PlayerGameLog(
            player_id=player_id, season=season,
            season_type_all_star="Regular Season",
            timeout=NBA_TIMEOUT,
        )
        df = log.get_data_frames()[0].head(last_n)
        if df.empty:
            return pd.DataFrame()
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
        df["PRA"]  = df["PTS"] + df["REB"] + df["AST"]
        df["HOME"] = df["MATCHUP"].apply(lambda x: 1 if "vs." in x else 0)
        df["OPP"]  = df["MATCHUP"].apply(
            lambda x: x.split("vs. ")[-1] if "vs." in x else x.split("@ ")[-1]
        )
        keep = ["GAME_DATE","MATCHUP","HOME","OPP","WL","MIN",
                "PTS","REB","AST","STL","BLK","FG3M","PRA"]
        return df[keep]
    except Exception as e:
        print(f"  [ERROR] game log for {player_id}: {e}")
        return pd.DataFrame()


def get_player_stats_bundle(player_name, season="2024-25"):
    pid = get_player_id(player_name)
    if pid is None:
        return {}
    time.sleep(NBA_REQUEST_DELAY)
    try:
        info_ep = commonplayerinfo.CommonPlayerInfo(
            player_id=pid, timeout=NBA_TIMEOUT)
        info_df = info_ep.get_data_frames()[0]
        info = {
            "id":       pid,
            "name":     player_name,
            "team":     info_df["TEAM_ABBREVIATION"].iloc[0],
            "position": info_df["POSITION"].iloc[0],
        }
    except Exception as e:
        print(f"  [ERROR] player info for {player_name}: {e}")
        info = {"id": pid, "name": player_name, "team": "N/A", "position": "N/A"}
    return {"info": info, "game_log": get_player_game_log(pid, season=season)}


def get_team_pace_stats(season="2024-25"):
    time.sleep(NBA_REQUEST_DELAY)
    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            season=season, measure_type_simple="Advanced",
            timeout=NBA_TIMEOUT,
        )
        df = stats.get_data_frames()[0]
        return df[["TEAM_ABBREVIATION","TEAM_NAME","PACE","DEF_RATING",
                   "OPP_PTS_PAINT","OPP_PTS_FB","OPP_PTS_2ND_CHANCE"]]
    except Exception as e:
        print(f"  [ERROR] pace stats: {e}")
        return pd.DataFrame()


def get_todays_games():
    today_str = date.today().strftime("%m/%d/%Y")
    time.sleep(NBA_REQUEST_DELAY)
    try:
        finder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=today_str, date_to_nullable=today_str,
            league_id_nullable="00", timeout=NBA_TIMEOUT,
        )
        df = finder.get_data_frames()[0]
        if df.empty:
            return []
        games, seen = [], set()
        for _, row in df.iterrows():
            gid = row["GAME_ID"]
            if gid in seen:
                continue
            seen.add(gid)
            matchup = row["MATCHUP"]
            if "vs." in matchup:
                home = row["TEAM_ABBREVIATION"]
                away = matchup.split("vs. ")[-1].strip()
            else:
                away = row["TEAM_ABBREVIATION"]
                home = matchup.split("@ ")[-1].strip()
            games.append({"game_id": gid, "home_team": home,
                          "away_team": away, "date": row["GAME_DATE"]})
        return games
    except Exception as e:
        print(f"  [ERROR] schedule: {e}")
        return []


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
        time.sleep(0.3)
    if not all_lines:
        return pd.DataFrame()
    df = pd.DataFrame(all_lines)
    df["player"] = df["player"].str.strip()
    return df


def load_todays_data(player_list=None, season="2024-25"):
    if player_list is None:
        player_list = TOP_20_PLAYERS
    print("▶ Loading schedule...")
    games = get_todays_games()
    print(f"  Found {len(games)} game(s).")
    print("▶ Loading pace stats...")
    pace_stats = get_team_pace_stats(season)
    print("▶ Loading lines...")
    lines = fetch_sportsbook_lines()
    if "player_name" in lines.columns:
        lines = lines.rename(columns={"player_name": "player"})
    print("▶ Loading player logs...")
    player_data = {}
    for name in player_list:
        bundle = get_player_stats_bundle(name, season)
        if bundle:
            player_data[name] = bundle
    return {"games": games, "pace_stats": pace_stats,
            "lines": lines, "players": player_data}
