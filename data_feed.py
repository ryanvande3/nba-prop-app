# ─────────────────────────────────────────────
#  data_feed.py  ·  NBA Prop Research Tool
#  Pulls: player game logs, team pace stats,
#         today's schedule, and prop lines.
# ─────────────────────────────────────────────

import time
import requests
import pandas as pd
from datetime import date
from nba_api.stats.endpoints import (
    playergamelog,
    leaguegamefinder,
    leaguedashteamstats,
    commonplayerinfo,
    commonallplayers,
)
from nba_api.stats.static import players
from config import API_KEYS, TOP_20_PLAYERS
import config

# Alias so all other modules can reference either name
TOP_PLAYERS = TOP_20_PLAYERS

# nba_api rate-limit guard
NBA_REQUEST_DELAY = 0.7   # seconds between calls


# ─────────────────────────────────────────────
#  fetch_nba_stats  (your interface, real engine)
# ─────────────────────────────────────────────

def fetch_nba_stats(season: str = "2024-25") -> pd.DataFrame:
    """
    Pulls current player stats (minutes, points, assists, rebounds)
    using nba_api. Returns a DataFrame filtered to TOP_20_PLAYERS
    with columns: player_name, team, position, MIN, PTS, REB, AST,
                  STL, BLK, FG3M, PRA.
    """
    print("▶ Fetching NBA player stats via nba_api...")
    rows = []
    for name in TOP_20_PLAYERS:
        bundle = get_player_stats_bundle(name, season=season)
        if not bundle:
            continue
        info = bundle["info"]
        log  = bundle["game_log"]
        if log.empty:
            continue
        # Season averages from the full log
        avg = log[["MIN", "PTS", "REB", "AST", "STL", "BLK", "FG3M", "PRA"]].mean()
        rows.append({
            "player_name": name,
            "team":        info.get("team", ""),
            "position":    info.get("position", ""),
            **{col: round(avg[col], 2) for col in avg.index},
            "_game_log":   log,   # stash full log for projection engine
        })

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df[df["player_name"].isin(TOP_20_PLAYERS)]
    return df


# ─────────────────────────────────────────────
#  fetch_sportsbook_lines  (your interface)
# ─────────────────────────────────────────────

def fetch_sportsbook_lines() -> pd.DataFrame:
    """
    Pulls current prop lines. Tries Fanatics first (via API_KEYS["sportsbook"]),
    falls back to The Odds API if Fanatics key is a placeholder.
    Returns DataFrame filtered to TOP_20_PLAYERS with columns:
      player_name, market, line, over_odds, under_odds, bookmaker.
    """
    fanatics_key = API_KEYS.get("sportsbook", "")
    if fanatics_key and fanatics_key != "YOUR_FANATICS_API_KEY":
        lines = _fetch_fanatics_lines(fanatics_key)
    else:
        print("  [INFO] Fanatics key not set — using The Odds API fallback.")
        lines = get_all_prop_lines()

    if lines.empty:
        return lines

    # Normalise column name to match your interface
    if "player" in lines.columns and "player_name" not in lines.columns:
        lines = lines.rename(columns={"player": "player_name"})

    lines = lines[lines["player_name"].isin(TOP_20_PLAYERS)]
    return lines.reset_index(drop=True)


def _fetch_fanatics_lines(api_key: str) -> pd.DataFrame:
    """
    Fetch prop lines from Fanatics Sportsbook API.
    NOTE: Fanatics does not yet have a documented public props endpoint.
    This will be updated when they release one. Currently returns empty
    and falls back to The Odds API automatically.
    """
    url = "https://api.fanatics.com/sportsbook/lines/nba"
    headers = {"X-API-KEY": api_key}
    try:
        response = requests.get(url, headers=headers, timeout=15)
        response.raise_for_status()
        data = response.json()
        df = pd.DataFrame(data)
        if not df.empty and "player_name" not in df.columns:
            # Map whatever Fanatics returns to our standard schema
            df = df.rename(columns={
                "name":        "player_name",
                "prop_type":   "market",
                "prop_value":  "line",
            })
        return df
    except Exception as e:
        print(f"  [WARN] Fanatics API unavailable ({e}). Falling back to The Odds API.")
        return pd.DataFrame()


# ─────────────────────────────────────────────
#  Player lookup helpers
# ─────────────────────────────────────────────

def get_player_id(full_name: str) -> int | None:
    """Return nba_api player ID for a given full name."""
    matches = players.find_players_by_full_name(full_name)
    if not matches:
        print(f"  [WARN] Player not found: {full_name}")
        return None
    active = [p for p in matches if p["is_active"]]
    return (active or matches)[0]["id"]


# ─────────────────────────────────────────────
#  Player game logs
# ─────────────────────────────────────────────

def get_player_game_log(player_id: int, season: str = "2024-25",
                        last_n: int = 25) -> pd.DataFrame:
    """Fetch recent game log for a player. Returns cleaned DataFrame."""
    time.sleep(NBA_REQUEST_DELAY)
    try:
        log = playergamelog.PlayerGameLog(
            player_id=player_id,
            season=season,
            season_type_all_star="Regular Season",
            timeout=30,
        )
        df = log.get_data_frames()[0].head(last_n)
        df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"])
        df["PRA"]  = df["PTS"] + df["REB"] + df["AST"]
        df["HOME"] = df["MATCHUP"].apply(lambda x: 1 if "vs." in x else 0)
        df["OPP"]  = df["MATCHUP"].apply(
            lambda x: x.split("vs. ")[-1] if "vs." in x else x.split("@ ")[-1]
        )
        keep = ["GAME_DATE", "MATCHUP", "HOME", "OPP", "WL", "MIN",
                "PTS", "REB", "AST", "STL", "BLK", "FG3M", "PRA"]
        return df[keep]
    except Exception as e:
        print(f"  [ERROR] game log for player_id={player_id}: {e}")
        return pd.DataFrame()


def get_player_stats_bundle(player_name: str, season: str = "2024-25") -> dict:
    """
    Return dict: { info: {id, name, team, position}, game_log: DataFrame }
    """
    pid = get_player_id(player_name)
    if pid is None:
        return {}

    time.sleep(NBA_REQUEST_DELAY)
    try:
        info_ep = commonplayerinfo.CommonPlayerInfo(player_id=pid, timeout=30)
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

    game_log = get_player_game_log(pid, season=season)
    return {"info": info, "game_log": game_log}


# ─────────────────────────────────────────────
#  Team pace / defense stats
# ─────────────────────────────────────────────

def get_team_pace_stats(season: str = "2024-25") -> pd.DataFrame:
    """Fetch league-wide team pace and defensive rating."""
    time.sleep(NBA_REQUEST_DELAY)
    try:
        stats = leaguedashteamstats.LeagueDashTeamStats(
            season=season,
            measure_type_simple="Advanced",
            timeout=30,
        )
        df = stats.get_data_frames()[0]
        keep = ["TEAM_ABBREVIATION", "TEAM_NAME", "PACE", "DEF_RATING",
                "OPP_PTS_PAINT", "OPP_PTS_FB", "OPP_PTS_2ND_CHANCE"]
        return df[keep]
    except Exception as e:
        print(f"  [ERROR] team pace stats: {e}")
        return pd.DataFrame()


# ─────────────────────────────────────────────
#  Today's schedule
# ─────────────────────────────────────────────

def get_todays_games() -> list[dict]:
    """Return list of today's NBA games as dicts."""
    today_str = date.today().strftime("%m/%d/%Y")
    time.sleep(NBA_REQUEST_DELAY)
    try:
        finder = leaguegamefinder.LeagueGameFinder(
            date_from_nullable=today_str,
            date_to_nullable=today_str,
            league_id_nullable="00",
            timeout=30,
        )
        df = finder.get_data_frames()[0]
        if df.empty:
            print("  [INFO] No games found for today.")
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
            games.append({
                "game_id":   gid,
                "home_team": home,
                "away_team": away,
                "date":      row["GAME_DATE"],
            })
        return games
    except Exception as e:
        print(f"  [ERROR] today's schedule: {e}")
        return []


# ─────────────────────────────────────────────
#  The Odds API — prop lines (fallback)
# ─────────────────────────────────────────────

def get_prop_lines(market: str) -> list[dict]:
    """Fetch player prop lines for a market from The Odds API."""
    key = API_KEYS.get("odds_api", config.THE_ODDS_API_KEY)
    if not key or key == "YOUR_ODDS_API_KEY_HERE":
        print("  [WARN] Odds API key not set — skipping lines.")
        return []

    events_url = f"{config.ODDS_BASE_URL}/sports/{config.ODDS_SPORT}/events"
    try:
        r = requests.get(events_url, params={"apiKey": key}, timeout=15)
        r.raise_for_status()
        events = r.json()
    except Exception as e:
        print(f"  [ERROR] fetching events: {e}")
        return []

    results = []
    odds_url = (
        f"{config.ODDS_BASE_URL}/sports/{config.ODDS_SPORT}/events/{{event_id}}/odds"
    )
    for event in events:
        event_id = event["id"]
        try:
            r = requests.get(
                odds_url.format(event_id=event_id),
                params={
                    "apiKey":     key,
                    "regions":    config.ODDS_REGION,
                    "markets":    market,
                    "oddsFormat": "american",
                },
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            print(f"  [ERROR] odds for event {event_id}: {e}")
            continue

        for bookmaker in data.get("bookmakers", []):
            bk_name = bookmaker["key"]
            if bk_name not in config.ODDS_BOOKMAKERS:
                continue
            for mkt in bookmaker.get("markets", []):
                if mkt["key"] != market:
                    continue
                player_lines: dict[str, dict] = {}
                for outcome in mkt.get("outcomes", []):
                    player  = outcome.get("description", outcome.get("name", ""))
                    side    = outcome["name"]
                    price   = outcome["price"]
                    point   = outcome.get("point", None)
                    if player not in player_lines:
                        player_lines[player] = {
                            "player":     player,
                            "line":       point,
                            "over_odds":  None,
                            "under_odds": None,
                            "bookmaker":  bk_name,
                            "market":     market,
                            "event_id":   event_id,
                        }
                    if side == "Over":
                        player_lines[player]["over_odds"] = price
                    else:
                        player_lines[player]["under_odds"] = price
                results.extend(player_lines.values())

    return results


def get_all_prop_lines() -> pd.DataFrame:
    """Pull prop lines for all configured markets. Returns unified DataFrame."""
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


# ─────────────────────────────────────────────
#  Master loader
# ─────────────────────────────────────────────

def load_todays_data(player_list: list[str] = None, season: str = "2024-25") -> dict:
    """
    Master loader used by main.py. Returns:
      { games, pace_stats, lines, players }
    """
    if player_list is None:
        player_list = TOP_20_PLAYERS

    print("▶ Loading today's schedule...")
    games = get_todays_games()
    print(f"  Found {len(games)} game(s).")

    print("▶ Loading team pace/defense stats...")
    pace_stats = get_team_pace_stats(season)

    print("▶ Loading prop lines...")
    lines = fetch_sportsbook_lines()
    # Rename back to 'player' for edge_detector compatibility
    if "player_name" in lines.columns:
        lines = lines.rename(columns={"player_name": "player"})
    print(f"  Found {len(lines)} prop lines.")

    print("▶ Loading player game logs...")
    player_data = {}
    for name in player_list:
        print(f"  {name}...")
        bundle = get_player_stats_bundle(name, season)
        if bundle:
            player_data[name] = bundle

    return {
        "games":      games,
        "pace_stats": pace_stats,
        "lines":      lines,
        "players":    player_data,
    }
