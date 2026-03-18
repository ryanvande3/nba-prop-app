# ─────────────────────────────────────────────
#  config.py  ·  NBA Prop Research Tool
# ─────────────────────────────────────────────

import os
from dotenv import load_dotenv

load_dotenv()   # loads .env file automatically

# ── API Keys (from .env) ──────────────────────
API_KEYS = {
    "nba_stats":  os.getenv("NBA_STATS_API_KEY",  ""),
    "sportsbook": os.getenv("SPORTSBOOK_API_KEY", ""),
    "odds_api":   os.getenv("ODDS_API_KEY",       ""),
}

THE_ODDS_API_KEY = API_KEYS["odds_api"]   # convenience alias

# ── Database ──────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///nba_props.db")
# SQLite fallback means zero setup required — swap for Postgres in .env

# ── Google Sheets ─────────────────────────────
GOOGLE_SHEETS_CREDS_JSON = os.getenv("GOOGLE_SHEETS_CREDS_JSON", "")
EDGE_BOARD_SHEET_ID      = os.getenv("EDGE_BOARD_SHEET_ID",      "")

# ── Flask ─────────────────────────────────────
FLASK_SECRET_KEY = os.getenv("FLASK_SECRET_KEY", "dev-secret-change-me")
FLASK_PORT       = int(os.getenv("FLASK_PORT", 5000))

# ── The Odds API Settings ─────────────────────
ODDS_BASE_URL  = "https://api.the-odds-api.com/v4"
ODDS_SPORT     = "basketball_nba"
ODDS_REGION    = "us"
ODDS_MARKETS   = [
    "player_points",
    "player_rebounds",
    "player_assists",
    "player_threes",
    "player_blocks",
    "player_steals",
    "player_points_rebounds_assists",
]
ODDS_BOOKMAKERS = ["draftkings", "fanduel", "betmgm", "prizepicks"]

# ── Edge Thresholds (stat units) ──────────────
EDGE_THRESHOLDS = {
    "points":   3,
    "assists":  2,
    "rebounds": 2,
    "pra":      4,
    "threes":   1,
    "blocks":   1,
    "steals":   1,
}

MIN_EDGE_PCT         = 5.0
STRONG_EDGE_PCT      = 10.0
SHARP_MOVEMENT_UNITS = 1.5
MIN_SAMPLE_GAMES     = 10

# ── Unit Sizing ───────────────────────────────
UNITS = {
    "base":            1,
    "high_confidence": 2,
}

# ── Projection Settings ───────────────────────
ROLLING_WINDOW_SHORT = 5
ROLLING_WINDOW_LONG  = 15
HOME_AWAY_SPLIT      = True
PACE_ADJUST          = True
BACK_TO_BACK_PENALTY = 0.06

# ── Prop Markets ──────────────────────────────
PROP_MARKETS = {
    "points":   {"label": "PTS", "odds_key": "player_points"},
    "rebounds": {"label": "REB", "odds_key": "player_rebounds"},
    "assists":  {"label": "AST", "odds_key": "player_assists"},
    "threes":   {"label": "3PM", "odds_key": "player_threes"},
    "blocks":   {"label": "BLK", "odds_key": "player_blocks"},
    "steals":   {"label": "STL", "odds_key": "player_steals"},
    "pra":      {"label": "PRA", "odds_key": "player_points_rebounds_assists"},
}

# ── Top 20 Players ────────────────────────────
TOP_PLAYERS = TOP_20_PLAYERS = [
    "Nikola Jokic",
    "Tyrese Haliburton",
    "Domantas Sabonis",
    "Victor Wembanyama",
    "Jayson Tatum",
    "LeBron James",
    "Luka Doncic",
    "Joel Embiid",
    "Jalen Brunson",
    "Devin Booker",
    "Anthony Edwards",
    "Shai Gilgeous-Alexander",
    "Kevin Durant",
    "Bam Adebayo",
    "Pascal Siakam",
    "Jrue Holiday",
    "Ja Morant",
    "DeAaron Fox",
    "Cade Cunningham",
    "Julius Randle",
]

# ── Logging (CSV fallback) ────────────────────
LOG_DIR         = "logs"
BET_LOG_FILE    = "logs/bet_tracker.csv"
EDGE_BOARD_FILE = "logs/daily_edge_board.csv"
