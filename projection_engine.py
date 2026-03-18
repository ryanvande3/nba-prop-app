# ─────────────────────────────────────────────
#  projection_engine.py  ·  NBA Prop Research Tool
#  Projection = minutes * per_minute_rate * pace_adj * usage
# ─────────────────────────────────────────────

import numpy as np
import pandas as pd
import config


# ─────────────────────────────────────────────
#  calculate_projections  (your interface)
# ─────────────────────────────────────────────

def calculate_projections(stats_df: pd.DataFrame) -> pd.DataFrame:
    """
    Projection = minutes * per_minute_rate * pace_adj * usage

    Accepts the DataFrame from fetch_nba_stats(). Expects columns:
      MIN, PTS, REB, AST, STL, BLK, FG3M, PRA
    and optionally: pace_adj, usage_rate, _game_log (full game log)

    Returns df with added columns:
      proj_points, proj_assists, proj_rebounds, proj_pra,
      proj_steals, proj_blocks, proj_threes,
      floor_*, ceiling_* for each market.
    """
    df = stats_df.copy()

    # ── Derive per-minute rates ───────────────
    # Guard against zero minutes
    mins = df["MIN"].replace(0, np.nan)

    df["points_per_min"]   = df["PTS"]  / mins
    df["assists_per_min"]  = df["AST"]  / mins
    df["rebounds_per_min"] = df["REB"]  / mins
    df["steals_per_min"]   = df["STL"]  / mins
    df["blocks_per_min"]   = df["BLK"]  / mins
    df["threes_per_min"]   = df["FG3M"] / mins

    # ── Pace adjustment ───────────────────────
    # Use pre-computed column if present (set by build_all_projections),
    # otherwise default to 1.0 (no adjustment)
    if "pace_adj" not in df.columns:
        df["pace_adj"] = 1.0

    # ── Usage rate ────────────────────────────
    # Normalised to 1.0 = league average (~20% usage)
    # Pre-computed by build_all_projections when available
    if "usage" not in df.columns:
        df["usage"] = 1.0

    # ── Core projections ──────────────────────
    df["proj_points"]   = df["MIN"] * df["points_per_min"]   * df["pace_adj"] * df["usage"]
    df["proj_assists"]  = df["MIN"] * df["assists_per_min"]  * df["pace_adj"] * df["usage"]
    df["proj_rebounds"] = df["MIN"] * df["rebounds_per_min"] * df["pace_adj"] * df["usage"]
    df["proj_steals"]   = df["MIN"] * df["steals_per_min"]   * df["pace_adj"] * df["usage"]
    df["proj_blocks"]   = df["MIN"] * df["blocks_per_min"]   * df["pace_adj"] * df["usage"]
    df["proj_threes"]   = df["MIN"] * df["threes_per_min"]   * df["pace_adj"] * df["usage"]
    df["proj_pra"]      = df["proj_points"] + df["proj_rebounds"] + df["proj_assists"]

    # ── Back-to-back penalty ──────────────────
    if "is_b2b" in df.columns:
        penalty = 1 - config.BACK_TO_BACK_PENALTY
        for col in ["proj_points", "proj_assists", "proj_rebounds",
                    "proj_steals", "proj_blocks", "proj_threes", "proj_pra"]:
            df.loc[df["is_b2b"] == True, col] *= penalty

    # ── Floor / ceiling from game log std ────
    df = _add_floor_ceiling(df)

    # ── Round all proj columns ────────────────
    proj_cols = [c for c in df.columns if c.startswith("proj_")
                                       or c.startswith("floor_")
                                       or c.startswith("ceiling_")]
    df[proj_cols] = df[proj_cols].round(2)

    return df


# ─────────────────────────────────────────────
#  Floor / ceiling via rolling std
# ─────────────────────────────────────────────

_STAT_MAP = {
    "points":   ("proj_points",   "PTS"),
    "assists":  ("proj_assists",  "AST"),
    "rebounds": ("proj_rebounds", "REB"),
    "steals":   ("proj_steals",   "STL"),
    "blocks":   ("proj_blocks",   "BLK"),
    "threes":   ("proj_threes",   "FG3M"),
    "pra":      ("proj_pra",      "PRA"),
}


def _add_floor_ceiling(df: pd.DataFrame) -> pd.DataFrame:
    """
    For each market, compute floor = proj - 1 std, ceiling = proj + 1 std.
    Uses _game_log column if present for a real std; falls back to 15% of proj.
    """
    for market, (proj_col, stat_col) in _STAT_MAP.items():
        if proj_col not in df.columns:
            continue

        floors, ceilings = [], []
        for _, row in df.iterrows():
            proj  = row[proj_col]
            std   = _get_std(row, stat_col)
            floors.append(max(0.0, proj - std))
            ceilings.append(proj + std)

        df[f"floor_{market}"]   = floors
        df[f"ceiling_{market}"] = ceilings

    return df


def _get_std(row: pd.Series, stat_col: str,
             window: int = None) -> float:
    """
    Extract std from _game_log if available, else use 15% fallback.
    """
    if window is None:
        window = config.ROLLING_WINDOW_SHORT

    game_log = row.get("_game_log", None)
    if game_log is not None and not game_log.empty and stat_col in game_log.columns:
        recent = game_log[stat_col].head(window).dropna()
        if len(recent) >= 3:
            return float(recent.std())

    # Fallback: 15% of projection value
    proj_col = f"proj_{stat_col.lower()}"
    proj     = row.get(proj_col, row.get("proj_points", 10))
    return max(1.0, proj * 0.15)


# ─────────────────────────────────────────────
#  build_all_projections  (used by main.py)
# ─────────────────────────────────────────────

def build_all_projections(
    player_data: dict,
    games: list[dict],
    pace_stats: pd.DataFrame,
    back_to_back_teams: set = None,
) -> list[dict]:
    """
    Wraps calculate_projections() for use by main.py pipeline.
    Accepts raw player_data bundles, computes pace_adj + usage,
    then calls calculate_projections() and converts to list-of-dicts
    format expected by edge_detector.
    """
    if back_to_back_teams is None:
        back_to_back_teams = set()

    # Build matchup lookup: team → {opponent, is_home}
    matchup_map: dict[str, dict] = {}
    for g in games:
        matchup_map[g["home_team"]] = {"opponent": g["away_team"], "is_home": True}
        matchup_map[g["away_team"]] = {"opponent": g["home_team"], "is_home": False}

    league_avg_pace = (
        float(pace_stats["PACE"].mean())
        if not pace_stats.empty and "PACE" in pace_stats.columns
        else 100.0
    )

    rows = []
    for name, bundle in player_data.items():
        info     = bundle.get("info", {})
        game_log = bundle.get("game_log", pd.DataFrame())
        team     = info.get("team", "")

        if team not in matchup_map:
            continue

        matchup  = matchup_map[team]
        opp      = matchup["opponent"]
        is_home  = matchup["is_home"]

        # Season averages as base stats
        if game_log.empty or len(game_log) < config.MIN_SAMPLE_GAMES:
            continue

        avg = game_log[["MIN", "PTS", "REB", "AST",
                        "STL", "BLK", "FG3M", "PRA"]].mean()

        # Pace adjustment
        p_adj = _pace_adj(opp, pace_stats, league_avg_pace)

        # Usage proxy: normalise player's MIN vs league avg (28 mpg)
        usage = float(avg["MIN"]) / 28.0

        rows.append({
            "player_name": name,
            "team":        team,
            "opponent":    opp,
            "is_home":     is_home,
            "is_b2b":      team in back_to_back_teams,
            "pace_adj":    p_adj,
            "usage":       usage,
            "_game_log":   game_log,
            **{col: round(float(avg[col]), 3) for col in avg.index},
        })

    if not rows:
        return []

    stats_df   = pd.DataFrame(rows)
    proj_df    = calculate_projections(stats_df)

    # Convert to list-of-dicts format for edge_detector
    return _to_projection_list(proj_df)


def _pace_adj(opp: str, pace_stats: pd.DataFrame,
              league_avg: float) -> float:
    """Pace multiplier: opp_pace / league_avg."""
    if pace_stats.empty or not config.PACE_ADJUST:
        return 1.0
    row = pace_stats[pace_stats["TEAM_ABBREVIATION"] == opp]
    if row.empty:
        return 1.0
    return round(float(row["PACE"].iloc[0]) / league_avg, 4)


def _to_projection_list(proj_df: pd.DataFrame) -> list[dict]:
    """
    Convert wide projection DataFrame to list-of-dicts expected
    by edge_detector.detect_edges().
    """
    results = []
    market_map = {
        "points":   "proj_points",
        "assists":  "proj_assists",
        "rebounds": "proj_rebounds",
        "steals":   "proj_steals",
        "blocks":   "proj_blocks",
        "threes":   "proj_threes",
        "pra":      "proj_pra",
    }

    for _, row in proj_df.iterrows():
        projections = {}
        for market, proj_col in market_map.items():
            if proj_col not in proj_df.columns:
                continue
            projections[market] = {
                "projection": row[proj_col],
                "floor":      row.get(f"floor_{market}",   max(0, row[proj_col] * 0.85)),
                "ceiling":    row.get(f"ceiling_{market}", row[proj_col] * 1.15),
                "sample_n":   len(row["_game_log"]) if row.get("_game_log") is not None else 0,
            }

        results.append({
            "player":      row["player_name"],
            "team":        row.get("team", ""),
            "opponent":    row.get("opponent", ""),
            "is_home":     row.get("is_home", False),
            "projections": projections,
        })

    return results


# ─────────────────────────────────────────────
#  Utility: flat DataFrame view
# ─────────────────────────────────────────────

def projections_to_dataframe(projections: list[dict]) -> pd.DataFrame:
    """Flatten projection list into wide DataFrame for easy inspection."""
    rows = []
    for p in projections:
        base = {
            "player":   p["player"],
            "team":     p.get("team", ""),
            "opponent": p["opponent"],
            "home":     p["is_home"],
        }
        for market, stats in p.get("projections", {}).items():
            rows.append({**base, "market": market, **stats})
    return pd.DataFrame(rows) if rows else pd.DataFrame()
