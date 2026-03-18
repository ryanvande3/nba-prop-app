# ─────────────────────────────────────────────
#  edge_detector.py  ·  NBA Prop Research Tool
#  Merge projections with lines, calculate edge,
#  flag bets, assign units.
# ─────────────────────────────────────────────

import math
import pandas as pd
from config import EDGE_THRESHOLDS, UNITS
import config


# ─────────────────────────────────────────────
#  calculate_edge  (your interface)
# ─────────────────────────────────────────────

def calculate_edge(projections_df: pd.DataFrame,
                   lines_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merge projections with lines and calculate edge.

    projections_df columns (from projection_engine):
      player_name, proj_points, proj_assists, proj_rebounds, proj_pra,
      proj_steals, proj_blocks, proj_threes,
      floor_*, ceiling_*

    lines_df columns (from fetch_sportsbook_lines):
      player_name, market, line, over_odds, under_odds, bookmaker

    Returns wide DataFrame with edge_*, bet_*, units_*, prob_* columns.
    """
    # ── Pivot lines to wide format ────────────
    # so we can merge on player_name like your original
    lines_wide = _pivot_lines(lines_df)

    # ── Merge ─────────────────────────────────
    df = pd.merge(projections_df, lines_wide, on="player_name", how="inner")

    if df.empty:
        return df

    # ── Edge = projection - line ──────────────
    # (your exact formula)
    for market in ["points", "assists", "rebounds", "pra",
                   "steals", "blocks", "threes"]:
        proj_col = f"proj_{market}"
        line_col = f"line_{market}"
        edge_col = f"edge_{market}"

        if proj_col in df.columns and line_col in df.columns:
            df[edge_col] = (df[proj_col] - df[line_col]).round(2)
        else:
            df[edge_col] = None

    # ── Flag bets via EDGE_THRESHOLDS ─────────
    # (your exact logic, extended to all markets)
    for market, threshold in EDGE_THRESHOLDS.items():
        edge_col = f"edge_{market}"
        bet_col  = f"bet_{market}"
        if edge_col in df.columns:
            df[bet_col] = df[edge_col] >= threshold
        else:
            df[bet_col] = False

    # ── Assign units ──────────────────────────
    # base vs high_confidence split:
    #   high_confidence → edge >= STRONG_EDGE_PCT or edge >= 2x threshold
    #   base            → edge >= threshold (qualifies but not dominant)
    #   0               → no edge
    for market, threshold in EDGE_THRESHOLDS.items():
        edge_col  = f"edge_{market}"
        bet_col   = f"bet_{market}"
        units_col = f"units_{market}"
        if edge_col not in df.columns:
            df[units_col] = 0
            continue

        df[units_col] = df.apply(
            lambda row, ec=edge_col, bc=bet_col, th=threshold: (
                UNITS["high_confidence"] if row[bc] and row[ec] >= th * 2
                else UNITS["base"]       if row[bc]
                else 0
            ),
            axis=1,
        )

    # ── Implied probability columns ───────────
    # Strips vig so you can compare our prob vs market prob
    for market in EDGE_THRESHOLDS:
        over_col  = f"over_odds_{market}"
        under_col = f"under_odds_{market}"
        if over_col in df.columns and under_col in df.columns:
            df[[f"true_over_prob_{market}", f"true_under_prob_{market}"]] = (
                df.apply(
                    lambda row, oc=over_col, uc=under_col: pd.Series(
                        remove_vig(row.get(oc), row.get(uc))
                    ),
                    axis=1,
                )
            )

    # ── Rating label ──────────────────────────
    df["top_market"]    = _top_market(df)
    df["top_edge"]      = df.apply(_top_edge_val, axis=1)
    df["top_units"]     = df.apply(_top_units_val, axis=1)
    df["rating"]        = df["top_edge"].apply(_edge_label)

    return df


# ─────────────────────────────────────────────
#  detect_edges  (used by main.py pipeline)
# ─────────────────────────────────────────────

def detect_edges(projections: list[dict],
                 lines_df: pd.DataFrame) -> pd.DataFrame:
    """
    Adapter: converts projection list-of-dicts from projection_engine
    into the wide DataFrame format, then calls calculate_edge().
    Returns rows only where at least one bet_* flag is True.
    """
    if not projections or lines_df.empty:
        return pd.DataFrame()

    # Flatten projections list → wide DataFrame
    rows = []
    for p in projections:
        row = {
            "player_name": p["player"],
            "team":        p.get("team", ""),
            "opponent":    p.get("opponent", ""),
            "is_home":     p.get("is_home", False),
        }
        for market, stats in p.get("projections", {}).items():
            row[f"proj_{market}"]    = stats.get("projection")
            row[f"floor_{market}"]   = stats.get("floor")
            row[f"ceiling_{market}"] = stats.get("ceiling")
        rows.append(row)

    proj_df = pd.DataFrame(rows)
    result  = calculate_edge(proj_df, lines_df)

    if result.empty:
        return result

    # Filter to rows with at least one active bet flag
    bet_cols = [c for c in result.columns if c.startswith("bet_")]
    flagged  = result[result[bet_cols].any(axis=1)].copy()
    return flagged.sort_values("top_edge", ascending=False).reset_index(drop=True)


# ─────────────────────────────────────────────
#  Odds / probability utilities
# ─────────────────────────────────────────────

def american_to_implied_prob(odds) -> float:
    """Convert American odds to raw implied probability."""
    try:
        odds = float(odds)
    except (TypeError, ValueError):
        return 0.5
    if odds < 0:
        return abs(odds) / (abs(odds) + 100)
    return 100 / (odds + 100)


def remove_vig(over_odds, under_odds) -> tuple[float, float]:
    """Strip bookmaker vig. Returns (true_over_prob, true_under_prob)."""
    raw_over  = american_to_implied_prob(over_odds)
    raw_under = american_to_implied_prob(under_odds)
    total     = raw_over + raw_under
    if total == 0:
        return 0.5, 0.5
    return round(raw_over / total, 4), round(raw_under / total, 4)


def get_best_lines(lines_df: pd.DataFrame) -> pd.DataFrame:
    """Return best Over and Under odds per player/market across all books."""
    if lines_df.empty:
        return pd.DataFrame()
    rows = []
    for (player, market), grp in lines_df.groupby(["player_name", "market"]):
        best_over  = grp.loc[grp["over_odds"].idxmax()]  if grp["over_odds"].notna().any()  else None
        best_under = grp.loc[grp["under_odds"].idxmax()] if grp["under_odds"].notna().any() else None
        rows.append({
            "player_name":     player,
            "market":          market,
            "line":            grp["line"].iloc[0],
            "best_over_odds":  best_over["over_odds"]   if best_over  is not None else None,
            "best_over_book":  best_over["bookmaker"]   if best_over  is not None else None,
            "best_under_odds": best_under["under_odds"] if best_under is not None else None,
            "best_under_book": best_under["bookmaker"]  if best_under is not None else None,
        })
    return pd.DataFrame(rows)


# ─────────────────────────────────────────────
#  Internal helpers
# ─────────────────────────────────────────────

def _pivot_lines(lines_df: pd.DataFrame) -> pd.DataFrame:
    """
    Pivot long lines DataFrame to wide format so it can merge
    on player_name with the projections DataFrame.

    Input:  player_name | market | line | over_odds | under_odds | bookmaker
    Output: player_name | line_points | over_odds_points | under_odds_points | ...
    """
    if lines_df.empty:
        return pd.DataFrame()

    # Normalise player_name column
    df = lines_df.copy()
    if "player" in df.columns and "player_name" not in df.columns:
        df = df.rename(columns={"player": "player_name"})

    # Map odds_key → market name
    odds_key_map = {v["odds_key"]: k for k, v in config.PROP_MARKETS.items()}
    if "market" in df.columns:
        df["market_name"] = df["market"].map(odds_key_map).fillna(df["market"])
    else:
        return pd.DataFrame()

    pivot_rows: dict[str, dict] = {}
    for _, row in df.iterrows():
        player = str(row["player_name"]).strip()
        market = row["market_name"]
        if player not in pivot_rows:
            pivot_rows[player] = {"player_name": player}
        pivot_rows[player][f"line_{market}"]       = row.get("line")
        pivot_rows[player][f"over_odds_{market}"]  = row.get("over_odds")
        pivot_rows[player][f"under_odds_{market}"] = row.get("under_odds")
        pivot_rows[player][f"bookmaker_{market}"]  = row.get("bookmaker")

    return pd.DataFrame(list(pivot_rows.values()))


def _top_market(df: pd.DataFrame) -> pd.Series:
    """Return the market with the highest edge for each row."""
    markets = list(EDGE_THRESHOLDS.keys())
    def _best(row):
        best_m, best_e = "points", -999
        for m in markets:
            e = row.get(f"edge_{m}", None)
            if e is not None and e > best_e:
                best_e, best_m = e, m
        return best_m
    return df.apply(_best, axis=1)


def _top_edge_val(row) -> float:
    edge_col = f"edge_{row.get('top_market', 'points')}"
    return row.get(edge_col, 0) or 0


def _top_units_val(row) -> int:
    units_col = f"units_{row.get('top_market', 'points')}"
    return row.get(units_col, 0) or 0


def _edge_label(edge_val: float) -> str:
    if edge_val >= config.STRONG_EDGE_PCT:
        return "🔥 STRONG"
    elif edge_val >= config.MIN_EDGE_PCT:
        return "✅ EDGE"
    elif edge_val > 0:
        return "👀 WATCH"
    return "— No edge"
