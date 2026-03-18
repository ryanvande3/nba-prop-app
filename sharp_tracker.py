# ─────────────────────────────────────────────
#  sharp_tracker.py  ·  NBA Prop Research Tool
#  Detects sharp line movement and SGP
#  correlations.
# ─────────────────────────────────────────────

import json
from datetime import date
from pathlib import Path

import pandas as pd
import config

SNAPSHOT_DIR  = Path(config.LOG_DIR) / "line_snapshots"
SNAPSHOT_FILE = SNAPSHOT_DIR / f"lines_{date.today().isoformat()}.json"


# ─────────────────────────────────────────────
#  Snapshot helpers
# ─────────────────────────────────────────────

def save_line_snapshot(lines_df: pd.DataFrame) -> None:
    """Persist opening lines to disk for movement comparison later."""
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    if lines_df.empty:
        return
    with open(SNAPSHOT_FILE, "w") as f:
        json.dump(lines_df.to_dict(orient="records"), f)
    print(f"  [Snapshot] {len(lines_df)} lines saved → {SNAPSHOT_FILE}")


def load_line_snapshot() -> pd.DataFrame:
    """Load today's opening lines from disk."""
    if not SNAPSHOT_FILE.exists():
        return pd.DataFrame()
    with open(SNAPSHOT_FILE) as f:
        return pd.DataFrame(json.load(f))


# ─────────────────────────────────────────────
#  detect_sharp_movement  (your interface)
# ─────────────────────────────────────────────

def detect_sharp_movement(df: pd.DataFrame,
                          previous_lines_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Detect line changes indicating sharp action.

    Keeps your exact merge/line_move/sharp_* pattern and adds:
      - All markets (points, assists, rebounds, pra, steals, blocks, threes)
      - Reverse line movement flag (line moves up but odds shift toward under)
      - Sharp signal label per market
      - Magnitude filter via config.SHARP_MOVEMENT_UNITS

    df:                current lines (wide format from edge_detector._pivot_lines)
    previous_lines_df: opening lines — loads today's snapshot if not passed
    """
    if previous_lines_df is None:
        previous_lines_df = load_line_snapshot()
        if not previous_lines_df.empty:
            previous_lines_df = _to_wide(previous_lines_df)

    if previous_lines_df is None or previous_lines_df.empty:
        print("  [INFO] No previous lines available for movement detection.")
        return df.copy()

    df = df.copy()

    # ── Your exact merge ──────────────────────
    df = df.merge(previous_lines_df, on="player_name",
                  suffixes=("", "_prev"), how="inner")

    # ── Your exact line move columns ──────────
    markets = ["points", "assists", "rebounds", "pra",
               "steals", "blocks", "threes"]

    for market in markets:
        line_col      = f"line_{market}"
        prev_col      = f"line_{market}_prev"
        move_col      = f"line_move_{market}"
        sharp_col     = f"sharp_{market}"
        rlm_col       = f"reverse_line_move_{market}"
        signal_col    = f"sharp_signal_{market}"

        if line_col not in df.columns or prev_col not in df.columns:
            continue

        # Your formula
        df[move_col]  = df[line_col] - df[prev_col]

        # Your flag (any upward move = sharp) — now uses threshold
        df[sharp_col] = df[move_col].abs() >= config.SHARP_MOVEMENT_UNITS

        # Reverse line movement:
        # Line rises but over_odds shortens (money on Under despite rising line)
        over_now  = f"over_odds_{market}"
        over_prev = f"over_odds_{market}_prev"
        if over_now in df.columns and over_prev in df.columns:
            df[rlm_col] = (
                ((df[move_col] > 0) & (df[over_now] < df[over_prev] - 5)) |
                ((df[move_col] < 0) & (df[over_now] > df[over_prev] + 5))
            )
        else:
            df[rlm_col] = False

        # Human-readable signal
        df[signal_col] = df.apply(
            lambda row, sc=sharp_col, rc=rlm_col, mc=move_col: (
                "🚨 REVERSE LINE MOVE" if row.get(rc)
                else f"📈 Sharp move ({row[mc]:+.1f})" if row.get(sc)
                else "—"
            ),
            axis=1,
        )

    return df


# ─────────────────────────────────────────────
#  detect_sgp_correlations  (your interface)
# ─────────────────────────────────────────────

# Full correlation map — each pair is (player_a, market_a, player_b, market_b)
# Values: positive = markets likely to hit together, negative = inverse
SGP_PLAYER_CORRELATIONS = {
    # Jokic-anchored
    ("Nikola Jokic",      "assists", "Jamal Murray",     "points"):   0.72,
    ("Nikola Jokic",      "assists", "Michael Porter Jr","points"):   0.55,
    ("Nikola Jokic",      "points",  "Jamal Murray",     "points"):   0.40,
    ("Nikola Jokic",      "rebounds","Jamal Murray",     "assists"):  0.30,
    # Haliburton-anchored
    ("Tyrese Haliburton", "assists", "Bennedict Mathurin","points"):  0.60,
    ("Tyrese Haliburton", "assists", "Pascal Siakam",    "points"):   0.55,
    # LeBron-anchored
    ("LeBron James",      "assists", "Anthony Davis",    "points"):   0.50,
    ("LeBron James",      "points",  "Anthony Davis",    "rebounds"): 0.35,
    # Tatum-anchored
    ("Jayson Tatum",      "points",  "Jrue Holiday",     "assists"):  0.45,
    # SGA-anchored
    ("Shai Gilgeous-Alexander","points","Chet Holmgren", "rebounds"): 0.40,
    # Single-player market correlations (same player, different markets)
    ("*",                 "points",  "*",                "pra"):      0.92,
    ("*",                 "rebounds","*",                "pra"):      0.85,
    ("*",                 "assists", "*",                "pra"):      0.83,
    ("*",                 "points",  "*",                "threes"):   0.58,
    ("*",                 "points",  "*",                "assists"):  0.38,
    ("*",                 "steals",  "*",                "blocks"):   0.28,
}

SGP_RATING_THRESHOLDS = {
    "🔥 PRIME SGP":       (0.60, 8.0),   # (min_corr, min_avg_edge)
    "✅ SOLID SGP":       (0.35, 5.0),
    "👀 SPECULATIVE SGP": (0.10, 2.0),
}


def detect_sgp_correlations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Detect correlations for Same Game Parlays.

    Extends your Jokic/Murray example to all TOP_20_PLAYERS pairs
    using the full SGP_PLAYER_CORRELATIONS map.

    df: the edge DataFrame from calculate_edge() — must have
        player_name, edge_* and bet_* columns.

    Returns DataFrame of correlated SGP candidates with columns:
      player_a, market_a, edge_a,
      player_b, market_b, edge_b,
      correlation, combined_edge, sgp_rating
    """
    if df.empty:
        return pd.DataFrame()

    # Only consider players with at least one positive edge
    edge_cols = [c for c in df.columns if c.startswith("edge_")]
    if not edge_cols:
        return pd.DataFrame()

    # Build flat list of (player, market, edge) for active edges
    active_edges = []
    for _, row in df.iterrows():
        player = row["player_name"]
        for col in edge_cols:
            market = col.replace("edge_", "")
            edge   = row.get(col, 0) or 0
            if edge > 0:
                active_edges.append({
                    "player": player,
                    "market": market,
                    "edge":   round(edge, 2),
                })

    if len(active_edges) < 2:
        return pd.DataFrame()

    # Find correlated pairs
    candidates = []
    seen = set()
    for i, a in enumerate(active_edges):
        for b in active_edges[i + 1:]:
            pair_key = tuple(sorted([
                f"{a['player']}|{a['market']}",
                f"{b['player']}|{b['market']}",
            ]))
            if pair_key in seen:
                continue
            seen.add(pair_key)

            corr = _lookup_correlation(
                a["player"], a["market"],
                b["player"], b["market"],
            )
            if corr <= 0:
                continue

            combined = round((a["edge"] + b["edge"]) / 2, 2)
            rating   = _sgp_rating(corr, combined)

            candidates.append({
                "player_a":     a["player"],
                "market_a":     a["market"],
                "edge_a":       a["edge"],
                "player_b":     b["player"],
                "market_b":     b["market"],
                "edge_b":       b["edge"],
                "correlation":  corr,
                "combined_edge":combined,
                "sgp_rating":   rating,
            })

    if not candidates:
        return pd.DataFrame()

    result = pd.DataFrame(candidates)
    return result.sort_values(
        ["correlation", "combined_edge"], ascending=False
    ).reset_index(drop=True)


# ─────────────────────────────────────────────
#  Internal helpers
# ─────────────────────────────────────────────

def _lookup_correlation(player_a: str, market_a: str,
                        player_b: str, market_b: str) -> float:
    """
    Look up correlation between two (player, market) legs.
    Falls back to wildcard (*) same-player correlations.
    """
    # Exact player pair
    key   = (player_a, market_a, player_b, market_b)
    key_r = (player_b, market_b, player_a, market_a)
    if key   in SGP_PLAYER_CORRELATIONS: return SGP_PLAYER_CORRELATIONS[key]
    if key_r in SGP_PLAYER_CORRELATIONS: return SGP_PLAYER_CORRELATIONS[key_r]

    # Wildcard: same player, different markets
    if player_a == player_b:
        wkey   = ("*", market_a, "*", market_b)
        wkey_r = ("*", market_b, "*", market_a)
        if wkey   in SGP_PLAYER_CORRELATIONS: return SGP_PLAYER_CORRELATIONS[wkey]
        if wkey_r in SGP_PLAYER_CORRELATIONS: return SGP_PLAYER_CORRELATIONS[wkey_r]

    return 0.0


def _sgp_rating(corr: float, avg_edge: float) -> str:
    for label, (min_corr, min_edge) in SGP_RATING_THRESHOLDS.items():
        if corr >= min_corr and avg_edge >= min_edge:
            return label
    return "—"


def _to_wide(lines_df: pd.DataFrame) -> pd.DataFrame:
    """Convert long snapshot lines back to wide format for merging."""
    from edge_detector import _pivot_lines
    # Normalise column name
    if "player" in lines_df.columns and "player_name" not in lines_df.columns:
        lines_df = lines_df.rename(columns={"player": "player_name"})
    return _pivot_lines(lines_df)


# ─────────────────────────────────────────────
#  Output formatters
# ─────────────────────────────────────────────

def format_movement_report(movement_df: pd.DataFrame) -> str:
    """Print sharp movement summary."""
    markets = ["points", "assists", "rebounds", "pra",
               "steals", "blocks", "threes"]
    lines = ["", "SHARP LINE MOVEMENT", "─" * 50]
    found = False
    for _, row in movement_df.iterrows():
        for market in markets:
            signal = row.get(f"sharp_signal_{market}", "—")
            if signal == "—":
                continue
            found = True
            move  = row.get(f"line_move_{market}", 0)
            lines.append(
                f"  {row['player_name']} | {market.upper():10s} "
                f"move: {move:+.1f}  {signal}"
            )
    if not found:
        lines.append("  No significant movement detected.")
    return "\n".join(lines)


def format_sgp_report(sgp_df: pd.DataFrame) -> str:
    """Print SGP opportunities summary."""
    lines = ["", "SGP OPPORTUNITIES", "─" * 50]
    if sgp_df.empty:
        lines.append("  None identified today.")
        return "\n".join(lines)
    for _, row in sgp_df.iterrows():
        lines.append(
            f"\n  {row['sgp_rating']}"
            f"  (corr: {row['correlation']:.2f} | avg edge: {row['combined_edge']:+.1f})"
        )
        lines.append(
            f"    ▸ {row['player_a']} {row['market_a'].upper():10s}"
            f" edge: {row['edge_a']:+.1f}"
        )
        lines.append(
            f"    ▸ {row['player_b']} {row['market_b'].upper():10s}"
            f" edge: {row['edge_b']:+.1f}"
        )
    return "\n".join(lines)
