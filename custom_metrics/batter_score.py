from typing import Any, Dict


def safe_div(n: float, d: float) -> float:
    return float(n) / float(d) if d else 0.0


def to_100(raw: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 50.0
    x = (raw - lo) / (hi - lo) * 100.0
    return max(0.0, min(100.0, round(x, 2)))


def compute_batting_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    AB = int(row.get("AB", 0) or 0)
    H = int(row.get("H", 0) or 0)
    BB = int(row.get("BB", 0) or 0)
    HBP = int(row.get("HBP", 0) or 0)
    SF = int(row.get("SF", 0) or 0)
    HR = int(row.get("HR", 0) or 0)
    D2 = int(row.get("doubles", row.get("2B", row.get("Doubles", 0))) or 0)
    D3 = int(row.get("triples", row.get("3B", row.get("Triples", 0))) or 0)
    R = int(row.get("R", 0) or 0)
    RBI = int(row.get("RBI", 0) or 0)
    SB = int(row.get("SB", 0) or 0)

    singles = max(H - D2 - D3 - HR, 0)
    TB = singles + 2 * D2 + 3 * D3 + 4 * HR

    AVG = safe_div(H, AB)
    OBP = safe_div(H + BB + HBP, AB + BB + HBP + SF)
    SLG = safe_div(TB, AB)
    OPS = OBP + SLG

    BAT_LO, BAT_HI = 0.0, 20.0  # the wider the range, the more difficult it is to achieve 100

    BAT_SCORE_RAW = 5 * HR + 3 * (D2 + D3) + 2 * (BB + HBP + SB) + singles + 2.0 * RBI + 1.0 * R
    BAT_SCORE = to_100(BAT_SCORE_RAW, BAT_LO, BAT_HI)

    row.update(
        {
            "AVG": round(AVG, 3),
            "OBP": round(OBP, 3),
            "SLG": round(SLG, 3),
            "OPS": round(OPS, 3),
            "BAT_SCORE": float(BAT_SCORE),
        }
    )

    return row
