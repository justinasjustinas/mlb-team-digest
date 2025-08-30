from typing import Any, Dict


def parse_ip_to_outs(ip_val: Any) -> int:
    if ip_val is None:
        return 0
    if isinstance(ip_val, int):
        return ip_val if ip_val >= 3 else ip_val * 3
    try:
        f = float(ip_val)
        whole = int(f)
        dec = round((f - whole) * 10)
        return whole * 3 + min(max(dec, 0), 2)
    except Exception:
        return 0


def safe_div(n: float, d: float) -> float:
    return float(n) / float(d) if d else 0.0


def to_100(raw: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 50.0
    x = (raw - lo) / (hi - lo) * 100.0
    return max(0.0, min(100.0, round(x, 2)))


def compute_pitching_metrics(row: Dict[str, Any]) -> Dict[str, Any]:
    outs = int(row.get("outs") or parse_ip_to_outs(row.get("IP")))
    ip = outs / 3.0
    ER = float(row.get("ER", 0) or 0)
    H = float(row.get("H", 0) or 0)
    BB = float(row.get("BB", 0) or 0)
    HR = float(row.get("HR", 0) or 0)
    SO = float(row.get("SO", row.get("K", 0)) or 0)

    ERA = round(safe_div(ER * 9.0, ip), 2) if ip else 0.0
    WHIP = round(safe_div(H + BB, ip), 2) if ip else 0.0

    PITCH_LO, PITCH_HI = -10.0, 40.0

    PITCH_SCORE_RAW = 6 * ip + 2 * SO - 4 * ER - 2 * (H - HR) - 1 * BB - 3 * HR
    PITCH_SCORE = to_100(PITCH_SCORE_RAW, PITCH_LO, PITCH_HI)

    row.update(
        {
            "outs": outs,
            "IP": ip,  # convenience
            "ERA": ERA,
            "WHIP": WHIP,
            "SO": SO,
            "PITCH_SCORE": float(PITCH_SCORE),
        }
    )
    return row
