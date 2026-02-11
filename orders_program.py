from __future__ import annotations
 
import datetime as dt
import logging
from dataclasses import dataclass
from typing import Iterable, Iterator, List, Dict, Tuple, Optional, Any
 
# ------------------------- Logging setup (simple) -----------------------------
 
logger = logging.getLogger("orders_program")
if not logger.handlers:
    _h = logging.StreamHandler()
    _fmt = logging.Formatter("%(levelname)s %(message)s")
    _h.setFormatter(_fmt)
    logger.addHandler(_h)
logger.setLevel(logging.INFO)
 
# ------------------------- Exceptions ----------------------------------------
 
 
class OrdersError(Exception):
    """Base error."""
 
 
class ParseError(OrdersError):
    """Raised when a line cannot be parsed into fields."""
 
 
class ValidationError(OrdersError):
    """Raised when a field violates domain constraints."""
 
 
# ------------------------- Data model ----------------------------------------
 
 
@dataclass(frozen=True)
class Order:
    order_id: str
    timestamp: dt.datetime  # must be timezone-aware UTC
    customer_id: str
    item_id: str
    qty: int
    price: float
    currency: str
    status: str
    coupon_code: Optional[str] = None
 
 
# ------------------------- Utility functions ---------------------------------
 
 
def _to_utc(ts: str) -> dt.datetime:
    """
    Convert epoch seconds or ISO 8601 to timezone-aware UTC datetime.
 
    Accepted forms:
        - "1709251200"                      (epoch seconds)
        - "2025-03-01T12:00:00Z"            (ISO with Z)
        - "2025-03-01T12:00:00+00:00"       (ISO with offset)
    """
    ts = ts.strip()
    # Control flow with exception handling for resilience.
    try:
        if ts.isdigit():
            # Epoch seconds
            return dt.datetime.fromtimestamp(int(ts), tz=dt.timezone.utc)
        # ISO 8601 variants
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return dt.datetime.fromisoformat(ts).astimezone(dt.timezone.utc)
    except Exception as exc:  # narrow later if you wish
        raise ParseError(f"Unrecognized timestamp: {ts!r}") from exc
 
 
def parse_line(line: str) -> Order:
    """
    Parse a CSV-like line into an Order object.
 
    Expected columns:
    order_id,timestamp,customer_id,item_id,qty,price,currency,status,coupon_code?
 
    Raises:
        ParseError, ValidationError
    """
    parts = [p.strip() for p in line.strip().split(",")]
    if len(parts) < 8:
        raise ParseError(f"Expected ≥8 fields, got {len(parts)}: {line!r}")
 
    order_id, ts, cust, item, qty, price, cur, status, *rest = parts
    coupon = rest[0] if rest else None
 
    # Variables & control flow with explicit validations
    try:
        qty_i = int(qty)
    except ValueError as exc:
        raise ValidationError(f"qty must be int, got {qty!r}") from exc
    try:
        price_f = float(price)
    except ValueError as exc:
        raise ValidationError(f"price must be number, got {price!r}") from exc
 
    if qty_i <= 0:
        raise ValidationError(f"qty must be > 0, got {qty_i}")
    if price_f < 0:
        raise ValidationError(f"price must be >= 0, got {price_f}")
    if status not in {"PLACED", "SHIPPED", "CANCELLED"}:
        raise ValidationError(f"unknown status {status!r}")
 
    ts_dt = _to_utc(ts)
 
    return Order(
        order_id=order_id,
        timestamp=ts_dt,
        customer_id=cust,
        item_id=item,
        qty=qty_i,
        price=price_f,
        currency=cur,
        status=status,
        coupon_code=coupon or None,
    )
 
 
def parse_lines(lines: Iterable[str]) -> Iterator[Order]:
    """
    Stream-parse lines. Malformed lines raise ParseError/ValidationError.
    Caller decides whether to catch or fail-fast.
    """
    for ln in lines:
        if not ln.strip():
            continue
        yield parse_line(ln)
 
 
# ------------------------- Core transforms -----------------------------------
 
 
def deduplicate_latest(orders: Iterable[Order]) -> List[Order]:
    """
    Keep ONLY the latest record per (order_id, item_id) by timestamp.
    If timestamps tie, choose lexicographically smaller item_id (stable & deterministic).
 
    Invariant (state explanation for reasoning):
      For any key k = (order_id, item_id), after processing i lines,
      store[k] is the latest Order among the first i lines under that key.
 
    Complexity: O(n) with O(u) memory where u = unique (order_id, item_id).
    """
    store: Dict[Tuple[str, str], Order] = {}
    for o in orders:
        k = (o.order_id, o.item_id)
        if k not in store:
            store[k] = o
            continue
        existing = store[k]
        if o.timestamp > existing.timestamp:
            store[k] = o
        elif o.timestamp == existing.timestamp and o.item_id < existing.item_id:
            store[k] = o
    # Return in deterministic order for reproducibility
    return sorted(store.values(), key=lambda x: (x.order_id, x.item_id, x.timestamp))
 
 
def daily_gmv(orders: Iterable[Order]) -> Dict[str, float]:
    """
    GMV per UTC calendar day: sum(qty * price).
    Returns ordered by day string.
    """
    totals: Dict[str, float] = {}
    for o in orders:
        day = o.timestamp.date().isoformat()
        totals[day] = totals.get(day, 0.0) + (o.qty * o.price)
    return dict(sorted(totals.items(), key=lambda kv: kv[0]))
 
 
def rolling_7d_gmv(daily: Dict[str, float]) -> Dict[str, float]:
    """
    Rolling 7-day GMV over the *sorted* day keys.
    """
    days = sorted(daily.keys())
    res: Dict[str, float] = {}
    window: List[Tuple[str, float]] = []
    running = 0.0
    for d in days:
        v = daily[d]
        window.append((d, v))
        running += v
        while len(window) > 7:
            _, v_old = window.pop(0)
            running -= v_old
        res[d] = running
    return res
 
 
def top_n_items_by_gmv(orders: Iterable[Order], n: int = 5) -> List[Tuple[str, float]]:
    """
    Top-N item_ids by GMV with stable tie-break: (-gmv, item_id).
    """
    item_totals: Dict[str, float] = {}
    for o in orders:
        item_totals[o.item_id] = item_totals.get(o.item_id, 0.0) + (o.qty * o.price)
    ranked = sorted(item_totals.items(), key=lambda t: (-t[1], t[0]))
    return ranked[: max(0, n)]
 
 
def weekly_cancellation_rate(orders: Iterable[Order]) -> Dict[str, float]:
    """
    ISO week cancellation rate: cancelled / total per week.
    Key format: "YYYY-Www" (e.g., "2025-W09")
    """
    total: Dict[str, int] = {}
    cancelled: Dict[str, int] = {}
    for o in orders:
        y, w, _ = o.timestamp.isocalendar()  # (year, week, weekday)
        key = f"{y}-W{w:02d}"
        total[key] = total.get(key, 0) + 1
        if o.status == "CANCELLED":
            cancelled[key] = cancelled.get(key, 0) + 1
    out = {}
    for k in sorted(total.keys()):
        c = cancelled.get(k, 0)
        out[k] = c / total[k]
    return out
 
 
# ------------------------- Intentional Bugs (debugging task) ------------------
# Task: find & fix both. Write how you found them in debug_notes.md
 
def unsafe_bucketize(values: List[int], bucket: Optional[List[int]] = None) -> List[int]:
    """
    FIXED: Avoids mutable default argument that accumulates across calls.
    Creates a new list when bucket is not provided.
    """
    if bucket is None:
        bucket = []
    for v in values:
        if v % 2 == 0:
            bucket.append(v)
    return bucket
 
 
def merge_intervals_bad(intervals: List[Tuple[int, int]]) -> List[Tuple[int, int]]:
    """
    FIXED: Correct merging logic; no input aliasing; stable & deterministic.
 
    Approach:
      - Work on a sorted copy of intervals by start.
      - Maintain a current interval (cur_start, cur_end) and merge while
        next.start <= cur_end (including adjacency).
      - Append as tuples to avoid mutability/aliasing issues for callers.
    """
    if not intervals:
        return []
 
    sorted_intervals = sorted(intervals, key=lambda ab: (ab[0], ab[1]))
    merged: List[Tuple[int, int]] = []
 
    cur_start, cur_end = sorted_intervals[0]
    for a, b in sorted_intervals[1:]:
        if a <= cur_end:  # merge overlap or adjacency
            cur_end = max(cur_end, b)
        else:
            merged.append((cur_start, cur_end))
            cur_start, cur_end = a, b
    merged.append((cur_start, cur_end))
    return merged
 
 
# ------------------------- High-level pipeline (no I/O) ----------------------
 
 
def compute_report(lines: Iterable[str], top_n: int = 5) -> Dict[str, Any]:
    """
    End-to-end orchestration using only Python structures:
      lines -> parse -> validate -> deduplicate -> KPIs
    Returns a nested dict with results for unit testing or printing.
 
    This is YOUR public entry point. Do not do any file I/O here.
    """
    parsed: List[Order] = []
    errors: List[str] = []
 
    for ln in lines:
        try:
            parsed.append(parse_line(ln))
        except (ParseError, ValidationError) as exc:
            # Example of resilient error handling with context; no silent passes.
            msg = f"skip line due to {exc.__class__.__name__}: {exc}"
            logger.debug(msg)
            errors.append(msg)
 
    dedup = deduplicate_latest(parsed)
    daily = daily_gmv(dedup)
    roll7 = rolling_7d_gmv(daily)
    top_items = top_n_items_by_gmv(dedup, n=top_n)
    cancel_rate = weekly_cancellation_rate(dedup)
 
    return {
        "counts": {
            "input_lines": len(list(lines)) if hasattr(lines, "__iter__") else None,  # may be None for single-pass iter
            "parsed": len(parsed),
            "deduplicated": len(dedup),
            "errors": len(errors),
        },
        "daily_gmv": daily,
        "rolling_7d_gmv": roll7,
        "top_items": top_items,
        "cancel_rate": cancel_rate,
        "errors_sample": errors[:3],
    }
 
 
# ------------------------- Minimal self-test harness -------------------------
 
SAMPLE_LINES = [
    # valid ISO
    "o1,2025-03-01T12:00:00Z,c1,i1,2,10.0,USD,PLACED",
    # same (order_id,item_id) later in time (dedup should keep this)
    "o1,2025-03-02T12:00:00Z,c1,i1,1,15.0,USD,SHIPPED",
    # epoch ts, cancellation
    "o2,1709251200,c2,i2,3,7.5,USD,CANCELLED",
    # invalid qty -> should be collected as error, not crash
    "o3,2025-03-01T10:00:00Z,c3,i3,0,9.0,USD,PLACED",
    # adjacency test for intervals (used in your bug demo)
]
 
def _demo_run():
    logger.info("Running demo on SAMPLE_LINES (no files used).")
    result = compute_report(SAMPLE_LINES, top_n=3)
    print("=== Report (demo) ===")
    for k, v in result.items():
        print(f"{k}: {v}")
 
    # Show the buggy functions (now fixed) behavior
    print("\n=== Bug demos (fixed) ===")
    print("unsafe_bucketize first call:", unsafe_bucketize([1, 2, 3, 4]))
    print("unsafe_bucketize second call (should NOT carry over evens):", unsafe_bucketize([6]))
    print("merge_intervals_bad (fixed):", merge_intervals_bad([(1, 3), (2, 4), (6, 7), (7, 9)]))
 
 
if __name__ == "__main__":
    # You can insert `breakpoint()` anywhere and run: python orders_program.py
    _demo_run()
