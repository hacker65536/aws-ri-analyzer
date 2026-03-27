"""CUR ベースのセクション表示（Running Instances / Coverage / Unused RI / Factcheck）"""

from __future__ import annotations

from ri_analyzer.analyzers.cur_detail import (
    CurInstanceRow,
    CurCoverageRow,
    UnusedRiRow,
    RecommendationFactcheck,
)
from ri_analyzer.reporter._base import _RED, _YELLOW, _GREEN, _CYAN, _BOLD, _RESET, _c, _header


# ──────────────────────────────────────────────
# CUR: Running Instances
# ──────────────────────────────────────────────

def print_cur_instances(
    rows: list[CurInstanceRow],
    service: str,
    year: int,
    month: int,
) -> None:
    _header(f"CUR Running Instances  [{service.upper()}]  ({year}-{month:02d})")

    if not rows:
        print("\n  No data.")
        return

    col = (
        f"\n  {'Account ID':<14}  {'Region':<16}  {'Instance Type':<22}"
        f"  {'Engine':<20}  {'Deployment':<12}"
        f"  {'Avg Inst':>8}  {'Usage hrs':>10}  {'Cost (USD)':>11}"
    )
    print(col)
    print(f"  {'-' * 117}")

    # engine でグループ化して表示
    engines: list[str] = []
    seen: set[str] = set()
    for r in rows:
        if r.engine not in seen:
            engines.append(r.engine)
            seen.add(r.engine)

    total_cost = 0.0
    for eng in engines:
        group = [r for r in rows if r.engine == eng]
        print(f"\n  [{eng}]")
        for r in group:
            avg = r.avg_instances
            avg_str = _c(f"{avg:7.2f}", _YELLOW if avg < 1 else _RESET)
            print(
                f"  {r.account_id:<14}  {r.region:<16}  {r.instance_type:<22}"
                f"  {r.engine:<20}  {r.deployment:<12}"
                f"  {avg_str}  {r.usage_hours:>10.1f}  ${r.unblended_cost:>10.2f}"
            )
            total_cost += r.unblended_cost

    print()
    print(_c(f"  Total cost: ${total_cost:,.2f}  |  {len(rows)} rows", _BOLD))


# ──────────────────────────────────────────────
# CUR: RI Coverage Detail
# ──────────────────────────────────────────────

def print_cur_coverage(
    rows: list[CurCoverageRow],
    service: str,
    year: int,
    month: int,
) -> None:
    _header(f"CUR RI Coverage Detail  [{service.upper()}]  ({year}-{month:02d})")

    if not rows:
        print("\n  No data.")
        return

    col = (
        f"\n  {'Account ID':<14}  {'Region':<16}  {'Instance Type':<22}"
        f"  {'Coverage':>9}  {'RI hrs':>9}  {'OD hrs':>9}  {'Total hrs':>10}"
    )
    print(col)
    print(f"  {'-' * 96}")

    for r in rows:
        if r.status == "ok":
            pct_str = _c(f"{r.coverage_pct:8.1f}%", _GREEN)
        elif r.status == "warning":
            pct_str = _c(f"{r.coverage_pct:8.1f}%", _YELLOW)
        else:
            pct_str = _c(f"{r.coverage_pct:8.1f}%", _RED)

        print(
            f"  {r.account_id:<14}  {r.region:<16}  {r.instance_type:<22}"
            f"  {pct_str}  {r.ri_hours:>9.1f}  {r.od_hours:>9.1f}  {r.total_hours:>10.1f}"
        )

    low     = [r for r in rows if r.status == "low"]
    warning = [r for r in rows if r.status == "warning"]
    print()
    if low:
        od = sum(r.od_hours for r in low)
        print(_c(f"  [!] Low coverage (<50%): {len(low)} row(s)  OD total: {od:,.1f} hrs", _RED))
    if warning:
        od = sum(r.od_hours for r in warning)
        print(_c(f"  [!] Mid coverage (50-90%): {len(warning)} row(s)  OD total: {od:,.1f} hrs", _YELLOW))
    if not low and not warning:
        print(_c("  [v] All rows have coverage >= 90%", _GREEN))


# ──────────────────────────────────────────────
# CUR: Unused RI Fee
# ──────────────────────────────────────────────

def print_unused_ri(
    rows: list[UnusedRiRow],
    service: str,
    year: int,
    month: int,
) -> None:
    _header(f"CUR Unused RI Fee  [{service.upper()}]  ({year}-{month:02d})")

    if not rows:
        print("\n  No unused RI found.")
        return

    col = (
        f"\n  {'Account ID':<14}  {'Region':<16}  {'Usage Type':<40}"
        f"  {'Fee (USD)':>10}  {'Qty':>6}"
    )
    print(col)
    print(f"  {'-' * 94}")

    total = 0.0
    for r in rows:
        cost_str = _c(f"${r.ri_fee_cost:>9.2f}", _RED if r.ri_fee_cost > 0 else _RESET)
        # ARN は末尾の ID 部分だけ表示（長いため）
        arn_short = r.reservation_arn.split(":")[-1][:20] if r.reservation_arn else ""
        print(
            f"  {r.account_id:<14}  {r.region:<16}  {r.usage_type:<40}"
            f"  {cost_str}  {r.quantity:>6.0f}"
            + (f"  ({arn_short})" if arn_short else "")
        )
        total += r.ri_fee_cost

    print()
    total_str = _c(f"${total:,.2f}", _RED if total > 0 else _GREEN)
    print(_c(f"  Total unused RI fee: {total_str}", _BOLD))


# ──────────────────────────────────────────────
# CE Recommendation × CUR ファクトチェック
# ──────────────────────────────────────────────

def print_ce_factcheck(
    checks: list[RecommendationFactcheck],
    service: str,
    year: int,
    month: int,
) -> None:
    _header(f"CE Recommendation Fact-check  [{service.upper()}]  (CUR: {year}-{month:02d})")

    if not checks:
        print("\n  No recommendations to check.")
        return

    col = (
        f"\n  {'Instance Type':<22}  {'Platform':<22}  {'Region':<16}"
        f"  {'CE cnt':>6}  {'CUR avg':>7}  {'Gap':>6}  {'OD hrs':>8}  {'Judge'}"
    )
    print(col)
    print(f"  {'-' * 104}")

    for c in checks:
        gap = c.gap
        if c.cur_usage_hours == 0:
            judge = _c("  [?] no CUR data", _YELLOW)
            gap_str = _c(f"{gap:>+6.1f}", _YELLOW)
        elif abs(gap) <= 0.5:
            judge = _c("  [v] match", _GREEN)
            gap_str = _c(f"{gap:>+6.1f}", _GREEN)
        elif gap > 0.5:
            judge = _c("  [+] buy candidate", _YELLOW)
            gap_str = _c(f"{gap:>+6.1f}", _YELLOW)
        else:
            judge = _c("  [-] already covered", _GREEN)
            gap_str = _c(f"{gap:>+6.1f}", _GREEN)

        print(
            f"  {c.instance_type:<22}  {c.platform:<22}  {c.region:<16}"
            f"  {c.ce_count:>6}  {c.cur_avg_instances:>7.2f}  {gap_str}  {c.cur_od_hours:>8.1f}"
            f"  {judge}"
        )

    no_data = [c for c in checks if c.cur_usage_hours == 0]
    candidates = [c for c in checks if c.gap > 0.5]
    if no_data:
        print(_c(f"\n  [?] {len(no_data)} item(s) have no CUR data (check year/month or engine filter)", _YELLOW))
    if candidates:
        print(_c(f"  [+] {len(candidates)} item(s) are buy candidates (CE recommends more than CUR shows)", _YELLOW))
