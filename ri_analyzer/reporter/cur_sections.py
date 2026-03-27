"""CUR ベースのセクション表示（Running Instances / Coverage / Unused RI / Factcheck）"""

from __future__ import annotations

from ri_analyzer.analyzers.cur_detail import (
    CurInstanceRow,
    CurInstanceDetailRow,
    CurCoverageRow,
    UnusedRiRow,
    RecommendationFactcheck,
)
from datetime import date as _date

from ri_analyzer.reporter._base import _RED, _YELLOW, _GREEN, _CYAN, _BOLD, _RESET, _c, _header


# ──────────────────────────────────────────────
# CUR: Running Instances
# ──────────────────────────────────────────────

def print_cur_instances(
    rows: list[CurInstanceRow],
    service: str,
    start_date: str,
    end_date: str,
) -> None:
    _header(f"CUR Running Instances  [{service.upper()}]  ({start_date} to {end_date})")

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
# CUR: Instance Detail（resource_id 単位）
# ──────────────────────────────────────────────

def print_cur_instance_detail(
    rows: list[CurInstanceDetailRow],
    service: str,
    start_date: str,
    end_date: str,
    *,
    min_hours: float | None = None,
) -> None:
    """resource_id 単位のインスタンス稼働実績を表示する。

    Parameters
    ----------
    min_hours : RI 購入候補フィルタ。指定時は usage_hours >= min_hours の行のみ表示。
                未指定時は全行表示し、短命なインスタンスを黄色でハイライト。
    """
    # クエリ期間の合計時間を算出（短命判定の基準に使う）
    period_days = (_date.fromisoformat(end_date) - _date.fromisoformat(start_date)).days
    period_hours = period_days * 24
    # 期間の 50% 未満しか稼働していないインスタンスを「短命」とみなす
    short_lived_threshold = period_hours * 0.5

    filter_label = f"  (min {min_hours:.0f}h filter applied)" if min_hours is not None else ""
    _header(
        f"CUR Instance Detail  [{service.upper()}]  ({start_date} to {end_date})"
        f"  period={period_hours:.0f}h{filter_label}"
    )

    if not rows:
        print("\n  No data.")
        return

    display_rows = [r for r in rows if r.usage_hours >= min_hours] if min_hours is not None else rows
    short_lived_skipped = len(rows) - len(display_rows) if min_hours is not None else 0

    if not display_rows:
        print(f"\n  All {len(rows)} instance(s) are below the {min_hours:.0f}h threshold.")
        return

    col = (
        f"\n  {'Resource Name':<30}  {'Account ID':<14}  {'Region':<16}  {'Type':<18}"
        f"  {'Engine':<16}  {'Period%':>7}  {'hrs':>6}  {'RI%':>5}  {'RI hrs':>7}  {'OD hrs':>7}"
    )
    print(col)
    print(f"  {'-' * 140}")

    # engine でグループ化
    engines: list[str] = []
    seen: set[str] = set()
    for r in display_rows:
        if r.engine not in seen:
            engines.append(r.engine)
            seen.add(r.engine)

    total_od_hours = 0.0
    short_lived_count = 0

    for eng in engines:
        group = [r for r in display_rows if r.engine == eng]
        print(f"\n  [{eng}]")
        for r in group:
            # 期間中の稼働割合（期間時間に対する usage_hours の比率）
            run_pct = (r.usage_hours / period_hours * 100) if period_hours > 0 else 0.0
            is_short = r.usage_hours < short_lived_threshold
            if is_short:
                short_lived_count += 1

            name = r.resource_name[:30]
            run_pct_str = f"{run_pct:6.1f}%"
            hrs_str     = f"{r.usage_hours:6.0f}"
            ri_pct_str  = f"{r.coverage_pct:4.0f}%"

            if is_short:
                # 短命なインスタンスは行全体を黄色
                line = (
                    f"  {name:<30}  {r.account_id:<14}  {r.region:<16}  {r.instance_type:<18}"
                    f"  {r.engine:<16}  {run_pct_str}  {hrs_str}  {ri_pct_str}"
                    f"  {r.ri_hours:>7.1f}  {r.od_hours:>7.1f}"
                )
                print(_c(line, _YELLOW))
            else:
                ri_pct_color = _GREEN if r.coverage_pct >= 90 else (_YELLOW if r.coverage_pct >= 50 else _RED)
                ri_pct_colored = _c(ri_pct_str, ri_pct_color)
                print(
                    f"  {name:<30}  {r.account_id:<14}  {r.region:<16}  {r.instance_type:<18}"
                    f"  {r.engine:<16}  {run_pct_str}  {hrs_str}  {ri_pct_colored}"
                    f"  {r.ri_hours:>7.1f}  {r.od_hours:>7.1f}"
                )
            total_od_hours += r.od_hours

    print()
    stable_count = len(display_rows) - short_lived_count
    print(_c(
        f"  Instances shown: {len(display_rows)}"
        f"  |  stable (>={short_lived_threshold:.0f}h / 50% of period): {stable_count}"
        f"  |  short-lived: {short_lived_count}",
        _BOLD,
    ))
    print(_c(f"  Total OD hours (RI 購入対象候補): {total_od_hours:,.1f} hrs", _BOLD))
    if short_lived_skipped:
        print(_c(f"  Skipped (< {min_hours:.0f}h): {short_lived_skipped} instance(s)", _YELLOW))


# ──────────────────────────────────────────────
# CUR: RI Coverage Detail
# ──────────────────────────────────────────────

def print_cur_coverage(
    rows: list[CurCoverageRow],
    service: str,
    start_date: str,
    end_date: str,
) -> None:
    _header(f"CUR RI Coverage Detail  [{service.upper()}]  ({start_date} to {end_date})")

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
    start_date: str,
    end_date: str,
) -> None:
    _header(f"CUR Unused RI Fee  [{service.upper()}]  ({start_date} to {end_date})")

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
    start_date: str,
    end_date: str,
) -> None:
    _header(f"CE Recommendation Fact-check  [{service.upper()}]  (CUR: {start_date} to {end_date})")

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
