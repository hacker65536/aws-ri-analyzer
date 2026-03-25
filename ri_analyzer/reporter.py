"""Console output (English)"""

from __future__ import annotations

from ri_analyzer.analyzers.expiration import ExpirationResult
from ri_analyzer.analyzers.coverage import CoverageSummary
from ri_analyzer.analyzers.utilization import UtilizationSummary

_RED    = "\033[91m"
_YELLOW = "\033[93m"
_GREEN  = "\033[92m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"

_use_color = True


def set_color(enabled: bool) -> None:
    global _use_color
    _use_color = enabled


def _c(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}" if _use_color else text


def _header(title: str) -> None:
    print()
    print(_c("=" * 80, _CYAN))
    print(_c(f"  {title}", _BOLD))
    print(_c("=" * 80, _CYAN))


# ──────────────────────────────────────────────
# Expiration
# ──────────────────────────────────────────────

def print_expiration(
    expired: list[ExpirationResult],
    warning: list[ExpirationResult],
    ok:      list[ExpirationResult],
    warn_days: int,
) -> None:
    _header(f"RI Expiration  (warn threshold: {warn_days} days)")

    if expired:
        print(_c(f"\n  [EXPIRED] {len(expired)} item(s)", _RED))
        for r in expired:
            ri = r.ri
            print(
                f"    {_c('x', _RED)}  {ri.instance_class:<20} x{ri.count:<4d}"
                f"  {ri.engine:<20}  {ri.region:<20}"
                f"  expires: {ri.end_time.strftime('%Y-%m-%d')}"
                f"  ({abs(r.days_remaining)} days ago)"
            )

    if warning:
        print(_c(f"\n  [WARNING] {len(warning)} item(s) expiring within {warn_days} days", _YELLOW))
        for r in warning:
            ri = r.ri
            print(
                f"    {_c('!', _YELLOW)}  {ri.instance_class:<20} x{ri.count:<4d}"
                f"  {ri.engine:<20}  {ri.region:<20}"
                f"  expires: {ri.end_time.strftime('%Y-%m-%d')}"
                f"  ({r.days_remaining} days left)"
            )

    if ok:
        print(_c(f"\n  [OK] {len(ok)} item(s)", _GREEN))
        for r in ok:
            ri = r.ri
            print(
                f"    {_c('v', _GREEN)}  {ri.instance_class:<20} x{ri.count:<4d}"
                f"  {ri.engine:<20}  {ri.region:<20}"
                f"  expires: {ri.end_time.strftime('%Y-%m-%d')}"
                f"  ({r.days_remaining} days left)"
            )

    if not (expired or warning or ok):
        print("\n  No active RIs found.")


# ──────────────────────────────────────────────
# Coverage
# ──────────────────────────────────────────────

def print_coverage(summaries: list[CoverageSummary], max_coverage: float | None = None) -> None:
    title = "RI Coverage  (reserved vs on-demand hours)"
    if max_coverage is not None:
        title += f"  [filter: coverage <= {max_coverage}%]"
    _header(title)

    if max_coverage is not None:
        summaries = [s for s in summaries if s.coverage_pct <= max_coverage]

    if not summaries:
        print("\n  No data.")
        return

    low     = [s for s in summaries if s.status == "low"]
    warning = [s for s in summaries if s.status == "warning"]
    ok      = [s for s in summaries if s.status == "ok"]

    print(
        f"\n  {'Account ID':<14}  {'Region':<16}  {'Instance Type':<20}"
        f"  {'Coverage':>9}  {'RI hrs':>9}  {'OD hrs':>9}  {'Total hrs':>10}"
    )
    print(f"  {'-' * 96}")

    for s in summaries:
        if s.status == "ok":
            pct_str = _c(f"{s.coverage_pct:8.1f}%", _GREEN)
        elif s.status == "warning":
            pct_str = _c(f"{s.coverage_pct:8.1f}%", _YELLOW)
        else:
            pct_str = _c(f"{s.coverage_pct:8.1f}%", _RED)

        print(
            f"  {s.account_id:<14}  {s.region:<16}  {s.instance_type:<20}"
            f"  {pct_str}  {s.covered_hours:>9.1f}  {s.on_demand_hours:>9.1f}  {s.total_hours:>10.1f}"
        )

    print()
    if low:
        total_od = sum(s.on_demand_hours for s in low)
        print(_c(f"  [!] Low coverage (<50%): {len(low)} group(s)  on-demand total: {total_od:,.1f} hrs", _RED))
    if warning:
        total_od = sum(s.on_demand_hours for s in warning)
        print(_c(f"  [!] Mid coverage (50-90%): {len(warning)} group(s)  on-demand total: {total_od:,.1f} hrs", _YELLOW))
    if ok and not low and not warning:
        print(_c("  [v] All groups have coverage >= 90%", _GREEN))


# ──────────────────────────────────────────────
# Utilization
# ──────────────────────────────────────────────

def print_utilization(summaries: list[UtilizationSummary], max_util: float | None = None) -> None:
    title = "RI Utilization  (Cost Explorer)"
    if max_util is not None:
        title += f"  [filter: util <= {max_util}%]"
    _header(title)

    if max_util is not None:
        summaries = [s for s in summaries if s.avg_utilization_pct <= max_util]

    if not summaries:
        print("\n  No data.")
        return

    col_header = (
        f"  {'Subscription ID':<16}  {'Instance Type':<20}"
        f"  {'Region':<16}  {'Avg Util':>9}  {'Unused hrs':>11}"
        f"  {'OD Cost':>10}  {'RI Cost':>10}  {'Net Savings':>12}  {'Judge':>5}"
    )
    col_sep = f"  {'-' * 118}"

    # platform ごとにグループ化（出現順を保持しつつ重複排除）
    platforms: list[str] = []
    seen_platforms: set[str] = set()
    for s in summaries:
        if s.platform not in seen_platforms:
            platforms.append(s.platform)
            seen_platforms.add(s.platform)

    for platform in platforms:
        group = [s for s in summaries if s.platform == platform]
        print(f"\n  [{platform}]")
        print(col_header)
        print(col_sep)
        for s in group:
            pct = s.avg_utilization_pct
            if s.status == "ok":
                pct_str = _c(f"{pct:8.1f}%", _GREEN)
            elif s.status == "warning":
                pct_str = _c(f"{pct:8.1f}%", _YELLOW)
            else:
                pct_str = _c(f"{pct:8.1f}%", _RED)

            if s.savings_status == "saving":
                savings_str = _c(f"{s.total_net_savings:>12.2f}", _GREEN)
                judge_str   = _c("  [+]", _GREEN)
            else:
                savings_str = _c(f"{s.total_net_savings:>12.2f}", _RED)
                judge_str   = _c("  [-]", _RED)

            print(
                f"  {s.subscription_id:<16}  {s.instance_type:<20}"
                f"  {s.region:<16}  {pct_str}  {s.total_unused_hours:>11.1f}"
                f"  {s.total_on_demand_cost:>10.2f}  {s.total_amortized_fee:>10.2f}"
                f"  {savings_str}{judge_str}"
            )
