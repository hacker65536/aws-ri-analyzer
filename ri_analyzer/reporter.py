"""Console output (English)"""

from __future__ import annotations

from ri_analyzer.analyzers.expiration import ExpirationResult
from ri_analyzer.analyzers.coverage import CoverageSummary
from ri_analyzer.analyzers.utilization import UtilizationSummary, _parse_instance_family, _parse_instance_prefix, _norm_factor_for_engine
from ri_analyzer.analyzers.cur_detail import CurInstanceRow, CurCoverageRow, UnusedRiRow, RecommendationFactcheck
from ri_analyzer.fetchers.cost_explorer import RiRecommendationGroup

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

def print_coverage(
    summaries: list[CoverageSummary],
    max_coverage: float | None = None,
    engines: list[str] | None = None,
    families: list[str] | None = None,
) -> None:
    title = "RI Coverage  (reserved vs on-demand hours)"
    if max_coverage is not None:
        title += f"  [filter: coverage <= {max_coverage}%]"
    if engines:
        title += f"  [engine: {', '.join(engines)}]"
    if families:
        title += f"  [family: {', '.join(families)}]"
    _header(title)

    if max_coverage is not None:
        summaries = [s for s in summaries if s.coverage_pct <= max_coverage]
    if engines:
        engines_lower = [e.lower() for e in engines]
        summaries = [s for s in summaries if any(e in s.platform.lower() for e in engines_lower)]
    if families:
        summaries = [s for s in summaries if _parse_instance_family(s.instance_type) in families]

    if not summaries:
        print("\n  No data.")
        return

    col_header = (
        f"\n  {'Account ID':<14}  {'Instance Type':<20}  {'Region':<16}"
        f"  {'Coverage':>9}  {'RI (hrs)':>9}  {'OD (hrs)':>9}  {'Total (hrs)':>12}"
    )
    col_sep = f"  {'-' * 99}"

    # platform ごとにグループ化（出現順を保持しつつ重複排除）
    platforms: list[str] = []
    seen_platforms: set[str] = set()
    for s in summaries:
        if s.platform not in seen_platforms:
            platforms.append(s.platform)
            seen_platforms.add(s.platform)

    for platform in platforms:
        plat_group = [s for s in summaries if s.platform == platform]
        print()
        print(_c(f"  ## {platform}", _BOLD))
        print(_c(f"  {'─' * 60}", _CYAN))

        # instance family ごとにグループ化
        families: list[str] = []
        seen_families: set[str] = set()
        for s in plat_group:
            fam = _parse_instance_family(s.instance_type)
            if fam not in seen_families:
                families.append(fam)
                seen_families.add(fam)

        for family in families:
            fam_group = [s for s in plat_group if _parse_instance_family(s.instance_type) == family]
            prefix = _parse_instance_prefix(fam_group[0].instance_type)
            print(f"\n  [{prefix}.{family}.*]")
            print(col_header)
            print(col_sep)

            for s in fam_group:
                if s.status == "ok":
                    pct_str = _c(f"{s.coverage_pct:8.1f}%", _GREEN)
                elif s.status == "warning":
                    pct_str = _c(f"{s.coverage_pct:8.1f}%", _YELLOW)
                else:
                    pct_str = _c(f"{s.coverage_pct:8.1f}%", _RED)

                print(
                    f"  {s.account_id:<14}  {s.instance_type:<20}  {s.region:<16}"
                    f"  {pct_str}  {s.covered_hours:>9.1f}  {s.on_demand_hours:>9.1f}  {s.total_hours:>10.1f}"
                )

            # family サマリ行（2件以上の場合のみ）
            if len(fam_group) >= 2:
                total_covered_nus  = sum(s.covered_nus for s in fam_group)
                total_od_nus       = sum(s.on_demand_nus for s in fam_group)
                total_nus          = sum(s.total_nus for s in fam_group)
                cov_pct = (total_covered_nus / total_nus * 100) if total_nus > 0 else 0.0
                if cov_pct >= 90:
                    pct_str = _c(f"{cov_pct:8.1f}%", _GREEN)
                elif cov_pct >= 50:
                    pct_str = _c(f"{cov_pct:8.1f}%", _YELLOW)
                else:
                    pct_str = _c(f"{cov_pct:8.1f}%", _RED)
                print(
                    _c(
                        f"  {'(total, NUs)':<14}  {prefix + '.' + family + '.*':<20}  {'':<16}"
                        f"  {cov_pct:8.1f}%  {total_covered_nus:>8.1f}N  {total_od_nus:>8.1f}N  {total_nus:>9.1f}N",
                        _CYAN,
                    )
                )

    # フッター統計
    low     = [s for s in summaries if s.status == "low"]
    warning = [s for s in summaries if s.status == "warning"]
    ok      = [s for s in summaries if s.status == "ok"]
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

# CE GetReservationUtilization が返す短縮 platform 名を
# GetReservationCoverage と同じ命名に正規化するマッピング。
# フィルタ時にこのマッピングを通すことで --engine の挙動を両セクションで統一する。
_UTIL_PLATFORM_NORMALIZE: dict[str, str] = {
    "aurora": "aurora mysql",
}


def _normalize_util_platform(platform: str) -> str:
    lower = platform.lower()
    return _UTIL_PLATFORM_NORMALIZE.get(lower, lower)


def print_utilization(
    summaries: list[UtilizationSummary],
    max_util: float | None = None,
    engines: list[str] | None = None,
    families: list[str] | None = None,
    show_sub_id: bool = False,
) -> None:
    title = "RI Utilization  (Cost Explorer)"
    if max_util is not None:
        title += f"  [filter: util <= {max_util}%]"
    if engines:
        title += f"  [engine: {', '.join(engines)}]"
    if families:
        title += f"  [family: {', '.join(families)}]"
    _header(title)

    if max_util is not None:
        summaries = [s for s in summaries if s.avg_utilization_pct <= max_util]
    if engines:
        engines_lower = [e.lower() for e in engines]
        summaries = [s for s in summaries if any(e in _normalize_util_platform(s.platform) for e in engines_lower)]
    if families:
        summaries = [s for s in summaries if _parse_instance_family(s.instance_type) in families]

    if not summaries:
        print("\n  No data.")
        return

    sub_id_col = f"{'Subscription ID':<16}  " if show_sub_id else ""
    col_header = (
        f"  {sub_id_col}{'Instance Type':<20}  {'Cnt':>3}  {'NUs':>6}"
        f"  {'Region':<16}  {'Avg Util':>9}  {'Unused':>11}"
        f"  {'OD Cost':>10}  {'RI Cost':>10}  {'Net Savings':>12}  {'Judge':>5}"
    )
    sep_width = 124 + (18 if show_sub_id else 0)
    col_sep = f"  {'-' * sep_width}"

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

        # family ごとにまとめて出力
        families: list[str] = []
        seen_families: set[str] = set()
        for s in group:
            fam = _parse_instance_family(s.instance_type)
            if fam not in seen_families:
                families.append(fam)
                seen_families.add(fam)

        for family in families:
            fam_group = [s for s in group if _parse_instance_family(s.instance_type) == family]

            for s in fam_group:
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

                sub_id_part = f"{s.subscription_id:<16}  " if show_sub_id else ""
                print(
                    f"  {sub_id_part}{s.instance_type:<20}  {s.count:>3}  {s.normalized_units:>6.1f}"
                    f"  {s.region:<16}  {pct_str}  {s.total_unused_hours:>7.1f} hrs"
                    f"  {s.total_on_demand_cost:>10.2f}  {s.total_amortized_fee:>10.2f}"
                    f"  {savings_str}{judge_str}"
                )

            # family サマリ行（2件以上の場合のみ表示）
            if len(fam_group) >= 2:
                total_nus     = sum(s.normalized_units for s in fam_group)
                unused_nus    = sum(s.total_unused_hours * _norm_factor_for_engine(s.instance_type, s.platform) for s in fam_group)
                total_od      = sum(s.total_on_demand_cost for s in fam_group)
                total_ri      = sum(s.total_amortized_fee for s in fam_group)
                total_sav     = sum(s.total_net_savings for s in fam_group)
                avg_pct       = (
                    sum(s.avg_utilization_pct * s.normalized_units for s in fam_group)
                    / total_nus
                ) if total_nus > 0 else 0.0
                prefix = _parse_instance_prefix(fam_group[0].instance_type)
                sub_id_pad = " " * 18 if show_sub_id else ""
                print(
                    _c(
                        f"  {sub_id_pad}{prefix + '.' + family + '.*':<20}    -  {total_nus:>6.1f}"
                        f"  {'(total)':<16}  {avg_pct:>8.1f}%  {unused_nus:>7.1f} NUs"
                        f"  {total_od:>10.2f}  {total_ri:>10.2f}  {total_sav:>12.2f}",
                        _CYAN,
                    )
                )


# ──────────────────────────────────────────────
# Recommendations
# ──────────────────────────────────────────────

def print_recommendations(
    groups: list[RiRecommendationGroup],
    service: str,
    term: str,
    payment_option: str,
    engines: list[str] | None = None,
    families: list[str] | None = None,
) -> None:
    term_label    = "1yr" if term == "ONE_YEAR" else "3yr"
    payment_label = payment_option.replace("_", " ").title()
    title = f"RI Recommendations  ({term_label} / {payment_label})"
    if engines:
        title += f"  [engine: {', '.join(engines)}]"
    if families:
        title += f"  [family: {', '.join(families)}]"
    _header(title)

    if not groups:
        print("\n  No recommendations available.")
        return

    col_header = (
        f"\n  {'Instance Type':<22}  {'Platform':<16}  {'Region':<16}"
        f"  {'Cnt':>3}  {'NUs':>6}  {'Upfront ($)':>11}  {'Savings/mo':>11}"
        f"  {'Savings%':>8}  {'Breakeven':>9}"
    )
    col_sep = f"  {'-' * 108}"

    engines_lower = [e.lower() for e in engines] if engines else None

    for group in groups:
        sorted_details = sorted(
            group.details,
            key=lambda d: (-d.estimated_monthly_savings, d.instance_type),
        )
        if engines_lower:
            sorted_details = [d for d in sorted_details if any(e in d.platform.lower() for e in engines_lower)]
        if families:
            sorted_details = [d for d in sorted_details if _parse_instance_family(d.instance_type) in families]

        if not sorted_details:
            continue

        print(f"\n  [{service.upper()}]  {group.currency}")
        print(col_header)
        print(col_sep)

        for d in sorted_details:
            savings_str = _c(f"${d.estimated_monthly_savings:>10.2f}", _GREEN)
            print(
                f"  {d.instance_type:<22}  {d.platform:<16}  {d.region:<16}"
                f"  {d.count:>3}  {d.normalized_units:>6.1f}  ${d.upfront_cost:>10.2f}"
                f"  {savings_str}  {d.estimated_savings_pct:>7.1f}%  {d.breakeven_months:>7.1f} mo"
            )

        print()
        filtered_total = sum(d.estimated_monthly_savings for d in sorted_details)
        total_str = _c(f"${filtered_total:,.2f}", _GREEN)
        print(
            _c(
                f"  Total estimated monthly savings: {total_str}",
                _BOLD,
            )
        )


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
            # CUR にデータなし → CE の推奨根拠が不明
            judge = _c("  [?] no CUR data", _YELLOW)
            gap_str = _c(f"{gap:>+6.1f}", _YELLOW)
        elif abs(gap) <= 0.5:
            judge = _c("  [v] match", _GREEN)
            gap_str = _c(f"{gap:>+6.1f}", _GREEN)
        elif gap > 0.5:
            # CE が多め → OD で稼働中のものがある → 購入余地あり
            judge = _c("  [+] buy candidate", _YELLOW)
            gap_str = _c(f"{gap:>+6.1f}", _YELLOW)
        else:
            # CE が少なめ → すでに RI でカバー済み
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
