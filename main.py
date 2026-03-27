#!/usr/bin/env python3
# PYTHON_ARGCOMPLETE_OK
"""AWS RI Analyzer

Usage:
  uv run ri-analyzer
  uv run ri-analyzer --config path/to/config.yaml
  uv run ri-analyzer --service rds elasticache
  uv run ri-analyzer --section expiration coverage
  uv run ri-analyzer --max-util 80
  uv run ri-analyzer --max-coverage 90
  uv run ri-analyzer --no-color
  uv run ri-analyzer --athena                              # CUR セクションを追加
  uv run ri-analyzer --athena --cur-year 2026 --cur-month 2
"""

from __future__ import annotations

import argparse
import argcomplete
import logging
import sys
from datetime import datetime, timezone

from botocore.exceptions import TokenRetrievalError, SSOTokenLoadError

from ri_analyzer.cache import CacheStore
from ri_analyzer.config import Config
from ri_analyzer.profile_resolver import resolve_profile
from ri_analyzer.fetchers.cost_explorer import fetch_ri_subscriptions, fetch_ri_coverage, fetch_ri_recommendations, _ce_time_period
from ri_analyzer.analyzers import expiration as exp_analyzer
from ri_analyzer.analyzers import coverage as cov_analyzer
from ri_analyzer.analyzers import utilization as util_analyzer
from ri_analyzer.analyzers.cur_detail import (
    parse_rds_instances, parse_elasticache_nodes, parse_opensearch_domains,
    parse_rds_instance_detail, parse_elasticache_node_detail, parse_opensearch_domain_detail,
    parse_cur_coverage, parse_unused_ri,
    factcheck_recommendations,
)
from ri_analyzer import reporter
from ri_analyzer.service_registry import get_service


_ALL_SERVICES = ["rds", "elasticache", "opensearch"]
_ALL_SECTIONS = ["expiration", "coverage", "utilization", "recommendations",
                 "cur_instance_detail", "cur_instances", "cur_coverage", "unused_ri"]

# Athena が必要なセクション
_ATHENA_SECTIONS = {"cur_instance_detail", "cur_instances", "cur_coverage", "unused_ri"}


def _prompt_multiselect(label: str, choices: list[str]) -> list[str]:
    """Interactive multi-select prompt. Returns a non-empty list."""
    print(f"\n  {label}")
    for i, c in enumerate(choices, 1):
        print(f"    {i}) {c}")
    while True:
        raw = input("  Enter numbers (space-separated) or 'all': ").strip()
        if raw.lower() == "all":
            return list(choices)
        try:
            indices = [int(x) for x in raw.split()]
            selected = [choices[i - 1] for i in indices if 1 <= i <= len(choices)]
            if selected:
                return selected
        except (ValueError, IndexError):
            pass
        print("  Invalid input. Try again.")


def _sso_expired_error(profile: str) -> None:
    logger.error(
        "AWS SSO session has expired.\n  Run the following command to log in again:\n\n"
        "    aws sso login --profile \"%s\"", profile
    )
    sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Analyze AWS Reserved Instance status")
    p.add_argument("--config", default=None, help="Path to config file (default: config.yaml)")
    p.add_argument(
        "--service",
        nargs="+",
        choices=["rds", "elasticache", "opensearch"],
        default=None,
        metavar="SERVICE",
        help="Services to analyze (default: from config.yaml). e.g. --service rds elasticache",
    )
    p.add_argument(
        "--section",
        nargs="+",
        choices=_ALL_SECTIONS,
        default=None,
        metavar="SECTION",
        help="Sections to display (default: all). e.g. --section expiration coverage recommendations",
    )
    p.add_argument(
        "--max-util",
        type=float,
        default=None,
        metavar="PCT",
        help="Show only RI subscriptions with avg utilization <= PCT%%",
    )
    p.add_argument(
        "--max-coverage",
        type=float,
        default=None,
        metavar="PCT",
        help="Show only coverage groups with coverage <= PCT%%",
    )
    p.add_argument(
        "--engine",
        nargs="+",
        default=None,
        metavar="ENGINE",
        help="Filter coverage by database engine (case-insensitive, partial match). e.g. --engine aurora mysql",
    )
    p.add_argument(
        "--family",
        nargs="+",
        default=None,
        metavar="FAMILY",
        help="Filter coverage by instance family. e.g. --family r6g t4g",
    )
    p.add_argument("--no-color", action="store_true", help="Disable colored output")
    p.add_argument("--show-sub-id", action="store_true", help="Show subscription ID column in utilization table")
    p.add_argument("--no-cache", action="store_true", help="Bypass cache and fetch fresh data from AWS")
    p.add_argument("--split-engine", action="store_true", help="Show Redis and Valkey as separate groups in coverage")
    p.add_argument("--output", choices=["console", "json"], default="console",
                   help="Output format: console (default) or json")
    p.add_argument("--verbose", action="store_true", help="Enable debug logging to stderr")

    p.add_argument(
        "--min-hours",
        type=float,
        default=None,
        metavar="HRS",
        help="cur_instance_detail: usage_hours >= HRS のインスタンスのみ表示（RI 購入候補絞り込み）",
    )

    # Athena / CUR オプション
    athena_grp = p.add_argument_group("Athena / CUR options")
    athena_grp.add_argument(
        "--athena",
        action="store_true",
        help="Enable Athena CUR sections (cur_instances, cur_coverage, unused_ri, ce_factcheck)",
    )
    athena_grp.add_argument(
        "--cur-year",
        type=int,
        default=None,
        metavar="YYYY",
        help="CUR query year (default: last month)",
    )
    athena_grp.add_argument(
        "--cur-month",
        type=int,
        default=None,
        metavar="M",
        help="CUR query month 1-12 (default: last month)",
    )
    argcomplete.autocomplete(p)
    return p


logger = logging.getLogger(__name__)


def main() -> None:
    args = build_parser().parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.WARNING,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )
    if not args.verbose:
        logging.getLogger("botocore").setLevel(logging.ERROR)

    if args.no_color:
        reporter.set_color(False)

    try:
        cfg = Config.load(args.config)
    except (FileNotFoundError, ValueError) as e:
        logger.error("Failed to load config: %s", e)
        sys.exit(1)

    # ── Resolve services ──────────────────────────────────────────
    # Priority: CLI --service > config.yaml services > interactive prompt
    config_dirty = False
    if args.service:
        services = args.service
    elif cfg.analysis.services:
        services = cfg.analysis.services
    else:
        services = _prompt_multiselect("Select services to analyze:", _ALL_SERVICES)
        cfg.analysis.services = services
        config_dirty = True

    # ── Resolve sections ──────────────────────────────────────────
    # Priority: CLI --section > config.yaml sections > interactive prompt
    if args.section:
        sections = args.section
    elif cfg.analysis.sections:
        sections = cfg.analysis.sections
    else:
        sections = _prompt_multiselect("Select sections to display:", _ALL_SECTIONS)
        cfg.analysis.sections = sections
        config_dirty = True

    if config_dirty:
        cfg.save()
        print(f"  [Saved] Selections written to config.yaml")

    # --athena フラグ → CUR セクションを sections に追加
    if args.athena:
        for s in ["cur_instance_detail", "cur_instances", "cur_coverage", "unused_ri"]:
            if s not in sections:
                sections.append(s)

    # CUR の期間を決定
    #   デフォルト: CE API と同じ期間（UTC now - 48h を end とした lookback_days 分）
    #   --cur-year / --cur-month 指定時: その月全体（月初〜翌月初）
    ce_start, ce_end = _ce_time_period(cfg.analysis.lookback_days)
    if args.cur_year and args.cur_month:
        import calendar
        cur_start = f"{args.cur_year}-{args.cur_month:02d}-01"
        last_day = calendar.monthrange(args.cur_year, args.cur_month)[1]
        next_y = args.cur_year + (1 if args.cur_month == 12 else 0)
        next_m = 1 if args.cur_month == 12 else args.cur_month + 1
        cur_end = f"{next_y}-{next_m:02d}-01"
    else:
        cur_start, cur_end = ce_start, ce_end

    # Athena セクションが必要な場合はクライアントを初期化
    athena_client = None
    needs_athena = args.athena or any(s in sections for s in _ATHENA_SECTIONS)
    if needs_athena:
        if cfg.athena is None:
            logger.error("config.yaml に athena セクションがありません。--athena を使う場合は設定してください。")
            sys.exit(1)
        from ri_analyzer.fetchers.athena import AthenaClient
        from ri_analyzer.fetchers.cur_queries import (
            running_rds_instances, running_elasticache_nodes, running_opensearch_domains,
            rds_instance_detail, elasticache_node_detail, opensearch_domain_detail,
            ri_coverage_detail, unused_ri_cost,
        )
        athena_client = AthenaClient(cfg.athena, payer_profile=cfg.payer.profile if cfg.payer.profile else None)

    cache = CacheStore(ttl_hours=cfg.analysis.cache_ttl_hours)
    use_json = args.output == "json"
    json_results: dict = {}  # --output json 時に収集する

    if not use_json:
        print(f"\nAWS RI Analyzer  [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
        print(f"  Config        : {cfg._path}")
        print(f"  Payer account : {cfg.payer.account_id}")
        print(f"  Services      : {', '.join(services)}")
        print(f"  Sections      : {', '.join(sections)}")
        print(f"  Regions       : {', '.join(cfg.analysis.regions)}")
        if args.max_util is not None:
            print(f"  Filter        : utilization <= {args.max_util}%")
        if args.max_coverage is not None:
            print(f"  Filter        : coverage <= {args.max_coverage}%")
        if args.engine:
            print(f"  Filter        : engine = {', '.join(args.engine)}")
        if args.family:
            print(f"  Filter        : family = {', '.join(args.family)}")
        if needs_athena:
            print(f"  CUR period    : {cur_start} to {cur_end}")

    if cfg.payer.profile:
        payer_profile = cfg.payer.profile
    else:
        try:
            payer_profile = resolve_profile(account_id=cfg.payer.account_id)
        except ValueError as e:
            logger.error("Failed to resolve payer profile:\n  %s", e)
            sys.exit(1)

    print(f"  Payer profile : {payer_profile}")

    for svc in services:
        start, end = _ce_time_period(cfg.analysis.lookback_days)
        if not use_json:
            print(f"\n  -- {svc.upper()} --")
            print(f"  CE period     : {start} to {end}  (end = UTC now - 48h)")
        json_results[svc] = {"ce_period": {"start": start, "end": end}}

        rec_groups: list = []  # recommendations セクションで設定される（factcheck で参照）

        # Fetch RI subscriptions + utilization from CE (payer account)
        sub_key = f"subscriptions:{payer_profile}:{svc}:{cfg.analysis.lookback_days}:{end}"
        cached_sub = None if args.no_cache else cache.get(sub_key)
        if cached_sub is not None:
            subscriptions, util_records = cached_sub
            print(f"  RI data (GetReservationUtilization)          : {len(subscriptions)} subscription(s)  [cache {cache.created_at(sub_key)}]")
        else:
            print("  Fetching RI data (GetReservationUtilization)...", end="", flush=True)
            try:
                subscriptions, util_records = fetch_ri_subscriptions(
                    payer_profile=payer_profile,
                    service=svc,
                    lookback_days=cfg.analysis.lookback_days,
                )
            except (TokenRetrievalError, SSOTokenLoadError):
                _sso_expired_error(payer_profile)
            except PermissionError as e:
                logger.error("%s", e)
                sys.exit(1)
            cache.set(sub_key, (subscriptions, util_records))
            print(f" {len(subscriptions)} subscription(s)")

        # Fetch coverage from CE (payer account)
        # TODO: For per-instance detail, query CUR via Athena (plan B)
        coverage_records = []
        if "coverage" in sections:
            cov_key = f"coverage:{payer_profile}:{svc}:{cfg.analysis.lookback_days}:{end}"
            cached_cov = None if args.no_cache else cache.get(cov_key)
            if cached_cov is not None:
                coverage_records = cached_cov
                print(f"  Coverage (GetReservationCoverage)            : {len(coverage_records)} record(s)  [cache {cache.created_at(cov_key)}]")
            else:
                print("  Fetching coverage (GetReservationCoverage)...", end="", flush=True)
                try:
                    coverage_records = fetch_ri_coverage(
                        payer_profile=payer_profile,
                        service=svc,
                        lookback_days=cfg.analysis.lookback_days,
                    )
                except (TokenRetrievalError, SSOTokenLoadError):
                    _sso_expired_error(payer_profile)
                except PermissionError as e:
                    logger.warning("Skipped coverage: %s", e)
                else:
                    cache.set(cov_key, coverage_records)
                print(f" {len(coverage_records)} record(s)")

        if "expiration" in sections:
            expired, warning, ok = exp_analyzer.analyze(
                subscriptions, warn_days=cfg.analysis.expiration_warn_days
            )
            if use_json:
                json_results[svc]["expiration"] = {
                    "expired": expired,
                    "warning": warning,
                    "ok":      ok,
                }
            else:
                reporter.print_expiration(
                    expired, warning, ok, warn_days=cfg.analysis.expiration_warn_days
                )

        svc_cfg = get_service(svc)

        if "coverage" in sections:
            coverage_summaries = cov_analyzer.analyze(coverage_records, split_engine=args.split_engine)
            if use_json:
                json_results[svc]["coverage"] = coverage_summaries
            else:
                reporter.print_coverage(
                    coverage_summaries,
                    max_coverage=args.max_coverage,
                    engines=args.engine,
                    families=args.family,
                    use_family_summary=svc_cfg.has_nu_flexibility,
                )

        if "utilization" in sections:
            summaries = util_analyzer.summarize(util_records)
            if use_json:
                json_results[svc]["utilization"] = summaries
            else:
                reporter.print_utilization(summaries, max_util=args.max_util, engines=args.engine, families=args.family, show_sub_id=args.show_sub_id, use_family_summary=svc_cfg.has_nu_flexibility)

        if "recommendations" in sections:
            rec_cfg = cfg.recommendation
            rec_key = f"recommendations:{payer_profile}:{svc}:{rec_cfg.term}:{rec_cfg.payment_option}:{rec_cfg.lookback_days}:{end}"
            cached_rec = None if args.no_cache else cache.get(rec_key)
            if cached_rec is not None:
                rec_groups = cached_rec
                print(f"  Recommendations (GetReservationPurchaseRecommendation): {sum(len(g.details) for g in rec_groups)} item(s)  [cache {cache.created_at(rec_key)}]")
            else:
                print("  Fetching recommendations (GetReservationPurchaseRecommendation)...", end="", flush=True)
                try:
                    rec_groups = fetch_ri_recommendations(
                        payer_profile=payer_profile,
                        service=svc,
                        term=rec_cfg.term,
                        payment_option=rec_cfg.payment_option,
                        lookback_days=rec_cfg.lookback_days,
                    )
                except (TokenRetrievalError, SSOTokenLoadError):
                    _sso_expired_error(payer_profile)
                except PermissionError as e:
                    logger.warning("Skipped recommendations: %s", e)
                    rec_groups = []
                else:
                    cache.set(rec_key, rec_groups)
                print(f" {sum(len(g.details) for g in rec_groups)} item(s)")
            if use_json:
                json_results[svc]["recommendations"] = rec_groups
            else:
                reporter.print_recommendations(
                    rec_groups,
                    service=svc,
                    term=rec_cfg.term,
                    payment_option=rec_cfg.payment_option,
                    engines=args.engine,
                    families=args.family,
                )

        # ── Athena / CUR セクション ───────────────────────────────
        if athena_client is None:
            continue

        _svc_label = {"rds": "AmazonRDS", "elasticache": "AmazonElastiCache", "opensearch": "AmazonES"}.get(svc)
        if _svc_label is None:
            continue  # Athena クエリ未対応サービスはスキップ

        if "cur_instance_detail" in sections:
            detail_key = f"athena:instance_detail:{svc}:{cur_start}:{cur_end}"
            cached_detail = None if args.no_cache else cache.get(detail_key)
            if cached_detail is not None:
                detail_rows = cached_detail
                print(f"  CUR instance detail                                  : {len(detail_rows)} instance(s)  [cache {cache.created_at(detail_key)}]")
            else:
                print(f"  Fetching CUR instance detail (Athena, {cur_start} to {cur_end})...", end="", flush=True)
                try:
                    if svc == "rds":
                        raw = rds_instance_detail(athena_client, start_date=cur_start, end_date=cur_end, regions=cfg.analysis.regions or None)
                        detail_rows = parse_rds_instance_detail(raw)
                    elif svc == "elasticache":
                        raw = elasticache_node_detail(athena_client, start_date=cur_start, end_date=cur_end, regions=cfg.analysis.regions or None)
                        detail_rows = parse_elasticache_node_detail(raw)
                    else:  # opensearch
                        raw = opensearch_domain_detail(athena_client, start_date=cur_start, end_date=cur_end, regions=cfg.analysis.regions or None)
                        detail_rows = parse_opensearch_domain_detail(raw)
                    cache.set(detail_key, detail_rows)
                    print(f" {len(detail_rows)} instance(s)")
                except Exception as e:
                    logger.warning("Skipped CUR instance detail: %s", e)
                    detail_rows = []
            if detail_rows:
                if use_json:
                    json_results[svc]["cur_instance_detail"] = detail_rows
                else:
                    reporter.print_cur_instance_detail(
                        detail_rows, svc, cur_start, cur_end,
                        min_hours=args.min_hours,
                    )

        if "cur_instances" in sections or args.athena:
            cur_inst_key = f"athena:instances:{svc}:{cur_start}:{cur_end}"
            cached_inst = None if args.no_cache else cache.get(cur_inst_key)
            if cached_inst is not None:
                cur_inst_rows = cached_inst
                print(f"  CUR instances                                        : {len(cur_inst_rows)} row(s)  [cache {cache.created_at(cur_inst_key)}]")
            else:
                print(f"  Fetching CUR instances (Athena, {cur_start} to {cur_end})...", end="", flush=True)
                try:
                    if svc == "rds":
                        raw = running_rds_instances(athena_client, start_date=cur_start, end_date=cur_end, regions=cfg.analysis.regions or None)
                        cur_inst_rows = parse_rds_instances(raw)
                    elif svc == "elasticache":
                        raw = running_elasticache_nodes(athena_client, start_date=cur_start, end_date=cur_end, regions=cfg.analysis.regions or None)
                        cur_inst_rows = parse_elasticache_nodes(raw)
                    else:  # opensearch
                        raw = running_opensearch_domains(athena_client, start_date=cur_start, end_date=cur_end, regions=cfg.analysis.regions or None)
                        cur_inst_rows = parse_opensearch_domains(raw)
                    cache.set(cur_inst_key, cur_inst_rows)
                    print(f" {len(cur_inst_rows)} row(s)")
                except Exception as e:
                    logger.warning("Skipped CUR instances: %s", e)
                    cur_inst_rows = []

            if "cur_instances" in sections and cur_inst_rows:
                if use_json:
                    json_results[svc]["cur_instances"] = cur_inst_rows
                else:
                    reporter.print_cur_instances(cur_inst_rows, svc, cur_start, cur_end)

            # CE Recommendation ファクトチェック（recommendations も実行済みの場合）
            if args.athena and "recommendations" in sections and rec_groups and cur_inst_rows:
                all_details = [d for g in rec_groups for d in g.details]
                if args.engine:
                    engines_lower = [e.lower() for e in args.engine]
                    all_details = [d for d in all_details if any(e in d.platform.lower() for e in engines_lower)]
                checks = factcheck_recommendations(all_details, cur_inst_rows)
                if use_json:
                    json_results[svc]["ce_factcheck"] = checks
                else:
                    reporter.print_ce_factcheck(checks, svc, cur_start, cur_end)

        if "cur_coverage" in sections:
            cur_cov_key = f"athena:coverage:{svc}:{cur_start}:{cur_end}"
            cached_cov = None if args.no_cache else cache.get(cur_cov_key)
            if cached_cov is not None:
                cur_cov_rows = cached_cov
                print(f"  CUR coverage                                         : {len(cur_cov_rows)} row(s)  [cache {cache.created_at(cur_cov_key)}]")
            else:
                print(f"  Fetching CUR coverage (Athena, {cur_start} to {cur_end})...", end="", flush=True)
                try:
                    raw = ri_coverage_detail(athena_client, start_date=cur_start, end_date=cur_end, service=_svc_label)
                    cur_cov_rows = parse_cur_coverage(raw)
                    cache.set(cur_cov_key, cur_cov_rows)
                    print(f" {len(cur_cov_rows)} row(s)")
                except Exception as e:
                    logger.warning("Skipped CUR coverage: %s", e)
                    cur_cov_rows = []
            if cur_cov_rows:
                if use_json:
                    json_results[svc]["cur_coverage"] = cur_cov_rows
                else:
                    reporter.print_cur_coverage(cur_cov_rows, svc, cur_start, cur_end)

        if "unused_ri" in sections:
            unused_key = f"athena:unused_ri:{svc}:{cur_start}:{cur_end}"
            cached_unused = None if args.no_cache else cache.get(unused_key)
            if cached_unused is not None:
                unused_rows = cached_unused
                print(f"  CUR unused RI                                        : {len(unused_rows)} row(s)  [cache {cache.created_at(unused_key)}]")
            else:
                print(f"  Fetching CUR unused RI (Athena, {cur_start} to {cur_end})...", end="", flush=True)
                try:
                    raw = unused_ri_cost(athena_client, start_date=cur_start, end_date=cur_end, service=_svc_label)
                    unused_rows = parse_unused_ri(raw)
                    cache.set(unused_key, unused_rows)
                    print(f" {len(unused_rows)} row(s)")
                except Exception as e:
                    logger.warning("Skipped CUR unused RI: %s", e)
                    unused_rows = []
            if use_json:
                json_results[svc]["unused_ri"] = unused_rows
            else:
                reporter.print_unused_ri(unused_rows, svc, cur_year, cur_month)

    if use_json:
        from ri_analyzer.reporter.json_output import dump
        dump(json_results)
    else:
        print()


if __name__ == "__main__":
    main()
