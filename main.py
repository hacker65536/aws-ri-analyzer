#!/usr/bin/env python3
"""AWS RI Analyzer

Usage:
  python main.py
  python main.py --config path/to/config.yaml
  python main.py --service rds elasticache
  python main.py --section expiration coverage
  python main.py --max-util 80
  python main.py --max-coverage 90
  python main.py --no-color
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from ri_analyzer.config import Config
from ri_analyzer.profile_resolver import resolve_profile
from ri_analyzer.fetchers.cost_explorer import fetch_ri_subscriptions, fetch_ri_coverage, _ce_time_period
from ri_analyzer.analyzers import expiration as exp_analyzer
from ri_analyzer.analyzers import coverage as cov_analyzer
from ri_analyzer.analyzers import utilization as util_analyzer
from ri_analyzer import reporter


_ALL_SERVICES = ["rds", "elasticache", "opensearch"]
_ALL_SECTIONS = ["expiration", "coverage", "utilization"]


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
        help="Sections to display (default: all). e.g. --section expiration coverage",
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
    p.add_argument("--no-color", action="store_true", help="Disable colored output")
    return p


def main() -> None:
    args = build_parser().parse_args()

    if args.no_color:
        reporter.set_color(False)

    try:
        cfg = Config.load(args.config)
    except (FileNotFoundError, ValueError) as e:
        print(f"[ERROR] Failed to load config: {e}", file=sys.stderr)
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

    print(f"\nAWS RI Analyzer  [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    print(f"  Payer account : {cfg.payer.account_id}")
    print(f"  Services      : {', '.join(services)}")
    print(f"  Sections      : {', '.join(sections)}")
    print(f"  Regions       : {', '.join(cfg.analysis.regions)}")
    if args.max_util is not None:
        print(f"  Filter        : utilization <= {args.max_util}%")
    if args.max_coverage is not None:
        print(f"  Filter        : coverage <= {args.max_coverage}%")

    try:
        payer_profile = resolve_profile(account_id=cfg.payer.account_id)
    except ValueError as e:
        print(f"[ERROR] Failed to resolve payer profile:\n  {e}", file=sys.stderr)
        sys.exit(1)

    print(f"  Payer profile : {payer_profile}")

    for svc in services:
        if svc != "rds":
            print(f"\n  [{svc.upper()}] not yet implemented (TODO), skipping.")
            continue

        start, end = _ce_time_period(cfg.analysis.lookback_days)
        print(f"\n  -- RDS --")
        print(f"  CE period     : {start} to {end}  (end = UTC now - 48h)")

        # Fetch RI subscriptions + utilization from CE (payer account)
        print("  Fetching RI data (GetReservationUtilization)...", end="", flush=True)
        try:
            subscriptions, util_records = fetch_ri_subscriptions(
                payer_profile=payer_profile,
                service=svc,
                lookback_days=cfg.analysis.lookback_days,
            )
        except PermissionError as e:
            print(f"\n[ERROR] {e}", file=sys.stderr)
            sys.exit(1)
        print(f" {len(subscriptions)} subscription(s)")

        # Fetch coverage from CE (payer account)
        # TODO: For per-instance detail, query CUR via Athena (plan B)
        coverage_records = []
        if "coverage" in sections:
            print("  Fetching coverage (GetReservationCoverage)...", end="", flush=True)
            try:
                coverage_records = fetch_ri_coverage(
                    payer_profile=payer_profile,
                    service=svc,
                    lookback_days=cfg.analysis.lookback_days,
                )
            except PermissionError as e:
                print(f"\n  [WARN] Skipped coverage: {e}")
            print(f" {len(coverage_records)} record(s)")

        if "expiration" in sections:
            expired, warning, ok = exp_analyzer.analyze(
                subscriptions, warn_days=cfg.analysis.expiration_warn_days
            )
            reporter.print_expiration(
                expired, warning, ok, warn_days=cfg.analysis.expiration_warn_days
            )

        if "coverage" in sections:
            coverage_summaries = cov_analyzer.analyze(coverage_records)
            reporter.print_coverage(coverage_summaries, max_coverage=args.max_coverage)

        if "utilization" in sections:
            summaries = util_analyzer.summarize(util_records)
            reporter.print_utilization(summaries, max_util=args.max_util)

    print()


if __name__ == "__main__":
    main()
