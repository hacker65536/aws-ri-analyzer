#!/usr/bin/env python
"""CUR（Athena）と CE API のカバレッジ数値を突き合わせて精度を検証する。

CE と同じ期間（end = UTC now - 48h、start = end - lookback_days）で
CUR を line_item_usage_start_date でフィルタして比較する。

使い方:
    python compare_cur_ce.py --year 2026 --month 3 --service rds
    python compare_cur_ce.py --year 2026 --month 3 --service rds \
        --instance-type-prefix db.r8g --engine "Aurora MySQL"
"""

from __future__ import annotations

import argparse
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

from ri_analyzer.config import Config
from ri_analyzer.fetchers.athena import AthenaClient
from ri_analyzer.fetchers.cost_explorer import fetch_ri_coverage_range
from ri_analyzer.service_registry import SERVICES
from cur_analyzer import render_template

_TEMPLATE_DIR = Path(__file__).parent / "queries" / "templates"


def _ce_period(lookback_days: int) -> Tuple[str, str]:
    """CE と同じ (start_date, end_date) を返す（YYYY-MM-DD 形式）。"""
    now_utc    = datetime.now(timezone.utc)
    end_dt     = now_utc - timedelta(hours=48)
    end_date   = end_dt.date()
    start_date = end_date - timedelta(days=lookback_days)
    return str(start_date), str(end_date)


def _run_cur_coverage(
    client: AthenaClient,
    cfg,
    year: int,
    month: int,
    service: str,
    start_date: str,
    end_date: str,
) -> List[Dict]:
    """ri_coverage_period.sql を Athena で実行して CUR 側の集計を得る。"""
    tmpl = (_TEMPLATE_DIR / "ri_coverage_period.sql").read_text(encoding="utf-8")
    params = {
        "year":       str(year),
        "month":      str(month),
        "service":    service,
        "start_date": start_date,
        "end_date":   end_date,
        "database":   cfg.athena.database,
        "table":      cfg.athena.table,
    }
    sql = render_template(tmpl, params)

    tmp = tempfile.NamedTemporaryFile(suffix=".sql", mode="w", encoding="utf-8", delete=False)
    tmp.write(sql)
    tmp.close()

    result = client.run_from_file(Path(tmp.name), enforce_partition=True)
    return result.rows or []


def _key(account_id: str, region: str, instance_type: str) -> Tuple[str, str, str]:
    return (account_id, region, instance_type)


def _print_comparison(
    cur_rows: List[Dict],
    ce_records,
    instance_type_prefix: str | None,
    engine_filter: str | None,
) -> None:
    # CUR: account/region/instance_type → 集計（engine フィルタを CE 側と同じ条件で適用）
    cur_map: Dict[Tuple, Dict] = {}
    for row in cur_rows:
        itype = row.get("instance_type", "")
        engine = row.get("engine", "")
        if instance_type_prefix and not itype.startswith(instance_type_prefix):
            continue
        if engine_filter and engine_filter.lower() not in engine.lower():
            continue
        k = _key(row["account_id"], row["region"], itype)
        if k in cur_map:
            # 同キーが複数ある場合（エンジン名の表記ゆれ等）は合算
            cur_map[k]["ri_hours"]    += float(row.get("ri_hours", 0))
            cur_map[k]["od_hours"]    += float(row.get("od_hours", 0))
            cur_map[k]["total_hours"] += float(row.get("total_hours", 0))
        else:
            cur_map[k] = {
                "ri_hours":     float(row.get("ri_hours", 0)),
                "od_hours":     float(row.get("od_hours", 0)),
                "total_hours":  float(row.get("total_hours", 0)),
                "coverage_pct": float(row.get("coverage_pct", 0)),
            }

    # CE: account/region/instance_type → 集計（同キーは合算）
    ce_map: Dict[Tuple, Dict] = {}
    for rec in ce_records:
        if instance_type_prefix and not rec.instance_type.startswith(instance_type_prefix):
            continue
        if engine_filter and engine_filter.lower() not in rec.platform.lower():
            continue
        k = _key(rec.account_id, rec.region, rec.instance_type)
        if k in ce_map:
            ce_map[k]["ri_hours"]    += rec.covered_hours
            ce_map[k]["od_hours"]    += rec.on_demand_hours
            ce_map[k]["total_hours"] += rec.total_hours
        else:
            ce_map[k] = {
                "ri_hours":    rec.covered_hours,
                "od_hours":    rec.on_demand_hours,
                "total_hours": rec.total_hours,
                "coverage_pct": rec.coverage_pct,
            }

    all_keys = sorted(set(cur_map) | set(ce_map))
    if not all_keys:
        print("[INFO] 比較対象データが 0 件でした。フィルタ条件を確認してください。")
        return

    print(f"\n{'account_id':<14} {'region':<15} {'instance_type':<16} "
          f"{'total_CUR':>10} {'total_CE':>9} {'Δtotal':>8} "
          f"{'ri_CUR':>8} {'ri_CE':>8} {'Δri':>7} "
          f"{'cov%_CUR':>9} {'cov%_CE':>8}  {'status'}")
    print("-" * 135)

    sum_cur_total = sum_ce_total = 0.0
    sum_cur_ri    = sum_ce_ri    = 0.0
    ok = mismatch = only_cur = only_ce = 0

    for k in all_keys:
        account_id, region, instance_type = k
        cur = cur_map.get(k)
        ce  = ce_map.get(k)

        cur_total = cur["total_hours"]  if cur else 0.0
        ce_total  = ce["total_hours"]   if ce  else 0.0
        cur_ri    = cur["ri_hours"]     if cur else 0.0
        ce_ri     = ce["ri_hours"]      if ce  else 0.0
        cur_cov   = cur["coverage_pct"] if cur else 0.0
        ce_cov    = ce["coverage_pct"]  if ce  else 0.0

        delta_total = cur_total - ce_total
        delta_ri    = cur_ri    - ce_ri

        if   cur and not ce:          status = "CUR-only"; only_cur  += 1
        elif ce  and not cur:         status = "CE-only";  only_ce   += 1
        elif abs(delta_total) <= max(1.0, cur_total * 0.01):
                                      status = "OK";       ok        += 1
        else:                         status = "MISMATCH"; mismatch  += 1

        print(f"{account_id:<14} {region:<15} {instance_type:<16} "
              f"{cur_total:>10.1f} {ce_total:>9.1f} {delta_total:>+8.1f} "
              f"{cur_ri:>8.1f} {ce_ri:>8.1f} {delta_ri:>+7.1f} "
              f"{cur_cov:>9.1f} {ce_cov:>8.1f}  {status}")

        sum_cur_total += cur_total;  sum_ce_total += ce_total
        sum_cur_ri    += cur_ri;     sum_ce_ri    += ce_ri

    delta_total_sum = sum_cur_total - sum_ce_total
    delta_ri_sum    = sum_cur_ri    - sum_ce_ri

    print("-" * 135)
    print(f"\n合計 total : CUR={sum_cur_total:.1f}h  CE={sum_ce_total:.1f}h  "
          f"Δ={delta_total_sum:+.1f}h ({delta_total_sum / sum_ce_total * 100:+.2f}%)"
          if sum_ce_total else f"\n合計 total : CUR={sum_cur_total:.1f}h  CE={sum_ce_total:.1f}h")
    print(f"合計 ri    : CUR={sum_cur_ri:.1f}h  CE={sum_ce_ri:.1f}h  "
          f"Δ={delta_ri_sum:+.1f}h ({delta_ri_sum / sum_ce_ri * 100:+.2f}%)"
          if sum_ce_ri else f"合計 ri    : CUR={sum_cur_ri:.1f}h  CE={sum_ce_ri:.1f}h")
    print(f"\n判定: OK={ok}  MISMATCH={mismatch}  CUR-only={only_cur}  CE-only={only_ce}")


def main() -> None:
    parser = argparse.ArgumentParser(description="CUR vs CE カバレッジ比較")
    parser.add_argument("--year",  type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    parser.add_argument("--service", default="rds", choices=[k for k, v in SERVICES.items() if v.cur_product_code])
    parser.add_argument("--instance-type-prefix", default=None,
                        help="絞り込み (例: db.r8g)")
    parser.add_argument("--engine", default=None,
                        help="エンジン絞り込み (例: Aurora MySQL)")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()

    cfg = Config.load(args.config)
    if cfg.athena is None:
        print("[ERROR] config.yaml に athena セクションがありません", file=sys.stderr)
        sys.exit(1)

    # CE と同じ期間を計算
    start_date, end_date = _ce_period(cfg.analysis.lookback_days)

    service_code = SERVICES[args.service].cur_product_code
    print(f"[INFO] 比較期間   : {start_date} 〜 {end_date}  (lookback={cfg.analysis.lookback_days}d)")
    print(f"[INFO] CUR 月     : {args.year}-{args.month:02d}  (year/month パーティション)")
    print(f"[INFO] サービス   : {service_code}")
    if args.instance_type_prefix:
        print(f"[INFO] タイプ絞込 : {args.instance_type_prefix}.*")
    if args.engine:
        print(f"[INFO] エンジン   : {args.engine}")

    # CUR 側（Athena）
    print("\n[INFO] CUR (Athena) クエリ実行中...")
    client = AthenaClient(cfg.athena, payer_profile=cfg.payer.profile)
    cur_rows = _run_cur_coverage(
        client, cfg, args.year, args.month, service_code, start_date, end_date
    )
    print(f"[INFO] CUR: {len(cur_rows)} 行取得")

    # CE 側（API）
    print("[INFO] CE API 呼び出し中...")
    ce_records = fetch_ri_coverage_range(
        payer_profile=cfg.payer.profile,
        service=args.service,
        start_date=start_date,
        end_date=end_date,
    )
    print(f"[INFO] CE : {len(ce_records)} 行取得")

    _print_comparison(cur_rows, ce_records, args.instance_type_prefix, args.engine)


if __name__ == "__main__":
    main()
