#!/usr/bin/env python3
"""
aurora-qps.py  ─  Aurora DB インスタンス / クラスターの QPS を調査する

CloudWatch の Queries メトリクスと Performance Insights (Database Insights) の
カウンターメトリクスから最大 QPS・平均 QPS を取得して比較表示します。

Performance Insights と Database Insights は同一の boto3 `pi` クライアント /
API エンドポイントを使用しているため、本スクリプトはどちらのティアでも動作します。

メトリクスソース
----------------
  [CloudWatch]
    Namespace : AWS/RDS
    MetricName: Queries
    単位      : Count/Second (既に QPS として格納済み)
    対応エンジン: Aurora MySQL / Aurora PostgreSQL

  [Performance Insights / Database Insights]
    Aurora MySQL      : db.SQL.Queries.sum / period_sec → QPS
    Aurora PostgreSQL : 直接的な QPS カウンターなし → CloudWatch を参照

Usage
-----
  # DB インスタンス ARN を直接指定
  python scripts/aurora-qps.py arn:aws:rds:<region>:<account>:db:<id>

  # クラスター ARN を指定 (クラスター合算 + インスタンス別を 1 表で表示)
  python scripts/aurora-qps.py arn:aws:rds:<region>:<account>:cluster:<id>

  # AWS プロファイルを指定 (SSO の場合は AWS_CONFIG_FILE も設定すること)
  python scripts/aurora-qps.py <ARN> --profile awssso-myaccount-123456789012:AWSReadOnlyAccess

  # 日別テーブルで表示 (period を自動的に 86400s に設定)
  python scripts/aurora-qps.py <ARN> --daily

  # 期間・粒度を変更
  python scripts/aurora-qps.py <ARN> --days 7 --period 300

  # Performance Insights をスキップ (CloudWatch のみ)
  python scripts/aurora-qps.py <ARN> --no-pi

Prerequisites
-------------
  pip install boto3

  # AWS SSO を使う場合
  export AWS_CONFIG_FILE=~/.aws/aws-sso-config
"""

from __future__ import annotations

import argparse
import sys
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import boto3
from botocore.exceptions import ClientError

JST = timezone(timedelta(hours=9))


# ============================================================
# ARN ユーティリティ
# ============================================================

@dataclass
class RdsArn:
    region: str
    account_id: str
    resource_type: str  # 'db' or 'cluster'
    resource_id: str


def parse_rds_arn(arn: str) -> RdsArn:
    parts = arn.split(':')
    if len(parts) < 7 or parts[2] != 'rds':
        raise ValueError(
            f"無効な RDS ARN: {arn!r}\n"
            "期待する形式:\n"
            "  arn:aws:rds:<region>:<account>:db:<id>\n"
            "  arn:aws:rds:<region>:<account>:cluster:<id>"
        )
    return RdsArn(
        region=parts[3],
        account_id=parts[4],
        resource_type=parts[5],
        resource_id=parts[6],
    )


# ============================================================
# RDS ヘルパー
# ============================================================

def get_cluster_instance_ids(
    rds, cluster_id: str
) -> tuple[list[str], dict[str, bool]]:
    """
    Returns
    -------
    (instance_ids, roles)
      instance_ids : クラスターメンバーの DBInstanceIdentifier リスト
      roles        : {instance_id: is_writer} の辞書
    """
    resp = rds.describe_db_clusters(DBClusterIdentifier=cluster_id)
    clusters = resp.get('DBClusters', [])
    if not clusters:
        raise ValueError(f"クラスターが見つかりません: {cluster_id}")
    members = clusters[0].get('DBClusterMembers', [])
    instance_ids = [m['DBInstanceIdentifier'] for m in members]
    roles = {m['DBInstanceIdentifier']: m['IsClusterWriter'] for m in members}
    return instance_ids, roles


def describe_instances(rds, instance_ids: list[str]) -> dict[str, dict]:
    """
    Returns: {instance_id: {dbi_resource_id, engine}}

    Note: PerformanceInsightsEnabled フラグは Database Insights 移行中に実態と
    乖離することがあるため取得しない。PI の有効/無効は API 呼び出し時のエラーで判断する。
    """
    result: dict[str, dict] = {}
    for iid in instance_ids:
        try:
            resp = rds.describe_db_instances(DBInstanceIdentifier=iid)
            inst = resp['DBInstances'][0]
            result[iid] = {
                'dbi_resource_id': inst['DbiResourceId'],
                'engine': inst.get('Engine', ''),
            }
        except ClientError as e:
            print(
                f"  [WARN] describe_db_instances({iid}): "
                f"{e.response['Error']['Message']}",
                file=sys.stderr,
            )
    return result


# ============================================================
# CloudWatch
# ============================================================

def _build_cw_stats(
    ts_max: dict[datetime, float],
    ts_avg: dict[datetime, float],
) -> dict:
    common_ts = sorted(set(ts_max.keys()) & set(ts_avg.keys()))
    timeseries = [
        {'ts': ts, 'max': ts_max[ts], 'avg': ts_avg[ts]}
        for ts in common_ts
    ]
    max_vals = list(ts_max.values())
    avg_vals = [ts_avg[ts] for ts in common_ts] or list(ts_avg.values())
    return {
        'max_qps': max(max_vals) if max_vals else 0.0,
        'avg_qps': sum(avg_vals) / len(avg_vals) if avg_vals else 0.0,
        'data_points': len(avg_vals),
        'timeseries': timeseries,
    }


def fetch_cloudwatch_qps(
    cw,
    instance_ids: list[str],
    start_time: datetime,
    end_time: datetime,
    period_sec: int,
) -> Optional[dict]:
    """
    Returns
    -------
    {
      'cluster':   {max_qps, avg_qps, data_points, timeseries},
      'instances': {instance_id: {max_qps, avg_qps, data_points, timeseries}},
    }
    or None

    timeseries 各要素: {ts: datetime, max: float, avg: float}
      max = 集計 period 内で最も高かった 1 分平均 QPS (クラスター合算 or インスタンス単体)
      avg = 集計 period 全体の平均 QPS
    """
    cluster_ts_max: dict[datetime, float] = {}
    cluster_ts_avg: dict[datetime, float] = {}
    per_instance: dict[str, dict] = {}

    for instance_id in instance_ids:
        inst_ts_max: dict[datetime, float] = {}
        inst_ts_avg: dict[datetime, float] = {}

        metric_queries = [
            {
                'Id': 'q_max',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'Queries',
                        'Dimensions': [
                            {'Name': 'DBInstanceIdentifier', 'Value': instance_id}
                        ],
                    },
                    'Period': period_sec,
                    'Stat': 'Maximum',
                },
                'ReturnData': True,
            },
            {
                'Id': 'q_avg',
                'MetricStat': {
                    'Metric': {
                        'Namespace': 'AWS/RDS',
                        'MetricName': 'Queries',
                        'Dimensions': [
                            {'Name': 'DBInstanceIdentifier', 'Value': instance_id}
                        ],
                    },
                    'Period': period_sec,
                    'Stat': 'Average',
                },
                'ReturnData': True,
            },
        ]

        next_token = None
        while True:
            kwargs: dict = dict(
                MetricDataQueries=metric_queries,
                StartTime=start_time,
                EndTime=end_time,
            )
            if next_token:
                kwargs['NextToken'] = next_token
            try:
                resp = cw.get_metric_data(**kwargs)
            except ClientError as e:
                print(
                    f"  [WARN] CloudWatch get_metric_data({instance_id}): "
                    f"{e.response['Error']['Message']}",
                    file=sys.stderr,
                )
                break

            for mdr in resp.get('MetricDataResults', []):
                inst_t = inst_ts_max if mdr['Id'] == 'q_max' else inst_ts_avg
                clst_t = cluster_ts_max if mdr['Id'] == 'q_max' else cluster_ts_avg
                for ts, val in zip(mdr['Timestamps'], mdr['Values']):
                    inst_t[ts] = inst_t.get(ts, 0.0) + val
                    clst_t[ts] = clst_t.get(ts, 0.0) + val

            next_token = resp.get('NextToken')
            if not next_token:
                break

        if inst_ts_avg or inst_ts_max:
            per_instance[instance_id] = _build_cw_stats(inst_ts_max, inst_ts_avg)

    if not cluster_ts_avg and not cluster_ts_max:
        return None

    return {
        'cluster': _build_cw_stats(cluster_ts_max, cluster_ts_avg),
        'instances': per_instance,
    }


# ============================================================
# Performance Insights / Database Insights
# ============================================================

_PI_QPS_METRIC: dict[str, Optional[str]] = {
    'mysql':      'db.SQL.Queries.sum',
    'postgresql': None,
}


def _pi_metric_for_engine(engine: str) -> Optional[str]:
    for key, metric in _PI_QPS_METRIC.items():
        if key in engine.lower():
            return metric
    return None


def fetch_pi_qps(
    pi,
    dbi_resource_id: str,
    engine: str,
    start_time: datetime,
    end_time: datetime,
    period_sec: int,
) -> dict:
    """
    Returns
    -------
    {max_qps, avg_qps, data_points, metric, period_sec, timeseries}  or  {note}

    timeseries: [{ts: datetime, avg: float}]  ※PI カウンターは avg のみ
    """
    metric_name = _pi_metric_for_engine(engine)
    if metric_name is None:
        return {
            'note': (
                f"{engine} には PI に直接的な QPS カウンターがありません。"
                " CloudWatch の値を使用してください。"
            )
        }

    pi_period = period_sec
    span_days = (end_time - start_time).days
    if span_days > 28 and pi_period < 3600:
        print(f"  [PI] 期間 {span_days}日 > 28日 のため集計粒度を {pi_period}s → 3600s に変更")
        pi_period = 3600

    ts_qps: dict[datetime, float] = {}
    next_token = None

    while True:
        kwargs: dict = dict(
            ServiceType='RDS',
            Identifier=dbi_resource_id,
            MetricQueries=[{'Metric': metric_name}],
            StartTime=start_time,
            EndTime=end_time,
            PeriodInSeconds=pi_period,
        )
        if next_token:
            kwargs['NextToken'] = next_token
        try:
            resp = pi.get_resource_metrics(**kwargs)
        except ClientError as e:
            code = e.response['Error']['Code']
            msg = e.response['Error']['Message']
            if code in ('InvalidArgumentException', 'NotAuthorizedException',
                        'InvalidParameterCombinationException'):
                return {'note': f"PI 利用不可 ({code}): {msg}"}
            raise

        for metric_result in resp.get('MetricList', []):
            for dp in metric_result.get('DataPoints', []):
                val = dp.get('Value')
                ts = dp.get('Timestamp')
                if val is not None and ts is not None:
                    ts_qps[ts] = ts_qps.get(ts, 0.0) + val / pi_period

        next_token = resp.get('NextToken')
        if not next_token:
            break

    if not ts_qps:
        return {
            'note': (
                'PI に値なし (db.SQL.Queries.sum が null)。'
                ' このインスタンスでは PI がクエリ数を計測していません。'
                ' Writer/Reader の役割によっては計測対象外となる場合があります。'
                ' CloudWatch の値を参照してください。'
            )
        }

    qps_values = list(ts_qps.values())
    return {
        'max_qps': max(qps_values),
        'avg_qps': sum(qps_values) / len(qps_values),
        'data_points': len(qps_values),
        'metric': metric_name,
        'period_sec': pi_period,
        'timeseries': sorted(
            [{'ts': ts, 'avg': v} for ts, v in ts_qps.items()],
            key=lambda x: x['ts'],
        ),
    }


# ============================================================
# 表示ヘルパー
# ============================================================

def _alias_map(instance_ids: list[str]) -> dict[str, str]:
    """インスタンス ID を短縮エイリアス (inst-1, inst-2, ...) に変換する。"""
    return {iid: f"inst-{i + 1}" for i, iid in enumerate(instance_ids)}


def _print_legend(
    instance_ids: list[str],
    aliases: dict[str, str],
    roles: Optional[dict[str, bool]] = None,
) -> None:
    print("  凡例:")
    for iid in instance_ids:
        role_str = ""
        if roles is not None:
            role_str = " (Writer)" if roles.get(iid) else " (Reader)"
        print(f"    {aliases[iid]}: {iid}{role_str}")
    print()


# --- CloudWatch 統合テーブル ---

def print_cw_table(
    cw_data: Optional[dict],
    daily: bool,
    multi_instance: bool,
    instance_ids: list[str],
    roles: Optional[dict[str, bool]] = None,
) -> None:
    """
    CloudWatch 結果を 1 つのテーブルで表示する。

    daily=True  : 行=日付、列=cluster+各インスタンス の max/avg
    daily=False : サマリー行のみ
    """
    if cw_data is None:
        print("  データなし")
        return

    aliases = _alias_map(instance_ids)
    if multi_instance:
        _print_legend(instance_ids, aliases, roles)

    # 列定義: (header, lookup={ts: dp_dict})
    # cluster 合算 は multi_instance のときのみ先頭に追加
    ColDef = tuple  # (label: str, lookup: dict[datetime, dict])
    cols: list[ColDef] = []
    if multi_instance:
        cols.append(("cluster", {dp['ts']: dp for dp in cw_data['cluster']['timeseries']}))
    for iid in instance_ids:
        if iid in cw_data['instances']:
            alias = aliases[iid]
            cols.append((alias, {dp['ts']: dp for dp in cw_data['instances'][iid]['timeseries']}))

    if daily:
        _print_cw_daily_table(cols, cw_data, multi_instance, instance_ids, aliases)
    else:
        _print_cw_summary(cw_data, multi_instance, instance_ids, aliases)


def _print_cw_daily_table(cols, cw_data, multi_instance, instance_ids, aliases) -> None:
    W_DATE = 10   # "2026-03-14"
    W_NUM  = 7    # "12345.6"
    W_PAIR = W_NUM * 2 + 2   # "max     avg" = 7 + 2sp + 7 = 16
    W_GAP  = 2    # 列間スペース

    # 全タイムスタンプ収集
    all_ts = sorted({ts for _, lookup in cols for ts in lookup})

    # ヘッダー行1: 列ラベル (max/avg ペアの中央に配置)
    hdr1 = " " * (W_DATE + W_GAP)
    for label, _ in cols:
        hdr1 += f"{label:^{W_PAIR + W_GAP}}"

    # ヘッダー行2: "Date" + "max  avg" の繰り返し
    hdr2 = f"{'Date (JST)':<{W_DATE}}{' ' * W_GAP}"
    for _ in cols:
        hdr2 += f"{'max':>{W_NUM}}  {'avg':>{W_NUM}}{' ' * W_GAP}"

    sep = "─" * W_DATE + "─" * W_GAP + ("─" * W_PAIR + "─" * W_GAP) * len(cols)

    print(f"  {hdr1}")
    print(f"  {hdr2}")
    print(f"  {sep}")

    for ts in all_ts:
        date_str = ts.astimezone(JST).strftime('%Y-%m-%d')
        row = f"{date_str:<{W_DATE}}{' ' * W_GAP}"
        for _, lookup in cols:
            dp = lookup.get(ts)
            if dp:
                row += f"{dp['max']:>{W_NUM}.1f}  {dp['avg']:>{W_NUM}.1f}{' ' * W_GAP}"
            else:
                row += f"{'─':>{W_NUM}}  {'─':>{W_NUM}}{' ' * W_GAP}"
        print(f"  {row}")

    print(f"  {sep}")

    # 月間サマリー行
    row = f"{'Total':<{W_DATE}}{' ' * W_GAP}"
    if multi_instance:
        c = cw_data['cluster']
        row += f"{c['max_qps']:>{W_NUM}.1f}  {c['avg_qps']:>{W_NUM}.1f}{' ' * W_GAP}"
    for iid in instance_ids:
        if iid in cw_data['instances']:
            s = cw_data['instances'][iid]
            row += f"{s['max_qps']:>{W_NUM}.1f}  {s['avg_qps']:>{W_NUM}.1f}{' ' * W_GAP}"
    print(f"  {row}")


def _print_cw_summary(cw_data, multi_instance, instance_ids, aliases) -> None:
    def _line(label, stats):
        return (f"  {label:<28}  "
                f"最大: {stats['max_qps']:>8.1f} QPS   "
                f"平均: {stats['avg_qps']:>8.1f} QPS   "
                f"({stats['data_points']} pts)")

    if multi_instance:
        print(_line("cluster (合算)", cw_data['cluster']))
    for iid in instance_ids:
        if iid in cw_data['instances']:
            label = aliases[iid] if multi_instance else iid
            print(_line(label, cw_data['instances'][iid]))


# --- PI 統合テーブル ---

def print_pi_table(
    pi_results: dict[str, dict],
    daily: bool,
    instance_ids: list[str],
    roles: Optional[dict[str, bool]] = None,
) -> None:
    """
    PI 結果を 1 つのテーブルで表示する (インスタンス別・avg のみ)。

    daily=True  : 行=日付、列=各インスタンスの avg
    daily=False : サマリー行のみ
    """
    aliases = _alias_map(instance_ids)
    multi = len(instance_ids) > 1

    if daily:
        # 凡例は _print_pi_daily_table 内で有効インスタンスのみ表示
        _print_pi_daily_table(pi_results, instance_ids, aliases, multi, roles)
    else:
        if multi:
            _print_legend(instance_ids, aliases, roles)
        _print_pi_summary(pi_results, instance_ids, aliases, multi)


def _print_pi_daily_table(pi_results, instance_ids, aliases, multi, roles=None) -> None:
    W_DATE = 10
    W_NUM  = 7
    W_GAP  = 2

    valid = [iid for iid in instance_ids
             if iid in pi_results and 'timeseries' in pi_results[iid]]
    skipped = [iid for iid in instance_ids if iid not in valid]

    # 有効インスタンスの凡例のみ表示
    if multi and valid:
        _print_legend(valid, {iid: aliases[iid] for iid in valid}, roles)

    # PI 無効 / データなし インスタンスの注記
    for iid in skipped:
        note = pi_results.get(iid, {}).get('note', 'データなし')
        print(f"  {aliases[iid]} ({iid}): {note}")
    if skipped:
        print()

    if not valid:
        print("  データなし (PI 未有効またはデータ保持期間外)")
        return

    cols: list[tuple[str, dict]] = [
        (aliases[iid], {dp['ts']: dp for dp in pi_results[iid]['timeseries']})
        for iid in valid
    ]

    all_ts = sorted({ts for _, lookup in cols for ts in lookup})

    hdr1 = " " * (W_DATE + W_GAP)
    for label, _ in cols:
        hdr1 += f"{label:^{W_NUM + W_GAP}}"

    hdr2 = f"{'Date (JST)':<{W_DATE}}{' ' * W_GAP}"
    for _ in cols:
        hdr2 += f"{'avg':>{W_NUM}}{' ' * W_GAP}"

    note = "  ※ PI カウンターは avg のみ (Maximum 統計なし)"
    sep = "─" * W_DATE + "─" * W_GAP + ("─" * W_NUM + "─" * W_GAP) * len(cols)

    print(note)
    print(f"  {hdr1}")
    print(f"  {hdr2}")
    print(f"  {sep}")

    for ts in all_ts:
        date_str = ts.astimezone(JST).strftime('%Y-%m-%d')
        row = f"{date_str:<{W_DATE}}{' ' * W_GAP}"
        for _, lookup in cols:
            dp = lookup.get(ts)
            row += f"{dp['avg']:>{W_NUM}.1f}{' ' * W_GAP}" if dp else f"{'─':>{W_NUM}}{' ' * W_GAP}"
        print(f"  {row}")

    print(f"  {sep}")

    row = f"{'Total avg':<{W_DATE}}{' ' * W_GAP}"
    for iid in valid:
        s = pi_results[iid]
        row += f"{s['avg_qps']:>{W_NUM}.1f}{' ' * W_GAP}"
    print(f"  {row}")

    # メトリクス名を footnote で表示
    metrics = {pi_results[iid].get('metric') for iid in valid if pi_results[iid].get('metric')}
    if metrics:
        print(f"  metric: {', '.join(sorted(metrics))}")


def _print_pi_summary(pi_results, instance_ids, aliases, multi) -> None:
    for iid in instance_ids:
        result = pi_results.get(iid, {})
        label = aliases[iid] if multi else iid
        if 'max_qps' not in result:
            print(f"  {label:<28}: {result.get('note', 'N/A')}")
        else:
            print(f"  {label:<28}  "
                  f"最大: {result['max_qps']:>8.1f} QPS   "
                  f"平均: {result['avg_qps']:>8.1f} QPS   "
                  f"({result['data_points']} pts, metric={result.get('metric', '-')})")


# ============================================================
# エントリポイント
# ============================================================

def validate_period(value: str) -> int:
    n = int(value)
    if n < 60:
        raise argparse.ArgumentTypeError("period は 60 秒以上を指定してください")
    if n % 60 != 0:
        raise argparse.ArgumentTypeError("period は 60 の倍数を指定してください")
    return n


def main() -> None:
    ap = argparse.ArgumentParser(
        description='Aurora の最大 QPS / 平均 QPS を CloudWatch + Performance Insights から取得',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    ap.add_argument('arn', help='RDS DB インスタンス ARN または クラスター ARN')
    ap.add_argument('--profile', metavar='PROFILE',
                    help='AWS プロファイル名')
    ap.add_argument('--days', type=int, default=30, metavar='N',
                    help='集計期間 日数 (default: 30)')
    ap.add_argument('--period', type=validate_period, default=3600, metavar='SECONDS',
                    help='集計粒度 秒 (default: 3600=1時間, --daily 使用時は無視)')
    ap.add_argument('--daily', action='store_true',
                    help='日別テーブルで表示 (period を 86400s に固定)')
    ap.add_argument('--no-pi', action='store_true',
                    help='Performance Insights をスキップ')
    args = ap.parse_args()

    period_sec = 86400 if args.daily else args.period

    try:
        arn_info = parse_rds_arn(args.arn)
    except ValueError as e:
        print(f"エラー: {e}", file=sys.stderr)
        sys.exit(1)

    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(days=args.days)

    sep = '─' * 64
    print(sep)
    print(f"  Aurora QPS Analyzer")
    print(sep)
    print(f"  対象      : {arn_info.resource_type}/{arn_info.resource_id}")
    print(f"  期間      : {start_time.astimezone(JST).strftime('%Y-%m-%d')} 〜 "
          f"{end_time.astimezone(JST).strftime('%Y-%m-%d')} ({args.days}日間, JST)")
    print(f"  表示モード : {'日別テーブル (period=86400s)' if args.daily else f'サマリー (period={period_sec}s)'}")
    print(f"  リージョン : {arn_info.region}")
    print(sep)
    print()

    session = boto3.Session(profile_name=args.profile, region_name=arn_info.region)
    rds = session.client('rds')
    cw  = session.client('cloudwatch')
    pi  = session.client('pi')

    # ── インスタンス ID 確定 ────────────────────────────────
    if arn_info.resource_type == 'cluster':
        print("クラスターのメンバーインスタンスを取得中...")
        try:
            instance_ids, roles = get_cluster_instance_ids(rds, arn_info.resource_id)
        except (ClientError, ValueError) as e:
            print(f"エラー: {e}", file=sys.stderr)
            sys.exit(1)
        for iid in instance_ids:
            role_str = "Writer" if roles.get(iid) else "Reader"
            print(f"    {iid} ({role_str})")
        print()
    else:
        instance_ids = [arn_info.resource_id]
        roles: Optional[dict[str, bool]] = None

    multi = len(instance_ids) > 1

    # ── [1] CloudWatch ──────────────────────────────────────
    print("[1] CloudWatch  (AWS/RDS Queries メトリクス)")
    print()
    cw_data = fetch_cloudwatch_qps(cw, instance_ids, start_time, end_time, period_sec)
    print_cw_table(cw_data, daily=args.daily, multi_instance=multi, instance_ids=instance_ids, roles=roles)
    print()

    # ── [2] Performance Insights / Database Insights ────────
    if not args.no_pi:
        print("[2] Performance Insights / Database Insights")
        print()
        inst_info = describe_instances(rds, instance_ids)

        # 全インスタンス分まとめて取得してから表示
        # PerformanceInsightsEnabled フラグは使わず PI API を直接呼ぶ。
        # PI が実際に無効な場合は fetch_pi_qps 内のエラーハンドリングで捕捉する。
        pi_results: dict[str, dict] = {}
        for iid in instance_ids:
            if iid not in inst_info:
                pi_results[iid] = {'note': 'インスタンス情報取得失敗 (describe_db_instances エラー)'}
                continue
            info = inst_info[iid]
            pi_results[iid] = fetch_pi_qps(
                pi,
                info['dbi_resource_id'],
                info['engine'],
                start_time,
                end_time,
                period_sec,
            )

        print_pi_table(pi_results, daily=args.daily, instance_ids=instance_ids, roles=roles)
        print()

    print(sep)
    print("  完了")
    print(sep)


if __name__ == '__main__':
    main()
