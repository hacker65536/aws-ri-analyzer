#!/usr/bin/env python
# PYTHON_ARGCOMPLETE_OK
"""CUR (Cost and Usage Report) を Athena 経由で分析する CLI

使い方:
    # 組み込みテンプレートを実行
    cur-analyzer rds_instances -p year=2026 -p month=3

    # カスタム SQL ファイルを実行（変数あり）
    cur-analyzer ./my_query.sql -p year=2026 -p month=3

    # テンプレート一覧を表示
    cur-analyzer --list

    # オプション指定
    cur-analyzer rds_instances -p year=2026 -p month=3 --limit-mb 50 --head 20
"""

from __future__ import annotations

import argparse
import argcomplete
import csv
import json
import logging
import re
import sys
from datetime import date as _date, datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

logger = logging.getLogger(__name__)


def _template_completer(prefix, **kwargs):
    """Tab 補完: テンプレート名の候補を返す。"""
    names = [p.stem for p in sorted(_TEMPLATE_DIR.glob("*.sql"))]
    return [n for n in names if n.startswith(prefix)]

from ri_analyzer.config import Config
from ri_analyzer.fetchers.athena import AthenaClient, PartitionMissingError
from ri_analyzer.service_registry import SERVICES

# 組み込みテンプレートのディレクトリ
_TEMPLATE_DIR = Path(__file__).parent / "queries" / "templates"
# カスタムクエリのディレクトリ（templates/ の直接の親）
_QUERIES_DIR = Path(__file__).parent / "queries"

# デフォルト設定
_DEFAULT_HEAD = 100

# タイムゾーンエイリアス（略称 → IANA 名）
_TZ_ALIASES: dict[str, str] = {
    "JST": "Asia/Tokyo",
    "KST": "Asia/Seoul",
    "CST": "Asia/Shanghai",
    "IST": "Asia/Kolkata",
    "CET": "Europe/Berlin",
    "EST": "America/New_York",
    "PST": "America/Los_Angeles",
    "UTC": "UTC",
}
_DEFAULT_LIMIT_MB = 10.0
_MAX_CELL = 40


# ---------------------------------------------------------------------------
# テンプレート処理
# ---------------------------------------------------------------------------

def ce_period_months(lookback_days: int) -> List[Tuple[int, int]]:
    """CE と同じ期間に含まれる (year, month) のリストを返す。

    CE の期間:
      end   = UTC now - 48h
      start = end - lookback_days

    Returns: [(year, month), ...] 昇順
    """
    now_utc = datetime.now(timezone.utc)
    end_date = (now_utc - timedelta(hours=48)).date()
    start_date = end_date - timedelta(days=lookback_days)

    months: List[Tuple[int, int]] = []
    y, m = start_date.year, start_date.month
    while (y, m) <= (end_date.year, end_date.month):
        months.append((y, m))
        if m == 12:
            y, m = y + 1, 1
        else:
            m += 1
    return months


def ce_period_dates(lookback_days: int) -> Tuple[str, str]:
    """CE と同じ (start_date, end_date) を YYYY-MM-DD 形式で返す。"""
    now_utc = datetime.now(timezone.utc)
    end_date = (now_utc - timedelta(hours=48)).date()
    start_date = end_date - timedelta(days=lookback_days)
    return str(start_date), str(end_date)


def _extract_desc(path: Path) -> str:
    """SQL ファイルの先頭コメントから1行説明を抽出する。"""
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line.startswith("-- テンプレート:"):
            return line[len("-- テンプレート:"):].strip()
        # テンプレートキーワードがなければ最初の -- コメントを使う
        if line.startswith("--") and not line.startswith("---"):
            desc = line[2:].strip()
            if desc:
                return desc
    return ""


def list_templates() -> list[tuple[str, str, str]]:
    """(種別, テンプレート名/パス, 説明1行目) のリストを返す。"""
    result = []
    for path in sorted(_TEMPLATE_DIR.glob("*.sql")):
        result.append(("template", path.stem, _extract_desc(path)))
    for path in sorted(_QUERIES_DIR.glob("*.sql")):
        result.append(("custom", str(path.relative_to(_QUERIES_DIR.parent)), _extract_desc(path)))
    return result


def resolve_sql_source(target: str) -> Path:
    """テンプレート名またはファイルパスを SQL ファイルパスに解決する。"""
    path = Path(target)

    # ファイルパスとして存在する場合はそのまま使う
    if path.exists() and path.suffix == ".sql":
        return path

    # テンプレート名として検索
    tmpl = _TEMPLATE_DIR / f"{target}.sql"
    if tmpl.exists():
        return tmpl

    # どちらでもなければエラー
    available = [p.stem for p in sorted(_TEMPLATE_DIR.glob("*.sql"))]
    raise FileNotFoundError(
        f"テンプレート '{target}' が見つかりません。\n"
        f"利用可能なテンプレート: {available}\n"
        f"カスタム SQL ファイルの場合は .sql 拡張子付きのパスを指定してください。"
    )


def render_template(sql: str, params: dict[str, str]) -> str:
    """{{ variable }} を params で置換する。未定義変数はエラー。"""
    errors: list[str] = []

    def replacer(m: re.Match) -> str:
        key = m.group(1).strip()
        if key not in params:
            errors.append(key)
            return m.group(0)
        return params[key]

    rendered = re.sub(r"\{\{\s*(\w+)\s*\}\}", replacer, sql)

    if errors:
        raise ValueError(
            f"テンプレート変数が未指定です: {list(dict.fromkeys(errors))}\n"
            f"-p KEY=VALUE で渡してください。例: -p year=2026 -p month=3"
        )
    return rendered


def parse_params(param_list: list[str]) -> dict[str, str]:
    """['key=value', ...] → {'key': 'value', ...}

    month キーはゼロ埋めを除去する（'03' → '3'）。
    CUR のパーティション値は '3' 形式のため。
    """
    result: dict[str, str] = {}
    for item in param_list:
        if "=" not in item:
            raise ValueError(f"-p の値は KEY=VALUE 形式で指定してください: '{item}'")
        key, _, val = item.partition("=")
        key = key.strip()
        val = val.strip()
        if key == "month":
            try:
                val = str(int(val))  # '03' → '3'
            except ValueError:
                pass
        result[key] = val
    return result


# ---------------------------------------------------------------------------
# タイムゾーン処理
# ---------------------------------------------------------------------------

def apply_tz_params(params: dict[str, str], tz_name: str) -> dict[str, str]:
    """start_date / end_date をローカル日付として UTC に変換し、
    テンプレート変数 partition_cond / start_date_utc / end_date_utc を注入する。

    Parameters
    ----------
    params  : テンプレート変数辞書（start_date / end_date が必要）
    tz_name : タイムゾーン名（IANA 形式 or エイリアス。例: JST, Asia/Tokyo）

    Returns
    -------
    更新された params（元の辞書は変更しない）

    Raises
    ------
    ValueError : tz_name が不正、または start_date / end_date が未指定の場合
    """
    iana = _TZ_ALIASES.get(tz_name.upper(), tz_name)
    try:
        tz = ZoneInfo(iana)
    except ZoneInfoNotFoundError:
        raise ValueError(
            f"タイムゾーン '{tz_name}' が見つかりません。\n"
            f"IANA 形式（例: Asia/Tokyo）またはエイリアス（JST, UTC など）で指定してください。"
        )

    start_str = params.get("start_date")
    end_str   = params.get("end_date")
    if not start_str or not end_str:
        raise ValueError(
            "--tz を使用する場合は -p start_date=YYYY-MM-DD と -p end_date=YYYY-MM-DD が必要です。"
        )

    # ローカル 00:00:00 / 23:59:59 → UTC
    start_utc = datetime.fromisoformat(f"{start_str} 00:00:00").replace(tzinfo=tz).astimezone(timezone.utc)
    end_utc   = datetime.fromisoformat(f"{end_str} 23:59:59").replace(tzinfo=tz).astimezone(timezone.utc)

    # UTC 範囲に含まれる月を列挙してパーティション条件を生成
    months: list[tuple[int, int]] = []
    d = _date(start_utc.year, start_utc.month, 1)
    end_month = _date(end_utc.year, end_utc.month, 1)
    while d <= end_month:
        months.append((d.year, d.month))
        d = _date(d.year + (d.month // 12), d.month % 12 + 1, 1)

    if len(months) == 1:
        y, m = months[0]
        partition_cond = f"year = '{y}' AND month = '{m}'"
    else:
        parts = [f"(year = '{y}' AND month = '{m}')" for y, m in months]
        partition_cond = "(\n    " + "\n    OR ".join(parts) + "\n  )"

    return {
        **params,
        "partition_cond": partition_cond,
        "start_date_utc": start_utc.strftime("%Y-%m-%d %H:%M:%S"),
        "end_date_utc":   end_utc.strftime("%Y-%m-%d %H:%M:%S"),
    }


# ---------------------------------------------------------------------------
# 表示
# ---------------------------------------------------------------------------

def print_table(rows: list[dict]) -> None:
    if not rows:
        return
    headers = list(rows[0].keys())
    widths = {
        h: min(
            max(len(h), max((len(_clip(str(r.get(h, "")), _MAX_CELL)) for r in rows), default=0)),
            _MAX_CELL,
        )
        for h in headers
    }
    sep = "  ".join("-" * widths[h] for h in headers)
    print("  ".join(h.ljust(widths[h]) for h in headers))
    print(sep)
    for row in rows:
        print("  ".join(_clip(str(row.get(h, "")), widths[h]).ljust(widths[h]) for h in headers))


def print_csv(rows: list[dict]) -> None:
    if not rows:
        return
    writer = csv.DictWriter(sys.stdout, fieldnames=list(rows[0].keys()))
    writer.writeheader()
    writer.writerows(rows)


def print_json(rows: list[dict]) -> None:
    print(json.dumps(rows, ensure_ascii=False, indent=2))


def _clip(val: str, maxlen: int) -> str:
    return val if len(val) <= maxlen else val[: maxlen - 1] + "…"


def info(msg: str) -> None:
    """診断メッセージを stderr に出力する。ログレベルに応じてフィルタされる。"""
    # [ERROR] / [WARN] プレフィックスに基づいてログレベルを振り分ける
    if msg.startswith("[ERROR]"):
        logger.error("%s", msg[len("[ERROR]"):].strip())
    elif msg.startswith("[WARN]"):
        logger.warning("%s", msg[len("[WARN]"):].strip())
    else:
        logger.info("%s", msg.lstrip("[INFO]").strip())


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="CUR (Cost and Usage Report) を Athena 経由で分析する",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  cur-analyzer --list
  cur-analyzer rds_instances -p year=2026 -p month=3
  cur-analyzer ce_factcheck_rds -p year=2026 -p month=3 -p instance_type=db.r6g.large -p region=ap-northeast-1 -p engine=Aurora
  cur-analyzer ./my_query.sql -p year=2026 -p month=3
  cur-analyzer rds_instances -p year=2026 -p month=3 --format csv > out.csv
  cur-analyzer rds_instances -p year=2026 -p month=3 --format json
        """,
    )
    target_arg = parser.add_argument(
        "target",
        nargs="?",
        help="テンプレート名 or .sql ファイルパス",
    )
    target_arg.completer = _template_completer
    parser.add_argument(
        "-p", "--param",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="テンプレート変数（複数指定可）",
    )
    parser.add_argument(
        "--list", "-l",
        action="store_true",
        help="利用可能なテンプレート一覧を表示して終了",
    )
    parser.add_argument(
        "--limit-mb",
        type=float,
        default=_DEFAULT_LIMIT_MB,
        metavar="MB",
        help=f"ダウンロード閾値 MB（デフォルト: {_DEFAULT_LIMIT_MB}）",
    )
    parser.add_argument(
        "--head",
        type=int,
        default=_DEFAULT_HEAD,
        metavar="N",
        help=f"表示する最大行数（デフォルト: {_DEFAULT_HEAD}、0 = 無制限）",
    )
    parser.add_argument(
        "--download-dir",
        type=Path,
        default=None,
        metavar="DIR",
        help="CSV ダウンロード先（省略時: ~/.cache/ri-analyzer/query_results/）",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Athena に送信する前のレンダリング済み SQL を表示して終了（実行しない）",
    )
    parser.add_argument(
        "--no-cache",
        action="store_true",
        help="キャッシュを使わず毎回 Athena に問い合わせる",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="既存キャッシュを削除して再実行する（--no-cache と組み合わせ不要）",
    )
    parser.add_argument(
        "--no-partition-check",
        action="store_true",
        help="year / month パーティション条件チェックをスキップ",
    )
    parser.add_argument(
        "--config",
        default=None,
        metavar="PATH",
        help="config.yaml のパス",
    )
    parser.add_argument(
        "--format",
        choices=["table", "csv", "json"],
        default="table",
        help="出力フォーマット（デフォルト: table）",
    )
    parser.add_argument(
        "--tz",
        default=None,
        metavar="TZ",
        help=(
            "タイムゾーン（JST, UTC, Asia/Tokyo など）。"
            "-p start_date / end_date と併用すると UTC 変換・パーティション条件を自動生成し "
            "{{ partition_cond }} / {{ start_date_utc }} / {{ end_date_utc }} を注入する。"
        ),
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="デバッグログを stderr に表示する",
    )
    argcomplete.autocomplete(parser)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(levelname)s: %(message)s",
        stream=sys.stderr,
    )

    # --list
    if args.list:
        entries = list_templates()
        if not entries:
            info("テンプレートがありません。")
            return
        templates = [(n, d) for kind, n, d in entries if kind == "template"]
        customs   = [(n, d) for kind, n, d in entries if kind == "custom"]
        name_width = max((len(n) for n, _ in templates + customs), default=20)
        if templates:
            info(f"\n[テンプレート]  cur-analyzer <名前> -p year=YYYY -p month=M")
            info(f"{'名前':<{name_width}}  説明")
            info("-" * (name_width + 40))
            for name, desc in templates:
                info(f"{name:<{name_width}}  {desc}")
        if customs:
            info(f"\n[カスタムクエリ]  cur-analyzer <パス> -p ...")
            info(f"{'パス':<{name_width}}  説明")
            info("-" * (name_width + 40))
            for name, desc in customs:
                info(f"{name:<{name_width}}  {desc}")
        info("")
        return

    if not args.target:
        parser.print_help()
        sys.exit(1)

    # SQL 解決 & テンプレート変数展開
    try:
        sql_path = resolve_sql_source(args.target)
    except FileNotFoundError as e:
        info(f"[ERROR] {e}")
        sys.exit(1)

    sql_raw = sql_path.read_text(encoding="utf-8")

    # Athena 設定を先に読み込み、database / table をデフォルト変数として注入
    cfg = Config.load(args.config)
    if cfg.athena is None:
        info("[ERROR] config.yaml に athena セクションがありません")
        sys.exit(1)

    try:
        base_params = parse_params(args.param)
        # service キーをサービスキー（rds 等）から CUR プロダクトコードへ解決
        if "service" in base_params:
            svc_key = base_params["service"].lower()
            if svc_key in SERVICES and SERVICES[svc_key].cur_product_code:
                base_params["service"] = SERVICES[svc_key].cur_product_code
        # config の database / table を既定値として設定（-p で上書き可能）
        base_params.setdefault("database", cfg.athena.database)
        base_params.setdefault("table", cfg.athena.table)
        # start_date / end_date を CE 期間から自動注入（-p で上書き可能）
        ce_start, ce_end = ce_period_dates(cfg.analysis.lookback_days)
        base_params.setdefault("start_date", ce_start)
        base_params.setdefault("end_date", ce_end)
    except ValueError as e:
        info(f"[ERROR] {e}")
        sys.exit(1)

    # --tz: start_date / end_date をローカル時刻として UTC 変換し partition_cond を注入
    if args.tz:
        try:
            base_params = apply_tz_params(base_params, args.tz)
            info(f"[INFO] タイムゾーン : {args.tz}")
            info(f"[INFO] UTC 範囲    : {base_params['start_date_utc']} 〜 {base_params['end_date_utc']}")
            info(f"[INFO] パーティション: {base_params['partition_cond']}")
        except ValueError as e:
            info(f"[ERROR] {e}")
            sys.exit(1)

    # --tz 使用時は partition_cond が注入済みのため year/month ループは不要（1回だけ実行）
    # それ以外は year / month が未指定なら CE 期間（lookback_days）から自動導出
    if args.tz:
        run_iterations: List[Tuple[dict, str]] = [(base_params, base_params["start_date"])]
    elif "year" not in base_params and "month" not in base_params:
        ym_list = ce_period_months(cfg.analysis.lookback_days)
        info(f"[INFO] CE 期間     : lookback={cfg.analysis.lookback_days}d → "
             f"{ym_list[0][0]}-{ym_list[0][1]:02d} 〜 {ym_list[-1][0]}-{ym_list[-1][1]:02d} "
             f"({ce_start} 〜 {ce_end})")
        run_iterations = [
            ({**base_params, "year": str(y), "month": str(m)}, f"{y}-{m:02d}")
            for y, m in ym_list
        ]
    else:
        if "year" not in base_params or "month" not in base_params:
            info("[ERROR] -p year=YYYY と -p month=M は両方指定してください")
            sys.exit(1)
        y, m = int(base_params["year"]), int(base_params["month"])
        run_iterations = [({**base_params}, f"{y}-{m:02d}")]

    # --dry-run: レンダリング済み SQL を表示して終了
    if args.dry_run:
        for params, label in run_iterations:
            try:
                sql = render_template(sql_raw, params)
            except ValueError as e:
                info(f"[ERROR] {e}")
                sys.exit(1)
            print(f"-- ========== {label} ==========")
            print(sql)
        return

    use_cache = not args.no_cache
    client = AthenaClient(cfg.athena, payer_profile=cfg.payer.profile)

    info(f"[INFO] テンプレート : {sql_path.name}")
    info(f"[INFO] サイズ閾値  : {args.limit_mb} MB")
    if not use_cache:
        info("[INFO] キャッシュ  : 無効")

    all_rows: List[dict] = []

    for params, label in run_iterations:
        try:
            sql = render_template(sql_raw, params)
        except ValueError as e:
            info(f"[ERROR] {e}")
            sys.exit(1)

        # --refresh: 対象クエリのキャッシュを削除
        if args.refresh:
            from ri_analyzer.fetchers.athena import _sql_hash
            h = _sql_hash(sql)
            cache_dir = Path.home() / ".cache" / "ri-analyzer" / "query_results"
            for ext in (".csv", ".meta.json"):
                p = cache_dir / f"{h}{ext}"
                if p.exists():
                    p.unlink()
                    info(f"[INFO] キャッシュ削除: {p}")

        info(f"[INFO] 実行中      : {label} ...")

        tmp_path = _write_rendered_sql(sql)
        try:
            result = client.run_from_file(
                sql_path=tmp_path,
                enforce_partition=not args.no_partition_check,
                size_limit_mb=args.limit_mb,
                download_dir=args.download_dir,
                use_cache=use_cache,
            )
        except PartitionMissingError as e:
            info(f"[ERROR] {e}")
            tmp_path.unlink(missing_ok=True)
            sys.exit(1)
        finally:
            tmp_path.unlink(missing_ok=True)

        # 結果メタ情報
        cache_label = " [CACHE HIT]" if result.from_cache else ""
        info(f"{'─' * 60}")
        info(f"  {label}{cache_label}")
        if result.s3_path:
            info(f"  Query ID  : {result.query_id}")
        if result.size_bytes:
            info(f"  サイズ    : {result.size_mb:.2f} MB")
        if not result.from_cache:
            info(f"  実行時間  : {result.elapsed_sec:.1f} 秒")

        if not result.downloaded:
            info(f"  [SKIP] サイズが {args.limit_mb} MB 超のためダウンロードしません。")
            info(f"         S3 から直接確認: {result.s3_path}")
            continue

        month_rows = result.rows or []
        info(f"  行数      : {len(month_rows):,} 行")
        all_rows.extend(month_rows)

    info(f"{'─' * 60}\n")

    if not all_rows:
        info("[INFO] 結果が 0 行でした。")
        return

    info(f"  合計      : {len(all_rows):,} 行\n")
    display_rows = all_rows if args.head == 0 else all_rows[: args.head]

    if args.format == "csv":
        print_csv(display_rows)
    elif args.format == "json":
        print_json(display_rows)
    else:
        print_table(display_rows)

    if args.head > 0 and len(all_rows) > args.head:
        info(f"\n... {len(all_rows) - args.head} 行省略 (--head {args.head})")


def _write_rendered_sql(sql: str) -> Path:
    """レンダリング済み SQL を一時ファイルに書き出す。"""
    import tempfile
    tmp = tempfile.NamedTemporaryFile(
        suffix=".sql", mode="w", encoding="utf-8", delete=False
    )
    tmp.write(sql)
    tmp.close()
    return Path(tmp.name)


if __name__ == "__main__":
    main()
