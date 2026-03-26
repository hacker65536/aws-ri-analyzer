#!/usr/bin/env python
"""SQL テンプレート / カスタム SQL を Athena で実行する CLI

使い方:
    # 組み込みテンプレートを実行
    python athena_run.py rds_instances -p year=2026 -p month=3

    # カスタム SQL ファイルを実行（変数あり）
    python athena_run.py ./my_query.sql -p year=2026 -p month=3

    # テンプレート一覧を表示
    python athena_run.py --list

    # オプション指定
    python athena_run.py rds_instances -p year=2026 -p month=3 --limit-mb 50 --head 20
"""

from __future__ import annotations

import argparse
import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import List, Tuple

from ri_analyzer.config import Config
from ri_analyzer.fetchers.athena import AthenaClient, PartitionMissingError

# 組み込みテンプレートのディレクトリ
_TEMPLATE_DIR = Path(__file__).parent / "queries" / "templates"

# デフォルト設定
_DEFAULT_HEAD = 100
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


def list_templates() -> list[tuple[str, str]]:
    """(テンプレート名, 説明1行目) のリストを返す。"""
    result = []
    for path in sorted(_TEMPLATE_DIR.glob("*.sql")):
        name = path.stem
        desc = ""
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line.startswith("-- テンプレート:"):
                desc = line[len("-- テンプレート:"):].strip()
                break
        result.append((name, desc))
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
            f"テンプレート変数が未指定です: {errors}\n"
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


def _clip(val: str, maxlen: int) -> str:
    return val if len(val) <= maxlen else val[: maxlen - 1] + "…"


def info(msg: str) -> None:
    print(msg, file=sys.stderr)


# ---------------------------------------------------------------------------
# メイン
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Athena で SQL テンプレートまたはカスタム SQL を実行する",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python athena_run.py --list
  python athena_run.py rds_instances -p year=2026 -p month=3
  python athena_run.py ce_factcheck_rds -p year=2026 -p month=3 -p instance_type=db.r6g.large -p region=ap-northeast-1 -p engine=Aurora
  python athena_run.py ./my_query.sql -p year=2026 -p month=3
        """,
    )
    parser.add_argument(
        "target",
        nargs="?",
        help="テンプレート名 or .sql ファイルパス",
    )
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
    args = parser.parse_args()

    # --list
    if args.list:
        templates = list_templates()
        if not templates:
            info("テンプレートがありません。")
            return
        name_width = max(len(n) for n, _ in templates)
        info(f"\n{'テンプレート名':<{name_width}}  説明")
        info("-" * (name_width + 40))
        for name, desc in templates:
            info(f"{name:<{name_width}}  {desc}")
        info(f"\nテンプレートファイル: {_TEMPLATE_DIR}/\n")
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
        # config の database / table を既定値として設定（-p で上書き可能）
        base_params.setdefault("database", cfg.athena.database)
        base_params.setdefault("table", cfg.athena.table)
    except ValueError as e:
        info(f"[ERROR] {e}")
        sys.exit(1)

    # year / month が未指定なら CE 期間（lookback_days）から自動導出
    if "year" not in base_params and "month" not in base_params:
        months = ce_period_months(cfg.analysis.lookback_days)
        info(f"[INFO] CE 期間     : lookback={cfg.analysis.lookback_days}d → "
             f"{months[0][0]}-{months[0][1]:02d} 〜 {months[-1][0]}-{months[-1][1]:02d}")
    else:
        # 両方指定されていない場合はエラー
        if "year" not in base_params or "month" not in base_params:
            info("[ERROR] -p year=YYYY と -p month=M は両方指定してください")
            sys.exit(1)
        months = [(int(base_params["year"]), int(base_params["month"]))]

    use_cache = not args.no_cache
    client = AthenaClient(cfg.athena, payer_profile=cfg.payer.profile)

    info(f"[INFO] テンプレート : {sql_path.name}")
    info(f"[INFO] サイズ閾値  : {args.limit_mb} MB")
    if not use_cache:
        info("[INFO] キャッシュ  : 無効")

    all_rows: List[dict] = []

    for year, month in months:
        params = {**base_params, "year": str(year), "month": str(month)}

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

        info(f"[INFO] 実行中      : {year}-{month:02d} ...")

        try:
            result = client.run_from_file(
                sql_path=_write_rendered_sql(sql),
                enforce_partition=not args.no_partition_check,
                size_limit_mb=args.limit_mb,
                download_dir=args.download_dir,
                use_cache=use_cache,
            )
        except PartitionMissingError as e:
            info(f"[ERROR] {e}")
            sys.exit(1)

        # 結果メタ情報
        cache_label = " [CACHE HIT]" if result.from_cache else ""
        info(f"{'─' * 60}")
        info(f"  {year}-{month:02d}{cache_label}")
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
