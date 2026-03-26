"""Athena 経由で CUR にクエリするクライアント

主な責務:
- Athena 接続（AWS profile / SSO 対応）
- 非同期クエリの実行・ポーリング・完了待ち
- 結果取得: api モード（小規模）/ s3 モード（CSV 直読み）
- CUR スキーマ（カラム一覧）のローカルキャッシュ
- パーティション条件の強制インジェクション（year / month）
"""

from __future__ import annotations

import csv
import hashlib
import io
import json
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple

import boto3
import botocore.session

from ri_analyzer.config import AthenaConfig

# ダウンロードのデフォルト閾値
_DEFAULT_SIZE_LIMIT_MB = 10

# ローカルキャッシュディレクトリ
_CACHE_DIR = Path.home() / ".cache" / "ri-analyzer"

# Athena クエリポーリング間隔（秒）
_POLL_INITIAL = 1.0
_POLL_MAX = 10.0
_POLL_BACKOFF = 1.5

# Athena クエリタイムアウト（秒）
_QUERY_TIMEOUT = 300


class AthenaError(Exception):
    """Athena クエリ失敗"""


class PartitionMissingError(ValueError):
    """パーティション条件が SQL に含まれていない場合"""


@dataclass
class QueryResult:
    """run_query / run_from_file の返り値"""
    query_id: str
    s3_path: str                        # s3://bucket/key
    size_bytes: int
    elapsed_sec: float
    rows: Optional[List[Dict[str, Any]]]  # None = サイズ超過のためスキップ
    local_path: Optional[Path] = None    # ダウンロード済みファイルパス
    from_cache: bool = False             # True = ローカルキャッシュから返した

    @property
    def size_mb(self) -> float:
        return self.size_bytes / 1024 / 1024

    @property
    def downloaded(self) -> bool:
        return self.rows is not None


# ---------------------------------------------------------------------------
# クライアント本体
# ---------------------------------------------------------------------------

class AthenaClient:
    """Athena 経由で CUR にクエリするクライアント。

    Parameters
    ----------
    config      : AthenaConfig
    payer_profile : payer アカウントのプロファイル名（config.athena.profile が
                  未設定の場合に使うフォールバック）
    """

    def __init__(self, config: AthenaConfig, payer_profile: Optional[str] = None) -> None:
        self._cfg = config
        profile = config.profile or payer_profile
        session = boto3.Session(
            profile_name=profile,
            region_name=config.region,
        )
        self._athena = session.client("athena")
        self._s3 = session.client("s3")
        self._schema_cache: Optional[Dict[str, str]] = None  # column_name -> type

    # ------------------------------------------------------------------
    # パブリック API
    # ------------------------------------------------------------------

    def run_query(
        self,
        sql: str,
        *,
        enforce_partition: bool = True,
        params: Optional[List[str]] = None,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """SQL を実行して行のリストを返す。

        Parameters
        ----------
        sql               : 実行する SQL（プレースホルダは ? を使う）
        enforce_partition : True のとき、year / month パーティション条件が
                           含まれていない SQL は PartitionMissingError を raise する
        params            : プレースホルダに渡す値リスト（Athena の PreparedStatement
                           ではなく、クライアントサイドで文字列置換する）
        use_cache         : True のとき、TTL 内のキャッシュがあれば Athena を呼ばずに返す
        """
        if params:
            sql = _bind_params(sql, params)

        if enforce_partition:
            _assert_partition(sql)

        sql_hash = _sql_hash(sql)
        if use_cache:
            cached = self._load_query_cache(sql_hash)
            if cached is not None:
                return cached.rows  # type: ignore[return-value]

        query_id = self._start_query(sql)
        self._wait_query(query_id)

        if self._cfg.result_mode == "s3":
            rows = self._fetch_s3(query_id)
        else:
            rows = self._fetch_api(query_id)

        if use_cache:
            self._save_query_cache(sql_hash, QueryResult(
                query_id=query_id,
                s3_path="",
                size_bytes=0,
                elapsed_sec=0.0,
                rows=rows,
            ))
        return rows

    def iter_query(
        self,
        sql: str,
        *,
        enforce_partition: bool = True,
        params: Optional[List[str]] = None,
    ) -> Iterator[Dict[str, Any]]:
        """結果をイテレータで返す（大量行でもメモリを節約）。"""
        rows = self.run_query(sql, enforce_partition=enforce_partition, params=params)
        yield from rows

    def get_schema(self, *, force_refresh: bool = False) -> Dict[str, str]:
        """CUR テーブルのカラム名 → データ型マップを返す。

        キャッシュは ~/.cache/ri-analyzer/athena_schema_{db}_{table}.json に保存。
        TTL は config.athena.schema_cache_ttl_hours。
        """
        if self._schema_cache is not None and not force_refresh:
            return self._schema_cache

        cache_path = self._schema_cache_path()
        if not force_refresh and cache_path.exists():
            age_hours = (time.time() - cache_path.stat().st_mtime) / 3600
            if age_hours < self._cfg.schema_cache_ttl_hours:
                self._schema_cache = json.loads(cache_path.read_text())
                return self._schema_cache

        schema = self._fetch_schema()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(schema, ensure_ascii=False, indent=2))
        self._schema_cache = schema
        return schema

    def column_names(self, *, force_refresh: bool = False) -> List[str]:
        """CUR テーブルのカラム名一覧を返す。"""
        return list(self.get_schema(force_refresh=force_refresh).keys())

    def run_from_file(
        self,
        sql_path: str | Path,
        *,
        enforce_partition: bool = True,
        size_limit_mb: float = _DEFAULT_SIZE_LIMIT_MB,
        download_dir: Optional[Path] = None,
        use_cache: bool = True,
    ) -> "QueryResult":
        """SQL ファイルを実行し、サイズに応じて結果をダウンロードする。

        Parameters
        ----------
        sql_path          : 実行する SQL ファイルのパス
        enforce_partition : year / month パーティション条件の強制チェック
        size_limit_mb     : この MB 以内なら結果をダウンロード（デフォルト 10MB）
        download_dir      : ダウンロード先ディレクトリ（省略時は ~/.cache/ri-analyzer/query_results/）
        use_cache         : True のとき、TTL 内のキャッシュがあれば Athena を呼ばずに返す

        Returns
        -------
        QueryResult
        """
        sql = Path(sql_path).read_text(encoding="utf-8")

        if enforce_partition:
            _assert_partition(sql)

        sql_hash = _sql_hash(sql)
        if use_cache:
            cached = self._load_query_cache(sql_hash)
            if cached is not None:
                return cached

        t0 = time.monotonic()
        query_id = self._start_query(sql)
        self._wait_query(query_id)
        elapsed = time.monotonic() - t0

        s3_path, size_bytes = self._get_result_s3_info(query_id)

        limit_bytes = size_limit_mb * 1024 * 1024
        if size_bytes > limit_bytes:
            return QueryResult(
                query_id=query_id,
                s3_path=s3_path,
                size_bytes=size_bytes,
                elapsed_sec=elapsed,
                rows=None,
            )

        # ダウンロード先: キャッシュ有効時はハッシュ名、無効時は query_id 名
        dest_dir = download_dir or (_CACHE_DIR / "query_results")
        dest_dir.mkdir(parents=True, exist_ok=True)
        local_path = dest_dir / (f"{sql_hash}.csv" if use_cache else f"{query_id}.csv")
        self._download_s3(s3_path, local_path)

        rows = _read_csv(local_path)
        result = QueryResult(
            query_id=query_id,
            s3_path=s3_path,
            size_bytes=size_bytes,
            elapsed_sec=elapsed,
            rows=rows,
            local_path=local_path,
        )
        if use_cache:
            self._save_query_cache(sql_hash, result)
        return result

    # ------------------------------------------------------------------
    # 内部ヘルパー
    # ------------------------------------------------------------------

    def _start_query(self, sql: str, database: Optional[str] = None) -> str:
        """Athena にクエリを投げ QueryExecutionId を返す。

        Parameters
        ----------
        database : None のとき config.athena.database を使う。
                   空文字列 "" を渡すとデータベースコンテキストを設定しない。
        """
        db = self._cfg.database if database is None else database
        kwargs: Dict[str, Any] = {
            "QueryString": sql,
            "WorkGroup": self._cfg.workgroup,
        }
        if db:
            kwargs["QueryExecutionContext"] = {"Database": db}
        if self._cfg.output_location:
            kwargs["ResultConfiguration"] = {
                "OutputLocation": self._cfg.output_location,
            }

        resp = self._athena.start_query_execution(**kwargs)
        return resp["QueryExecutionId"]

    def _wait_query(self, query_id: str) -> None:
        """クエリ完了まで指数バックオフでポーリングする。"""
        interval = _POLL_INITIAL
        deadline = time.monotonic() + _QUERY_TIMEOUT

        while time.monotonic() < deadline:
            resp = self._athena.get_query_execution(QueryExecutionId=query_id)
            state = resp["QueryExecution"]["Status"]["State"]

            if state == "SUCCEEDED":
                return
            if state in ("FAILED", "CANCELLED"):
                reason = (
                    resp["QueryExecution"]["Status"]
                    .get("StateChangeReason", "unknown reason")
                )
                raise AthenaError(f"Query {query_id} {state}: {reason}")

            time.sleep(interval)
            interval = min(interval * _POLL_BACKOFF, _POLL_MAX)

        raise AthenaError(f"Query {query_id} timed out after {_QUERY_TIMEOUT}s")

    def _fetch_api(self, query_id: str) -> List[Dict[str, Any]]:
        """GetQueryResults API で結果を取得する（ページネーション対応）。"""
        rows: List[Dict[str, Any]] = []
        headers: Optional[List[str]] = None
        next_token: Optional[str] = None

        while True:
            kwargs: Dict[str, Any] = {
                "QueryExecutionId": query_id,
                "MaxResults": 1000,
            }
            if next_token:
                kwargs["NextToken"] = next_token

            resp = self._athena.get_query_results(**kwargs)
            result_set = resp["ResultSet"]

            if headers is None:
                headers = [
                    col["Label"]
                    for col in result_set["ResultSetMetadata"]["ColumnInfo"]
                ]
                # 最初のページは先頭行がヘッダー行なのでスキップ
                data_rows = result_set["Rows"][1:]
            else:
                data_rows = result_set["Rows"]

            for row in data_rows:
                values = [d.get("VarCharValue", "") for d in row["Data"]]
                rows.append(dict(zip(headers, values)))

            next_token = resp.get("NextToken")
            if not next_token:
                break

        return rows

    def _fetch_s3(self, query_id: str) -> List[Dict[str, Any]]:
        """S3 の結果 CSV を直接読み込む（大量行向け）。"""
        resp = self._athena.get_query_execution(QueryExecutionId=query_id)
        s3_path = (
            resp["QueryExecution"]["ResultConfiguration"]["OutputLocation"]
        )
        # s3://bucket/prefix/query_id.csv
        s3_path = s3_path.lstrip("s3://")
        bucket, key = s3_path.split("/", 1)

        obj = self._s3.get_object(Bucket=bucket, Key=key)
        body = obj["Body"].read().decode("utf-8")
        reader = csv.DictReader(io.StringIO(body))
        return list(reader)

    def _fetch_schema(self) -> Dict[str, str]:
        """information_schema.columns でスキーマを取得する。"""
        sql = f"""
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_schema = '{self._cfg.database}'
  AND table_name   = '{self._cfg.table}'
ORDER BY ordinal_position
"""
        # information_schema クエリはデータベースコンテキスト不要
        query_id = self._start_query(sql, database="")
        self._wait_query(query_id)
        rows = self._fetch_api(query_id)
        return {r["column_name"]: r["data_type"] for r in rows}

    def _get_result_s3_info(self, query_id: str) -> Tuple[str, int]:
        """クエリ結果の S3 パスとオブジェクトサイズ（bytes）を返す。"""
        resp = self._athena.get_query_execution(QueryExecutionId=query_id)
        s3_uri = resp["QueryExecution"]["ResultConfiguration"]["OutputLocation"]

        # s3://bucket/key → bucket, key
        without_scheme = s3_uri[len("s3://"):]
        bucket, key = without_scheme.split("/", 1)

        head = self._s3.head_object(Bucket=bucket, Key=key)
        size_bytes = head["ContentLength"]
        return s3_uri, size_bytes

    def _download_s3(self, s3_uri: str, dest: Path) -> None:
        """S3 オブジェクトをローカルにダウンロードする。"""
        without_scheme = s3_uri[len("s3://"):]
        bucket, key = without_scheme.split("/", 1)
        self._s3.download_file(bucket, key, str(dest))

    def _schema_cache_path(self) -> Path:
        db = self._cfg.database.replace("/", "_")
        tbl = self._cfg.table.replace("/", "_")
        return _CACHE_DIR / f"athena_schema_{db}_{tbl}.json"

    def _query_cache_dir(self) -> Path:
        return _CACHE_DIR / "query_results"

    def _load_query_cache(self, sql_hash: str) -> Optional["QueryResult"]:
        """TTL 内のキャッシュが存在すれば QueryResult を返す。"""
        csv_path = self._query_cache_dir() / f"{sql_hash}.csv"
        meta_path = self._query_cache_dir() / f"{sql_hash}.meta.json"

        if not csv_path.exists() or not meta_path.exists():
            return None

        age_hours = (time.time() - meta_path.stat().st_mtime) / 3600
        if age_hours >= self._cfg.query_cache_ttl_hours:
            return None

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        rows = _read_csv(csv_path)
        return QueryResult(
            query_id=meta.get("query_id", ""),
            s3_path=meta.get("s3_path", ""),
            size_bytes=meta.get("size_bytes", 0),
            elapsed_sec=meta.get("elapsed_sec", 0.0),
            rows=rows,
            local_path=csv_path,
            from_cache=True,
        )

    def _save_query_cache(self, sql_hash: str, result: "QueryResult") -> None:
        """クエリ結果を CSV + meta.json としてキャッシュに保存する。"""
        if result.rows is None:
            return  # サイズ超過でダウンロードなしの場合はキャッシュしない

        cache_dir = self._query_cache_dir()
        cache_dir.mkdir(parents=True, exist_ok=True)

        csv_path = cache_dir / f"{sql_hash}.csv"
        meta_path = cache_dir / f"{sql_hash}.meta.json"

        # CSV 書き出し（local_path が既に正しい場所なら不要）
        if result.local_path != csv_path:
            _write_csv(csv_path, result.rows)

        meta_path.write_text(json.dumps({
            "query_id": result.query_id,
            "s3_path": result.s3_path,
            "size_bytes": result.size_bytes,
            "elapsed_sec": result.elapsed_sec,
            "sql_hash": sql_hash,
            "cached_at": datetime.utcnow().isoformat(),
        }, ensure_ascii=False), encoding="utf-8")


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _sql_hash(sql: str) -> str:
    """SQL 文字列の SHA256 ハッシュ（先頭16文字）をキャッシュキーとして返す。"""
    return hashlib.sha256(sql.strip().encode("utf-8")).hexdigest()[:16]


def _read_csv(path: Path) -> List[Dict[str, Any]]:
    """ローカル CSV ファイルを dict のリストとして読み込む。"""
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: List[Dict[str, Any]]) -> None:
    """dict のリストを CSV ファイルに書き出す。"""
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def _assert_partition(sql: str) -> None:
    """year / month パーティション条件が WHERE 句に含まれているかチェックする。

    CUR の標準的なパーティションキーは year と month（どちらも string）。
    """
    lower = sql.lower()
    has_year = "year" in lower
    has_month = "month" in lower
    if not (has_year and has_month):
        missing = []
        if not has_year:
            missing.append("year")
        if not has_month:
            missing.append("month")
        raise PartitionMissingError(
            f"SQL にパーティション条件が不足しています: {missing}\n"
            "CUR クエリには必ず year / month 条件を含めてください。\n"
            "例: WHERE year = '2024' AND month = '01'\n"
            "パーティションなしで実行する場合は enforce_partition=False を指定してください。"
        )


def _bind_params(sql: str, params: List[str]) -> str:
    """? プレースホルダを params でクライアントサイド置換する。

    Note: Athena の PreparedStatement は別 API が必要なため、
    シンプルに文字列置換する。SQL インジェクションに注意して
    数値・文字列リテラル以外は渡さないこと。
    """
    result = []
    param_iter = iter(params)
    for ch in sql:
        if ch == "?":
            val = next(param_iter)
            # シングルクォートをエスケープして文字列リテラル化
            result.append(f"'{val.replace(chr(39), chr(39)*2)}'")
        else:
            result.append(ch)
    return "".join(result)


def partition_filter(year: int | str, month: int | str) -> str:
    """よく使うパーティション条件文字列を生成するヘルパー。

    Usage:
        pf = partition_filter(2024, 1)
        # => "year = '2024' AND month = '01'"
    """
    y = str(year)
    m = str(int(month))   # ゼロ埋めしない ('3' のまま)
    return f"year = '{y}' AND month = '{m}'"


def current_month_filter() -> str:
    """今月のパーティションフィルタを返す。"""
    now = datetime.utcnow()
    return partition_filter(now.year, now.month)


def last_month_filter() -> str:
    """先月のパーティションフィルタを返す。"""
    now = datetime.utcnow()
    if now.month == 1:
        return partition_filter(now.year - 1, 12)
    return partition_filter(now.year, now.month - 1)
