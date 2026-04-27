"""AWS オンデマンド時間単価の取得・キャッシュ

AWS Pricing API (us-east-1エンドポイント) からオンデマンド時間単価を取得する。
対応サービス: rds, elasticache, opensearch

単体テスト:
    python -m ri_analyzer.pricing rds db.r6g.large ap-northeast-1 --engine "aurora mysql"
    python -m ri_analyzer.pricing elasticache cache.r6g.large ap-northeast-1 --engine redis
"""

from __future__ import annotations

import json
from typing import Any

from ri_analyzer.cache import CacheStore

# Pricing API は us-east-1 のみ提供
_PRICING_API_REGION = "us-east-1"

_REGION_TO_LOCATION: dict[str, str] = {
    "ap-northeast-1": "Asia Pacific (Tokyo)",
    "ap-northeast-2": "Asia Pacific (Seoul)",
    "ap-northeast-3": "Asia Pacific (Osaka)",
    "ap-southeast-1": "Asia Pacific (Singapore)",
    "ap-southeast-2": "Asia Pacific (Sydney)",
    "ap-south-1":     "Asia Pacific (Mumbai)",
    "us-east-1":      "US East (N. Virginia)",
    "us-east-2":      "US East (Ohio)",
    "us-west-1":      "US West (N. California)",
    "us-west-2":      "US West (Oregon)",
    "eu-west-1":      "Europe (Ireland)",
    "eu-west-2":      "Europe (London)",
    "eu-west-3":      "Europe (Paris)",
    "eu-central-1":   "Europe (Frankfurt)",
    "eu-north-1":     "Europe (Stockholm)",
    "ca-central-1":   "Canada (Central)",
    "sa-east-1":      "South America (Sao Paulo)",
}

_SERVICE_CODE: dict[str, str] = {
    "rds":         "AmazonRDS",
    "elasticache": "AmazonElastiCache",
    "opensearch":  "AmazonES",
}


class AwsPricingClient:
    """AWS Pricing API からオンデマンド時間単価を取得・キャッシュするクライアント。

    料金はほぼ変わらないため TTL は 7 日 (168 時間) がデフォルト。
    """

    def __init__(
        self,
        session: Any = None,
        cache: CacheStore | None = None,
        cache_ttl_hours: float = 168.0,
    ) -> None:
        import boto3
        self._session = session or boto3.Session()
        self._cache = cache or CacheStore(ttl_hours=cache_ttl_hours)
        self._client = self._session.client("pricing", region_name=_PRICING_API_REGION)

    def get_od_price(
        self,
        service: str,
        instance_type: str,
        region: str,
        engine: str | None = None,
        deployment: str | None = None,
    ) -> float | None:
        """オンデマンド時間単価（USD/hr）を返す。取得失敗時は None。

        Parameters
        ----------
        service       : "rds" | "elasticache" | "opensearch"
        instance_type : "db.r6g.large" / "cache.r6g.large" など（サービスプレフィクス込み）
        region        : "ap-northeast-1" などのリージョンコード
        engine        : RDS なら "aurora mysql" など。省略可だが精度が下がる
        deployment    : RDS の "Single-AZ" / "Multi-AZ"。省略時はエンジンから自動決定
        """
        resolved_deployment = deployment or _auto_deployment(service, engine)
        cache_key = (
            f"pricing:v1:{service}:{instance_type}:{region}"
            f":{(engine or '').lower()}:{resolved_deployment}"
        )
        cached = self._cache.get(cache_key)
        if cached is not None:
            return cached

        price = self._fetch(service, instance_type, region, engine, resolved_deployment)
        if price is not None:
            self._cache.set(cache_key, price)
        return price

    def _fetch(
        self,
        service: str,
        instance_type: str,
        region: str,
        engine: str | None,
        deployment: str,
    ) -> float | None:
        service_code = _SERVICE_CODE.get(service.lower())
        if not service_code:
            raise ValueError(f"未対応サービス: {service!r}. 対応: {list(_SERVICE_CODE)}")
        location = _REGION_TO_LOCATION.get(region)
        if not location:
            raise ValueError(f"未対応リージョン: {region!r}")

        filters = [
            {"Type": "TERM_MATCH", "Field": "location",     "Value": location},
            {"Type": "TERM_MATCH", "Field": "instanceType", "Value": instance_type},
        ]
        filters += _service_filters(service, engine, deployment)

        try:
            paginator = self._client.get_paginator("get_products")
            prices: list[float] = []
            for page in paginator.paginate(ServiceCode=service_code, Filters=filters):
                for price_str in page.get("PriceList", []):
                    price = _extract_od_price(price_str)
                    if price is not None:
                        prices.append(price)
            # 複数ヒット時（IO最適化など高価格バリアントを除外）は最小値＝標準価格
            return min(prices) if prices else None
        except Exception as exc:
            import warnings
            warnings.warn(f"Pricing API エラー ({service} {instance_type}): {exc}")
        return None


# ──────────────────────────────────────────────
# フィルタ生成ヘルパー
# ──────────────────────────────────────────────

def _auto_deployment(service: str, engine: str | None) -> str:
    """engine 名から deploymentOption を自動決定する（RDS のみ意味あり）。

    Aurora の deploymentOption は Pricing API では "Single-AZ" として登録されている。
    """
    return "Single-AZ"


def _service_filters(service: str, engine: str | None, deployment: str) -> list[dict]:
    svc = service.lower()
    if svc == "rds":
        fs = [{"Type": "TERM_MATCH", "Field": "deploymentOption", "Value": deployment}]
        if engine:
            fs.append({
                "Type": "TERM_MATCH",
                "Field": "databaseEngine",
                "Value": _normalize_rds_engine(engine),
            })
        return fs
    if svc == "elasticache":
        if not engine:
            return []
        return [{
            "Type": "TERM_MATCH",
            "Field": "cacheEngine",
            "Value": _normalize_cache_engine(engine),
        }]
    # opensearch: instanceType + location だけで一意
    return []


def _normalize_rds_engine(engine: str) -> str:
    lower = engine.lower()
    if "aurora" in lower and "postgres" in lower:
        return "Aurora PostgreSQL"
    if "aurora" in lower:
        return "Aurora MySQL"
    if "postgres" in lower:
        return "PostgreSQL"
    if "mysql" in lower:
        return "MySQL"
    if "mariadb" in lower:
        return "MariaDB"
    if "oracle" in lower:
        return "Oracle"
    if "sql server" in lower or "sqlserver" in lower:
        return "SQL Server"
    return engine


def _normalize_cache_engine(engine: str) -> str:
    lower = engine.lower()
    if "redis" in lower or "valkey" in lower:
        return "Redis"
    if "memcached" in lower:
        return "Memcached"
    return engine


def _extract_od_price(price_str: str) -> float | None:
    """PriceList JSON 文字列からオンデマンド時間単価 (USD) を抽出する。"""
    try:
        product = json.loads(price_str)
        for term in product.get("terms", {}).get("OnDemand", {}).values():
            for dim in term.get("priceDimensions", {}).values():
                usd = dim.get("pricePerUnit", {}).get("USD")
                if usd is not None:
                    price = float(usd)
                    if price > 0:
                        return price
    except Exception:
        pass
    return None


# ──────────────────────────────────────────────
# 単体 CLI
# ──────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    import boto3

    parser = argparse.ArgumentParser(
        description="AWS オンデマンド時間単価を確認する",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
例:
  python -m ri_analyzer.pricing rds db.r6g.large ap-northeast-1 --engine "aurora mysql"
  python -m ri_analyzer.pricing rds db.t4g.medium ap-northeast-1 --engine "aurora mysql"
  python -m ri_analyzer.pricing elasticache cache.r6g.large ap-northeast-1 --engine redis
  python -m ri_analyzer.pricing opensearch r6g.large.search ap-northeast-1
        """,
    )
    parser.add_argument("service", choices=list(_SERVICE_CODE), help="サービス名")
    parser.add_argument("instance_type", help="インスタンスタイプ (例: db.r6g.large)")
    parser.add_argument("region", help="リージョンコード (例: ap-northeast-1)")
    parser.add_argument("--engine", default=None, help="エンジン名 (例: aurora mysql, redis)")
    parser.add_argument("--deployment", default=None, help="deploymentOption (デフォルト: エンジンから自動判定)")
    parser.add_argument("--profile", default=None, help="AWS プロファイル名")
    parser.add_argument("--no-cache", action="store_true", help="キャッシュを無視して再取得")
    args = parser.parse_args()

    session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
    cache = CacheStore(ttl_hours=0.0 if args.no_cache else 168.0)
    client = AwsPricingClient(session=session, cache=cache)

    price = client.get_od_price(
        service=args.service,
        instance_type=args.instance_type,
        region=args.region,
        engine=args.engine,
        deployment=args.deployment,
    )

    if price is not None:
        print(f"${price:.4f}/hr  ({args.service} {args.instance_type} {args.region}"
              f"{' engine=' + args.engine if args.engine else ''})")
    else:
        print("取得できませんでした。--engine を指定するか、インスタンスタイプ/リージョンを確認してください。")
