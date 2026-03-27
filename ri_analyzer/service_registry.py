"""サービス別マッピング定義の一元管理

新しいサービスを追加するときはこのファイルだけ編集すれば良い。
各マッピングは AWS Cost Explorer API の仕様に基づく。
"""

from __future__ import annotations
from dataclasses import dataclass


@dataclass(frozen=True)
class ServiceConfig:
    """サービスごとの CE API パラメータ定義"""
    ce_service_name: str      # CE に渡す Service 名
    engine_dimension: str     # GetReservationCoverage の GroupBy キー
    engine_attr: str          # CE レスポンスの Attributes キー
    instance_detail_key: str  # GetReservationPurchaseRecommendation の InstanceDetails キー


# サービスキー → ServiceConfig
# 新サービス追加時はここにエントリを追加する
SERVICES: dict[str, ServiceConfig] = {
    "rds": ServiceConfig(
        ce_service_name    = "Amazon Relational Database Service",
        engine_dimension   = "DATABASE_ENGINE",
        engine_attr        = "databaseEngine",
        instance_detail_key= "RDSInstanceDetails",
    ),
    "elasticache": ServiceConfig(
        ce_service_name    = "Amazon ElastiCache",
        engine_dimension   = "CACHE_ENGINE",
        engine_attr        = "cacheEngine",
        instance_detail_key= "ElastiCacheInstanceDetails",
    ),
    "opensearch": ServiceConfig(
        ce_service_name    = "Amazon OpenSearch Service",
        engine_dimension   = "DATABASE_ENGINE",
        engine_attr        = "databaseEngine",
        instance_detail_key= "ESInstanceDetails",
    ),
    "redshift": ServiceConfig(
        ce_service_name    = "Amazon Redshift",
        engine_dimension   = "DATABASE_ENGINE",
        engine_attr        = "databaseEngine",
        instance_detail_key= "",
    ),
    "ec2": ServiceConfig(
        ce_service_name    = "Amazon Elastic Compute Cloud - Compute",
        engine_dimension   = "PLATFORM",
        engine_attr        = "platform",
        instance_detail_key= "EC2InstanceDetails",
    ),
}


def get_service(service_key: str) -> ServiceConfig:
    """サービスキーから ServiceConfig を返す。未対応の場合は ValueError。"""
    cfg = SERVICES.get(service_key)
    if cfg is None:
        raise ValueError(
            f"未対応のサービスです: '{service_key}'。"
            f"対応: {sorted(SERVICES)}"
        )
    return cfg
