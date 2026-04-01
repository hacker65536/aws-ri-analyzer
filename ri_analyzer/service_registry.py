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
    cur_product_code: str = ""  # CUR の line_item_product_code 値
    has_nu_flexibility: bool = True  # 正規化ユニットによるファミリー内サイズ柔軟性の有無


# サービスキー → ServiceConfig
# 新サービス追加時はここにエントリを追加する
SERVICES: dict[str, ServiceConfig] = {
    "rds": ServiceConfig(
        ce_service_name    = "Amazon Relational Database Service",
        engine_dimension   = "DATABASE_ENGINE",
        engine_attr        = "databaseEngine",
        instance_detail_key= "RDSInstanceDetails",
        cur_product_code   = "AmazonRDS",
    ),
    "elasticache": ServiceConfig(
        ce_service_name    = "Amazon ElastiCache",
        engine_dimension   = "CACHE_ENGINE",
        engine_attr        = "cacheEngine",
        instance_detail_key= "ElastiCacheInstanceDetails",
        cur_product_code   = "AmazonElastiCache",
    ),
    "opensearch": ServiceConfig(
        ce_service_name    = "Amazon OpenSearch Service",
        engine_dimension   = "",          # Coverage GroupBy でエンジン次元は未サポート
        engine_attr        = "",
        instance_detail_key= "ESInstanceDetails",
        cur_product_code   = "AmazonES",  # 旧称 Elasticsearch のプロダクトコードが継続使用される
        has_nu_flexibility = False,       # OpenSearch RI はサイズ間の柔軟性なし
    ),
    "redshift": ServiceConfig(
        ce_service_name    = "Amazon Redshift",
        engine_dimension   = "DATABASE_ENGINE",
        engine_attr        = "databaseEngine",
        instance_detail_key= "",
        cur_product_code   = "AmazonRedshift",
    ),
    "ec2": ServiceConfig(
        ce_service_name    = "Amazon Elastic Compute Cloud - Compute",
        engine_dimension   = "PLATFORM",
        engine_attr        = "platform",
        instance_detail_key= "EC2InstanceDetails",
        cur_product_code   = "AmazonEC2",
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
