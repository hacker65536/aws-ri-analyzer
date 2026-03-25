"""RDS インスタンスサイズ正規化

AWS が定義する Normalization Factor を用いて、
異なるサイズの RI と実インスタンスを同一スケールで比較する。

参考: https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/USER_WorkingWithReservedDBInstances.html
     (Instance size flexibility は Single-AZ・対象エンジンのみ適用)
"""

from __future__ import annotations

# サイズ → 正規化係数
_SIZE_FACTOR: dict[str, float] = {
    "micro":    0.5,
    "small":    1.0,
    "medium":   2.0,
    "large":    4.0,
    "xlarge":   8.0,
    "2xlarge":  16.0,
    "4xlarge":  32.0,
    "8xlarge":  64.0,
    "10xlarge": 80.0,
    "12xlarge": 96.0,
    "16xlarge": 128.0,
    "24xlarge": 192.0,
    "32xlarge": 256.0,
}

# インスタンスサイズフレキシビリティが適用されるエンジン（Single-AZ のみ）
_FLEXIBLE_ENGINES: frozenset[str] = frozenset({
    "mysql",
    "postgres",
    "mariadb",
    "aurora",
    "aurora-mysql",
    "aurora-postgresql",
})


def normalization_factor(instance_class: str) -> float:
    """
    'db.m5.large' → 4.0 のように正規化係数を返す。
    未知のサイズの場合は 1.0 を返す。
    """
    size = _parse_size(instance_class)
    return _SIZE_FACTOR.get(size, 1.0)


def instance_family(instance_class: str) -> str:
    """'db.m5.large' → 'm5'"""
    parts = instance_class.lstrip("db.").split(".")
    return parts[0] if parts else instance_class


def instance_size(instance_class: str) -> str:
    """'db.m5.large' → 'large'"""
    return _parse_size(instance_class)


def is_size_flexible(engine: str, multi_az: bool) -> bool:
    """
    このエンジン × Multi-AZ 設定でインスタンスサイズフレキシビリティが
    適用されるかを返す。

    条件: Single-AZ (multi_az=False) かつ対象エンジン
    """
    return (not multi_az) and (engine.lower() in _FLEXIBLE_ENGINES)


def _parse_size(instance_class: str) -> str:
    """'db.m5.2xlarge' → '2xlarge'"""
    # db.m5.large / db.r6g.4xlarge など
    parts = instance_class.lstrip("db.").split(".")
    return parts[-1] if len(parts) >= 2 else parts[0]
