"""normalizer.py のユニットテスト"""

import pytest
from ri_analyzer.normalizer import (
    normalization_factor,
    instance_family,
    instance_size,
    is_size_flexible,
)


class TestNormalizationFactor:
    def test_large(self):
        assert normalization_factor("db.m5.large") == 4.0

    def test_micro(self):
        assert normalization_factor("db.t3.micro") == 0.5

    def test_xlarge(self):
        assert normalization_factor("db.r6g.xlarge") == 8.0

    def test_2xlarge(self):
        assert normalization_factor("db.r5.2xlarge") == 16.0

    def test_32xlarge(self):
        assert normalization_factor("db.x2g.32xlarge") == 256.0

    def test_unknown_size_returns_1(self):
        assert normalization_factor("db.m5.unknown") == 1.0

    def test_cache_prefix(self):
        assert normalization_factor("cache.r6g.large") == 4.0


class TestInstanceFamily:
    def test_rds(self):
        assert instance_family("db.m5.large") == "m5"

    def test_r6g(self):
        assert instance_family("db.r6g.4xlarge") == "r6g"

    def test_cache_prefix_not_supported(self):
        # instance_family は RDS (db.*) 専用のため cache.* はファミリーを正しく抽出できない
        # 実際の動作を文書化するテスト
        assert instance_family("cache.r6g.large") == "cache"


class TestInstanceSize:
    def test_large(self):
        assert instance_size("db.m5.large") == "large"

    def test_4xlarge(self):
        assert instance_size("db.r6g.4xlarge") == "4xlarge"

    def test_micro(self):
        assert instance_size("db.t3.micro") == "micro"


class TestIsSizeFlexible:
    def test_single_az_mysql(self):
        assert is_size_flexible("mysql", multi_az=False) is True

    def test_single_az_aurora(self):
        assert is_size_flexible("aurora", multi_az=False) is True

    def test_single_az_aurora_postgresql(self):
        assert is_size_flexible("aurora-postgresql", multi_az=False) is True

    def test_multi_az_mysql(self):
        assert is_size_flexible("mysql", multi_az=True) is False

    def test_single_az_oracle(self):
        # Oracle は非対応
        assert is_size_flexible("oracle-ee", multi_az=False) is False

    def test_case_insensitive(self):
        assert is_size_flexible("MySQL", multi_az=False) is True

    def test_case_insensitive_aurora(self):
        assert is_size_flexible("Aurora-MySQL", multi_az=False) is True
