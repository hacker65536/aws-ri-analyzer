"""analyzers/coverage.py のユニットテスト"""

import pytest
from ri_analyzer.analyzers.coverage import (
    analyze,
    CoverageSummary,
    _normalize_platform,
    _ELASTICACHE_UNIFIED_PLATFORM,
)
from ri_analyzer.fetchers.cost_explorer import RiCoverageRecord


def _make_record(
    account_id: str = "123456789012",
    region: str = "ap-northeast-1",
    instance_type: str = "db.r6g.large",
    platform: str = "MySQL",
    covered_hours: float = 80.0,
    on_demand_hours: float = 20.0,
    period_start: str = "2026-03-01",
    period_end: str = "2026-03-28",
) -> RiCoverageRecord:
    return RiCoverageRecord(
        account_id=account_id,
        region=region,
        instance_type=instance_type,
        platform=platform,
        period_start=period_start,
        period_end=period_end,
        covered_hours=covered_hours,
        on_demand_hours=on_demand_hours,
        total_hours=covered_hours + on_demand_hours,
        coverage_pct=covered_hours / (covered_hours + on_demand_hours) * 100,
    )


class TestNormalizePlatform:
    def test_redis_exact(self):
        assert _normalize_platform("redis") == _ELASTICACHE_UNIFIED_PLATFORM

    def test_valkey_exact(self):
        assert _normalize_platform("valkey") == _ELASTICACHE_UNIFIED_PLATFORM

    def test_redis_with_version(self):
        assert _normalize_platform("redis 7.0") == _ELASTICACHE_UNIFIED_PLATFORM

    def test_redis_dot_version(self):
        assert _normalize_platform("redis.7.0") == _ELASTICACHE_UNIFIED_PLATFORM

    def test_redis_upper(self):
        assert _normalize_platform("Redis") == _ELASTICACHE_UNIFIED_PLATFORM

    def test_mysql_unchanged(self):
        assert _normalize_platform("MySQL") == "MySQL"

    def test_aurora_unchanged(self):
        assert _normalize_platform("Aurora") == "Aurora"


class TestAnalyze:
    def test_empty(self):
        assert analyze([]) == []

    def test_single_record(self):
        rec = _make_record(covered_hours=80.0, on_demand_hours=20.0)
        summaries = analyze([rec])
        assert len(summaries) == 1
        s = summaries[0]
        assert s.covered_hours == 80.0
        assert s.on_demand_hours == 20.0
        assert s.total_hours == 100.0
        assert s.coverage_pct == pytest.approx(80.0)

    def test_aggregates_same_key(self):
        # 同じ account/region/type/platform のレコードは集計される
        rec1 = _make_record(covered_hours=60.0, on_demand_hours=10.0, period_start="2026-03-01", period_end="2026-03-15")
        rec2 = _make_record(covered_hours=40.0, on_demand_hours=10.0, period_start="2026-03-15", period_end="2026-03-28")
        summaries = analyze([rec1, rec2])
        assert len(summaries) == 1
        s = summaries[0]
        assert s.covered_hours == pytest.approx(100.0)
        assert s.on_demand_hours == pytest.approx(20.0)

    def test_different_accounts_not_aggregated(self):
        rec1 = _make_record(account_id="111111111111")
        rec2 = _make_record(account_id="222222222222")
        summaries = analyze([rec1, rec2])
        assert len(summaries) == 2

    def test_redis_valkey_unified(self):
        rec_redis = _make_record(platform="redis", instance_type="cache.r6g.large", covered_hours=50.0, on_demand_hours=10.0)
        rec_valkey = _make_record(platform="valkey", instance_type="cache.r6g.large", covered_hours=30.0, on_demand_hours=5.0)
        summaries = analyze([rec_redis, rec_valkey])
        # Redis と Valkey は同一グループに統合されるはずだが、Valkey は係数が異なるため NUs が別
        # platform が統一されるので 1 件にまとまる
        assert len(summaries) == 1
        assert summaries[0].platform == _ELASTICACHE_UNIFIED_PLATFORM

    def test_split_engine_separates_redis_valkey(self):
        rec_redis = _make_record(platform="redis", instance_type="cache.r6g.large", covered_hours=50.0, on_demand_hours=10.0)
        rec_valkey = _make_record(platform="valkey", instance_type="cache.r6g.large", covered_hours=30.0, on_demand_hours=5.0)
        summaries = analyze([rec_redis, rec_valkey], split_engine=True)
        assert len(summaries) == 2
        platforms = {s.platform for s in summaries}
        assert "redis" in platforms
        assert "valkey" in platforms

    def test_split_engine_false_unifies(self):
        rec_redis = _make_record(platform="redis", instance_type="cache.r6g.large")
        rec_valkey = _make_record(platform="valkey", instance_type="cache.r6g.large")
        summaries = analyze([rec_redis, rec_valkey], split_engine=False)
        assert len(summaries) == 1

    def test_coverage_pct_property(self):
        s = CoverageSummary(
            account_id="x", region="r", instance_type="db.r5.large",
            platform="MySQL",
            covered_hours=75.0, on_demand_hours=25.0, total_hours=100.0,
            covered_nus=0, on_demand_nus=0, total_nus=0,
        )
        assert s.coverage_pct == pytest.approx(75.0)

    def test_coverage_pct_zero_total(self):
        s = CoverageSummary(
            account_id="x", region="r", instance_type="db.r5.large",
            platform="MySQL",
            covered_hours=0, on_demand_hours=0, total_hours=0,
            covered_nus=0, on_demand_nus=0, total_nus=0,
        )
        assert s.coverage_pct == 0.0

    def test_status_ok(self):
        s = CoverageSummary(
            account_id="x", region="r", instance_type="db.r5.large",
            platform="MySQL",
            covered_hours=95.0, on_demand_hours=5.0, total_hours=100.0,
            covered_nus=0, on_demand_nus=0, total_nus=0,
        )
        assert s.status == "ok"

    def test_status_warning(self):
        s = CoverageSummary(
            account_id="x", region="r", instance_type="db.r5.large",
            platform="MySQL",
            covered_hours=70.0, on_demand_hours=30.0, total_hours=100.0,
            covered_nus=0, on_demand_nus=0, total_nus=0,
        )
        assert s.status == "warning"

    def test_status_low(self):
        s = CoverageSummary(
            account_id="x", region="r", instance_type="db.r5.large",
            platform="MySQL",
            covered_hours=30.0, on_demand_hours=70.0, total_hours=100.0,
            covered_nus=0, on_demand_nus=0, total_nus=0,
        )
        assert s.status == "low"
