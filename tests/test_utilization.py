"""analyzers/utilization.py のユニットテスト"""

import pytest
from ri_analyzer.analyzers.utilization import (
    summarize,
    UtilizationSummary,
    _parse_instance_family,
    _parse_instance_size,
    _norm_factor,
    _norm_factor_for_engine,
    _NORM_FACTOR,
    _VALKEY_NORM_FACTOR,
)
from ri_analyzer.fetchers.cost_explorer import RiUtilizationRecord


def _make_record(
    subscription_id: str = "sub-001",
    instance_type: str = "db.r6g.large",
    region: str = "ap-northeast-1",
    platform: str = "MySQL",
    count: int = 2,
    utilization_pct: float = 85.0,
    period_start: str = "2026-03-01",
    period_end: str = "2026-03-08",
    purchased_hours: float = 336.0,
    used_hours: float = 285.6,
    unused_hours: float = 50.4,
    net_savings: float = 100.0,
    on_demand_cost_if_used: float = 200.0,
    amortized_fee: float = 100.0,
) -> RiUtilizationRecord:
    return RiUtilizationRecord(
        subscription_id=subscription_id,
        period_start=period_start,
        period_end=period_end,
        instance_type=instance_type,
        region=region,
        platform=platform,
        count=count,
        utilization_pct=utilization_pct,
        purchased_hours=purchased_hours,
        used_hours=used_hours,
        unused_hours=unused_hours,
        net_savings=net_savings,
        on_demand_cost_if_used=on_demand_cost_if_used,
        amortized_fee=amortized_fee,
    )


class TestParseHelpers:
    def test_parse_instance_family(self):
        assert _parse_instance_family("db.r5.large") == "r5"
        assert _parse_instance_family("cache.r6g.xlarge") == "r6g"

    def test_parse_instance_size(self):
        assert _parse_instance_size("db.r5.large") == "large"
        assert _parse_instance_size("cache.r6g.2xlarge") == "2xlarge"

    def test_norm_factor_large(self):
        assert _norm_factor("db.r5.large") == 4.0

    def test_norm_factor_xlarge(self):
        assert _norm_factor("db.r5.xlarge") == 8.0

    def test_norm_factor_unknown(self):
        assert _norm_factor("db.r5.weird") == 1.0


class TestNormFactorForEngine:
    def test_mysql_large(self):
        assert _norm_factor_for_engine("cache.r6g.large", "MySQL") == _NORM_FACTOR["large"]

    def test_valkey_large(self):
        assert _norm_factor_for_engine("cache.r6g.large", "valkey") == _VALKEY_NORM_FACTOR["large"]

    def test_valkey_is_08x_redis(self):
        redis_factor = _norm_factor_for_engine("cache.r6g.large", "redis")
        valkey_factor = _norm_factor_for_engine("cache.r6g.large", "valkey")
        assert valkey_factor == pytest.approx(redis_factor * 0.8, rel=1e-6)

    def test_valkey_case_insensitive(self):
        lower = _norm_factor_for_engine("cache.r6g.large", "valkey")
        upper = _norm_factor_for_engine("cache.r6g.large", "Valkey")
        assert lower == upper


class TestSummarize:
    def test_empty(self):
        assert summarize([]) == []

    def test_single_record(self):
        rec = _make_record()
        summaries = summarize([rec])
        assert len(summaries) == 1
        s = summaries[0]
        assert s.subscription_id == "sub-001"
        assert s.avg_utilization_pct == pytest.approx(85.0)

    def test_groups_by_subscription_id(self):
        rec1 = _make_record(subscription_id="sub-001", period_start="2026-03-01", period_end="2026-03-08")
        rec2 = _make_record(subscription_id="sub-001", period_start="2026-03-08", period_end="2026-03-15")
        rec3 = _make_record(subscription_id="sub-002")
        summaries = summarize([rec1, rec2, rec3])
        assert len(summaries) == 2
        sub1 = next(s for s in summaries if s.subscription_id == "sub-001")
        assert len(sub1.periods) == 2

    def test_avg_utilization(self):
        rec1 = _make_record(subscription_id="sub-001", utilization_pct=80.0, period_start="2026-03-01", period_end="2026-03-08")
        rec2 = _make_record(subscription_id="sub-001", utilization_pct=60.0, period_start="2026-03-08", period_end="2026-03-15")
        summaries = summarize([rec1, rec2])
        assert summaries[0].avg_utilization_pct == pytest.approx(70.0)

    def test_periods_sorted(self):
        rec1 = _make_record(subscription_id="sub-001", period_start="2026-03-08", period_end="2026-03-15")
        rec2 = _make_record(subscription_id="sub-001", period_start="2026-03-01", period_end="2026-03-08")
        summaries = summarize([rec1, rec2])
        starts = [p.period_start for p in summaries[0].periods]
        assert starts == sorted(starts)

    def test_status_ok(self):
        rec = _make_record(utilization_pct=85.0)
        s = summarize([rec])[0]
        assert s.status == "ok"

    def test_status_warning(self):
        rec = _make_record(utilization_pct=65.0)
        s = summarize([rec])[0]
        assert s.status == "warning"

    def test_status_low(self):
        rec = _make_record(utilization_pct=30.0)
        s = summarize([rec])[0]
        assert s.status == "low"

    def test_total_unused_hours(self):
        rec1 = _make_record(subscription_id="sub-001", unused_hours=10.0, period_start="2026-03-01", period_end="2026-03-08")
        rec2 = _make_record(subscription_id="sub-001", unused_hours=15.0, period_start="2026-03-08", period_end="2026-03-15")
        s = summarize([rec1, rec2])[0]
        assert s.total_unused_hours == pytest.approx(25.0)

    def test_normalized_units_mysql_large(self):
        rec = _make_record(instance_type="db.r6g.large", platform="MySQL", count=3)
        s = summarize([rec])[0]
        # large = 4.0 NU, count=3 → 12.0 NU
        assert s.normalized_units == pytest.approx(12.0)

    def test_normalized_units_valkey(self):
        rec = _make_record(instance_type="cache.r6g.large", platform="valkey", count=2)
        s = summarize([rec])[0]
        # Valkey large = 3.2 NU, count=2 → 6.4 NU
        assert s.normalized_units == pytest.approx(6.4)
