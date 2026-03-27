"""analyzers/expiration.py のユニットテスト"""

from datetime import datetime, timezone, timedelta
from types import SimpleNamespace

import pytest
from ri_analyzer.analyzers.expiration import analyze, ExpirationResult


def _make_ri(days_remaining: int) -> object:
    """days_remaining プロパティを持つダミー RI オブジェクトを生成する。"""
    return SimpleNamespace(days_remaining=days_remaining, subscription_id="dummy")


class TestAnalyze:
    def test_empty_list(self):
        expired, warning, ok = analyze([])
        assert expired == [] and warning == [] and ok == []

    def test_single_expired(self):
        ri = _make_ri(-1)
        expired, warning, ok = analyze([ri])
        assert len(expired) == 1
        assert expired[0].status == "expired"
        assert warning == [] and ok == []

    def test_single_warning(self):
        ri = _make_ri(45)
        expired, warning, ok = analyze([ri], warn_days=90)
        assert len(warning) == 1
        assert warning[0].status == "warning"
        assert expired == [] and ok == []

    def test_single_ok(self):
        ri = _make_ri(120)
        expired, warning, ok = analyze([ri], warn_days=90)
        assert len(ok) == 1
        assert ok[0].status == "ok"

    def test_boundary_warn_days(self):
        # days_remaining == warn_days は warning に含まれる
        ri = _make_ri(90)
        expired, warning, ok = analyze([ri], warn_days=90)
        assert len(warning) == 1

    def test_boundary_zero(self):
        # days_remaining == 0 は expired（< 0 は false だが expired 扱いにはならない）
        ri = _make_ri(0)
        expired, warning, ok = analyze([ri], warn_days=90)
        # 0 は expired ではなく warning（<= warn_days）
        assert len(warning) == 1

    def test_sorted_by_days_remaining(self):
        ris = [_make_ri(d) for d in [80, 30, 60]]
        _, warning, _ = analyze(ris, warn_days=90)
        days = [r.days_remaining for r in warning]
        assert days == sorted(days)

    def test_mixed(self):
        ris = [_make_ri(-5), _make_ri(30), _make_ri(200)]
        expired, warning, ok = analyze(ris, warn_days=90)
        assert len(expired) == 1
        assert len(warning) == 1
        assert len(ok) == 1

    def test_result_ri_reference(self):
        ri = _make_ri(10)
        _, warning, _ = analyze([ri], warn_days=90)
        assert warning[0].ri is ri
