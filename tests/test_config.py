"""config.py のユニットテスト"""

import textwrap
from pathlib import Path

import pytest
import yaml

from ri_analyzer.config import (
    Config,
    PayerConfig,
    AnalysisConfig,
    RecommendationConfig,
    AthenaConfig,
)


def _write_config(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content))
    return p


class TestConfigLoad:
    def test_minimal_config(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: '123456789012'
            analysis:
              regions: [ap-northeast-1]
              services: [rds]
              sections: [coverage]
        """)
        cfg = Config.load(cfg_path)
        assert cfg.payer.account_id == "123456789012"
        assert cfg.payer.profile is None

    def test_recommendation_defaults(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: '123456789012'
            analysis:
              regions: [ap-northeast-1]
        """)
        cfg = Config.load(cfg_path)
        assert cfg.recommendation.term == "ONE_YEAR"
        assert cfg.recommendation.payment_option == "ALL_UPFRONT"
        assert cfg.recommendation.lookback_days == 30

    def test_missing_account_id_raises(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer: {}
            analysis:
              regions: [ap-northeast-1]
        """)
        with pytest.raises(ValueError, match="account_id"):
            Config.load(cfg_path)

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            Config.load(tmp_path / "nonexistent.yaml")

    def test_athena_config_parsed(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: '123456789012'
            analysis:
              regions: [ap-northeast-1]
            athena:
              database: my_database
              table: my_table
              workgroup: my_wg
              output_location: s3://my-bucket/
              result_mode: s3
              schema_cache_ttl_hours: 48
              query_cache_ttl_hours: 12
              region: us-east-1
        """)
        cfg = Config.load(cfg_path)
        assert cfg.athena is not None
        assert cfg.athena.database == "my_database"
        assert cfg.athena.table == "my_table"
        assert cfg.athena.workgroup == "my_wg"
        assert cfg.athena.result_mode == "s3"
        assert cfg.athena.schema_cache_ttl_hours == 48.0
        assert cfg.athena.query_cache_ttl_hours == 12.0
        assert cfg.athena.region == "us-east-1"

    def test_no_athena_section(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: '123456789012'
            analysis:
              regions: [ap-northeast-1]
        """)
        cfg = Config.load(cfg_path)
        assert cfg.athena is None

    def test_analysis_defaults(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: '123456789012'
            analysis:
              regions: [ap-northeast-1]
        """)
        cfg = Config.load(cfg_path)
        assert cfg.analysis.lookback_days == 7
        assert cfg.analysis.expiration_warn_days == 90
        assert cfg.analysis.cache_ttl_hours == 24.0

    def test_account_id_converted_to_str(self, tmp_path):
        # YAML で整数として解釈されても str に変換されること
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: 123456789012
            analysis:
              regions: [ap-northeast-1]
        """)
        cfg = Config.load(cfg_path)
        assert isinstance(cfg.payer.account_id, str)

    def test_payer_profile_parsed(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: '123456789012'
              profile: my-sso-profile
            analysis:
              regions: [ap-northeast-1]
        """)
        cfg = Config.load(cfg_path)
        assert cfg.payer.profile == "my-sso-profile"

    def test_services_none_when_missing(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: '123456789012'
            analysis:
              regions: [ap-northeast-1]
        """)
        cfg = Config.load(cfg_path)
        assert cfg.analysis.services is None

    def test_recommendation_custom(self, tmp_path):
        cfg_path = _write_config(tmp_path, """
            payer:
              account_id: '123456789012'
            analysis:
              regions: [ap-northeast-1]
            recommendation:
              term: THREE_YEARS
              payment_option: NO_UPFRONT
              lookback_days: 60
        """)
        cfg = Config.load(cfg_path)
        assert cfg.recommendation.term == "THREE_YEARS"
        assert cfg.recommendation.payment_option == "NO_UPFRONT"
        assert cfg.recommendation.lookback_days == 60
