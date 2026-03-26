"""config.yaml の読み込みと検証"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import List, Optional

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@dataclass
class PayerConfig:
    account_id: str
    profile: Optional[str] = None   # 省略時は profile_resolver で自動解決


@dataclass
class RecommendationConfig:
    term: str = "ONE_YEAR"               # ONE_YEAR / THREE_YEARS
    payment_option: str = "ALL_UPFRONT"  # ALL_UPFRONT / PARTIAL_UPFRONT / NO_UPFRONT
    lookback_days: int = 30              # CE が受け付ける値: 7 / 30 / 60


@dataclass
class AthenaConfig:
    database: str = "athenacurcfn_cur"
    table: str = "athenacurcfn_cur"
    workgroup: str = "primary"
    output_location: str = ""           # 必須: s3://bucket/prefix/
    result_mode: str = "api"            # "api" | "s3"
    schema_cache_ttl_hours: float = 168.0
    query_cache_ttl_hours: float = 24.0  # クエリ結果キャッシュの有効期間（時間）
    profile: Optional[str] = None       # 省略時は payer プロファイルを流用
    region: str = "ap-northeast-1"


@dataclass
class AnalysisConfig:
    services: Optional[List[str]]       # None = not configured (will prompt)
    sections: Optional[List[str]]       # None = not configured (will prompt)
    regions: List[str]
    lookback_days: int = 7
    expiration_warn_days: int = 90
    cache_ttl_hours: float = 24.0


@dataclass
class Config:
    payer: PayerConfig
    analysis: AnalysisConfig
    recommendation: RecommendationConfig = None  # type: ignore[assignment]
    athena: Optional[AthenaConfig] = None
    _path: Path = DEFAULT_CONFIG_PATH

    def __post_init__(self) -> None:
        if self.recommendation is None:
            self.recommendation = RecommendationConfig()

    @classmethod
    def load(cls, path: str | Path | None = None) -> "Config":
        config_path = Path(path) if path else DEFAULT_CONFIG_PATH
        if not config_path.exists():
            raise FileNotFoundError(f"設定ファイルが見つかりません: {config_path}")

        with open(config_path) as f:
            raw = yaml.safe_load(f)

        payer_raw = raw.get("payer", {})
        if not payer_raw.get("account_id"):
            raise ValueError("config.yaml に payer.account_id が設定されていません")

        analysis_raw = raw.get("analysis", {})
        rec_raw = raw.get("recommendation", {})
        athena_raw = raw.get("athena") or {}

        athena_cfg: Optional[AthenaConfig] = None
        if athena_raw:
            athena_cfg = AthenaConfig(
                database=athena_raw.get("database", "athenacurcfn_cur"),
                table=athena_raw.get("table", "athenacurcfn_cur"),
                workgroup=athena_raw.get("workgroup", "primary"),
                output_location=athena_raw.get("output_location", ""),
                result_mode=athena_raw.get("result_mode", "api"),
                schema_cache_ttl_hours=float(athena_raw.get("schema_cache_ttl_hours", 168.0)),
                query_cache_ttl_hours=float(athena_raw.get("query_cache_ttl_hours", 24.0)),
                profile=athena_raw.get("profile") or None,
                region=athena_raw.get("region", "ap-northeast-1"),
            )

        cfg = cls(
            payer=PayerConfig(
                account_id=str(payer_raw["account_id"]),
                profile=payer_raw.get("profile") or None,
            ),
            analysis=AnalysisConfig(
                services=analysis_raw.get("services") or None,
                sections=analysis_raw.get("sections") or None,
                regions=analysis_raw.get("regions", ["ap-northeast-1"]),
                lookback_days=analysis_raw.get("lookback_days", 7),
                expiration_warn_days=analysis_raw.get("expiration_warn_days", 90),
                cache_ttl_hours=float(analysis_raw.get("cache_ttl_hours", 24.0)),
            ),
            recommendation=RecommendationConfig(
                term=rec_raw.get("term", "ONE_YEAR"),
                payment_option=rec_raw.get("payment_option", "ALL_UPFRONT"),
                lookback_days=int(rec_raw.get("lookback_days", 30)),
            ),
            athena=athena_cfg,
        )
        cfg._path = config_path
        return cfg

    def save(self, path: str | Path | None = None) -> None:
        config_path = Path(path) if path else self._path
        payer_data: dict = {"account_id": self.payer.account_id}
        if self.payer.profile:
            payer_data["profile"] = self.payer.profile
        data = {
            "payer": payer_data,
            "analysis": {
                "services": self.analysis.services,
                "sections": self.analysis.sections,
                "regions": self.analysis.regions,
                "lookback_days": self.analysis.lookback_days,
                "expiration_warn_days": self.analysis.expiration_warn_days,
                "cache_ttl_hours": self.analysis.cache_ttl_hours,
            },
            "recommendation": {
                "term": self.recommendation.term,
                "payment_option": self.recommendation.payment_option,
                "lookback_days": self.recommendation.lookback_days,
            },
        }
        with open(config_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
