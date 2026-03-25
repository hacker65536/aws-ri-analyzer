"""config.yaml の読み込みと検証"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import yaml

DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config.yaml"


@dataclass
class PayerConfig:
    account_id: str


@dataclass
class AnalysisConfig:
    services: Optional[List[str]]       # None = not configured (will prompt)
    sections: Optional[List[str]]       # None = not configured (will prompt)
    regions: List[str]
    lookback_days: int = 7
    expiration_warn_days: int = 90


@dataclass
class Config:
    payer: PayerConfig
    analysis: AnalysisConfig
    _path: Path = DEFAULT_CONFIG_PATH

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
        cfg = cls(
            payer=PayerConfig(account_id=str(payer_raw["account_id"])),
            analysis=AnalysisConfig(
                services=analysis_raw.get("services") or None,
                sections=analysis_raw.get("sections") or None,
                regions=analysis_raw.get("regions", ["ap-northeast-1"]),
                lookback_days=analysis_raw.get("lookback_days", 7),
                expiration_warn_days=analysis_raw.get("expiration_warn_days", 90),
            ),
        )
        cfg._path = config_path
        return cfg

    def save(self, path: str | Path | None = None) -> None:
        config_path = Path(path) if path else self._path
        data = {
            "payer": {
                "account_id": self.payer.account_id,
            },
            "analysis": {
                "services": self.analysis.services,
                "sections": self.analysis.sections,
                "regions": self.analysis.regions,
                "lookback_days": self.analysis.lookback_days,
                "expiration_warn_days": self.analysis.expiration_warn_days,
            },
        }
        with open(config_path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
