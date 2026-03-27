"""JSON 出力フォーマッター

--output json 指定時に全セクションのデータを JSON として stdout に出力する。
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime


def _serialize(obj: object) -> object:
    """dataclass / datetime を JSON シリアライズ可能な型に変換する。"""
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        return {k: _serialize(v) for k, v in dataclasses.asdict(obj).items()}
    if isinstance(obj, datetime):
        return obj.isoformat()
    if isinstance(obj, list):
        return [_serialize(item) for item in obj]
    if isinstance(obj, dict):
        return {k: _serialize(v) for k, v in obj.items()}
    return obj


def dump(results: dict) -> None:
    """results を整形 JSON として stdout に出力する。"""
    print(json.dumps(_serialize(results), ensure_ascii=False, indent=2))
