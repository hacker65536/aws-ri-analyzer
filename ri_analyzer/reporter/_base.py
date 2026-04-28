"""コンソール出力の共通ユーティリティ（カラー制御・ヘッダー・タイムゾーン）"""

from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

_RED    = "\033[91m"
_YELLOW = "\033[93m"
_GREEN  = "\033[92m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"

_use_color = True
_display_tz: ZoneInfo | None = None  # None = OS ローカル TZ


def set_color(enabled: bool) -> None:
    global _use_color
    _use_color = enabled


def set_display_timezone(tz_name: str | None) -> None:
    """表示用タイムゾーンを設定する。None または空文字の場合は OS ローカル TZ を使用。"""
    global _display_tz
    _display_tz = ZoneInfo(tz_name) if tz_name else None


def to_display_tz(dt: datetime) -> datetime:
    """UTC-aware datetime を表示用 TZ に変換して返す。"""
    if _display_tz is not None:
        return dt.astimezone(_display_tz)
    return dt.astimezone()  # OS ローカル TZ


def _c(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}" if _use_color else text


def _header(title: str) -> None:
    print()
    print(_c("=" * 80, _CYAN))
    print(_c(f"  {title}", _BOLD))
    print(_c("=" * 80, _CYAN))
