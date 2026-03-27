"""コンソール出力の共通ユーティリティ（カラー制御・ヘッダー）"""

from __future__ import annotations

_RED    = "\033[91m"
_YELLOW = "\033[93m"
_GREEN  = "\033[92m"
_CYAN   = "\033[96m"
_BOLD   = "\033[1m"
_RESET  = "\033[0m"

_use_color = True


def set_color(enabled: bool) -> None:
    global _use_color
    _use_color = enabled


def _c(text: str, code: str) -> str:
    return f"{code}{text}{_RESET}" if _use_color else text


def _header(title: str) -> None:
    print()
    print(_c("=" * 80, _CYAN))
    print(_c(f"  {title}", _BOLD))
    print(_c("=" * 80, _CYAN))
