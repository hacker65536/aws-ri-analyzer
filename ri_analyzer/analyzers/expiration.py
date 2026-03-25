"""RI 有効期限分析

RI の end_time を計算し、期限切れ / 警告 / 正常 に分類する。
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

# RdsReservedInstance と同じインターフェースを持つオブジェクトに対して動作する。
# 将来 ElastiCache / OpenSearch 等にも使い回せるよう型を緩く受ける。


@dataclass
class ExpirationResult:
    ri: object                  # RdsReservedInstance など
    days_remaining: int
    status: str                 # "expired" / "warning" / "ok"


def analyze(
    reserved_instances: list,
    warn_days: int = 90,
) -> tuple[list[ExpirationResult], list[ExpirationResult], list[ExpirationResult]]:
    """
    RI リストを有効期限で分類する。

    Returns
    -------
    (expired, warning, ok) のタプル
      expired : 期限切れ（days_remaining < 0）
      warning : warn_days 以内に期限切れ
      ok      : それ以外
    """
    expired: list[ExpirationResult] = []
    warning: list[ExpirationResult] = []
    ok:      list[ExpirationResult] = []

    for ri in reserved_instances:
        days = ri.days_remaining
        if days < 0:
            status = "expired"
            expired.append(ExpirationResult(ri=ri, days_remaining=days, status=status))
        elif days <= warn_days:
            status = "warning"
            warning.append(ExpirationResult(ri=ri, days_remaining=days, status=status))
        else:
            status = "ok"
            ok.append(ExpirationResult(ri=ri, days_remaining=days, status=status))

    # 期限が近い順にソート
    for lst in (expired, warning, ok):
        lst.sort(key=lambda r: r.days_remaining)

    return expired, warning, ok
