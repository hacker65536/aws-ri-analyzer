"""RI データのローカルディスクキャッシュ

キャッシュファイルは ~/.cache/ri-analyzer/ に pickle 形式で保存する。
キャッシュキーは SHA-256 ハッシュ（先頭 16 文字）でファイル名を決定する。
"""

from __future__ import annotations

import hashlib
import pickle
from datetime import datetime, timezone
from pathlib import Path


_DEFAULT_CACHE_DIR = Path.home() / ".cache" / "ri-analyzer"

# データ構造が変わったときにインクリメントする。
# 古いキャッシュファイルはハッシュが変わるため自動的に無視される。
_CACHE_VERSION = "3"


class CacheStore:
    def __init__(
        self,
        cache_dir: Path | None = None,
        ttl_hours: float = 24.0,
    ) -> None:
        self.cache_dir = cache_dir or _DEFAULT_CACHE_DIR
        self.ttl_seconds = ttl_hours * 3600

    def _key_path(self, key: str) -> Path:
        digest = hashlib.sha256(f"{_CACHE_VERSION}:{key}".encode()).hexdigest()[:16]
        return self.cache_dir / f"{digest}.pkl"

    def get(self, key: str):
        """キャッシュを返す。TTL 切れ・存在しない場合は None。"""
        path = self._key_path(key)
        if not path.exists():
            return None
        try:
            with path.open("rb") as f:
                entry = pickle.load(f)
        except Exception:
            return None
        age = (datetime.now(timezone.utc) - entry["created_at"]).total_seconds()
        if age > self.ttl_seconds:
            return None
        return entry["data"]

    def set(self, key: str, data) -> None:
        """データをキャッシュに保存する。"""
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        path = self._key_path(key)
        entry = {"created_at": datetime.now(timezone.utc), "data": data}
        with path.open("wb") as f:
            pickle.dump(entry, f)

    def purge_expired(self) -> int:
        """TTL 切れの pkl ファイルを削除する。削除件数を返す。"""
        if not self.cache_dir.exists():
            return 0
        removed = 0
        for path in self.cache_dir.glob("*.pkl"):
            try:
                with path.open("rb") as f:
                    entry = pickle.load(f)
                age = (datetime.now(timezone.utc) - entry["created_at"]).total_seconds()
                if age > self.ttl_seconds:
                    path.unlink()
                    removed += 1
            except Exception:
                path.unlink()  # 壊れたファイルも削除
                removed += 1
        return removed

    def created_at(self, key: str) -> str | None:
        """キャッシュの作成日時をローカルタイム文字列で返す（表示用）。"""
        path = self._key_path(key)
        if not path.exists():
            return None
        try:
            with path.open("rb") as f:
                entry = pickle.load(f)
            return entry["created_at"].astimezone().strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return None
