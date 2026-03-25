"""AWS SSO プロファイル解決

プロファイル命名規則: awssso-{account_name}-{account_id}:AWSReadOnlyAccess
account_id または account_name の部分文字列で検索する。
"""

from __future__ import annotations

import botocore.session


def list_all_profiles() -> list[str]:
    """~/.aws/config に登録されている全プロファイル名を返す"""
    return botocore.session.Session().available_profiles


def resolve_profile(account_id: str | None = None, account_name: str | None = None) -> str:
    """
    account_id または account_name を手がかりにプロファイル名を解決する。

    命名規則: awssso-{account_name}-{account_id}:AWSReadOnlyAccess

    Parameters
    ----------
    account_id   : AWS アカウントID（12桁）
    account_name : アカウント名（部分一致）

    Returns
    -------
    str : 解決されたプロファイル名

    Raises
    ------
    ValueError : 一致するプロファイルが0件または複数件の場合
    """
    if not account_id and not account_name:
        raise ValueError("account_id か account_name のどちらかを指定してください")

    profiles = list_all_profiles()

    candidates = [
        p for p in profiles
        if "awsreadonlyaccess" in p.lower()
        and (
            (account_id and account_id in p)
            or (account_name and account_name.lower() in p.lower())
        )
    ]

    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) == 0:
        hint = account_id or account_name
        raise ValueError(
            f"プロファイルが見つかりません: '{hint}'\n"
            f"登録済みプロファイル (AWSReadOnlyAccess のみ): "
            f"{[p for p in profiles if 'awsreadonlyaccess' in p.lower()]}"
        )
    # 複数ヒット → account_id で絞り込めるか試みる
    if account_id:
        strict = [p for p in candidates if account_id in p]
        if len(strict) == 1:
            return strict[0]
    raise ValueError(
        f"プロファイルが複数ヒットしました: {candidates}\n"
        "account_id と account_name を両方指定して絞り込んでください"
    )
