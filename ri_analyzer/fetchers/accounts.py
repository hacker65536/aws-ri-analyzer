"""Organizations からアクティブな Linked Account 一覧を取得する"""

from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError


@dataclass
class AwsAccount:
    account_id: str
    name: str
    email: str


def list_active_accounts(payer_profile: str) -> list[AwsAccount]:
    """
    Payer アカウントの Organizations API から ACTIVE なアカウントを全件返す。

    Parameters
    ----------
    payer_profile : Payer アカウントのプロファイル名

    Returns
    -------
    list[AwsAccount]
    """
    session = boto3.Session(profile_name=payer_profile)
    org = session.client("organizations", region_name="us-east-1")

    accounts: list[AwsAccount] = []
    paginator = org.get_paginator("list_accounts")
    try:
        for page in paginator.paginate():
            for acc in page["Accounts"]:
                if acc["Status"] == "ACTIVE":
                    accounts.append(
                        AwsAccount(
                            account_id=acc["Id"],
                            name=acc["Name"],
                            email=acc.get("Email", ""),
                        )
                    )
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code in ("AccessDeniedException", "AWSOrganizationsNotInUseException"):
            raise PermissionError(
                f"Organizations API へのアクセスが拒否されました ({code})。"
                "Payer アカウントのプロファイルを確認してください。"
            ) from e
        raise

    return accounts
