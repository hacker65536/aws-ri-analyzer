"""RDS 実行中 DB インスタンスを取得する

RI データは Cost Explorer (cost_explorer.py) から取得するため、
ここでは実行中インスタンスの取得のみを担当する。

使用 API: rds:DescribeDBInstances (status=available)

マッチキー（coverage 分析用）:
  - DBInstanceClass  例: db.t4g.large
  - Engine           例: aurora-mysql / aurora-postgresql / mysql / postgres
  - MultiAZ          True / False
"""

from __future__ import annotations

from dataclasses import dataclass

import boto3
from botocore.exceptions import ClientError


@dataclass
class RdsInstance:
    instance_id: str
    account_id: str
    region: str
    instance_class: str
    engine: str
    multi_az: bool
    status: str
    az: str


def fetch_running_instances(
    profile: str,
    account_id: str,
    region: str,
) -> list[RdsInstance]:
    """
    指定アカウント・リージョンの実行中 RDS インスタンスを返す。
    status=available のもののみ対象とする。
    """
    session = boto3.Session(profile_name=profile, region_name=region)
    rds = session.client("rds")

    results: list[RdsInstance] = []
    paginator = rds.get_paginator("describe_db_instances")

    try:
        for page in paginator.paginate(
            Filters=[{"Name": "db-instance-status", "Values": ["available"]}]
        ):
            for inst in page["DBInstances"]:
                results.append(
                    RdsInstance(
                        instance_id    = inst["DBInstanceIdentifier"],
                        account_id     = account_id,
                        region         = region,
                        instance_class = inst["DBInstanceClass"],
                        engine         = inst["Engine"],
                        multi_az       = inst["MultiAZ"],
                        status         = inst["DBInstanceStatus"],
                        az             = inst.get("AvailabilityZone", ""),
                    )
                )
    except ClientError as e:
        _handle_client_error(e, account_id, region, "DescribeDBInstances")

    return results


def _handle_client_error(
    e: ClientError,
    account_id: str,
    region: str,
    api_name: str,
) -> None:
    code = e.response["Error"]["Code"]
    if code in ("AccessDeniedException", "AuthFailure", "InvalidClientTokenId"):
        raise PermissionError(
            f"[{account_id} / {region}] {api_name} へのアクセスが拒否されました ({code})。"
            "プロファイルの認証情報を確認してください。"
        ) from e
    raise
