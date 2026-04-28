"""AWS Pricing API デバッグスクリプト

実際に返ってくるフィールド名・値を確認する。

使い方:
    python scripts/debug_pricing.py --profile <profile>
"""

import argparse
import json
import boto3

parser = argparse.ArgumentParser()
parser.add_argument("--profile", default=None)
args = parser.parse_args()

session = boto3.Session(profile_name=args.profile) if args.profile else boto3.Session()
client = session.client("pricing", region_name="us-east-1")

# ① まず RDS の属性名一覧を確認
print("=== RDS describe_services ===")
resp = client.describe_services(ServiceCode="AmazonRDS", FormatVersion="aws_v1")
for svc in resp.get("Services", []):
    print("AttributeNames:", svc.get("AttributeNames", []))

print()

# ② location + instanceType で全件取得（paginate）
print("=== get_products: location + instanceType で全件 ===")
paginator = client.get_paginator("get_products")
all_items = []
for page in paginator.paginate(
    ServiceCode="AmazonRDS",
    Filters=[
        {"Type": "TERM_MATCH", "Field": "location",     "Value": "Asia Pacific (Tokyo)"},
        {"Type": "TERM_MATCH", "Field": "instanceType", "Value": "db.r6g.large"},
    ],
):
    all_items.extend(page.get("PriceList", []))

print(f"  全ヒット件数: {len(all_items)}")
for i, item in enumerate(all_items):
    product = json.loads(item)
    attrs = product.get("product", {}).get("attributes", {})
    od_prices = []
    for term in product.get("terms", {}).get("OnDemand", {}).values():
        for dim in term.get("priceDimensions", {}).values():
            usd = dim.get("pricePerUnit", {}).get("USD")
            if usd:
                od_prices.append(usd)
    print(f"  [{i}] engine={attrs.get('databaseEngine')!r:30s} "
          f"deployment={attrs.get('deploymentOption')!r:10s} "
          f"OD(USD)={od_prices}")

print()

# ③ databaseEngine = "Aurora MySQL" のみで絞り込み（deploymentOption フィルタなし）
print("=== get_products: databaseEngine=Aurora MySQL のみ ===")
items3 = []
for page in paginator.paginate(
    ServiceCode="AmazonRDS",
    Filters=[
        {"Type": "TERM_MATCH", "Field": "location",       "Value": "Asia Pacific (Tokyo)"},
        {"Type": "TERM_MATCH", "Field": "instanceType",   "Value": "db.r6g.large"},
        {"Type": "TERM_MATCH", "Field": "databaseEngine", "Value": "Aurora MySQL"},
    ],
):
    items3.extend(page.get("PriceList", []))

print(f"  ヒット件数: {len(items3)}")
for i, item in enumerate(items3):
    product = json.loads(item)
    attrs = product.get("product", {}).get("attributes", {})
    od_prices = []
    for term in product.get("terms", {}).get("OnDemand", {}).values():
        for dim in term.get("priceDimensions", {}).values():
            usd = dim.get("pricePerUnit", {}).get("USD")
            if usd:
                od_prices.append(usd)
        print(f"  [{i}] OD={od_prices}")
    for k, v in sorted(attrs.items()):
        if v and v not in ("No", ""):
            print(f"       {k}={v!r}")
