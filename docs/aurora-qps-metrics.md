# Aurora QPS メトリクス取得仕様

`scripts/aurora-qps.py` が取得するメトリクスの一覧と、クラスターレベル / インスタンスレベルの違いを説明します。

---

## 取得ソース一覧

| # | ソース | API | ディメンション | エンジン対応 |
|---|---|---|---|---|
| 1 | CloudWatch | `cloudwatch.get_metric_data` | **インスタンス** | MySQL / PostgreSQL |
| 2 | Performance Insights / Database Insights | `pi.get_resource_metrics` | **インスタンス** | MySQL のみ |

> **Database Insights について**  
> Database Insights は Performance Insights の後継サービスですが、boto3 クライアント (`pi`) と API エンドポイント (`pi.get_resource_metrics`) は共通です。スクリプトはどちらのティアが有効であっても同一コードで動作します。

---

## [1] CloudWatch — `AWS/RDS` 名前空間

### 取得メトリクス

| MetricName | Stat | 用途 | 単位 |
|---|---|---|---|
| `Queries` | `Maximum` | 期間内の最大 QPS（1 分平均の最大値）| Count/Second |
| `Queries` | `Average` | 期間内の平均 QPS | Count/Second |

### ディメンション（次元）

```
{ "Name": "DBInstanceIdentifier", "Value": "<instance-id>" }
```

- **インスタンスレベル**で取得し、スクリプト側で全インスタンスを同一タイムスタンプごとに **合算** してクラスター合計 QPS を算出します。
- `DBClusterIdentifier` ディメンションは Aurora の一部メトリクスにのみ存在し、`Queries` はインスタンスレベルのみ公開されています。

### 統計値の意味

| Stat | 解釈 |
|---|---|
| `Maximum` | 集計 Period 内の最高 1 分平均 QPS（Period=3600s なら「その 1 時間で最も忙しかった 1 分間の平均 QPS」）|
| `Average` | 集計 Period 全体の平均 QPS |

### Aurora エンジン別の計算元

| エンジン | ベース |
|---|---|
| Aurora MySQL | MySQL の `Queries` ステータス変数（サーバーが実行したすべての文、`COM_PING` 等を除く）|
| Aurora PostgreSQL | PostgreSQL の `pg_stat_database` 等から CloudWatch が算出 |

---

## [2] Performance Insights / Database Insights

### 取得メトリクス

| エンジン | MetricName | 換算式 | 単位 |
|---|---|---|---|
| Aurora MySQL | `db.SQL.Queries.sum` | `値 ÷ PeriodInSeconds` = QPS | queries/sec |
| Aurora PostgreSQL | ─ 取得なし ─ | 直接的な QPS カウンターが PI に存在しないため CloudWatch を使用 | ─ |

### ディメンション（次元）

```
Identifier: "<DbiResourceId>"   # 例: db-ABCDEFGHIJKLMNOPQRSTUVWX
```

- **インスタンスレベル**のみ（PI にはクラスターレベルの集計なし）。
- `DbiResourceId` は `rds.describe_db_instances` から取得します（ARN や `DBInstanceIdentifier` とは別）。
- スクリプトはクラスター ARN を指定された場合、`rds.describe_db_clusters` → 各インスタンスの `DbiResourceId` の順に自動解決します。

### `db.SQL.Queries.sum` の詳細

MySQL の `Queries` ステータス変数に相当するカウンターです。CloudWatch の `Queries` メトリクスと同じベースを持ちますが、PI は **生カウンターの合算値**（per period）を返すため、`÷ PeriodInSeconds` で QPS に換算します。

### 期間制限

| PeriodInSeconds | 最大取得期間 |
|---|---|
| 60 s | 7 日 |
| 300 s | 30 日 |
| 3600 s | 90 日 |
| 86400 s | 2 年 |

スクリプトは `--days 28` を超える場合に `PeriodInSeconds` を自動的に 3600s へ変更します。

---

## クラスターレベル vs インスタンスレベル まとめ

```
クラスター ARN を指定した場合のデータフロー:

  [cluster ARN]
       │
       ▼
  describe_db_clusters
       │
       ├─ instance-1 ──┬─ CloudWatch Queries (Maximum/Average)
       │                └─ PI db.SQL.Queries.sum
       │
       └─ instance-2 ──┬─ CloudWatch Queries (Maximum/Average)
                        └─ PI db.SQL.Queries.sum

  CloudWatch: タイムスタンプ単位で instance-1 + instance-2 を合算
              → クラスター全体の合計 QPS として表示

  PI:         インスタンスごとに個別表示
              （PI はクラスターレベル集計 API が存在しない）
```

---

## スクリプト実行例

```bash
# SSO 設定
export AWS_CONFIG_FILE=~/.aws/aws-sso-config

# プロファイル確認
aws configure list-profiles | grep <account_id> | grep AWSReadOnlyAccess

# 実行（クラスター ARN）
python scripts/aurora-qps.py \
  arn:aws:rds:ap-northeast-1:<account_id>:cluster:<cluster_name> \
  --profile awssso-<account_name>-<account_id>:AWSReadOnlyAccess

# CloudWatch のみ（PI スキップ）
python scripts/aurora-qps.py <ARN> --no-pi

# 粒度を 5 分に下げてピーク精度を上げる
python scripts/aurora-qps.py <ARN> --period 300
```
