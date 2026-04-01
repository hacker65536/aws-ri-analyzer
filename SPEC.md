# AWS RI Analyzer / CUR Analyzer — 仕様書

## 概要

コスト最適化を目的とした 2 つの CLI ツール。共通の Athena クライアント・クエリライブラリ（`ri_analyzer/` パッケージ）を共有する。

| コマンド | 主な役割 | データソース |
|---|---|---|
| `ri-analyzer` | RI ライフサイクル管理（有効期限・カバレッジ・利用率・推奨購入量） | CE API + Athena/CUR |
| `cur-analyzer` | CUR を Athena 経由でアドホック分析（SQL テンプレート実行） | Athena/CUR |

Payer（管理）アカウントの API を使用するため、個別アカウントへの AssumeRole は不要。

### ツール間の依存関係

```
ri-analyzer ──┐
               ├──→ ri_analyzer/ （共有ライブラリ）
cur-analyzer ──┘      ├── fetchers/athena.py
                       ├── fetchers/cur_queries.py
                       └── fetchers/cost_explorer.py
```

`ri-analyzer` は RI に関するドリルダウン・ファクトチェックのために CUR（Athena）を利用する。
`cur-analyzer` は RI に限らないアドホックな CUR 分析を担う。

### 将来の方向性

`cur-analyzer` は RI 以外のコスト最適化領域（Savings Plans、EC2 サイズ最適化、コスト異常検知など）への拡張を想定している。
機能が成熟した段階で独立したリポジトリ・パッケージとして分離することを検討する。
分離後も `ri-analyzer` は共有ライブラリまたは `cur-analyzer` への依存を通じて CUR 分析機能を継続利用できる。

### `ri-analyzer` の分析内容

| セクション | 内容 | データソース |
|---|---|---|
| Expiration | RI の有効期限チェック（期限切れ / 要注意 / 正常） | CE GetReservationUtilization |
| Coverage | アカウント × リージョン × インスタンスタイプ別の RI カバレッジ率 | CE GetReservationCoverage |
| Utilization | サブスクリプション別の利用率・未使用時間・削減額 | CE GetReservationUtilization |
| Recommendations | AWS が算出した RI 購入推奨・見込み節約額・回収期間 | CE GetReservationPurchaseRecommendation |
| cur_instance_detail | resource_id 単位の稼働実績・RI/OD 内訳（RI 購入精査の基本機能） | Athena / CUR |
| cur_instances | instance_type 集計の稼働一覧（実績使用時間・コスト・平均台数） | Athena / CUR |
| cur_coverage | CUR ベースの RI カバレッジ詳細（RI hrs vs OD hrs） | Athena / CUR |
| unused_ri | 未使用 RI 費用（RIFee 行集計、無駄なコスト特定） | Athena / CUR |
| CE factcheck | CE Recommendation の推奨台数を CUR 実績で検証（--athena 時に自動表示） | CE + Athena |

対応サービス: **RDS**, **ElastiCache**, **OpenSearch**

### サービスキーとフィルタ値のマッピング

ツール内部では `ri_analyzer/service_registry.py` の `SERVICES` dict で一元管理している。
新サービスを追加する際はそのファイルのみ編集すればよい。

| サービスキー | CE API `Service` フィルタ値 | CUR `line_item_product_code` 値 |
|---|---|---|
| `rds` | `Amazon Relational Database Service` | `AmazonRDS` |
| `elasticache` | `Amazon ElastiCache` | `AmazonElastiCache` |
| `opensearch` | `Amazon OpenSearch Service` | `AmazonES` ※ |
| `redshift` | `Amazon Redshift` | `AmazonRedshift` |
| `ec2` | `Amazon Elastic Compute Cloud - Compute` | `AmazonEC2` |

※ OpenSearch は旧称 Elasticsearch 時代のプロダクトコード（`AmazonES`）が CUR でも継続使用されている。

#### `cur-analyzer` でのサービス指定

`-p service=` にはサービスキー・プロダクトコードのどちらも指定できる（自動解決）。

```bash
# どちらも同じ結果になる
cur-analyzer ri_coverage -p year=2026 -p month=3 -p service=rds
cur-analyzer ri_coverage -p year=2026 -p month=3 -p service=AmazonRDS
```

#### CE API のエンジンフィルタ

Coverage / Utilization の GroupBy に使うエンジン次元は API ごとに異なる。

| サービスキー | GroupBy 次元キー | レスポンスの属性キー |
|---|---|---|
| `rds` | `DATABASE_ENGINE` | `databaseEngine` |
| `elasticache` | `CACHE_ENGINE` | `cacheEngine` |
| `opensearch` | —（未サポート） | — |

---

## ディレクトリ構成

```
aws-ri-analyzer/
├── config.yaml                        # 設定ファイル（.gitignore 対象）
├── config.yaml.example                # サンプル設定ファイル（リポジトリに含める）
├── .gitignore
├── main.py                            # ri-analyzer エントリポイント（CE + Athena 統合）
├── cur_analyzer.py                    # cur-analyzer エントリポイント（CUR アドホック分析）
├── requirements.txt
├── queries/
│   ├── rds_running_instances.sql      # カスタム SQL サンプル
│   ├── resource_uptime.sql            # リソース別日次稼働時間（停止時間帯調査用）
│   ├── resource_engine_check.sql      # リソースのエンジン表記・インスタンスタイプ確認
│   ├── resource_type_changes.sql      # CE 期間内にインスタンスタイプが変わったリソース一覧
│   ├── resource_latest_type.sql       # 期間末時点のインスタンスタイプ確認（移行後の状態確認）
│   └── templates/                     # SQL テンプレート（{{ variable }} 形式）
│       ├── rds_instances.sql          # 稼働中 RDS インスタンス一覧
│       ├── elasticache_nodes.sql      # 稼働中 ElastiCache ノード一覧
│       ├── ri_coverage.sql            # RI カバレッジ（全月）
│       ├── ri_coverage_period.sql     # RI カバレッジ（日付範囲指定、CE 突き合わせ用）
│       ├── rds_resource_ids.sql       # RDS リソース ID 別 OD/RI 時間・コスト内訳（CE 期間フィルタ付き）
│       ├── ce_factcheck_rds.sql       # CE 推奨の実績確認
│       └── unused_ri.sql             # 未使用 RI 費用
└── ri_analyzer/
    ├── cache.py                       # AWS API レスポンスのローカルディスクキャッシュ
    ├── config.py                      # config.yaml の読み込み・バリデーション（AthenaConfig 含む）
    ├── profile_resolver.py            # account_id → AWS SSO プロファイル名 解決
    ├── fetchers/
    │   ├── cost_explorer.py           # CE API 呼び出し（RI データ・カバレッジ）
    │   ├── athena.py                  # Athena クライアント（非同期ポーリング・スキーマキャッシュ）
    │   ├── cur_queries.py             # CUR よく使うクエリ集（Python API）
    │   └── rds.py                     # RDS 実行中インスタンス取得（現在は未使用、将来用）
    ├── analyzers/
    │   ├── expiration.py              # 有効期限分類ロジック
    │   ├── coverage.py                # カバレッジ集計ロジック
    │   ├── utilization.py             # 利用率集計ロジック
    │   └── cur_detail.py              # CUR データ構造体・パーサ・ファクトチェック
    └── reporter.py                    # コンソール出力（CE + CUR セクション、英語・カラー対応）
compare_cur_ce.py                      # CUR vs CE カバレッジ精度検証スクリプト
```

---

## セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp config.yaml.example config.yaml
# → config.yaml の payer.account_id を編集する
```

### 依存ライブラリ

```
boto3>=1.34.0
botocore>=1.34.0
pyyaml>=6.0
```

---

## 設定ファイル（config.yaml）

`config.yaml.example` をコピーして使う。`config.yaml` 自体は `.gitignore` 対象。

```yaml
payer:
  account_id: "123456789012"   # Payer (management) account ID
  # profile: "my-payer-profile"  # Optional. Omit to auto-resolve (AWS SSO only).
                                  # Specify for access keys / AssumeRole / non-SSO profiles.

athena:                          # Athena / CUR 機能（オプション）
  # Glue データベース名。
  # AWS CloudFormation の CUR セットアップ（AthenaAndS3BucketSetup）で作成した場合、
  # "athenacurcfn_{レポート名}" という命名になる。
  # aws athena list-databases --catalog-name AwsDataCatalog で確認可能。
  database: your_cur_database
  # CUR テーブル名。
  # aws athena list-table-metadata --catalog-name AwsDataCatalog --database-name {database} で確認可能。
  table: your_cur_table
  workgroup: primary                       # Athena ワークグループ
  output_location: s3://your-bucket/athena-results/   # 結果出力先 S3
  result_mode: api               # "api"（小規模）/ "s3"（大規模 CSV）
  schema_cache_ttl_hours: 168    # スキーマキャッシュ TTL（デフォルト: 1 週間）
  query_cache_ttl_hours: 24      # クエリ結果キャッシュ TTL（デフォルト: 24 時間）
  region: ap-northeast-1         # Athena エンドポイントのリージョン
  # profile: "..."               # 省略時は payer プロファイルを流用

analysis:
  services:                    # 対象サービス（省略時は初回起動でインタラクティブ選択 → 自動保存）
    - rds
    # - elasticache
    # - opensearch

  sections:                    # 表示セクション（省略時は初回起動でインタラクティブ選択 → 自動保存）
    - expiration
    - coverage
    - utilization
    - recommendations          # CE GetReservationPurchaseRecommendation

  # CUR データを product_region でフィルタする（Athena クエリの WHERE 句に適用）。
  # athena.region（Athena エンドポイントのリージョン）とは独立した設定。
  # 省略または空リストで全リージョン対象（デフォルト）。
  # regions:
  #   - ap-northeast-1

  # CE の参照期間（日数）
  # end   = UTC 現在 - 48h（CE のデータタイムラグ考慮）
  # start = end - lookback_days
  # 短すぎると CE がサブスクリプションを返さない場合があるため 7 日以上推奨
  lookback_days: 7

  # RI 有効期限の警告しきい値（日数）
  expiration_warn_days: 90

  # AWS API レスポンスのキャッシュ TTL（時間）
  # --no-cache フラグでバイパス可能
  cache_ttl_hours: 24

recommendation:
  term: ONE_YEAR          # ONE_YEAR / THREE_YEARS
  payment_option: ALL_UPFRONT  # ALL_UPFRONT / PARTIAL_UPFRONT / NO_UPFRONT
  lookback_days: 30       # CE が受け付ける値: 7 / 30 / 60
```

`services` / `sections` キーが config.yaml に存在しない場合、起動時にインタラクティブ選択が行われ、選択結果が config.yaml に書き戻される（`Config.save()` による全体書き直し）。

---

## CLI オプション優先度

```
CLI 引数  >  config.yaml の値  >  インタラクティブプロンプト（→ config.yaml に保存）
```

`services` / `sections` が config.yaml に存在しない場合、起動時に選択肢を提示し、選択結果を config.yaml に書き戻す（`Config.save()` による全体書き直し。既存コメントは消える）。

基本的な使い方は [README.md](README.md) を参照。

---

## AWS 認証・プロファイル解決

### プロファイル優先度

```
config.yaml の payer.profile  >  profile_resolver による自動解決（AWS SSO のみ）
```

`payer.profile` を明示すれば、アクセスキー・AssumeRole・非 SSO プロファイルでも動作する。

### 自動解決（AWS SSO）の命名規則

AWS SSO (IAM Identity Center) で発行されるプロファイル名の規則：

```
awssso-{account_name}-{account_id}:AWSReadOnlyAccess
```

例：`awssso-my-org-payer-123456789012:AWSReadOnlyAccess`

### 解決ロジック（`profile_resolver.py`）

`botocore.session.Session().available_profiles` で `~/.aws/config` の全プロファイルを取得し、
`awsreadonlyaccess` を含み、かつ `account_id` または `account_name` が部分一致するものを返す。

複数ヒットした場合は `account_id` で再絞り込みを試みる。それでも複数の場合はエラー。

```python
resolve_profile(account_id="123456789012")
# → "awssso-my-org-payer-123456789012:AWSReadOnlyAccess"
```

### SSO セッション切れのエラーハンドリング

AWS SSO のトークン切れ（`TokenRetrievalError` / `SSOTokenLoadError`）を検知し、
ログインコマンドを表示して終了する：

```
[ERROR] AWS SSO session has expired.
  Run the following command to log in again:

    aws sso login --profile "awssso-my-org-payer-123456789012:AWSReadOnlyAccess"
```

---

## データフロー

```
main.py
  │
  ├─ Config.load()                   # config.yaml 読み込み
  ├─ resolve_profile(account_id)     # Payer プロファイル解決
  ├─ AthenaClient(cfg.athena)        # --athena 時のみ初期化
  │
  └─ [for each service in config]
      │
      ├─ [CE セクション]
      │   ├─ fetch_ri_subscriptions()    # CE GetReservationUtilization
      │   │    → RiSubscription[]        # RI 一覧（有効期限・台数等）
      │   │    → RiUtilizationRecord[]   # 利用率レコード
      │   ├─ fetch_ri_coverage()         # CE GetReservationCoverage
      │   │    → RiCoverageRecord[]      # account × region × type 別カバレッジ
      │   ├─ fetch_ri_recommendations()  # CE GetReservationPurchaseRecommendation
      │   │    → RiRecommendationGroup[] # 購入推奨グループ
      │   ├─ exp_analyzer.analyze()      # 有効期限分類
      │   ├─ cov_analyzer.analyze()      # カバレッジ集計（Redis/Valkey 統合）
      │   ├─ util_analyzer.summarize()   # 利用率集計
      │   └─ reporter.print_*()          # コンソール出力
      │
      └─ [Athena/CUR セクション（--athena 時）]
          ├─ {rds,elasticache,opensearch}_instance_detail()   # CUR: resource_id 単位の稼働実績
          │    → parse_{rds,elasticache,opensearch}_*()       # CurInstanceDetailRow[]
          ├─ running_{rds,elasticache,opensearch}_*()         # CUR: instance_type 集計の稼働一覧
          │    → parse_{rds,elasticache,opensearch}_*()       # CurInstanceRow[]
          ├─ ri_coverage_detail()            # CUR: RI カバレッジ詳細（product_code で切り替え）
          │    → parse_cur_coverage()        # CurCoverageRow[]
          ├─ unused_ri_cost()                # CUR: 未使用 RI 費用（product_code で切り替え）
          │    → parse_unused_ri()           # UnusedRiRow[]
          ├─ factcheck_recommendations()     # CE 推奨 × CUR 実績 突き合わせ（RDS/ElastiCache のみ）
          │    → RecommendationFactcheck[]
          └─ reporter.print_cur_*() / print_ce_factcheck()

CUR の product_code マッピング:
  rds        → AmazonRDS
  elasticache→ AmazonElastiCache
  opensearch → AmazonES（旧 Elasticsearch コード。Service 名変更後も継続使用）
```

### Athena クエリの非同期フロー

```
AthenaClient.run_query(sql)
  │
  ├─ _start_query()      → QueryExecutionId
  ├─ _wait_query()       → 指数バックオフポーリング（初期 1s → 最大 10s、タイムアウト 300s）
  └─ _fetch_api()        → GetQueryResults（ページネーション対応）
     または
     _fetch_s3()         → S3 CSV 直接読み込み（result_mode=s3 時）

run_from_file(sql_path, size_limit_mb=10)
  ├─ SQL ファイル読み込み・パーティションチェック・クエリ実行
  ├─ _get_result_s3_info() → S3 パス + HEAD でサイズ確認
  ├─ size <= 閾値  → _download_s3() → ローカル CSV → dict リスト
  └─ size > 閾値   → メタ情報のみ返却（rows=None）
```

### CUR クエリ期間とパーティション

CUR クエリの期間は `start_date` / `end_date`（`YYYY-MM-DD` 形式）で指定する。
デフォルトは CE API と同じ期間（`_ce_time_period(lookback_days)` の結果）。

`date_range_filter(start_date, end_date)` が `(partition_cond, date_cond)` を返す：

```python
partition_cond, date_cond = date_range_filter("2026-03-18", "2026-03-25")
# partition_cond = "year = '2026' AND month = '3'"
# date_cond = "line_item_usage_start_date >= TIMESTAMP '2026-03-18 00:00:00'
#              AND line_item_usage_start_date <  TIMESTAMP '2026-03-25 00:00:00'"
```

月またぎの場合は OR でパーティションを列挙する：

```sql
-- 2026-02-25 to 2026-03-04 の場合
WHERE ((year = '2026' AND month = '2')
    OR (year = '2026' AND month = '3'))
  AND line_item_usage_start_date >= TIMESTAMP '2026-02-25 00:00:00'
  AND line_item_usage_start_date <  TIMESTAMP '2026-03-04 00:00:00'
```

`--cur-year` / `--cur-month` 指定時は `(月初, 翌月初)` の日付範囲に変換される。

`unused_ri_cost()` は `RIFee`（月次固定費）を対象とするため、
`date_cond`（`line_item_usage_start_date` フィルタ）は適用せず、パーティション条件のみを使う。

CUR テーブルのパーティションキーは `year`（文字列）と `month`（文字列、ゼロ埋めなし）。

- `partition_filter(year, month)` は `str(int(month))` でゼロ埋めを除去する
- `athena_run.py` の `-p month=03` も自動的に `'3'` に正規化する
- SQL に `year` / `month` 条件がない場合は `PartitionMissingError` を raise する（`enforce_partition=False` で無効化可）

---

## モジュール仕様

### `fetchers/cost_explorer.py`

#### 時刻計算（`_ce_time_period`）

CE のデータには最大 48 時間のタイムラグがあるため：

```
end   = UTC 現在時刻 - 48h の日付
start = end - lookback_days
```

#### `fetch_ri_subscriptions(payer_profile, service, lookback_days)`

**API**: `ce:GetReservationUtilization`
**エンドポイント**: `us-east-1`（CE は us-east-1 固定）
**GroupBy**: `SUBSCRIPTION_ID`
**Filter**: `SERVICE = "Amazon Relational Database Service"`（サービスにより変わる）
**注意**: `Granularity` と `GroupBy` は同時指定不可

レスポンスの `Groups[]` から以下を抽出：

| フィールド | CE レスポンスのキー |
|---|---|
| subscription_id | `group["Value"]`（`"Key"` は次元名なので使わない） |
| instance_type | `Attributes["instanceType"]` |
| platform | `Attributes["platform"]`（例: "Aurora", "MySQL"） |
| count | `Attributes["numberOfInstances"]` |
| start_time | `Attributes["startDateTime"]` |
| end_time | `Attributes["endDateTime"]` ← 有効期限として使用 |
| status | `Attributes["subscriptionStatus"]` |
| size_flexibility | `Attributes["sizeFlexibility"]`（例: "FlexRI"） |

戻り値: `(list[RiSubscription], list[RiUtilizationRecord])` のタプル

#### `fetch_ri_coverage(payer_profile, service, lookback_days)`

**API**: `ce:GetReservationCoverage`
**GroupBy**: `LINKED_ACCOUNT`, `REGION`, `INSTANCE_TYPE`, + サービス別エンジン次元（未サポートの場合は省略）
**注意**: `Granularity` と `GroupBy` は同時指定不可
**注意**: エンジン次元はサービスにより異なる。`engine_dimension` が空文字の場合は GroupBy に追加しない

| サービス | エンジン次元 | レスポンス属性キー | NU 柔軟性 |
|---|---|---|---|
| rds | `DATABASE_ENGINE` | `databaseEngine` | あり（ファミリー内サイズ間で適用可） |
| elasticache | `CACHE_ENGINE` | `cacheEngine` | あり（ファミリー内サイズ間で適用可） |
| opensearch | （なし）| （なし）— CE の制限により DATABASE_ENGINE 不可 | **なし**（購入時の exact タイプにのみ適用） |

`ServiceConfig.has_nu_flexibility` フラグで管理。`False` の場合、Coverage / Utilization のレポートでインスタンスファミリーグループヘッダーおよび NU サマリ行を省略し、インスタンスタイプ単位でフラット表示する。

レスポンスの `CoveragesByTime[].Groups[]` から以下を抽出（`Attributes` に格納）：

| フィールド | CE レスポンスのキー |
|---|---|
| account_id | `Attributes["linkedAccount"]` |
| region | `Attributes["region"]` |
| instance_type | `Attributes["instanceType"]` |
| platform | `Attributes["databaseEngine"]`（例: "Aurora MySQL", "MySQL"） |
| covered_hours | `Coverage.CoverageHours["ReservedHours"]`（`CoveredHours` ではない） |
| on_demand_hours | `Coverage.CoverageHours["OnDemandHours"]` |
| total_hours | `Coverage.CoverageHours["TotalRunningHours"]` |
| coverage_pct | `Coverage.CoverageHours["CoverageHoursPercentage"]` |

戻り値: `list[RiCoverageRecord]`

#### `fetch_ri_recommendations(payer_profile, service, term, payment_option, lookback_days)`

**API**: `ce:GetReservationPurchaseRecommendation`
**AccountScope**: `PAYER`（Payer アカウントから全体を集計）
**ページネーション**: `NextPageToken` 対応
**lookback_days の変換**: `7→"SEVEN_DAYS"`, `30→"THIRTY_DAYS"`, `60→"SIXTY_DAYS"`

戻り値: `list[RiRecommendationGroup]`

各 `RiRecommendationDetail` が持つフィールド：

| フィールド | 内容 |
|---|---|
| instance_type | 推奨対象インスタンスタイプ |
| region | リージョン |
| platform | エンジン（"Redis", "MySQL" 等） |
| count | 推奨購入台数 |
| normalized_units | 推奨 NU |
| upfront_cost | 初期費用（$） |
| estimated_monthly_savings | 月次節約見込み額（$） |
| estimated_savings_pct | 節約率（%） |
| breakeven_months | 投資回収期間（月）。API フィールド名は `EstimatedBreakEvenInMonths`（"Even" の E が大文字） |
| avg_utilization | 直近の平均使用率（推奨根拠） |

サービスごとの `InstanceDetails` キー:

| サービス | キー | instance_type の組み立て |
|---|---|---|
| rds | `RDSInstanceDetails` → `InstanceType`, `Region`, `DatabaseEngine`, `DeploymentOption` | `InstanceType` をそのまま使用 |
| elasticache | `ElastiCacheInstanceDetails` → `NodeType`, `Region`, `ProductDescription` | `NodeType` をそのまま使用 |
| opensearch | `ESInstanceDetails` → `InstanceClass`, `InstanceSize`, `Region` | `"{InstanceClass}.{InstanceSize}.search"` に結合 |

---

### `analyzers/expiration.py`

`RiSubscription.days_remaining`（= `end_time - now`）を基に 3 分類：

| ステータス | 条件 |
|---|---|
| expired | days_remaining < 0 |
| warning | 0 ≤ days_remaining ≤ warn_days |
| ok | days_remaining > warn_days |

どのサービスの RI オブジェクトでも `end_time` と `days_remaining` を持っていれば動作する設計。

---

### `analyzers/coverage.py`

`RiCoverageRecord` を `(account_id, region, instance_type, normalized_platform)` キーで集計し、
`CoverageSummary` に変換。

| ステータス | カバレッジ率 |
|---|---|
| ok | >= 90% |
| warning | 50% 〜 90% |
| low | < 50% |

ソート順: platform → instance family → 実効 NU/h 昇順 → account_id

#### ElastiCache: Redis / Valkey の統合

`_normalize_platform()` により、`"Redis"` と `"Valkey"` は `"Redis/Valkey"` に正規化されて同一グループに集計される。
これは ElastiCache の size-flexible RI が Redis と Valkey の間で互換性を持つためである。

#### 正規化ユニット（Normalized Units）

`covered_nus` / `on_demand_nus` / `total_nus` は **レコード単位でエンジン別係数を使って事前計算されたフィールド**。
`CoverageSummary` に格納済みのため、Redis/Valkey 混在グループでも正確な NU が集計される。

- Redis OSS 係数: `large=4`, `xlarge=8` ... (`utilization._NORM_FACTOR`)
- Valkey 係数: `large=3.2`, `xlarge=6.4` ... (`utilization._VALKEY_NORM_FACTOR`、Redis の 0.8 倍)
- family サマリの Coverage% は NU ベース加重平均で算出

---

### `analyzers/utilization.py`

`RiUtilizationRecord` をサブスクリプション ID ごとに集計し `UtilizationSummary` に変換。
複数期間にまたがるレコードは平均利用率・累計未使用時間・累計削減額を算出。

| ステータス | 平均利用率 |
|---|---|
| ok | >= 80% |
| warning | 50% 〜 80% |
| low | < 50% |

ソート順: instance family → サイズ（正規化ユニット昇順）

#### インスタンスタイプの形式とパース

サービスごとにインスタンスタイプの形式が異なるため、`_is_opensearch()` でサフィックスを検出して分岐する：

| サービス | 形式例 | family | size | prefix |
|---|---|---|---|---|
| rds | `db.r5.large` | `r5` | `large` | `db` |
| elasticache | `cache.r6g.large` | `r6g` | `large` | `cache` |
| opensearch | `m5.large.search` | `m5` | `large` | `search` |

`product_instance_type LIKE '%.search'` で OpenSearch のデータノードのみを対象とする（マスターノード・UltraWarm は除外）。

#### 正規化ユニット（Normalized Units）

エンジン別の係数テーブルを使い、インスタンスサイズから NUs を算出。
`_norm_factor_for_engine(instance_type, platform)` で切り替え。

**RDS / ElastiCache Redis OSS（`_NORM_FACTOR`）:**
```
nano=0.25 / micro=0.5 / small=1 / medium=2 / large=4 / xlarge=8 / 2xlarge=16 / ...
```

**ElastiCache Valkey（`_VALKEY_NORM_FACTOR`、Redis の 0.8 倍）:**
```
micro=0.4 / small=0.8 / medium=1.6 / large=3.2 / xlarge=6.4 / 2xlarge=12.8 / ...
```

参照: [AWS ドキュメント](https://docs.aws.amazon.com/AmazonElastiCache/latest/dg/CacheNodes.Reserved.html)

- `UtilizationSummary.normalized_units` = `count × _norm_factor_for_engine()`
- family サマリの `Avg Util` は NUs 加重平均で算出

---

### `reporter.py`

- カラー出力（ANSI）。`--no-color` または `set_color(False)` で無効化
- ファイル出力時は `--no-color` を推奨（ANSI コードが混入するため）
- カラム幅は ASCII 文字で固定しているため日本語文字列はカラムがずれる

#### CE セクション（既存）

- `print_utilization(summaries, max_util=None, engines=None, families=None, show_sub_id=False, use_family_summary=True)`
  - `max_util` 指定時は avg_utilization_pct がその値以下のレコードのみ表示
  - `engines` 指定時はエンジンで絞り込み（部分一致・大文字小文字無視）
  - `families` 指定時はインスタンスファミリーで絞り込み（完全一致）
  - `show_sub_id=True` で Subscription ID 列を追加表示
  - `use_family_summary=True`（デフォルト）: instance family 単位でサマリ行を表示（2件以上の場合）
  - `use_family_summary=False`: ファミリーグループなし、インスタンスタイプ単位でフラット表示（OpenSearch 用）
  - 詳細行の `Unused` 列は `hrs` 単位、サマリ行は `NUs` 単位（正規化ユニット時間）
  - **platform 正規化**: `GetReservationUtilization` が返す短縮 platform 名（例: `"Aurora"`）を `_UTIL_PLATFORM_NORMALIZE` で Coverage と同じ命名（`"Aurora MySQL"`）に正規化してからフィルタを適用する。これにより `--engine "aurora mysql"` が Coverage / Utilization の両セクションで一貫して動作する。
- `print_coverage(summaries, max_coverage=None, engines=None, families=None, use_family_summary=True)`
  - `max_coverage` 指定時は coverage_pct がその値以下のレコードのみ表示
  - `engines` 指定時はデータベースエンジンで絞り込み（部分一致・大文字小文字無視）
  - `families` 指定時はインスタンスファミリーで絞り込み（完全一致）
  - `use_family_summary=True`（デフォルト）: platform → instance family の 2 段階グループで表示
  - `use_family_summary=False`: ファミリーグループヘッダーおよび NU サマリ行を省略し、インスタンスタイプ単位でフラット表示（OpenSearch 用）
  - ElastiCache では Redis と Valkey が "Redis/Valkey" として統合表示される
  - ファミリーラベルはインスタンスタイプのプレフィックスを動的取得（`db.` / `cache.`）
  - 詳細行の列: Account ID / Instance Type / Region / Coverage / RI (hrs) / OD (hrs) / Total (hrs)
  - family サマリ行（2件以上の場合）はラベルが `(total, NUs)`、値は NUs 単位（エンジン別係数適用済み）
- `print_recommendations(groups, service, term, payment_option, engines=None, families=None)`
  - AWS CE が算出した購入推奨を節約額降順で表示
  - `engines` 指定時はエンジンで絞り込み（部分一致・大文字小文字無視）。`platform` は `"Aurora MySQL Single-AZ"` 形式なので正規化不要
  - `families` 指定時はインスタンスファミリーで絞り込み（完全一致）
  - フィルタ後の details から合計節約額を再計算して表示
  - 列: Instance Type / Platform / Region / Cnt / NUs / Upfront ($) / Savings/mo / Savings% / Breakeven
  - グループ末尾に合計節約額サマリを表示（フィルタ適用後の値）

#### CUR セクション（Athena）

全関数のシグネチャは `year, month` から `start_date, end_date` に変更。

- `print_cur_instance_detail(rows, service, start_date, end_date, *, min_hours=None)`
  - **resource_id 単位**でインスタンス稼働実績を表示（RI 購入精査の基本機能）
  - 列: Resource Name / Account ID / Region / Type / Engine / Period% / hrs / RI% / RI hrs / OD hrs
  - `Period%` = `usage_hours / period_hours × 100`（クエリ期間中の稼働率）
  - 短命インスタンス（`usage_hours < period_hours × 0.5`）を黄色ハイライト
  - `min_hours` 指定時はそれ未満の行を非表示にして RI 候補に絞り込む
  - フッターに stable 件数 / short-lived 件数 / 合計 OD hours
- `print_cur_instances(rows, service, start_date, end_date)`
  - instance_type 集計の稼働一覧を engine 別にグループ表示
  - 列: Account ID / Region / Instance Type / Engine / Deployment / Avg Inst / Usage hrs / Cost
  - `avg_instances = usage_hours / 720`（月間換算の平均台数）
- `print_cur_coverage(rows, service, start_date, end_date)`
  - CUR ベースの RI カバレッジ。CE Coverage と突き合わせ用
  - 列: Account ID / Region / Instance Type / Coverage% / RI hrs / OD hrs / Total hrs
  - フッターに low (<50%) / warning (50-90%) のサマリ
- `print_unused_ri(rows, service, start_date, end_date)`
  - RIFee 行の費用を降順表示。無駄な RI の特定に使う
  - 列: Account ID / Region / Usage Type / Fee (USD) / Qty
  - フッターに合計未使用 RI 費用
- `print_ce_factcheck(checks, service, start_date, end_date)`
  - CE 推奨台数と CUR 実績台数を比較してジャッジ
  - 列: Instance Type / Platform / Region / CE cnt / CUR avg / Gap / OD hrs / Judge
  - Judge の基準:
    - `[v] match` : Gap ≤ ±0.5（CE 推奨 ≈ CUR 実績）
    - `[+] buy candidate` : Gap > 0.5（OD 稼働中 → RI 購入余地あり）
    - `[-] already covered` : Gap < -0.5（すでに RI でカバー済み）
    - `[?] no CUR data` : CUR にデータなし（期間や engine を確認）

### `analyzers/cur_detail.py`

CUR Athena クエリ結果（raw dict リスト）を構造体に変換し、ファクトチェックを行う。

| 構造体 | 内容 |
|---|---|
| `CurInstanceDetailRow` | resource_id 単位の稼働実績（usage_hours / ri_hours / od_hours）。`coverage_pct`・`run_days`・`resource_name` をプロパティとして持つ |
| `CurInstanceRow` | instance_type 集計の稼働一覧（account × region × instance_type × engine）。`avg_instances = usage_hours / 720` |
| `CurCoverageRow` | CUR ベースのカバレッジ（RI hrs / OD hrs / coverage_pct） |
| `UnusedRiRow` | 未使用 RI 費用 1 行（RIFee 集計） |
| `RecommendationFactcheck` | CE 推奨 1 件と CUR 実績の突き合わせ結果 |

ファクトチェックのマッチングキー:
```
instance_type == instance_type
AND region == region
AND cur.engine.lower() in rec.platform.lower()
```

---

## キャッシュ

繰り返し実行を高速化するため、**2 層のキャッシュ**を持つ。

### 1. 汎用 API キャッシュ（`cache.py`）

AWS Cost Explorer API のレスポンスをローカルディスクに保存する。

| 項目 | 内容 |
|---|---|
| 保存先 | `~/.cache/ri-analyzer/*.pkl` |
| フォーマット | pickle（`datetime` 型を含むデータクラスをそのまま保存） |
| デフォルト TTL | 24 時間（`config.yaml` の `analysis.cache_ttl_hours` で変更可） |
| バイパス | `--no-cache` フラグ |
| キャッシュキー | `SHA-256("{_CACHE_VERSION}:{キー文字列}")` の先頭 16 文字 |
| スキーマ変更対応 | `_CACHE_VERSION` 定数をインクリメントすると既存キャッシュが自動無効化される |

キャッシュされるデータ：
- `subscriptions:{profile}:{svc}:{lookback_days}:{end_date}` → `(list[RiSubscription], list[RiUtilizationRecord])`
- `coverage:{profile}:{svc}:{lookback_days}:{end_date}` → `list[RiCoverageRecord]`
- `recommendations:{profile}:{svc}:{term}:{payment_option}:{lookback_days}:{end_date}` → `list[RiRecommendationGroup]`
- `athena:instance_detail:{svc}:{start_date}:{end_date}` → `list[CurInstanceDetailRow]`
- `athena:instances:{svc}:{start_date}:{end_date}` → `list[CurInstanceRow]`
- `athena:coverage:{svc}:{start_date}:{end_date}` → `list[CurCoverageRow]`
- `athena:unused_ri:{svc}:{start_date}:{end_date}` → `list[UnusedRiRow]`

### 2. Athena クエリキャッシュ（`fetchers/athena.py`）

Athena の実行コストを抑えるため、SQL クエリ結果をローカルに保存して再利用する。

#### クエリ結果キャッシュ

| 項目 | 内容 |
|---|---|
| 保存先 | `~/.cache/ri-analyzer/query_results/` |
| フォーマット | `{sql_hash}.csv`（結果）＋ `{sql_hash}.meta.json`（メタデータ） |
| キャッシュキー | `SHA-256(SQLテキスト)` の先頭 16 文字 |
| デフォルト TTL | 24 時間（`config.yaml` の `athena.query_cache_ttl_hours` で変更可） |
| TTL 判定 | `meta.json` のファイル更新日時で判定 |
| バイパス | `--no-cache` フラグ（`use_cache=False` で Athena を直接実行） |

`meta.json` に保存される情報：

```json
{
  "query_id": "uuid",
  "s3_path": "s3://...",
  "size_bytes": 1024,
  "elapsed_sec": 2.5,
  "sql_hash": "abcd1234...",
  "cached_at": "2026-04-01T00:00:00"
}
```

キャッシュヒット時は `QueryResult.from_cache = True` となり、`athena_run.py` の出力に `[CACHE HIT]` が表示される。  
サイズ超過でダウンロードをスキップした結果（`rows=None`）はキャッシュ対象外。

#### スキーマキャッシュ

| 項目 | 内容 |
|---|---|
| 保存先 | `~/.cache/ri-analyzer/athena_schema_{db}_{table}.json` |
| デフォルト TTL | 168 時間（1 週間）（`config.yaml` の `athena.schema_cache_ttl_hours` で変更可） |
| 用途 | CUR テーブルのカラム名 → データ型マップを保持し、スキーマ取得クエリを省略する |

---

## 必要な IAM 権限

### CE API（既存）

```json
{
  "Action": [
    "ce:GetReservationUtilization",
    "ce:GetReservationCoverage",
    "ce:GetReservationPurchaseRecommendation"
  ],
  "Resource": "*"
}
```

Cost Explorer は Payer アカウントから全 Linked Account の集計データにアクセスできるため、
個別アカウントへの AssumeRole は不要。

### Athena / CUR（`--athena` 使用時）

```json
{
  "Action": [
    "athena:StartQueryExecution",
    "athena:GetQueryExecution",
    "athena:GetQueryResults",
    "glue:GetTable",
    "glue:GetDatabase",
    "s3:GetObject",
    "s3:PutObject",
    "s3:GetBucketLocation"
  ],
  "Resource": "*"
}
```

- `athena:*` : クエリ実行・結果取得
- `glue:*` : Glue データカタログ（`information_schema` クエリに必要）
- `s3:*` : CUR バケット読み取り + クエリ結果出力バケット書き込み

---

## 既知の制約・注意点

| 項目 | 内容 |
|---|---|
| CE データ遅延 | 最大 48 時間のタイムラグがあるため `end` を `UTC現在 - 48h` に設定している |
| lookback_days | 短すぎると活動実績のないサブスクリプションが CE から返されない。7 日以上推奨 |
| Granularity 制限 | `GetReservationUtilization` / `GetReservationCoverage` は `GroupBy` と `Granularity` を同時指定不可 |
| Multi-AZ 情報 | CE の `GetReservationCoverage` では Multi-AZ フラグが取得できないため、カバレッジ分析では考慮されていない |
| size flexibility (ElastiCache) | Redis と Valkey は同一グループに集計し、各エンジン固有の NU 係数で正確に加重集計している |

---

## TODO（未実装）

### 優先度中

- **OpenSearch 対応**
  API: `opensearch:DescribeReservedInstances`
  CE の service 名: `"Amazon OpenSearch Service"`

- **CE ファクトチェックの RI/OD 内訳精度向上**
  現状の `factcheck_recommendations()` は `running_rds_instances` の結果（RI/OD 内訳なし）を使うため、
  `cur_od_hours` は常に 0。`ce_factcheck_rds.sql` テンプレートや `ri_coverage_detail()` の結果と組み合わせることで精度向上可能。

### 優先度低

- **Redshift 対応**（TODO として保留中）
- **RDS の size-flexible NU 計算**
  MySQL / PostgreSQL / MariaDB は size-flexible RI が有効なため、
  `db.r6g.xlarge` RI 1台 = `db.r6g.large` RI 2台 相当として扱う精緻な計算は未対応
  （ElastiCache の Redis/Valkey は実装済み）

---

## 他サービス追加時の手順

1. `config.yaml` の `services` に追加
2. `cost_explorer.py` の `_CE_SERVICE_NAMES` に CE サービス名を追加
3. `main.py` の `if svc != "rds"` のガードを更新
4. `fetchers/` に `{service}.py` を追加（実行中リソース取得用、Step3 実装時）
5. 必要に応じて `analyzers/` のロジックを拡張

CE ベースの Expiration / Coverage / Utilization の 3 セクションは、
`_CE_SERVICE_NAMES` にエントリを追加するだけで自動的に対応する。
