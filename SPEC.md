# AWS RI Analyzer — 仕様書

## 概要

AWS Organizations 配下の全アカウントを対象に、RDS Reserved Instance（RI）の状況を分析する CLI ツール。
Payer（管理）アカウントの Cost Explorer API を軸に据えることで、182 アカウントを個別に叩くオーバーヘッドなく全体像を把握できる。

### 分析内容

| セクション | 内容 | 使用 API |
|---|---|---|
| Expiration | RI の有効期限チェック（期限切れ / 要注意 / 正常） | CE GetReservationUtilization |
| Coverage | アカウント × リージョン × インスタンスタイプ別の RI カバレッジ率 | CE GetReservationCoverage |
| Utilization | サブスクリプション別の利用率・未使用時間・削減額 | CE GetReservationUtilization |

対応サービス: **RDS**, **ElastiCache**（OpenSearch は未実装）

---

## ディレクトリ構成

```
aws-ri-analyzer/
├── config.yaml                        # 設定ファイル（.gitignore 対象）
├── config.yaml.example                # サンプル設定ファイル（リポジトリに含める）
├── .gitignore
├── main.py                            # CLI エントリポイント
├── requirements.txt
└── ri_analyzer/
    ├── cache.py                       # AWS API レスポンスのローカルディスクキャッシュ
    ├── config.py                      # config.yaml の読み込み・バリデーション
    ├── profile_resolver.py            # account_id → AWS SSO プロファイル名 解決
    ├── fetchers/
    │   ├── cost_explorer.py           # CE API 呼び出し（RI データ・カバレッジ）
    │   └── rds.py                     # RDS 実行中インスタンス取得（現在は未使用、将来用）
    ├── analyzers/
    │   ├── expiration.py              # 有効期限分類ロジック
    │   ├── coverage.py                # カバレッジ集計ロジック
    │   └── utilization.py             # 利用率集計ロジック
    └── reporter.py                    # コンソール出力（英語・カラー対応）
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

analysis:
  services:                    # 対象サービス（省略時は初回起動でインタラクティブ選択 → 自動保存）
    - rds
    # - elasticache
    # - opensearch

  sections:                    # 表示セクション（省略時は初回起動でインタラクティブ選択 → 自動保存）
    - expiration
    - coverage
    - utilization

  regions:
    - ap-northeast-1

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
  │
  └─ [for each service in config]
      │
      ├─ fetch_ri_subscriptions()    # CE GetReservationUtilization
      │    → RiSubscription[]        # RI 一覧（有効期限・台数等）
      │    → RiUtilizationRecord[]   # 利用率レコード
      │
      ├─ fetch_ri_coverage()         # CE GetReservationCoverage
      │    → RiCoverageRecord[]      # account × region × type 別カバレッジ
      │
      ├─ exp_analyzer.analyze()      # 有効期限分類
      ├─ cov_analyzer.analyze()      # カバレッジ集計
      ├─ util_analyzer.summarize()   # 利用率集計
      │
      └─ reporter.print_*()          # コンソール出力
```

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
**GroupBy**: `LINKED_ACCOUNT`, `REGION`, `INSTANCE_TYPE`, + サービス別エンジン次元
**注意**: `Granularity` と `GroupBy` は同時指定不可
**注意**: エンジン次元はサービスにより異なる（`_CE_ENGINE_DIMENSION` マッピングで解決）

| サービス | エンジン次元 | レスポンス属性キー |
|---|---|---|
| rds | `DATABASE_ENGINE` | `databaseEngine` |
| elasticache | `CACHE_ENGINE` | `cacheEngine` |

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

`RiCoverageRecord` を `(account_id, region, instance_type, platform)` キーで集計し、
`CoverageSummary` に変換。

| ステータス | カバレッジ率 |
|---|---|
| ok | >= 90% |
| warning | 50% 〜 90% |
| low | < 50% |

ソート順: platform（database engine）→ instance family → サイズ（norm_factor 昇順）→ account_id

#### 正規化ユニット（Normalized Units）

`utilization.py` の `_NORM_FACTOR` / `_norm_factor()` を共有して使用。

- `CoverageSummary.covered_nus` = `covered_hours × norm_factor`
- `CoverageSummary.on_demand_nus` = `on_demand_hours × norm_factor`
- `CoverageSummary.total_nus` = `total_hours × norm_factor`
- family サマリの Coverage% は NUs 加重平均で算出

---

### `analyzers/utilization.py`

`RiUtilizationRecord` をサブスクリプション ID ごとに集計し `UtilizationSummary` に変換。
複数期間にまたがるレコードは平均利用率・累計未使用時間・累計削減額を算出。

| ステータス | 平均利用率 |
|---|---|
| ok | >= 80% |
| warning | 50% 〜 80% |
| low | < 50% |

ソート順: instance family（`db.r5.large` → `r5`）→ サイズ（正規化ユニット昇順）

#### 正規化ユニット（Normalized Units）

AWS 標準の係数テーブル（`_NORM_FACTOR`）を使い、インスタンスサイズから NUs を算出。

```
nano=0.25 / micro=0.5 / small=1 / medium=2 / large=4 / xlarge=8 / 2xlarge=16 / ...
```

- `UtilizationSummary.normalized_units` = `count × 係数`（例: `db.r5.large x10` → 40 NUs）
- family サマリの `Avg Util` は NUs 加重平均で算出

---

### `reporter.py`

- カラー出力（ANSI）。`--no-color` または `set_color(False)` で無効化
- ファイル出力時は `--no-color` を推奨（ANSI コードが混入するため）
- カラム幅は ASCII 文字で固定しているため日本語文字列はカラムがずれる
- `print_utilization(summaries, max_util=None, show_sub_id=False)`
  - `max_util` 指定時は avg_utilization_pct がその値以下のレコードのみ表示
  - `show_sub_id=True` で Subscription ID 列を追加表示
  - instance family 単位でサマリ行を表示（2件以上の場合）
  - 詳細行の `Unused` 列は `hrs` 単位、サマリ行は `NUs` 単位（正規化ユニット時間）
- `print_coverage(summaries, max_coverage=None, engines=None, families=None)`
  - `max_coverage` 指定時は coverage_pct がその値以下のレコードのみ表示
  - `engines` 指定時はデータベースエンジンで絞り込み（部分一致・大文字小文字無視）
  - `families` 指定時はインスタンスファミリーで絞り込み（完全一致）
  - database engine → instance family の 2 段階グループで表示
  - エンジンヘッダーは `## Engine` + 区切り線で視覚的に強調
  - 詳細行の列: Account ID / Instance Type / Region / Coverage / RI (hrs) / OD (hrs) / Total (hrs)
  - family サマリ行（2件以上の場合）はラベルが `(total, NUs)`、値は NUs 単位（`N` サフィックス）

---

## キャッシュ（`cache.py`）

AWS API レスポンスをローカルディスクにキャッシュし、繰り返し実行を高速化する。

| 項目 | 内容 |
|---|---|
| 保存先 | `~/.cache/ri-analyzer/*.pkl` |
| フォーマット | pickle（`datetime` 型を含むデータクラスをそのまま保存） |
| デフォルト TTL | 24 時間（`config.yaml` の `cache_ttl_hours` で変更可） |
| バイパス | `--no-cache` フラグ |
| キャッシュキー | `{type}:{payer_profile}:{service}:{lookback_days}` の SHA-256（先頭 16 文字）|
| スキーマ変更対応 | `_CACHE_VERSION` 定数をインクリメントすると既存キャッシュが自動無効化される |

キャッシュされるデータ：
- `subscriptions:{...}` → `(list[RiSubscription], list[RiUtilizationRecord])`
- `coverage:{...}` → `list[RiCoverageRecord]`

---

## 必要な IAM 権限

Payer アカウントのプロファイルに以下が必要：

```json
{
  "Action": [
    "ce:GetReservationUtilization",
    "ce:GetReservationCoverage"
  ],
  "Resource": "*"
}
```

Cost Explorer は Payer アカウントから全 Linked Account の集計データにアクセスできるため、
個別アカウントへの AssumeRole は不要（現在の実装では使用していない）。

---

## 既知の制約・注意点

| 項目 | 内容 |
|---|---|
| CE データ遅延 | 最大 48 時間のタイムラグがあるため `end` を `UTC現在 - 48h` に設定している |
| lookback_days | 短すぎると活動実績のないサブスクリプションが CE から返されない。7 日以上推奨 |
| Granularity 制限 | `GetReservationUtilization` / `GetReservationCoverage` は `GroupBy` と `Granularity` を同時指定不可 |
| Multi-AZ 情報 | CE の `GetReservationCoverage` では Multi-AZ フラグが取得できないため、カバレッジ分析では考慮されていない |
| size flexibility | CE の `sizeFlexibility: "FlexRI"` は取得できるが、正規化ユニット換算による精緻なカバレッジ計算は未実装 |

---

## TODO（未実装）

### 優先度高

- **実行中インスタンス詳細の取得（Step3）**
  現状の Coverage は CE の集計値のみで、実際に動いているインスタンス ID や設定詳細は取れない。
  方針: Athena で CUR（Cost and Usage Report）にクエリする（案B）。
  CUR のセットアップ（S3 + Athena）が前提。
  CE でまず `(account_id, region, instance_type)` の組み合わせを絞り込み、該当アカウントの `DescribeDBInstances` を呼ぶことでも実現可能（案A）。

### 優先度中

- **OpenSearch 対応**
  API: `opensearch:DescribeReservedInstances`
  CE の service 名: `"Amazon OpenSearch Service"`

### 優先度低

- **Redshift 対応**（TODO として保留中）
- **正規化ユニット（Normalization Factor）による精緻な差分計算**
  MySQL / PostgreSQL / MariaDB は size-flexible RI が有効なため、
  `db.r6g.xlarge` RI 1台 = `db.r6g.large` RI 2台 相当として扱う必要がある

---

## 他サービス追加時の手順

1. `config.yaml` の `services` に追加
2. `cost_explorer.py` の `_CE_SERVICE_NAMES` に CE サービス名を追加
3. `main.py` の `if svc != "rds"` のガードを更新
4. `fetchers/` に `{service}.py` を追加（実行中リソース取得用、Step3 実装時）
5. 必要に応じて `analyzers/` のロジックを拡張

CE ベースの Expiration / Coverage / Utilization の 3 セクションは、
`_CE_SERVICE_NAMES` にエントリを追加するだけで自動的に対応する。
