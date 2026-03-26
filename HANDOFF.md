# Handoff Notes  (2026-03-26)

## 現在の状態

RDS + ElastiCache の RI 分析が動作確認済み。キャッシュ機構・カバレッジのエンジン別表示・絞り込みフィルタを実装済み。

### 直近のコミット

```
b164500 Add ElastiCache support
cffbbbf Add cache, coverage engine/region/filter improvements
749451a Add coverage family summary, SSO error handling, non-SSO profile support
5d06524 Enhance utilization display with count, NUs, family summary
b6f1e5d Initial commit: RDS RI analyzer MVP
```

---

## 実装済みの機能

### CLI オプション（全量）

```bash
--service rds elasticache      # 複数指定可（rds / elasticache 実装済み、opensearch は未実装）
--section expiration coverage utilization
--max-util 80                  # 利用率 <= PCT% のみ表示
--max-coverage 90              # カバレッジ <= PCT% のみ表示
--engine aurora                # カバレッジをエンジンで絞り込み（部分一致・大文字小文字無視）
--family r6g t4g               # カバレッジをインスタンスファミリーで絞り込み
--show-sub-id                  # Utilization に Subscription ID 列を追加
--no-color                     # カラー出力を無効化
--no-cache                     # キャッシュをバイパスして AWS から再取得
--config PATH                  # 設定ファイルのパス（デフォルト: config.yaml）
```

### キャッシュ

- 保存先: `~/.cache/ri-analyzer/*.pkl`（pickle 形式）
- TTL: デフォルト 24h、`config.yaml` の `cache_ttl_hours` で変更可
- スキーマ変更時は `ri_analyzer/cache.py` の `_CACHE_VERSION` をインクリメントする
  → 現在の値: `"3"`

### config.yaml のキー

```yaml
payer:
  account_id: "..."
  # profile: "..."   # 省略時は profile_resolver で自動解決（AWS SSO）

analysis:
  services: [rds, elasticache]
  sections: [expiration, coverage, utilization]
  regions: [ap-northeast-1]
  lookback_days: 7
  expiration_warn_days: 90
  cache_ttl_hours: 24
```

### Coverage 表示の構造

```
## Aurora MySQL              ← database engine（太字・シアン区切り線）
  [db.r6g.*]                 ← instance family
    Account ID  Instance Type  Region  Coverage  RI (hrs)  OD (hrs)  Total (hrs)
    ...（個別行: raw hours）
    (total, NUs) db.r6g.*  ...N  ...N  ...N     ← family サマリ: Normalized Units
```

- エンジン次元の CE GroupBy キー: RDS=`DATABASE_ENGINE`, ElastiCache=`CACHE_ENGINE`
- レスポンス属性キー: RDS=`databaseEngine`, ElastiCache=`cacheEngine`
- マッピングは `cost_explorer.py` の `_CE_ENGINE_DIMENSION` / `_CE_ENGINE_ATTR`

---

## 既知の CE API 制約（ハマりやすい点）

| 制約 | 内容 |
|---|---|
| GroupBy + Granularity | 同時指定不可 → ValidationException |
| GetReservationUtilization の GroupBy レスポンス | `group["Key"]` = 次元名、`group["Value"]` = 実際の値 |
| GetReservationCoverage の GroupBy レスポンス | `Attributes{}` に格納。キー名は camelCase |
| Coverage の時間フィールド | `ReservedHours`（`CoveredHours` は存在しない） |
| RDS の Coverage エンジン次元 | `PLATFORM` は使用不可。`DATABASE_ENGINE` を使う |
| lookback_days | 短すぎると CE がサブスクリプションを返さない。7日以上推奨 |

---

## 設計上の重要決定

- **全データを Payer アカウントの CE API から取得**（個別アカウントへのアクセス不要）
- **出力は英語**（日本語全角文字は f-string のカラム幅計算でズレるため）
- **有効期限は CE の `Attributes["endDateTime"]` から取得**
- **Net Savings の判定**: 正 → `[+]`、負 → `[-]`
- **Config.save() は yaml.dump で全体書き直し**（コメントは消えるが合意済み）
- **キャッシュはサービス変数名ベース** (`_parse_instance_family` は `parts[1]` 方式なので `cache.` prefix も追加変更不要)

---

## 次に実装すること

### 1. 実行中インスタンス詳細（Step3 / 優先度高）

現状の Coverage は CE 集計値のみ。インスタンス ID 等の詳細は取れない。
方針B（Athena / CUR）で実装予定。CUR の S3/Athena セットアップが前提。
方針A（CE で account × region × type を絞り込み → 該当アカウントの `DescribeDBInstances`）も選択肢。

### 2. OpenSearch 対応（優先度中）

`_CE_SERVICE_NAMES` にはすでに登録済み。
ElastiCache と同様にガードを外し、エンジン次元のマッピングを追加すれば動く見込み。

### 3. Redshift 対応（優先度低）

---

詳細仕様は `SPEC.md` を参照。
