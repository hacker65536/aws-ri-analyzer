# Handoff Notes  (2026-03-26)

## 現在の状態

RDS + ElastiCache の RI 分析が動作確認済み。
`--engine` / `--family` フィルタを Utilization・Recommendations セクションにも適用。
Recommendations の Breakeven 表示バグ（API フィールド名 typo）を修正。

### 直近のコミット

```
（今回のコミット） Fix engine/family filter for Utilization and Recommendations; fix Breakeven field name
8f2da56 Add ElastiCache Redis/Valkey NU compatibility and Recommendations section
33b2ed6 Update HANDOFF.md for session handoff
b164500 Add ElastiCache support
cffbbbf Add cache, coverage engine/region/filter improvements
749451a Add coverage family summary, SSO error handling, non-SSO profile support
```

---

## 実装済みの機能

### CLI オプション（全量）

```bash
--service rds elasticache      # 複数指定可（rds / elasticache 実装済み、opensearch は未実装）
--section expiration coverage utilization recommendations
--max-util 80                  # 利用率 <= PCT% のみ表示
--max-coverage 90              # カバレッジ <= PCT% のみ表示
--engine aurora                # Coverage / Utilization / Recommendations をエンジンで絞り込み（部分一致・大文字小文字無視）
--family r6g t4g               # Coverage / Utilization / Recommendations をインスタンスファミリーで絞り込み
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
  sections: [expiration, coverage, utilization, recommendations]
  regions: [ap-northeast-1]
  lookback_days: 7
  expiration_warn_days: 90
  cache_ttl_hours: 24

recommendation:
  term: ONE_YEAR          # ONE_YEAR / THREE_YEARS
  payment_option: ALL_UPFRONT
  lookback_days: 30       # 7 / 30 / 60
```

### Coverage 表示の構造

```
## Redis/Valkey              ← ElastiCache は Redis と Valkey を統合表示
  [cache.r6g.*]              ← instance family（cache. プレフィックス自動判定）
    Account ID  Instance Type  Region  Coverage  RI (hrs)  OD (hrs)  Total (hrs)
    ...（個別行: raw hours）
    (total, NUs) cache.r6g.*  ...N  ...N  ...N  ← NUs はエンジン別係数適用済み
```

- Redis NU 係数: `large=4`, Valkey NU 係数: `large=3.2`（Redis の 0.8 倍）
- エンジン次元の CE GroupBy キー: RDS=`DATABASE_ENGINE`, ElastiCache=`CACHE_ENGINE`
- レスポンス属性キー: RDS=`databaseEngine`, ElastiCache=`cacheEngine`
- マッピングは `cost_explorer.py` の `_CE_ENGINE_DIMENSION` / `_CE_ENGINE_ATTR`

### --engine フィルタの platform 名正規化

`GetReservationUtilization` の `platform` 属性は短縮形（例: `"Aurora"`）で返る。
一方 `GetReservationCoverage` は `"Aurora MySQL"` という完全形で返る。
`reporter.py` の `_UTIL_PLATFORM_NORMALIZE` でフィルタ前に正規化することで、
両セクションで `--engine "aurora mysql"` が一貫して動作する。

| Utilization platform | 正規化後 |
|---|---|
| `"Aurora"` | `"Aurora MySQL"` |

Recommendations の platform は `"Aurora MySQL Single-AZ"` 形式なので正規化不要（通常の部分一致で動作）。

### Breakeven フィールド名

CE API の正しいフィールド名は `EstimatedBreakEvenInMonths`（"Even" の E が大文字）。
旧来 `EstimatedBreakevenInMonths` と記述していたため Breakeven が常に 0.0 になっていた。修正済み。

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

### 3. RDS の size-flexible NU 計算（優先度低）

MySQL / PostgreSQL / MariaDB は size-flexible RI が有効。
ElastiCache で実装した Redis/Valkey 統合と同様に、RDS ファミリー内でも NU 加重集計が本来は必要。
現状はエンジンをまたいだ NU 計算は未対応（hours ベースのカバレッジ計算のみ）。

### 4. Redshift 対応（優先度低）

---

詳細仕様は `SPEC.md` を参照。
