# AWS RI Analyzer

AWS Organizations 配下の全アカウントの Reserved Instance 状況を分析する CLI ツール。

Payer アカウントの Cost Explorer API を使うため、個別アカウントへの AssumeRole は不要。

## セットアップ

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cp config.yaml.example config.yaml
# config.yaml を開き、payer.account_id を Payer アカウント ID に書き換える
```

## 使い方

```bash
# 実行（初回は対象サービス・セクションをインタラクティブに選択して config.yaml に保存）
python main.py

# セクションを絞って実行
python main.py --section expiration utilization

# 利用率が低い RI だけ表示
python main.py --section utilization --max-util 80

# カバレッジが低いグループだけ表示
python main.py --section coverage --max-coverage 90

# カバレッジを特定エンジン・ファミリーに絞って表示
python main.py --section coverage --engine aurora
python main.py --section coverage --engine "aurora mysql" --family r6g r8g

# キャッシュを無視して AWS から再取得
python main.py --no-cache

# カラーなし出力（ログ保存・CI 用）
python main.py --no-color > output.log
```

### Athena / CUR 機能

CUR（Cost and Usage Report）を Athena 経由でクエリする機能。
`config.yaml` の `athena` セクション設定が前提（S3 + Athena / Glue のセットアップが必要）。

```bash
# CUR セクションを追加（先月が自動適用）
python main.py --service rds --athena

# 年月を明示指定
python main.py --service rds --athena --cur-year 2026 --cur-month 3

# CUR セクションだけ表示
python main.py --service rds --section cur_instances cur_coverage unused_ri

# CE Recommendation + CUR ファクトチェック
python main.py --service rds --section recommendations --athena
```

#### SQL テンプレートの直接実行（`athena_run.py`）

```bash
# テンプレート一覧
python athena_run.py --list

# テンプレートを実行
# year / month / start_date / end_date は CE 期間（lookback_days）から自動注入（-p で上書き可）
python athena_run.py rds_instances -p year=2026 -p month=3
python athena_run.py rds_resource_ids \
  -p instance_type_prefix=db.r8g -p engine="Aurora MySQL"
python athena_run.py ce_factcheck_rds -p year=2026 -p month=3 \
  -p instance_type=db.r6g.large -p region=ap-northeast-1 -p engine=Aurora

# カスタム SQL ファイルを実行（{{ variable }} 形式で変数埋め込み可）
python athena_run.py ./queries/my_query.sql -p year=2026 -p month=3

# リソース別の稼働時間・インスタンスタイプ変更調査（queries/ 配下のカスタム SQL）
python athena_run.py queries/resource_uptime.sql \
  -p resource_id=my-instance-00
python athena_run.py queries/resource_engine_check.sql \
  -p resource_id=my-instance-00
python athena_run.py queries/resource_type_changes.sql
python athena_run.py queries/resource_latest_type.sql

# サイズ閾値・表示行数の調整
python athena_run.py rds_instances -p year=2026 -p month=3 --limit-mb 50 --head 20

# キャッシュ制御
python athena_run.py rds_instances --refresh   # キャッシュ削除して再実行
python athena_run.py rds_instances --no-cache  # キャッシュを使わず毎回実行
```

> **注意**: CUR のパーティション `month` はゼロ埋めなし（`'3'` / `'12'`）。
> `athena_run.py` は `-p month=03` と渡されても自動的に `'3'` に正規化する。

#### CUR vs CE カバレッジ検証（`compare_cur_ce.py`）

CUR（Athena）と CE API のカバレッジ数値を突き合わせて精度を検証するスクリプト。
CE と同じ期間（`lookback_days` 設定から自動計算）で比較する。

```bash
# r8g Aurora MySQL の CUR vs CE を比較
python compare_cur_ce.py --year 2026 --month 3 --service rds \
  --instance-type-prefix db.r8g --engine "Aurora MySQL"
```

出力例：
```
account_id     region          instance_type     total_CUR  total_CE   Δtotal  ...  status
123456789012   ap-northeast-1  db.r8g.large         2922.0    2922.0     -0.0  ...  OK
...
合計 total : CUR=6187.4h  CE=6187.5h  Δ=-0.0h (-0.00%)
判定: OK=7  MISMATCH=0  CUR-only=0  CE-only=0
```

### オプション一覧

| オプション | 説明 |
|---|---|
| `--service SERVICE [...]` | 対象サービス（**rds** / **elasticache** / opensearch）複数指定可 |
| `--section SECTION [...]` | 表示セクション（後述）複数指定可 |
| `--max-util PCT` | 利用率が PCT% 以下のサブスクリプションのみ表示 |
| `--max-coverage PCT` | カバレッジが PCT% 以下のグループのみ表示 |
| `--engine ENGINE [...]` | エンジンで絞り込み（部分一致・大文字小文字無視）|
| `--family FAMILY [...]` | インスタンスファミリーで絞り込み（例: r6g t4g）|
| `--show-sub-id` | Utilization テーブルに Subscription ID 列を表示 |
| `--no-color` | カラー出力を無効化 |
| `--no-cache` | キャッシュを無視して AWS から再取得 |
| `--config PATH` | 設定ファイルのパス（デフォルト: config.yaml） |
| `--athena` | Athena/CUR セクションを有効化 |
| `--cur-year YYYY` | CUR クエリ年（省略時: 先月）|
| `--cur-month M` | CUR クエリ月 1〜12（省略時: 先月）|

#### セクション一覧

| セクション | データソース | 内容 |
|---|---|---|
| `expiration` | CE API | RI 有効期限チェック |
| `coverage` | CE API | RI カバレッジ率 |
| `utilization` | CE API | RI 利用率・未使用時間 |
| `recommendations` | CE API | RI 購入推奨 |
| `cur_instances` | Athena/CUR | 稼働中インスタンス一覧（実績使用時間・コスト）|
| `cur_coverage` | Athena/CUR | CUR ベースの RI カバレッジ詳細 |
| `unused_ri` | Athena/CUR | 未使用 RI 費用（RIFee 行）|

`--athena` フラグは `cur_instances` / `cur_coverage` / `unused_ri` を一括追加し、
`recommendations` と組み合わせると CE Recommendation のファクトチェックも自動表示する。

### キャッシュ

| 種別 | 保存先 | デフォルト TTL | 設定キー |
|---|---|---|---|
| AWS API レスポンス | `~/.cache/ri-analyzer/*.json` | 24 時間 | `analysis.cache_ttl_hours` |
| Athena スキーマ | `~/.cache/ri-analyzer/athena_schema_*.json` | 168 時間 | `athena.schema_cache_ttl_hours` |
| Athena クエリ結果 | `~/.cache/ri-analyzer/query_results/{sql_hash}.csv` | 24 時間 | `athena.query_cache_ttl_hours` |

Athena クエリ結果のキャッシュキーは **実体化された SQL 文字列の SHA256 ハッシュ**。
同じテンプレート・同じパラメータなら 2 回目以降は Athena を呼ばずにローカルから返す。
`--no-cache` / `--refresh` でバイパス可能。

詳細な仕様・設計については [SPEC.md](SPEC.md) を参照。
