# AWS RI Analyzer

AWS Organizations 配下の全アカウントの Reserved Instance 状況を分析する CLI ツール。

Payer アカウントの Cost Explorer API を使うため、個別アカウントへの AssumeRole は不要。

## セットアップ

[uv](https://docs.astral.sh/uv/) を使う場合（推奨）:

```bash
uv sync
uv pip install -e .

cp config.yaml.example config.yaml
# config.yaml を開き、payer.account_id を Payer アカウント ID に書き換える
```

> `uv sync` が `.venv` の作成と依存パッケージのインストールを一括で行います。
> `uv pip install -e .` で `ri-analyzer` コマンドが使えるようになります。

### どこからでも実行できるようにする（PATH 設定）

`~/.zshrc`（bash の場合は `~/.bashrc`）に以下を追加することで、ターミナルのカレントディレクトリに関わらず `ri-analyzer` を直接実行できます。

```bash
export PATH="/path/to/aws-ri-analyzer/.venv/bin:$PATH"
```

追加後に反映：

```bash
source ~/.zshrc
```

> `/path/to/aws-ri-analyzer` はこのリポジトリをクローンした実際のパスに置き換えてください。

### シェル補完（zsh / bash）

`~/.zshrc`（bash の場合は `~/.bashrc`）に以下を追加することで、オプションの Tab 補完が有効になります。

```bash
eval "$(/path/to/aws-ri-analyzer/.venv/bin/register-python-argcomplete ri-analyzer)"
```

補完対象：`--service`、`--section`、`--output` などの `choices` を持つオプション。

<details>
<summary>pip を使う場合</summary>

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

</details>

## 使い方

```bash
# 実行（初回は対象サービス・セクションをインタラクティブに選択して config.yaml に保存）
uv run ri-analyzer

# セクションを絞って実行
uv run ri-analyzer --section expiration utilization

# 利用率が低い RI だけ表示
uv run ri-analyzer --section utilization --max-util 80

# カバレッジが低いグループだけ表示
uv run ri-analyzer --section coverage --max-coverage 90

# カバレッジを特定エンジン・ファミリーに絞って表示
uv run ri-analyzer --section coverage --engine aurora
uv run ri-analyzer --section coverage --engine "aurora mysql" --family r6g r8g

# キャッシュを無視して AWS から再取得
uv run ri-analyzer --no-cache

# カラーなし出力（ログ保存・CI 用）
uv run ri-analyzer --no-color > output.log
```

### Athena / CUR 機能

CUR（Cost and Usage Report）を Athena 経由でクエリする機能。
`config.yaml` の `athena` セクション設定が前提（S3 + Athena / Glue のセットアップが必要）。

CUR クエリのデフォルト期間は **CE API と同じ期間**（UTC now − 48h を end とした lookback_days 分）。
`--cur-year` / `--cur-month` を指定するとその月全体（月初〜翌月初）に切り替わる。

```bash
# RI 購入精査: resource_id 単位でインスタンス稼働実績を表示（CE と同期間、デフォルト）
uv run ri-analyzer --service rds --section cur_instance_detail

# 稼働率が低い（期間の 50% 未満）インスタンスを除外して RI 候補に絞り込む
uv run ri-analyzer --service rds --section cur_instance_detail --min-hours 100

# 先月全体を指定して確認
uv run ri-analyzer --service rds --section cur_instance_detail --cur-year 2026 --cur-month 2

# CUR セクションを一括追加（cur_instance_detail / cur_instances / cur_coverage / unused_ri）
uv run ri-analyzer --service rds --athena

# 年月を明示指定
uv run ri-analyzer --service rds --athena --cur-year 2026 --cur-month 3

# CUR セクションだけ表示
uv run ri-analyzer --service rds --section cur_instance_detail cur_instances cur_coverage unused_ri

# CE Recommendation + CUR ファクトチェック
uv run ri-analyzer --service rds --section recommendations --athena
```

#### SQL テンプレートの直接実行（`athena_run.py`）

```bash
# テンプレート一覧
uv run python athena_run.py --list

# テンプレートを実行
# year / month / start_date / end_date は CE 期間（lookback_days）から自動注入（-p で上書き可）
uv run python athena_run.py rds_instances -p year=2026 -p month=3
uv run python athena_run.py rds_resource_ids \
  -p instance_type_prefix=db.r8g -p engine="Aurora MySQL"
uv run python athena_run.py ce_factcheck_rds -p year=2026 -p month=3 \
  -p instance_type=db.r6g.large -p region=ap-northeast-1 -p engine=Aurora

# カスタム SQL ファイルを実行（{{ variable }} 形式で変数埋め込み可）
uv run python athena_run.py ./queries/my_query.sql -p year=2026 -p month=3

# リソース別の稼働時間・インスタンスタイプ変更調査（queries/ 配下のカスタム SQL）
uv run python athena_run.py queries/resource_uptime.sql \
  -p resource_id=my-instance-00
uv run python athena_run.py queries/resource_engine_check.sql \
  -p resource_id=my-instance-00
uv run python athena_run.py queries/resource_type_changes.sql
uv run python athena_run.py queries/resource_latest_type.sql

# サイズ閾値・表示行数の調整
uv run python athena_run.py rds_instances -p year=2026 -p month=3 --limit-mb 50 --head 20

# キャッシュ制御
uv run python athena_run.py rds_instances --refresh   # キャッシュ削除して再実行
uv run python athena_run.py rds_instances --no-cache  # キャッシュを使わず毎回実行
```

> **注意**: CUR のパーティション `month` はゼロ埋めなし（`'3'` / `'12'`）。
> `athena_run.py` は `-p month=03` と渡されても自動的に `'3'` に正規化する。

#### CUR vs CE カバレッジ検証（`compare_cur_ce.py`）

CUR（Athena）と CE API のカバレッジ数値を突き合わせて精度を検証するスクリプト。
CE と同じ期間（`lookback_days` 設定から自動計算）で比較する。

```bash
# r8g Aurora MySQL の CUR vs CE を比較
uv run python compare_cur_ce.py --year 2026 --month 3 --service rds \
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
| `--min-hours HRS` | `cur_instance_detail`: usage_hours >= HRS のインスタンスのみ表示 |
| `--athena` | Athena/CUR セクションを有効化 |
| `--cur-year YYYY` | CUR クエリ年（省略時: CE API と同じ期間）|
| `--cur-month M` | CUR クエリ月 1〜12（省略時: CE API と同じ期間）|

#### セクション一覧

| セクション | データソース | 内容 |
|---|---|---|
| `expiration` | CE API | RI 有効期限チェック |
| `coverage` | CE API | RI カバレッジ率 |
| `utilization` | CE API | RI 利用率・未使用時間 |
| `recommendations` | CE API | RI 購入推奨 |
| `cur_instance_detail` | Athena/CUR | **resource_id 単位**の稼働実績・RI/OD 内訳（RI 購入精査の基本機能）|
| `cur_instances` | Athena/CUR | instance_type 集計の稼働一覧（実績使用時間・コスト）|
| `cur_coverage` | Athena/CUR | CUR ベースの RI カバレッジ詳細 |
| `unused_ri` | Athena/CUR | 未使用 RI 費用（RIFee 行）|

`--athena` フラグは `cur_instance_detail` / `cur_instances` / `cur_coverage` / `unused_ri` を一括追加し、
`recommendations` と組み合わせると CE Recommendation のファクトチェックも自動表示する。

##### `cur_instance_detail` の見方

| 列 | 内容 |
|---|---|
| `Resource Name` | ARN 末尾の DB 識別子（短縮表示）|
| `Period%` | クエリ期間中の稼働率（100% = 期間ずっと稼働）|
| `hrs` | usage_hours（実績使用時間）|
| `RI%` | RI カバレッジ率（RI hrs / 合計 hrs）|
| `RI hrs` / `OD hrs` | RI 適用時間 / オンデマンド時間 |

- **黄色ハイライト**: 期間の 50% 未満しか稼働していない「短命なインスタンス」。RI 購入候補から除外を検討する
- `--min-hours` で閾値を指定すると、それ以上稼働したインスタンスのみ表示できる

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
