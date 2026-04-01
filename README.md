# AWS RI Analyzer / CUR Analyzer

コスト最適化を目的とした 2 つの CLI ツール。

| コマンド | 主な役割 |
|---|---|
| `ri-analyzer` | Reserved Instance のライフサイクル管理（有効期限・カバレッジ・利用率・推奨購入量） |
| `cur-analyzer` | CUR（Cost and Usage Report）を Athena 経由でアドホック分析 |

両ツールは Payer アカウントの API を使用するため、個別アカウントへの AssumeRole は不要。
内部では同じ Athena クライアントとクエリライブラリを共有している。

> **将来の方向性**: CUR を使ったコスト最適化分析は RI 以外の領域（Savings Plans、EC2 サイズ最適化など）へも拡張予定です。
> 現時点では 1 リポジトリ内で共存していますが、`cur-analyzer` の機能が成熟した段階で独立したツールとして分離することを想定しています。

## セットアップ

[uv](https://docs.astral.sh/uv/) を使う場合（推奨）:

```bash
uv sync
uv pip install -e .

cp config.yaml.example config.yaml
# config.yaml を開き、payer.account_id を Payer アカウント ID に書き換える
```

> `uv sync` が `.venv` の作成と依存パッケージのインストールを一括で行います。
> `uv pip install -e .` で `ri-analyzer` と `cur-analyzer` の両コマンドが使えるようになります。
> 次の「PATH 設定」を済ませると、`uv run` なしで直接コマンドを実行できます。シェル補完も PATH 設定が前提です。

### どこからでも実行できるようにする（PATH 設定）

`~/.zshrc`（bash の場合は `~/.bashrc`）に以下を追加することで、ターミナルのカレントディレクトリに関わらず各コマンドを直接実行できます。

```bash
export PATH="/path/to/aws-ri-analyzer/.venv/bin:$PATH"
```

追加後に反映：

```bash
source ~/.zshrc
```

> `/path/to/aws-ri-analyzer` はこのリポジトリをクローンした実際のパスに置き換えてください。

### シェル補完（zsh / bash）

`~/.zshrc`（bash の場合は `~/.bashrc`）に以下を追加することで、両コマンドの Tab 補完が有効になります。

```bash
eval "$(/path/to/aws-ri-analyzer/.venv/bin/register-python-argcomplete ri-analyzer)"
eval "$(/path/to/aws-ri-analyzer/.venv/bin/register-python-argcomplete cur-analyzer)"
```

| コマンド | 補完対象 |
|---|---|
| `ri-analyzer` | `--service`、`--section`、`--output` などの choices を持つオプション |
| `cur-analyzer` | テンプレート名（`rds_instances` など）、各種オプション |

<details>
<summary>pip を使う場合</summary>

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

</details>

## ri-analyzer の使い方

```bash
# 実行（初回は対象サービス・セクションをインタラクティブに選択して config.yaml に保存）
ri-analyzer

# セクションを絞って実行
ri-analyzer --section expiration utilization

# 利用率が低い RI だけ表示
ri-analyzer --section utilization --max-util 80

# カバレッジが低いグループだけ表示
ri-analyzer --section coverage --max-coverage 90

# カバレッジを特定エンジン・ファミリーに絞って表示
ri-analyzer --section coverage --engine aurora
ri-analyzer --section coverage --engine aurora mysql --family r6g r8g

# キャッシュを無視して AWS から再取得
ri-analyzer --no-cache

# カラーなし出力（ログ保存・CI 用）
ri-analyzer --no-color > output.log
```

### Athena / CUR 機能

CUR（Cost and Usage Report）を Athena 経由でクエリする機能。
`config.yaml` の `athena` セクション設定が前提（S3 + Athena / Glue のセットアップが必要）。

CUR クエリのデフォルト期間は **CE API と同じ期間**（UTC now − 48h を end とした lookback_days 分）。
`--cur-year` / `--cur-month` を指定するとその月全体（月初〜翌月初）に切り替わる。

```bash
# RI 購入精査: resource_id 単位でインスタンス稼働実績を表示（CE と同期間、デフォルト）
ri-analyzer --service rds --section cur_instance_detail
ri-analyzer --service opensearch --section cur_instance_detail

# 稼働時間が短いインスタンスを除外して RI 候補に絞り込む
ri-analyzer --service rds --section cur_instance_detail --min-hours 100

# 先月全体を指定して確認
ri-analyzer --service rds --section cur_instance_detail --cur-year 2026 --cur-month 2

# CUR セクションを一括追加（cur_instance_detail / cur_instances / cur_coverage / unused_ri）
ri-analyzer --service rds --athena

# 年月を明示指定
ri-analyzer --service rds --athena --cur-year 2026 --cur-month 3

# CUR セクションだけ表示
ri-analyzer --service rds --section cur_instance_detail cur_instances cur_coverage unused_ri

# CE Recommendation + CUR ファクトチェック
ri-analyzer --service rds --section recommendations --athena
```

### オプション一覧

| オプション | 説明 |
|---|---|
| `--service SERVICE [...]` | 対象サービス（**rds** / **elasticache** / **opensearch**）複数指定可 |
| `--section SECTION [...]` | 表示セクション（後述）複数指定可 |
| `--max-util PCT` | 利用率が PCT% 以下のサブスクリプションのみ表示 |
| `--max-coverage PCT` | カバレッジが PCT% 以下のグループのみ表示 |
| `--engine ENGINE [...]` | エンジンで絞り込み（部分一致・大文字小文字無視）|
| `--family FAMILY [...]` | インスタンスファミリーで絞り込み（例: r6g t4g）|
| `--split-engine` | Redis と Valkey をカバレッジ表示で別グループに分ける |
| `--show-sub-id` | Utilization テーブルに Subscription ID 列を表示 |
| `--no-color` | カラー出力を無効化 |
| `--no-cache` | キャッシュを無視して AWS から再取得 |
| `--output console\|json` | 出力形式（デフォルト: console） |
| `--config PATH` | 設定ファイルのパス（デフォルト: config.yaml） |
| `--verbose` | デバッグログを stderr に表示する |
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
| `Account ID` | AWS アカウント ID |
| `Region` | リソースのリージョン（`product_region`）|
| `Period%` | クエリ期間中の稼働率（100% = 期間ずっと稼働）|
| `hrs` | usage_hours（実績使用時間）|
| `RI%` | RI カバレッジ率（RI hrs / 合計 hrs）|
| `RI hrs` / `OD hrs` | RI 適用時間 / オンデマンド時間 |

- **黄色ハイライト**: 期間の 50% 未満しか稼働していない「短命なインスタンス」。RI 購入候補から除外を検討する
- `--min-hours` で閾値を指定すると、それ以上稼働したインスタンスのみ表示できる

## cur-analyzer — CUR アドホック分析

CUR（Cost and Usage Report）を Athena 経由で直接クエリするツール。
SQL テンプレートとカスタム SQL の両方に対応している。

```bash
# テンプレート一覧（Tab 補完も対応）
cur-analyzer --list

# テンプレートを実行
# year / month / start_date / end_date は CE 期間（lookback_days）から自動注入（-p で上書き可）
cur-analyzer rds_instances -p year=2026 -p month=3
cur-analyzer rds_resource_ids \
  -p instance_type_prefix=db.r8g -p engine="Aurora MySQL"
cur-analyzer ce_factcheck_rds -p year=2026 -p month=3 \
  -p instance_type=db.r6g.large -p region=ap-northeast-1 -p engine=Aurora

# カスタム SQL ファイルを実行（{{ variable }} 形式で変数埋め込み可）
cur-analyzer ./queries/my_query.sql -p year=2026 -p month=3

# リソース別の稼働時間・エンジン確認（queries/ 配下のカスタム SQL）
cur-analyzer queries/resource_uptime.sql -p resource_id=my-instance-00
cur-analyzer queries/resource_engine_check.sql -p resource_id=my-instance-00

# インスタンスタイプ変更調査（サービス・使用タイプ・エンジン列の指定が必要）
cur-analyzer queries/resource_type_changes.sql \
  -p product_code=AmazonRDS \
  -p usage_type_pattern=%InstanceUsage% \
  -p engine_col=product_database_engine
cur-analyzer queries/resource_latest_type.sql \
  -p product_code=AmazonRDS \
  -p usage_type_pattern=%InstanceUsage% \
  -p engine_col=product_database_engine

# 出力フォーマット指定（パイプや他ツールへの連携に便利）
cur-analyzer rds_instances -p year=2026 -p month=3 --format csv > out.csv
cur-analyzer rds_instances -p year=2026 -p month=3 --format json

# サイズ閾値・表示行数の調整
cur-analyzer rds_instances -p year=2026 -p month=3 --limit-mb 50 --head 20

# キャッシュ制御
cur-analyzer rds_instances --refresh   # キャッシュ削除して再実行
cur-analyzer rds_instances --no-cache  # キャッシュを使わず毎回実行
```

> **注意**: CUR のパーティション `month` はゼロ埋めなし（`'3'` / `'12'`）。
> `-p month=03` と渡されても自動的に `'3'` に正規化する。

### カスタムクエリの書き方

任意の SQL ファイルを作成して `cur-analyzer` で実行できます。

#### ファイルの置き場所

| 場所 | `--list` への表示 | 用途 |
|---|---|---|
| `queries/*.sql` | `[カスタムクエリ]` として表示 | 繰り返し使うアドホック分析 |
| 任意の `.sql` ファイル | 表示されない | 一時的な調査など |

`queries/templates/` 配下は組み込みテンプレートの置き場所。ユーザーが作成するカスタムクエリは `queries/` 直下に置くことを推奨。

#### テンプレート変数

SQL 内で `{{ 変数名 }}` と書くと `-p` で渡した値に置換されます。

```sql
-- my_query.sql の先頭コメントが --list の説明として使われる
SELECT *
FROM {{ database }}.{{ table }}
WHERE year  = '{{ year }}'
  AND month = '{{ month }}'
  AND line_item_usage_start_date >= TIMESTAMP '{{ start_date }} 00:00:00'
  AND line_item_usage_start_date <  TIMESTAMP '{{ end_date }} 00:00:00'
  AND line_item_resource_id = '{{ resource_id }}'
```

#### 自動注入される変数

以下の変数は `-p` で指定しなくても config.yaml と CE 期間から自動的に設定されます（`-p` で上書き可）。

| 変数 | 自動値 | 説明 |
|---|---|---|
| `database` | `athena.database`（config.yaml） | CUR テーブルの Glue データベース名 |
| `table` | `athena.table`（config.yaml） | CUR テーブル名 |
| `year` | CE 期間の年（lookback_days から計算） | CUR パーティションの年 |
| `month` | CE 期間の月（lookback_days から計算） | CUR パーティションの月（ゼロ埋めなし） |
| `start_date` | CE period start（YYYY-MM-DD） | 日付フィルタの開始日 |
| `end_date` | CE period end（YYYY-MM-DD） | 日付フィルタの終了日 |

> `year` / `month` を明示指定する場合は **両方** 指定してください。片方だけはエラーになります。

#### 実行例

```bash
# queries/ 直下に置いたカスタムクエリ（--list にも表示される）
cur-analyzer queries/my_query.sql -p resource_id=my-db-instance

# 任意の場所の SQL ファイル
cur-analyzer /tmp/check.sql -p year=2026 -p month=3

# 自動注入変数を上書きして別テーブルを参照する
cur-analyzer queries/my_query.sql -p database=other_db -p table=other_table
```

### CUR vs CE カバレッジ検証（`compare_cur_ce.py`）

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

## キャッシュ

| 種別 | 保存先 | デフォルト TTL | 設定キー |
|---|---|---|---|
| AWS API レスポンス | `~/.cache/ri-analyzer/*.pkl` | 24 時間 | `analysis.cache_ttl_hours` |
| Athena スキーマ | `~/.cache/ri-analyzer/athena_schema_*.json` | 168 時間 | `athena.schema_cache_ttl_hours` |
| Athena クエリ結果 | `~/.cache/ri-analyzer/query_results/{sql_hash}.csv` | 24 時間 | `athena.query_cache_ttl_hours` |

Athena クエリ結果のキャッシュキーは **実体化された SQL 文字列の SHA256 ハッシュ（先頭 16 文字）**。
同じテンプレート・同じパラメータなら 2 回目以降は Athena を呼ばずにローカルから返す。
`--no-cache` / `--refresh` でバイパス可能。

詳細な仕様・設計については [SPEC.md](SPEC.md) を参照。
