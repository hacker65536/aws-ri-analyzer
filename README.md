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

### オプション一覧

| オプション | 説明 |
|---|---|
| `--service SERVICE [...]` | 対象サービス（rds / elasticache / opensearch）複数指定可 |
| `--section SECTION [...]` | 表示セクション（expiration / coverage / utilization）複数指定可 |
| `--max-util PCT` | 利用率が PCT% 以下のサブスクリプションのみ表示 |
| `--max-coverage PCT` | カバレッジが PCT% 以下のグループのみ表示 |
| `--engine ENGINE [...]` | カバレッジをデータベースエンジンで絞り込み（部分一致・大文字小文字無視）|
| `--family FAMILY [...]` | カバレッジをインスタンスファミリーで絞り込み（例: r6g t4g）|
| `--show-sub-id` | Utilization テーブルに Subscription ID 列を表示 |
| `--no-color` | カラー出力を無効化 |
| `--no-cache` | キャッシュを無視して AWS から再取得 |
| `--config PATH` | 設定ファイルのパス（デフォルト: config.yaml） |

### キャッシュ

AWS API レスポンスは `~/.cache/ri-analyzer/` にキャッシュされる（デフォルト TTL: 24 時間）。
TTL は `config.yaml` の `cache_ttl_hours` で変更可能。`--no-cache` でバイパス可能。

詳細な仕様・設計については [SPEC.md](SPEC.md) を参照。
