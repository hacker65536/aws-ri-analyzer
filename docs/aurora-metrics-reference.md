# Aurora メトリクスリファレンス

AWS 公式ドキュメント [metrics-reference](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/metrics-reference.html) を元に、有用なメトリクスをカテゴリ別にまとめたもの。

取得手段は 3 種類。現在の `aurora-qps.py` が使っているものを **★** で示す。

| 取得手段 | API / クライアント | ディメンション | 粒度 |
|---|---|---|---|
| **CloudWatch** `AWS/RDS` | `cloudwatch.get_metric_data` | インスタンス or クラスター | 最小 1 分 |
| **Performance Insights** カウンター | `pi.get_resource_metrics` | インスタンス（`DbiResourceId`） | 最小 1 秒 |
| **PI → CloudWatch** 連携メトリクス | `cloudwatch.get_metric_data` | インスタンス | 最小 1 分 |
| Enhanced Monitoring (OS) | CloudWatch Logs（JSON） | インスタンス | 最小 1 秒 |

---

## 1. CloudWatch — `AWS/RDS` 名前空間

### 1-1. クラスターレベルメトリクス

ディメンション: `DBClusterIdentifier`

| メトリクス名 | 説明 | 単位 | エンジン |
|---|---|---|---|
| `VolumeBytesUsed` | クラスター全体のストレージ使用量 | Bytes | MySQL / PG |
| `VolumeReadIOPs` | 課金対象の読み取り I/O（5 分間隔） | Count/5min | MySQL / PG |
| `VolumeWriteIOPs` | 書き込み I/O（5 分間隔） | Count/5min | MySQL / PG |
| `AuroraVolumeBytesLeftTotal` | クラスターボリュームの残余領域 | Bytes | MySQL |
| `AuroraReplicaLagMaximum` | プライマリ〜最大ラグレプリカの遅延 | ms | MySQL / PG |
| `AuroraReplicaLagMinimum` | プライマリ〜最小ラグレプリカの遅延 | ms | MySQL / PG |
| `AuroraGlobalDBReplicationLag` | Global Database のレプリケーション遅延 | ms | MySQL / PG |
| `ServerlessDatabaseCapacity` | Aurora Serverless の現在キャパシティ | ACU | MySQL / PG |

### 1-2. インスタンスレベル — クエリ・スループット

ディメンション: `DBInstanceIdentifier`

| メトリクス名 | 説明 | 単位 | エンジン | 備考 |
|---|---|---|---|---|
| **`Queries` ★** | 実行クエリ数/秒（全ステートメント） | Count/sec | MySQL | QPS 取得に使用中 |
| `SelectThroughput` | SELECT クエリ数/秒 | Count/sec | MySQL | 種別分解に有用 |
| `DMLThroughput` | INSERT + UPDATE + DELETE 数/秒 | Count/sec | MySQL | 書き込み負荷把握 |
| `DDLThroughput` | DDL 文（CREATE/ALTER/DROP）数/秒 | Count/sec | MySQL | |
| `CommitThroughput` | コミット数/秒 | Count/sec | MySQL / PG | |
| `ActiveTransactions` | 同時実行トランザクション数/秒 | Count/sec | MySQL | |
| `BlockedTransactions` | ブロックされたトランザクション数/秒 | Count/sec | MySQL | ロック競合の指標 |

### 1-3. インスタンスレベル — レイテンシ

| メトリクス名 | 説明 | 単位 | エンジン |
|---|---|---|---|
| `SelectLatency` | SELECT の平均応答時間 | ms | MySQL |
| `InsertLatency` | INSERT の平均応答時間 | ms | MySQL |
| `UpdateLatency` | UPDATE の平均応答時間 | ms | MySQL |
| `DeleteLatency` | DELETE の平均応答時間 | ms | MySQL |
| `DMLLatency` | DML 全体の平均応答時間 | ms | MySQL |
| `CommitLatency` | COMMIT の平均応答時間 | ms | MySQL / PG |
| `DDLLatency` | DDL の平均応答時間 | ms | MySQL |

### 1-4. インスタンスレベル — 接続

| メトリクス名 | 説明 | 単位 | エンジン |
|---|---|---|---|
| `DatabaseConnections` | 接続中クライアント数 | Count | MySQL / PG |
| `ConnectionAttempts` | 接続試行数（成否問わず） | Count | MySQL |
| `AbortedClients` | 不正切断されたクライアント数 | Count | MySQL |
| `LoginFailures` | ログイン失敗数/秒 | Count/sec | MySQL |

### 1-5. インスタンスレベル — リソース（CPU / メモリ / ネットワーク）

| メトリクス名 | 説明 | 単位 | エンジン |
|---|---|---|---|
| `CPUUtilization` | CPU 使用率 | % | MySQL / PG |
| `FreeableMemory` | 利用可能な RAM | Bytes | MySQL / PG |
| `SwapUsage` | スワップ使用量 | Bytes | MySQL / PG |
| `NetworkReceiveThroughput` | クライアントからの受信スループット | Bytes/sec | MySQL / PG |
| `NetworkTransmitThroughput` | クライアントへの送信スループット | Bytes/sec | MySQL / PG |
| `StorageNetworkReceiveThroughput` | Aurora ストレージからの受信スループット | Bytes/sec | MySQL / PG |

### 1-6. インスタンスレベル — I/O・キャッシュ・ロック

| メトリクス名 | 説明 | 単位 | エンジン |
|---|---|---|---|
| `ReadIOPS` | ディスク読み取り I/O 数/秒 | Count/sec | MySQL / PG |
| `WriteIOPS` | Aurora ストレージ書き込みレコード数/秒 | Count/sec | MySQL / PG |
| `BufferCacheHitRatio` | バッファキャッシュヒット率 | % | MySQL / PG |
| `ResultSetCacheHitRatio` | Resultset キャッシュヒット率 | % | MySQL v2 |
| `Deadlocks` | デッドロック数/秒 | Count/sec | MySQL / PG |
| `RowLockTime` | InnoDB 行ロック取得待ち時間 | ms | MySQL |
| `AuroraReplicaLag` | このレプリカのラグ | ms | MySQL / PG |

### 1-7. インスタンスレベル — OOM 管理（MySQL 3.06.1+）

| メトリクス名 | 説明 | 単位 |
|---|---|---|
| `AuroraMemoryHealthState` | メモリ健全性（0=NORMAL, 10=RESERVED） | Gauge |
| `AuroraMemoryNumDeclinedSqlTotal` | OOM 回避で拒否されたクエリ数（累計） | Count |
| `AuroraMemoryNumKillConnTotal` | OOM 回避で切断された接続数（累計） | Count |
| `AuroraMemoryNumKillQueryTotal` | OOM 回避で強制終了されたクエリ数（累計） | Count |

---

## 2. Performance Insights カウンターメトリクス

API: `pi.get_resource_metrics`  
ディメンション: `DbiResourceId`（インスタンス固有 ID）  
メトリクス名の末尾に `.avg` / `.sum` / `.max` を付けて取得する。

### 2-1. Aurora MySQL

#### SQL アクティビティ

| PI メトリクス名 | 説明 | 単位 | 備考 |
|---|---|---|---|
| **`db.SQL.Queries.sum` ★** | 全クエリ実行数（per period） | Count | ÷ period_sec = QPS（取得中） |
| `db.SQL.Questions.sum` | クライアント送信ステートメント数 | Count | stored proc 内を除く |
| `db.SQL.Com_select.sum` | SELECT 実行数 | Count | |
| `db.SQL.Slow_queries.sum` | スロークエリ数 | Count | |
| `db.SQL.Innodb_rows_read.sum` | InnoDB 読み取り行数 | Rows | |
| `db.SQL.Innodb_rows_inserted.sum` | InnoDB 挿入行数 | Rows | |
| `db.SQL.Innodb_rows_updated.sum` | InnoDB 更新行数 | Rows | |
| `db.SQL.Innodb_rows_deleted.sum` | InnoDB 削除行数 | Rows | |
| `db.SQL.innodb_rows_changed.sum` | InnoDB 行変更合計 | Rows | |

#### キャッシュ

| PI メトリクス名 | 説明 | 単位 |
|---|---|---|
| `db.Cache.Innodb_buffer_pool_read_requests.sum` | バッファプール読み取りリクエスト数 | Pages/sec |
| `db.Cache.Innodb_buffer_pool_reads.sum` | バッファプールからディスク読み取りが必要だった数 | Pages/sec |
| `db.Cache.innoDB_buffer_pool_hit_rate.avg` | バッファプールヒット率 | % |
| `db.Cache.innoDB_buffer_pool_usage.avg` | バッファプール使用率 | % |

#### ロック

| PI メトリクス名 | 説明 | 単位 |
|---|---|---|
| `db.Locks.innodb_row_lock_waits.sum` | 行ロック待ち発生数 | Count |
| `db.Locks.Innodb_row_lock_time.sum` | 行ロック待ち合計時間 | ms |
| `db.Locks.innodb_deadlocks.sum` | デッドロック発生数 | Count |

#### 接続

| PI メトリクス名 | 説明 | 単位 |
|---|---|---|
| `db.Users.Threads_running.avg` | 実行中スレッド数 | Connections |
| `db.Users.Threads_connected.avg` | 接続中スレッド数 | Connections |
| `db.Users.Aborted_connects.sum` | 接続失敗数 | Connections |

### 2-2. Aurora PostgreSQL

#### SQL アクティビティ

| PI メトリクス名 | 説明 | 単位 | 備考 |
|---|---|---|---|
| `db.SQL.tup_fetched.sum` | フェッチされたタプル数 | Tuples/sec | QPS の代替指標 |
| `db.SQL.tup_returned.sum` | 返却されたタプル数 | Tuples/sec | |
| `db.SQL.tup_inserted.sum` | 挿入されたタプル数 | Tuples/sec | |
| `db.SQL.tup_updated.sum` | 更新されたタプル数 | Tuples/sec | |
| `db.SQL.tup_deleted.sum` | 削除されたタプル数 | Tuples/sec | |

#### キャッシュ・I/O

| PI メトリクス名 | 説明 | 単位 |
|---|---|---|
| `db.Cache.blks_hit.sum` | バッファキャッシュヒット数 | Blocks/sec |
| `db.IO.blks_read.sum` | ディスク読み取りブロック数 | Blocks/sec |
| `db.IO.blk_read_time.sum` | ブロック読み取り時間 | ms |

#### トランザクション

| PI メトリクス名 | 説明 | 単位 |
|---|---|---|
| `db.Transactions.xact_commit.sum` | コミット数/秒 | Commits/sec |
| `db.Transactions.xact_rollback.sum` | ロールバック数/秒 | Rollbacks/sec |
| `db.Transactions.active_transactions.avg` | アクティブトランザクション数 | Transactions |

---

## 3. PI が CloudWatch へ発行するメトリクス

API: `cloudwatch.get_metric_data` / Namespace: `AWS/RDS`  
ディメンション: `DBInstanceIdentifier`

| メトリクス名 | 説明 | 用途 |
|---|---|---|
| `DBLoad` | アクティブセッション数（平均） | 全体負荷の把握（DB Load） |
| `DBLoadCPU` | 待機イベントが CPU のアクティブセッション数 | CPU バウンドかどうかの判定 |
| `DBLoadNonCPU` | 待機イベントが CPU 以外のアクティブセッション数 | I/O / ロックバウンドの判定 |

> `DBLoad` は vCPU 数と比較することで飽和度を判断できる（`DBLoadRelativeToNumVCPUs` も利用可）。

---

## 4. Enhanced Monitoring（OS メトリクス）

CloudWatch Logs で JSON として配信（`RDSOSMetrics` ロググループ）。  
Enhanced Monitoring 有効時のみ取得可能。

| カテゴリ | メトリクス名 | 説明 | 単位 |
|---|---|---|---|
| CPU | `cpu.total` | 全 CPU 使用率 | % |
| CPU | `cpu.wait` | I/O 待ちによる CPU ストール率 | % |
| CPU | `cpu.steal` | 他 VM による CPU 使用率（ノイジーネイバー検出）| % |
| メモリ | `memory.free` | 未使用メモリ | KB |
| メモリ | `memory.active` | 使用中メモリ | KB |
| メモリ | `memory.dirty` | ストレージ未書き込みのダーティページ | KB |
| ディスク I/O | `diskIO.readIOsPS` | 読み取り IOPS | ops/sec |
| ディスク I/O | `diskIO.writeIOsPS` | 書き込み IOPS | ops/sec |
| ディスク I/O | `diskIO.await` | I/O リクエスト応答時間（キュー待ち含む）| ms |
| ディスク I/O | `diskIO.util` | ディスク使用率 | % |
| ネットワーク | `network.rx` | 受信スループット | Bytes/sec |
| ネットワーク | `network.tx` | 送信スループット | Bytes/sec |
| 負荷 | `loadAverageMinute.one` | 1 分ロードアベレージ | Count |
| 負荷 | `loadAverageMinute.five` | 5 分ロードアベレージ | Count |

---

## 5. 用途別ピックアップ

### QPS・クエリ活動の把握

| 用途 | 推奨メトリクス | ソース |
|---|---|---|
| 総 QPS（現行）| `Queries` | CloudWatch ★ |
| 総 QPS（高精度）| `db.SQL.Queries.sum` | PI（MySQL のみ）★ |
| SELECT / DML 種別 | `SelectThroughput`, `DMLThroughput` | CloudWatch |
| スロークエリ数 | `db.SQL.Slow_queries.sum` | PI（MySQL）|
| PG の書き込み量 | `db.SQL.tup_inserted/updated/deleted.sum` | PI（PG）|

### レイテンシ分析

| 用途 | 推奨メトリクス | ソース |
|---|---|---|
| クエリ種別レイテンシ | `SelectLatency`, `DMLLatency`, `CommitLatency` | CloudWatch |
| 全体 DB 負荷 | `DBLoad`, `DBLoadCPU`, `DBLoadNonCPU` | CloudWatch（PI 連携）|

### リソース飽和の検出

| 用途 | 推奨メトリクス | ソース |
|---|---|---|
| CPU 逼迫 | `CPUUtilization`, `cpu.wait`, `cpu.steal` | CloudWatch / Enhanced Monitoring |
| メモリ逼迫 | `FreeableMemory`, `AuroraMemoryHealthState` | CloudWatch |
| バッファヒット率低下 | `BufferCacheHitRatio`, `db.Cache.innoDB_buffer_pool_hit_rate` | CloudWatch / PI |
| I/O 飽和 | `ReadIOPS`, `diskIO.await`, `diskIO.util` | CloudWatch / Enhanced Monitoring |
| ロック競合 | `Deadlocks`, `BlockedTransactions`, `db.Locks.innodb_row_lock_waits.sum` | CloudWatch / PI |

### 接続管理

| 用途 | 推奨メトリクス | ソース |
|---|---|---|
| 現在の接続数 | `DatabaseConnections` | CloudWatch |
| 実行中スレッド数 | `db.Users.Threads_running.avg` | PI（MySQL）|
| 接続失敗 | `AbortedClients`, `LoginFailures` | CloudWatch |

### レプリケーション健全性

| 用途 | 推奨メトリクス | ソース |
|---|---|---|
| レプリカ全体のラグ状況 | `AuroraReplicaLagMaximum` / `Minimum` | CloudWatch（クラスターレベル）|
| 個別レプリカのラグ | `AuroraReplicaLag` | CloudWatch（インスタンスレベル）|
| binlog レプリカ遅延 | `AuroraBinlogReplicaLag` | CloudWatch（MySQL）|

---

## 参考リンク

- [CloudWatch metrics for Aurora](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/Aurora.AuroraMonitoring.Metrics.html)
- [PI CloudWatch metrics](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights.Cloudwatch.html)
- [PI counter metrics](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights_Counters.html)
- [Enhanced Monitoring OS metrics](https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_Monitoring-Available-OS-Metrics.html)
