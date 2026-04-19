# Databricks Certified Data Engineer Associate

## 試験概要

| 項目 | 内容 |
|------|------|
| 試験時間 | 90分 |
| 問題数 | 45問 |
| 合格ライン | 70%（32問正解） |
| 有効期限 | 2年 |
| 受験料 | $200 |
| 形式 | 多肢選択・複数選択 |

---

## 出題範囲と配点

| セクション | 配点 | 主なトピック |
|-----------|------|------------|
| Databricks Lakehouse Platform | 24% | アーキテクチャ・Delta Lake・Unity Catalog |
| ELT with Apache Spark | 29% | DataFrame API・SQL・変換処理 |
| Incremental Data Processing | 22% | Delta Live Tables・Auto Loader・Structured Streaming |
| Production Pipelines | 16% | Jobs・クラスター・CI/CD |
| Data Governance | 9% | Unity Catalog・アクセス制御 |

---

## セクション1：Lakehouse Platform（24%）

### Lakeouseアーキテクチャ

```
Data Lake  = 安い・柔軟・信頼性低い
DWH        = 高い・高速・硬直的
Lakehouse  = 両方の良いとこ取り（Delta Lakeで実現）
```

### Delta Lakeの特徴

- **ACID**トランザクション
- **スキーマ強制** と **スキーマ進化**
- **Time Travel**（バージョン・タイムスタンプ）
- **OPTIMIZE**（ファイル結合・Z-Order）
- **VACUUM**（古いファイル削除）
- **MERGE**（Upsert）

```sql
-- 頻出コマンド
DESCRIBE HISTORY table_name;
DESCRIBE DETAIL table_name;
OPTIMIZE table_name ZORDER BY (col1, col2);
VACUUM table_name RETAIN 168 HOURS;
```

### Unity Catalog 階層

```
Metastore（1テナント1つ）
└── Catalog
    └── Schema（= Database）
        └── Table / View / Function
```

---

## セクション2：ELT with Spark（29%）

### DataFrame API 必須パターン

```python
from pyspark.sql import functions as F

# 変換系
df.select(), df.filter(), df.withColumn()
df.groupBy().agg(), df.orderBy()
df.join(other, on=, how=)

# 関数
F.col(), F.lit(), F.when().otherwise()
F.count(), F.sum(), F.avg(), F.max(), F.min()
F.to_date(), F.date_format(), F.year(), F.month()
F.upper(), F.lower(), F.trim(), F.concat()
F.isNull(), F.isNotNull(), F.coalesce()
```

### SparkSQL 必須パターン

```sql
-- ウィンドウ関数
SELECT
    horse_name,
    race_date,
    prize,
    ROW_NUMBER() OVER (PARTITION BY horse_name ORDER BY race_date) AS race_num,
    LAG(prize, 1) OVER (PARTITION BY horse_name ORDER BY race_date) AS prev_prize
FROM races;

-- CTEを使った複雑なクエリ
WITH winners AS (
    SELECT horse_name, COUNT(*) AS wins
    FROM races WHERE finish_position = 1
    GROUP BY horse_name
)
SELECT * FROM winners WHERE wins >= 5;
```

### 高階関数（Spark 3.x）

```sql
-- 配列操作
SELECT
    FILTER(array_col, x -> x > 0),        -- 条件でフィルタ
    TRANSFORM(array_col, x -> x * 2),      -- 変換
    AGGREGATE(array_col, 0, (acc, x) -> acc + x)  -- 集計
FROM table;
```

---

## セクション3：Incremental Data Processing（22%）

### Auto Loader

```python
# ファイルが届いたら自動取り込み
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "json") \
    .option("cloudFiles.schemaLocation", "/checkpoint/schema") \
    .load("/path/to/landing/")

df.writeStream \
    .format("delta") \
    .option("checkpointLocation", "/checkpoint/data") \
    .trigger(availableNow=True) \
    .start("/path/to/delta_table")
```

### Structured Streaming

```python
# ストリーミングの読み込み
stream_df = spark.readStream.table("bronze_table")

# ウォーターマーク（遅延データの許容）
stream_df = stream_df \
    .withWatermark("event_time", "10 minutes")

# 書き込み
stream_df.writeStream \
    .format("delta") \
    .outputMode("append")  # append / complete / update
    .trigger(processingTime="1 minute")  # または availableNow=True
    .option("checkpointLocation", "/checkpoint")
    .start()
```

### Delta Live Tables（DLT）

```python
import dlt
from pyspark.sql import functions as F

# Bronzeレイヤー（生データ）
@dlt.table(
    name="raw_races",
    comment="生のレースデータ"
)
def raw_races():
    return spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "json") \
        .load("/landing/races/")

# Silverレイヤー（クレンジング済み）
@dlt.table(
    name="cleaned_races",
    comment="クレンジング済みレースデータ"
)
@dlt.expect("valid_distance", "distance > 0")  # データ品質
@dlt.expect_or_drop("not_null_race_id", "race_id IS NOT NULL")
def cleaned_races():
    return dlt.read_stream("raw_races") \
        .filter(F.col("race_id").isNotNull()) \
        .withColumn("race_date", F.to_date("race_date"))
```

---

## セクション4：Production Pipelines（16%）

### Databricks Jobs

| 設定項目 | ポイント |
|---------|---------|
| Cluster type | Job Cluster（コスト効率）vs All-Purpose |
| Retry | リトライ回数・待機時間の設定 |
| Email notification | 失敗時のアラート |
| Multi-task | タスク間の依存関係設定 |
| Schedule | Cron式での定期実行 |

---

## セクション5：Data Governance（9%）

### Unity Catalog の権限管理

```sql
-- 権限付与
GRANT SELECT ON TABLE catalog.schema.table TO user@example.com;
GRANT ALL PRIVILEGES ON SCHEMA catalog.schema TO group_name;

-- 権限確認
SHOW GRANTS ON TABLE catalog.schema.table;

-- 権限剥奪
REVOKE SELECT ON TABLE catalog.schema.table FROM user@example.com;
```

---

## 頻出問題パターン

!!! question "Q1: Delta LakeのTime Travelでバージョン5を参照するコードは？"
??? success "A1"
    ```python
    df = spark.read.format("delta").option("versionAsOf", 5).load("/path/")
    # または
    df = spark.sql("SELECT * FROM table VERSION AS OF 5")
    ```

!!! question "Q2: Auto LoaderがIncrementalに新しいファイルを検知する仕組みは？"
??? success "A2"
    デフォルトでは **Directory Listing**（ディレクトリを定期スキャン）。
    スケールする場合は **File Notification**（クラウドのイベント通知）に切り替える。

!!! question "Q3: DLTで行が品質チェックに失敗した場合にDropするアノテーションは？"
??? success "A3"
    `@dlt.expect_or_drop("rule_name", "condition")`

!!! question "Q4: StructuredStreamingのOutputModeの違いは？"
??? success "A4"
    - `append`: 新しい行のみ出力（デフォルト）
    - `complete`: 全行を毎回出力（集計に使う）
    - `update`: 変化した行のみ出力

---

## 学習リソース

- Databricks Academy（無料の公式ラーニングパス）
- Databricks Academy（無料の公式ラーニングパス）のノートブックを活用
- 模擬試験：Databricks公式の Practice Exam

!!! tip "合格の目安"
    模擬試験で80%以上安定したら受験。
    70%が合格ラインなので10%のバッファを持つ。
