# Delta Lake

## Delta Lakeとは

Parquetファイルに **トランザクションログ** を追加したストレージ形式。
Databricksのデフォルトフォーマット。DEとして最重要概念の一つ。

```mermaid
graph LR
    subgraph Delta Lake の構造
        PQ[Parquet Files\n実際のデータ\n.parquet]
        LOG[_delta_log/\nトランザクションログ\n.json / .checkpoint.parquet]
    end
    
    W[書き込み操作] --> LOG
    LOG --> PQ
    R[読み込み操作] --> LOG
    LOG -->|最新バージョンを特定| PQ
    
    style LOG fill:#4f46e5,color:#fff
```

---

## ACID特性（なぜDelta Lakeか）

```mermaid
graph TD
    ACID[ACID保証]
    A[Atomicity\n原子性\n全部成功か全部失敗\n途中で止まってもロールバック]
    C[Consistency\n一貫性\n常に整合性が保たれる\n不整合な状態にならない]
    I[Isolation\n分離性\n同時書き込みでも衝突しない\n楽観的並行性制御]
    D[Durability\n耐久性\nコミットしたデータは消えない\n障害時も保護]
    
    ACID --> A
    ACID --> C
    ACID --> I
    ACID --> D
    
    style ACID fill:#4f46e5,color:#fff
    style A fill:#10b981,color:#fff
    style C fill:#10b981,color:#fff
    style I fill:#10b981,color:#fff
    style D fill:#10b981,color:#fff
```

| 特性 | Parquetだけだと | Delta Lakeなら |
|------|----------------|---------------|
| **Atomicity** | 途中で止まるとデータ破損 | ロールバック可能 |
| **Consistency** | 読み書きが競合する | 常に整合性が保たれる |
| **Isolation** | データ競合が起きる | 楽観的並行性制御 |
| **Durability** | 障害時にデータ消失 | コミット済みデータは保護 |

---

## 基本操作（CRUD）

### テーブル作成・書き込み

```python
# DataFrameからDeltaテーブルを作成
df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/path/to/delta_table")

# テーブルとして登録（Unity Catalog推奨）
df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("catalog.schema.table_name")

# 書き込みモードの使い分け
# overwrite  → テーブル全体を置き換える（注意！）
# append     → データを追加する
# ignore     → テーブルが存在する場合は何もしない
# errorIfExists → テーブルが存在する場合はエラー（デフォルト）
```

```sql
-- SQLでテーブル作成
CREATE TABLE IF NOT EXISTS races (
    race_id     STRING NOT NULL,
    race_date   DATE,
    race_course STRING,
    distance    INT,
    prize       BIGINT
)
USING DELTA
LOCATION '/path/to/delta_table'
PARTITIONED BY (race_date);  -- パーティション（オプション）
```

### 読み込み

```python
# パスから読む
df = spark.read.format("delta").load("/path/to/delta_table")

# テーブル名から読む（推奨）
df = spark.table("catalog.schema.table_name")

# SQLから
df = spark.sql("SELECT * FROM catalog.schema.table_name")
```

### 更新・削除・MERGE（重要！）

```sql
-- UPDATE（特定行の更新）
UPDATE races
SET prize = prize * 1.1
WHERE race_course = 'Tokyo';

-- DELETE（特定行の削除）
DELETE FROM races
WHERE race_date < '2020-01-01';

-- MERGE（Upsert = 存在すれば更新、なければ挿入）
MERGE INTO races AS target
USING new_races AS source
ON target.race_id = source.race_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *
WHEN NOT MATCHED BY SOURCE THEN
  DELETE;  -- ソースにない行を削除（SCD Type 1の実装に使う）
```

```python
# Pythonからのmerge（DeltaTable API）
from delta.tables import DeltaTable

target = DeltaTable.forName(spark, "catalog.schema.races")
target.alias("target").merge(
    source=new_df.alias("source"),
    condition="target.race_id = source.race_id"
).whenMatchedUpdateAll() \
 .whenNotMatchedInsertAll() \
 .execute()
```

---

## Time Travel（タイムトラベル）

```mermaid
graph LR
    V0["Version 0\n2024-01-01\n初期データ\n1000行"] 
    V1["Version 1\n2024-01-05\nINSERT 200行"] 
    V2["Version 2\n2024-01-10\nUPDATE 50行"] 
    V3["Version 3\n2024-01-15\nDELETE 100行"] 
    V4["Version 4\n2024-01-20\n現在"]
    
    V0 --> V1 --> V2 --> V3 --> V4
    
    TV["TIME TRAVEL\nどの時点のデータも参照可能"] -.->|VERSION AS OF 1| V1
    TV -.->|TIMESTAMP AS OF '2024-01-10'| V2
    
    style TV fill:#4f46e5,color:#fff
    style V4 fill:#10b981,color:#fff
```

```python
# バージョン指定
df = spark.read \
    .format("delta") \
    .option("versionAsOf", 0) \
    .load("/path/to/delta_table")

# タイムスタンプ指定
df = spark.read \
    .format("delta") \
    .option("timestampAsOf", "2024-01-01") \
    .load("/path/to/delta_table")
```

```sql
-- SQLでのタイムトラベル
SELECT * FROM races VERSION AS OF 0;
SELECT * FROM races TIMESTAMP AS OF '2024-01-01';

-- 変更履歴の確認（試験頻出！）
DESCRIBE HISTORY races;
-- version, timestamp, operation, operationParameters が返る

-- テーブル詳細
DESCRIBE DETAIL races;
-- location, partitionColumns, numFiles, sizeInBytes が返る
```

**Time Travelの活用場面**:
- 誤ってDELETE/UPDATEした場合の復元
- 過去時点のデータで集計・比較
- データ品質監査

---

## OPTIMIZE と Z-Ordering

### スモールファイル問題

```mermaid
graph LR
    subgraph Before OPTIMIZE
        F1[1MB file]
        F2[500KB file]
        F3[2MB file]
        F4[300KB file]
        F5[1.5MB file]
        F6[800KB file]
    end
    
    OPT[OPTIMIZE\nファイルを結合]
    
    subgraph After OPTIMIZE
        G1[128MB file]
        G2[128MB file]
    end
    
    F1 --> OPT
    F2 --> OPT
    F3 --> OPT
    F4 --> OPT
    F5 --> OPT
    F6 --> OPT
    OPT --> G1
    OPT --> G2
    
    style OPT fill:#10b981,color:#fff
```

```sql
-- OPTIMIZEでファイルを結合（定期実行推奨）
OPTIMIZE races;

-- Z-Orderと組み合わせる（読み込み最適化）
OPTIMIZE races ZORDER BY (race_course, race_date);
```

### Z-Ordering（データスキッピング）

```mermaid
graph LR
    subgraph Without Z-Order
        S1[Scan all files\n全ファイルスキャン]
    end
    
    subgraph With Z-Order on race_course
        Z1[File 1\nTokyo, Osaka]
        Z2[File 2\nKyoto, Nakayama]
        Z3[File 3\nSapporo, Hakodate]
        WHERE["WHERE race_course = 'Tokyo'\n→ File 1のみスキャン！"]
        WHERE -->|skip| Z2
        WHERE -->|skip| Z3
        WHERE --> Z1
    end
    
    style WHERE fill:#10b981,color:#fff
```

**Z-Orderの使い分け**:
| 適切な列 | 不適切な列 |
|---------|-----------|
| カーディナリティが高い（日付・ID）| ブール値（true/false のみ）|
| WHERE句で頻繁に使う | 滅多に使わない |
| 最大4列まで | 5列以上は効果が薄れる |

---

## VACUUM（ストレージクリーンアップ）

```mermaid
graph LR
    TL[Time Travel\nで保持している\n古いファイル]
    VACUUM[VACUUM\n古いファイルを削除]
    SPACE[ストレージ節約\n✓]
    NOTT[Time Travelは\n制限される\n⚠️]
    
    TL --> VACUUM
    VACUUM --> SPACE
    VACUUM --> NOTT
    
    style VACUUM fill:#ef4444,color:#fff
    style SPACE fill:#10b981,color:#fff
    style NOTT fill:#f59e0b,color:#fff
```

```sql
-- デフォルト7日（168時間）以前のファイルを削除
VACUUM races;

-- 保持期間を指定（本番では7日以上推奨）
VACUUM races RETAIN 168 HOURS;

-- 削除されるファイルを確認（dry run）
VACUUM races RETAIN 168 HOURS DRY RUN;
```

> **重要**: VACUUMすると、その時点より前のTime Travelができなくなる。
> 本番では保持期間を慎重に設定すること。デフォルト7日は基本変えない。

---

## スキーマ管理（Schema Enforcement / Evolution）

```python
# デフォルト：スキーマ強制（Schema Enforcement）
# スキーマが合わない場合はエラーになる
df.write.format("delta").mode("append").save("/path/")  # スキーマ不一致 → エラー

# スキーマ進化を許可（Schema Evolution）
df.write \
  .format("delta") \
  .option("mergeSchema", "true") \
  .mode("append") \
  .save("/path/")

# スキーマ上書き（危険！使用注意）
df.write \
  .format("delta") \
  .option("overwriteSchema", "true") \
  .mode("overwrite") \
  .save("/path/")
```

**Schema Enforcementとは**:
- 既存のスキーマと一致しないデータを書き込もうとするとエラー
- データ品質を保護する重要な機能
- `mergeSchema=true` で新しい列の追加は許可できる

---

## Liquid Clustering（新機能 - Z-Orderの後継）

```sql
-- Liquid Clusteringを有効にしたテーブル作成
CREATE TABLE races
CLUSTER BY (race_course, race_date)
AS SELECT * FROM source_table;

-- 既存テーブルに適用
ALTER TABLE races CLUSTER BY (race_course, race_date);

-- クラスタリングの実行（OPTIMIZE と同じコマンド）
OPTIMIZE races;
```

| 比較 | Z-Order | Liquid Clustering |
|------|---------|------------------|
| 対象 | 既存テーブル向け | 新規テーブル推奨 |
| 変更 | OPTIMIZEのたびに指定 | 一度設定すれば自動 |
| 列数制限 | 最大4列 | 制限なし |
| パーティション | 別途設定 | 不要（代替する）|

---

## 試験で問われるポイント

**Q: Delta Lakeのトランザクションログはどこに保存されるか？**
> `_delta_log` ディレクトリにJSONファイルとして保存される（チェックポイントは.parquet）。

**Q: Time Travelでバージョン0を参照するコードは？**
> `.option("versionAsOf", 0)` を使う。SQLでは `VERSION AS OF 0`。

**Q: VACUUMの注意点は？**
> VACUUMした時点より前のTime Travelができなくなる。デフォルト保持期間は7日（168時間）。

**Q: Z-Orderに適した列は？**
> カーディナリティが高くWHERE句で頻繁に使う列。最大4列まで。

**Q: MERGE文はどのような処理に使うか？**
> Upsert（存在すれば更新・なければ挿入）。CDC（Change Data Capture）の取り込みに使う。

**Q: Schema EnforcementとSchema Evolutionの違いは？**
> Enforcementはスキーマ不一致をエラーにする（デフォルト）。Evolutionは`mergeSchema=true`で新列追加を許可する。
