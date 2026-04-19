# Delta Lake

## Delta Lakeとは

Parquetファイルに **トランザクションログ** を追加したストレージ形式。
Databricksのデフォルトフォーマット。DEとして最重要概念の一つ。

```
Delta Lake = Parquet ファイル + _delta_log（トランザクションログ）
```

---

## なぜDelta Lakeか（ACID保証）

| 特性 | 意味 | Parquetだけだと |
|------|------|----------------|
| **A**tomicity | 全部成功か全部失敗 | 途中で止まるとデータ破損 |
| **C**onsistency | 常に整合性が保たれる | 読み書きが競合する |
| **I**solation | 同時書き込みでも衝突しない | データ競合が起きる |
| **D**urability | コミットしたデータは消えない | 障害時にデータ消失 |

---

## 基本操作

### テーブル作成・書き込み

```python
# DataFrameからDeltaテーブルを作成
df.write \
  .format("delta") \
  .mode("overwrite") \
  .save("/path/to/delta_table")

# テーブルとして登録（Unity Catalog）
df.write \
  .format("delta") \
  .saveAsTable("catalog.schema.table_name")
```

```sql
-- SQLでも作れる
CREATE TABLE races (
    race_id     STRING,
    race_date   DATE,
    race_course STRING,
    distance    INT,
    prize       BIGINT
)
USING DELTA
LOCATION '/path/to/delta_table';
```

### 読み込み

```python
# パスから読む
df = spark.read.format("delta").load("/path/to/delta_table")

# テーブル名から読む
df = spark.table("catalog.schema.table_name")

# SQLから
df = spark.sql("SELECT * FROM catalog.schema.table_name")
```

### 更新・削除

```sql
-- UPDATE
UPDATE races
SET prize = prize * 1.1
WHERE race_course = 'Tokyo';

-- DELETE
DELETE FROM races
WHERE race_date < '2020-01-01';

-- MERGE（Upsert）
MERGE INTO races AS target
USING new_races AS source
ON target.race_id = source.race_id
WHEN MATCHED THEN
  UPDATE SET *
WHEN NOT MATCHED THEN
  INSERT *;
```

---

## Time Travel（タイムトラベル）

Delta Lakeの強力な機能。過去の状態のデータを参照できる。

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
```

### 変更履歴の確認

```sql
-- トランザクションログ確認
DESCRIBE HISTORY races;
```

---

## Z-Ordering（データスキッピング最適化）

よく絞り込む列でデータを物理的に並び替えて、読み込みを高速化。

```sql
-- Z-Orderの適用
OPTIMIZE races ZORDER BY (race_course, race_date);
```

**いつ使うか:** 
- カーディナリティが高い列（日付・ID系）
- WHERE句で頻繁に使う列
- 最大4列まで

---

## VACUUM（ストレージクリーンアップ）

Time Travelで不要になった古いファイルを削除する。

```sql
-- デフォルト7日以前のファイルを削除
VACUUM races;

-- 保持期間を指定（本番では7日以上推奨）
VACUUM races RETAIN 168 HOURS;
```

!!! warning "注意"
    VACUUMすると、その時点より前のTime Travelができなくなる。
    本番では保持期間を慎重に設定すること。

---

## スキーマ管理

```python
# スキーマ変更を許可して書き込み
df.write \
  .format("delta") \
  .option("mergeSchema", "true") \
  .mode("append") \
  .save("/path/to/delta_table")

# スキーマ強制（デフォルト）
# スキーマが合わない場合はエラーになる
```

---

## Liquid Clustering（新機能）

Z-Orderの後継。パーティションとZ-Orderを置き換える新しい最適化。

```sql
-- Liquid Clusteringを有効にしたテーブル作成
CREATE TABLE races
CLUSTER BY (race_course, race_date)
AS SELECT * FROM source_table;

-- クラスタリングの実行
OPTIMIZE races;
```

!!! tip "Z-Order vs Liquid Clustering"
    新規テーブルはLiquid Clusteringを検討。
    既存テーブルはZ-Orderのままで問題なし。

---

## 試験で問われるポイント

!!! question "よく出る問題"
    Q: Delta Lakeのトランザクションログはどこに保存されるか？
    
    A: `_delta_log` ディレクトリにJSONファイルとして保存される。

!!! question "よく出る問題"
    Q: Time Travelでバージョン0を参照するコードは？
    
    A: `.option("versionAsOf", 0)` を使う。

!!! question "よく出る問題"
    Q: VACUUMの注意点は？
    
    A: VACUUMした時点より前のTime Travelができなくなる。
    デフォルト保持期間は7日（168時間）。

!!! question "よく出る問題"
    Q: Z-Orderに適した列は？
    
    A: カーディナリティが高くWHERE句で頻繁に使う列。最大4列まで。
