# PySpark 基礎

## PySparkとは

Apache SparkのPython API。大規模データを分散処理するフレームワーク。
Databricksの中核技術。DEとして必須。

---

## SparkSession（エントリーポイント）

```python
from pyspark.sql import SparkSession

# Databricksでは自動生成されるため不要
# ローカルで動かす場合のみ必要
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \
    .getOrCreate()
```

!!! info "Databricksでは"
    ノートブック上では `spark` 変数が自動で使えます。
    `SparkSession.builder` の記述は不要。

---

## DataFrame 基本操作

### データ読み込み

```python
# CSVから読み込み
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/path/to/races.csv")

# Delta Tableから（推奨）
df = spark.read.format("delta").load("/path/to/delta_table")

# SQLでも読める
df = spark.sql("SELECT * FROM races")
```

### 基本確認

```python
df.printSchema()    # スキーマ確認
df.show(5)          # 先頭5行表示
df.count()          # 行数
df.columns          # 列名リスト
df.dtypes           # 型確認
```

### 列操作

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# 列選択
df.select("race_id", "horse_name", "finish_position")

# 新しい列を追加
df.withColumn("is_winner", F.col("finish_position") == 1)

# 列名変更
df.withColumnRenamed("finish_position", "rank")

# 型変換
df.withColumn("prize", F.col("prize").cast(IntegerType()))

# 列削除
df.drop("unnecessary_column")
```

### フィルタ・条件

```python
# 単一条件
df.filter(F.col("distance") >= 2000)
df.where(F.col("race_course") == "Tokyo")

# 複数条件（AND）
df.filter(
    (F.col("distance") >= 2000) &
    (F.col("race_course") == "Tokyo")
)

# 複数条件（OR）
df.filter(
    (F.col("race_course") == "Tokyo") |
    (F.col("race_course") == "Osaka")
)

# IN条件
df.filter(F.col("race_course").isin(["Tokyo", "Osaka", "Kyoto"]))

# NULL処理
df.filter(F.col("prize").isNotNull())
df.na.fill(0, subset=["prize"])     # NULL を 0 で埋める
df.na.drop(subset=["horse_name"])   # NULLの行を削除
```

### 集計

```python
# グループ集計
df.groupBy("race_course") \
  .agg(
      F.count("race_id").alias("race_count"),
      F.avg("prize").alias("avg_prize"),
      F.max("prize").alias("max_prize"),
      F.min("distance").alias("min_distance")
  )

# 複数列でグループ
df.groupBy("race_course", "distance") \
  .count() \
  .orderBy("count", ascending=False)
```

### JOIN

```python
# Inner Join（デフォルト）
result = races_df.join(
    horses_df,
    on="horse_id",
    how="inner"
)

# Left Join
result = races_df.join(
    horses_df,
    on=races_df["horse_id"] == horses_df["id"],
    how="left"
)

# Join種類
# inner, left, right, full, cross, semi, anti
```

---

## よく使うFunctions

```python
from pyspark.sql import functions as F

# 文字列
F.upper(F.col("name"))
F.lower(F.col("name"))
F.trim(F.col("name"))
F.concat(F.col("col1"), F.lit("-"), F.col("col2"))
F.substring(F.col("name"), 1, 3)

# 数値
F.round(F.col("odds"), 2)
F.abs(F.col("diff"))

# 日付
F.current_date()
F.to_date(F.col("race_date"), "yyyy-MM-dd")
F.year(F.col("race_date"))
F.month(F.col("race_date"))
F.datediff(F.col("end_date"), F.col("start_date"))

# 条件分岐（CASE WHEN）
F.when(F.col("finish_position") == 1, "Win") \
 .when(F.col("finish_position") <= 3, "Place") \
 .otherwise("Loss")
```

---

## UDF（ユーザー定義関数）

```python
from pyspark.sql.types import StringType

# UDF定義
def normalize_horse_name(name):
    if name is None:
        return None
    return name.strip().upper()

# UDFとして登録
normalize_udf = F.udf(normalize_horse_name, StringType())

# 使用
df.withColumn("clean_name", normalize_udf(F.col("horse_name")))
```

!!! warning "UDFのパフォーマンス注意"
    UDFはPythonとJVM間のシリアライズが発生し遅い。
    組み込み関数（F.xxx）で代替できないか先に検討すること。

---

## データ書き込み

```python
# Delta形式で保存（推奨）
df.write \
  .format("delta") \
  .mode("overwrite") \   # overwrite / append / ignore / errorIfExists
  .save("/path/to/output")

# テーブルとして保存
df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("default.races_cleaned")

# パーティション付き保存
df.write \
  .format("delta") \
  .partitionBy("race_year", "race_month") \
  .mode("overwrite") \
  .save("/path/to/output")
```

---

## パフォーマンスの基本

```python
# キャッシュ（繰り返し使うDFはキャッシュ）
df.cache()
df.persist()

# パーティション確認・調整
df.rdd.getNumPartitions()   # 現在のパーティション数
df.repartition(10)           # パーティション数を指定
df.coalesce(1)               # 削減（シャッフルなし）

# 実行計画確認
df.explain()
df.explain(True)  # 詳細
```

---

## 練習問題

!!! question "練習1"
    `races` テーブルから東京競馬場の2000m以上のレースを抽出し、
    賞金の平均・最大・件数を計算してください。

??? success "解答"
    ```python
    spark.sql("SELECT * FROM races") \
        .filter(
            (F.col("race_course") == "Tokyo") &
            (F.col("distance") >= 2000)
        ) \
        .agg(
            F.avg("prize").alias("avg_prize"),
            F.max("prize").alias("max_prize"),
            F.count("*").alias("race_count")
        ) \
        .show()
    ```

!!! question "練習2"
    馬ごとに勝率（1着回数 ÷ 出走回数）を計算して上位10頭を表示してください。

??? success "解答"
    ```python
    df.groupBy("horse_name") \
        .agg(
            F.count("*").alias("total_races"),
            F.sum(F.when(F.col("finish_position") == 1, 1).otherwise(0)).alias("wins")
        ) \
        .withColumn("win_rate", F.round(F.col("wins") / F.col("total_races"), 3)) \
        .orderBy("win_rate", ascending=False) \
        .limit(10) \
        .show()
    ```
