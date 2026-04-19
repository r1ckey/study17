# PySpark 基礎

## PySparkとは

Apache SparkのPython API。大規模データを分散処理するフレームワーク。
Databricksの中核技術。DEとして必須。

```mermaid
graph LR
    PD[pandas\nシングルノード\nメモリ内処理\n〜数GB] --> PS
    PS[PySpark\n分散処理\n複数ノード\nPB規模対応]
    PS --> DB[Databricks\n+ Delta Lake\n+ Auto Loader\n+ MLflow]
    
    style PD fill:#4f46e5,color:#fff
    style PS fill:#10b981,color:#fff
    style DB fill:#f59e0b,color:#fff
```

---

## SparkSession（エントリーポイント）

```python
from pyspark.sql import SparkSession

# Databricksでは自動生成されるため不要
# ローカルで動かす場合のみ必要
spark = SparkSession.builder \
    .appName("MyApp") \
    .master("local[*]") \     # ローカルの全CPUコアを使う
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# Databricksノートブックでは、spark変数が最初から使える
spark.version  # Sparkのバージョン確認
```

---

## DataFrame 基本操作

### データ読み込み

```python
# CSVから読み込み
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv("/path/to/races.csv")

# スキーマを明示（inferSchemaより高速・推奨）
from pyspark.sql.types import *

schema = StructType([
    StructField("race_id",        StringType(),  False),  # nullable=False
    StructField("race_date",      DateType(),    True),
    StructField("race_course",    StringType(),  True),
    StructField("distance",       IntegerType(), True),
    StructField("horse_name",     StringType(),  True),
    StructField("finish_position",IntegerType(), True),
    StructField("prize",          LongType(),    True),
])
df = spark.read.schema(schema).csv("/path/to/races.csv", header=True)

# JSON
df = spark.read.json("/path/to/races.json")

# Parquet / Delta（推奨）
df = spark.read.parquet("/path/to/data")
df = spark.read.format("delta").load("/path/to/delta_table")

# テーブルから
df = spark.table("catalog.schema.table_name")

# SQLから
df = spark.sql("SELECT * FROM races WHERE distance > 2000")
```

### 基本確認コマンド

```python
df.printSchema()    # スキーマ確認（型・nullable確認）
df.show(5)          # 先頭5行表示
df.show(5, truncate=False)  # 長い文字列も省略しない
df.count()          # 行数（Actionなので注意）
df.columns          # 列名リスト
df.dtypes           # [(列名, 型文字列)]のリスト
df.describe().show()  # 数値列の統計サマリ
df.summary().show()   # count/mean/std/min/max/percentileなど
```

### 列操作

```python
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType

# 列選択（Transformation）
df.select("race_id", "horse_name", "finish_position")
df.select(F.col("race_id"), F.col("prize"))  # F.col()推奨

# 新しい列を追加
df.withColumn("is_winner", F.col("finish_position") == 1)
df.withColumn("prize_million", F.col("prize") / 1_000_000)

# 複数列を同時追加（Spark 3.3+）
df.withColumns({
    "is_winner": F.col("finish_position") == 1,
    "prize_million": F.col("prize") / 1_000_000,
})

# 列名変更
df.withColumnRenamed("finish_position", "rank")

# 型変換
df.withColumn("prize", F.col("prize").cast(IntegerType()))
df.withColumn("prize", F.col("prize").cast("long"))  # 文字列でも指定可

# 列削除
df.drop("unnecessary_column")
df.drop("col1", "col2", "col3")  # 複数同時削除
```

---

## フィルタ・条件

```python
# 単一条件
df.filter(F.col("distance") >= 2000)
df.where(F.col("race_course") == "Tokyo")  # filterのエイリアス

# 複数条件（AND）
df.filter(
    (F.col("distance") >= 2000) &
    (F.col("race_course") == "Tokyo") &
    F.col("prize").isNotNull()
)

# 複数条件（OR）
df.filter(
    (F.col("race_course") == "Tokyo") |
    (F.col("race_course") == "Osaka")
)

# IN条件
df.filter(F.col("race_course").isin(["Tokyo", "Osaka", "Kyoto"]))
df.filter(~F.col("race_course").isin(["Sapporo"]))  # NOT IN

# パターンマッチ
df.filter(F.col("race_name").like("%天皇賞%"))   # LIKE
df.filter(F.col("race_name").rlike("^.*杯$"))   # 正規表現

# NULL処理
df.filter(F.col("prize").isNotNull())
df.filter(F.col("prize").isNull())
df.na.fill(0, subset=["prize"])        # NULLを0で埋める
df.na.fill({"prize": 0, "odds": 1.0}) # 列ごとに異なる値で埋める
df.na.drop(subset=["horse_name"])      # NULLの行を削除
df.na.drop(how="all")                  # 全列がNULLの行のみ削除
```

---

## 集計操作

```python
# グループ集計
df.groupBy("race_course") \
  .agg(
      F.count("race_id").alias("race_count"),
      F.countDistinct("horse_name").alias("unique_horses"),
      F.avg("prize").alias("avg_prize"),
      F.max("prize").alias("max_prize"),
      F.min("distance").alias("min_distance"),
      F.sum("prize").alias("total_prize"),
      F.stddev("prize").alias("stddev_prize"),
  )

# 複数列でグループ
df.groupBy("race_course", "distance") \
  .count() \
  .orderBy("count", ascending=False)

# 条件付き集計（CASE WHEN の合計）
df.groupBy("horse_name").agg(
    F.count("*").alias("total_races"),
    F.sum(F.when(F.col("finish_position") == 1, 1).otherwise(0)).alias("wins"),
    F.sum(F.when(F.col("finish_position") <= 3, 1).otherwise(0)).alias("top3"),
)
```

---

## JOIN

```mermaid
graph LR
    subgraph Join Types
        INNER[inner\n両方に存在する行]
        LEFT[left\n左テーブルの全行\n右は一致分のみ]
        RIGHT[right\n右テーブルの全行]
        FULL[full\n両方の全行]
        SEMI[left semi\n左の一致した行のみ\n右の列は含まない]
        ANTI[left anti\n左の一致しない行\n= NOT IN の代替]
    end
```

```python
# Inner Join（デフォルト）
result = races_df.join(
    horses_df,
    on="horse_id",
    how="inner"
)

# Left Join（キーが異なる場合）
result = races_df.join(
    horses_df,
    on=races_df["horse_id"] == horses_df["id"],
    how="left"
)

# 複合キーのJoin
result = df1.join(
    df2,
    on=["race_id", "horse_name"],
    how="left"
)

# Left Semi Join（EXISTS句の代替）
existing = df1.join(df2, on="race_id", how="left_semi")

# Left Anti Join（NOT EXISTS句の代替）
not_existing = df1.join(df2, on="race_id", how="left_anti")

# Broadcast Join（小テーブルにはこれを使う）
from pyspark.sql import functions as F
result = large_df.join(F.broadcast(small_df), on="key", how="left")
```

---

## よく使うFunctions（F.xxx）

```python
from pyspark.sql import functions as F

# ---- 文字列操作 ----
F.upper(F.col("name"))                              # 大文字
F.lower(F.col("name"))                              # 小文字
F.trim(F.col("name"))                               # 前後の空白除去
F.ltrim(F.col("name"))                              # 左の空白除去
F.concat(F.col("col1"), F.lit("-"), F.col("col2")) # 文字列結合
F.concat_ws("-", F.col("col1"), F.col("col2"))     # 区切り文字付き結合
F.substring(F.col("name"), 1, 3)                   # 部分文字列（1始まり）
F.length(F.col("name"))                            # 文字列長
F.regexp_replace(F.col("name"), "\\s+", "_")       # 正規表現置換
F.split(F.col("path"), "/")                        # 分割（配列を返す）

# ---- 数値 ----
F.round(F.col("odds"), 2)     # 四捨五入
F.floor(F.col("val"))         # 切り捨て
F.ceil(F.col("val"))          # 切り上げ
F.abs(F.col("diff"))          # 絶対値
F.pow(F.col("x"), 2)          # 冪乗
F.sqrt(F.col("x"))            # 平方根
F.log(F.col("x"))             # 自然対数

# ---- 日付 ----
F.current_date()                                     # 今日の日付
F.current_timestamp()                                # 現在のタイムスタンプ
F.to_date(F.col("race_date"), "yyyy-MM-dd")         # 文字列→日付
F.to_timestamp(F.col("dt"), "yyyy-MM-dd HH:mm:ss") # 文字列→タイムスタンプ
F.date_format(F.col("race_date"), "yyyyMM")         # フォーマット変換
F.year(F.col("race_date"))                          # 年
F.month(F.col("race_date"))                         # 月
F.dayofweek(F.col("race_date"))                     # 曜日（1=日曜）
F.datediff(F.col("end_date"), F.col("start_date")) # 日数差
F.add_months(F.col("date"), 1)                      # 月を加算

# ---- 条件分岐 ----
F.when(F.col("finish_position") == 1, "Win") \
 .when(F.col("finish_position") <= 3, "Place") \
 .otherwise("Loss")

F.coalesce(F.col("prize"), F.col("default_prize"), F.lit(0))  # NULL合体

# ---- 配列・マップ ----
F.array(F.col("a"), F.col("b"))     # 配列作成
F.array_contains(F.col("arr"), "v") # 配列に値が含まれるか
F.explode(F.col("array_col"))       # 配列を行に展開
F.collect_list(F.col("val"))        # グループ内の値をリストに
F.collect_set(F.col("val"))         # グループ内の重複なしリスト
```

---

## Window関数（試験頻出）

```python
from pyspark.sql import Window
from pyspark.sql import functions as F

# ウィンドウ定義
horse_window = Window.partitionBy("horse_name").orderBy("race_date")

# 順位関数
df.withColumn("row_num",    F.row_number().over(horse_window))  # 連番（重複なし）
df.withColumn("rank",       F.rank().over(horse_window))        # 順位（重複あり・飛びあり）
df.withColumn("dense_rank", F.dense_rank().over(horse_window))  # 順位（重複あり・連続）

# 前後の値
df.withColumn("prev_prize", F.lag("prize", 1).over(horse_window))   # 1つ前
df.withColumn("next_prize", F.lead("prize", 1).over(horse_window))  # 1つ後

# 累計（フレーム指定）
window_cum = Window \
    .partitionBy("horse_name") \
    .orderBy("race_date") \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

df.withColumn("cumulative_prize", F.sum("prize").over(window_cum))  # 累計賞金

# 移動平均（直近3レース）
window_roll = Window \
    .partitionBy("horse_name") \
    .orderBy("race_date") \
    .rowsBetween(-2, 0)  # 過去2レース + 現在

df.withColumn("avg_3_prize", F.avg("prize").over(window_roll))
```

**実践例: 馬ごとの初レースからの番号を付ける**

```python
# 実務でよく使うパターン
window = Window.partitionBy("horse_name").orderBy("race_date")

result = df.withColumn("race_number", F.row_number().over(window)) \
           .withColumn("prev_finish", F.lag("finish_position", 1).over(window)) \
           .withColumn("is_improving", 
               F.col("finish_position") < F.col("prev_finish"))
```

---

## UDF（ユーザー定義関数）

```python
from pyspark.sql.types import StringType, FloatType
from pyspark.sql import functions as F

# ❌ Python UDF（遅い！Python↔JVM間のシリアライズが発生）
@F.udf(StringType())
def normalize_horse_name(name):
    if name is None:
        return None
    return name.strip().upper()

# 使用
df.withColumn("clean_name", normalize_horse_name(F.col("horse_name")))

# ✓ pandas UDF（Vectorized UDF・高速）
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf(FloatType())
def calculate_win_rate(wins: pd.Series, total: pd.Series) -> pd.Series:
    return wins / total.replace(0, float('nan'))

df.withColumn("win_rate", calculate_win_rate(F.col("wins"), F.col("total")))

# ✓ 可能な限り組み込み関数を使う（最速）
df.withColumn("clean_name", F.trim(F.upper(F.col("horse_name"))))
```

---

## データ書き込み

```python
# Delta形式で保存（推奨）
df.write \
  .format("delta") \
  .mode("overwrite") \     # overwrite / append / ignore / errorIfExists
  .save("/path/to/output")

# テーブルとして保存
df.write \
  .format("delta") \
  .mode("overwrite") \
  .saveAsTable("catalog.schema.races_cleaned")

# パーティション付き保存
df.write \
  .format("delta") \
  .partitionBy("race_year", "race_month") \
  .mode("overwrite") \
  .save("/path/to/output")

# CSV出力（1ファイルに）
df.coalesce(1) \
  .write \
  .option("header", "true") \
  .csv("/path/to/output")
```

---

## 練習問題

<details>
<summary>練習1: 東京競馬場の2000m以上のレースの統計を出してください</summary>

```python
spark.table("races") \
    .filter(
        (F.col("race_course") == "Tokyo") &
        (F.col("distance") >= 2000)
    ) \
    .agg(
        F.avg("prize").alias("avg_prize"),
        F.max("prize").alias("max_prize"),
        F.count("*").alias("race_count"),
        F.countDistinct("horse_name").alias("unique_horses")
    ) \
    .show()
```
</details>

<details>
<summary>練習2: 馬ごとに勝率・連対率・複勝率を計算して上位10頭を表示してください</summary>

```python
df.groupBy("horse_name") \
    .agg(
        F.count("*").alias("total_races"),
        F.sum(F.when(F.col("finish_position") == 1, 1).otherwise(0)).alias("wins"),
        F.sum(F.when(F.col("finish_position") <= 2, 1).otherwise(0)).alias("top2"),
        F.sum(F.when(F.col("finish_position") <= 3, 1).otherwise(0)).alias("top3"),
        F.sum("prize").alias("total_prize"),
    ) \
    .withColumn("win_rate",  F.round(F.col("wins") / F.col("total_races"), 3)) \
    .withColumn("top2_rate", F.round(F.col("top2") / F.col("total_races"), 3)) \
    .withColumn("top3_rate", F.round(F.col("top3") / F.col("total_races"), 3)) \
    .filter(F.col("total_races") >= 10) \  # 出走数10以上に絞る
    .orderBy("win_rate", ascending=False) \
    .limit(10) \
    .show()
```
</details>

<details>
<summary>練習3: 各馬の直近3レースの賞金移動平均を計算してください</summary>

```python
from pyspark.sql import Window

window = Window \
    .partitionBy("horse_name") \
    .orderBy("race_date") \
    .rowsBetween(-2, 0)  # 直近3レース

df.withColumn("moving_avg_prize_3", F.round(F.avg("prize").over(window), 0)) \
  .select("horse_name", "race_date", "prize", "moving_avg_prize_3") \
  .orderBy("horse_name", "race_date") \
  .show()
```
</details>
