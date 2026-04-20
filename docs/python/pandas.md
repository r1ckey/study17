# Pandas 基礎

小〜中規模データの分析に必須。PySparkとの使い分けも重要。

---

## Pandas vs PySpark 使い分け

| 条件 | 選択 |
|------|------|
| データが数百MB以下 | Pandas |
| データがGB〜TB規模 | PySpark |
| ローカル環境 | Pandas |
| Databricks / クラスター | PySpark |
| ML前処理（小データ）| Pandas |
| 本番ETLパイプライン | PySpark |

!!! tip "Databricks での Pandas"
    `pandas on Spark`（`pyspark.pandas`）を使えば
    PySparkの上でPandasライクなAPIが使える。
    ただし本番は通常のPySparkの方が安定している。

---

## DataFrame の基本

```python
import pandas as pd
import numpy as np

# CSVから読み込み
df = pd.read_csv("races.csv", encoding="utf-8")

# Excelから
df = pd.read_excel("races.xlsx", sheet_name="Sheet1")

# JSONから
df = pd.read_json("races.json")

# 辞書から作成
df = pd.DataFrame({
    "race_id": ["R001", "R002", "R003"],
    "race_course": ["Tokyo", "Osaka", "Kyoto"],
    "prize": [10000000, 8000000, 6000000]
})
```

### 基本確認

```python
df.shape          # (行数, 列数)
df.dtypes         # 各列の型
df.head(5)        # 先頭5行
df.tail(5)        # 末尾5行
df.info()         # 概要（型・NULL数等）
df.describe()     # 数値列の統計量
df.isnull().sum() # NULL数の確認
```

---

## データ選択

```python
# 列選択
df["race_course"]              # Series
df[["race_course", "prize"]]   # DataFrame

# 行選択（loc: ラベル、iloc: 番号）
df.loc[0]                      # 0行目（index=0）
df.iloc[0]                     # 先頭行
df.loc[0:3]                    # 0〜3行目
df.iloc[0:3]                   # 先頭3行

# 条件フィルタ
df[df["race_course"] == "Tokyo"]
df[(df["prize"] >= 5000000) & (df["race_course"] == "Tokyo")]
df[df["race_course"].isin(["Tokyo", "Osaka"])]
df[df["horse_name"].str.contains("イクイノックス")]
```

---

## データ加工

```python
# 列の追加・変換
df["prize_m"] = df["prize"] / 1_000_000          # 百万円単位
df["is_grade1"] = df["grade"] == "G1"             # bool列
df["race_year"] = pd.to_datetime(df["race_date"]).dt.year

# 列名変更
df.rename(columns={"finish_position": "rank"}, inplace=True)

# 型変換
df["prize"] = df["prize"].astype(int)
df["race_date"] = pd.to_datetime(df["race_date"])

# NULL処理
df["prize"].fillna(0)                         # 0で埋める
df["horse_name"].fillna("不明")
df.dropna(subset=["race_id", "horse_name"])   # 特定列がNULLの行を削除
df.dropna()                                    # 任意の列がNULLの行を全削除
```

---

## 集計

```python
# groupby
summary = df.groupby("race_course").agg(
    race_count=("race_id", "count"),
    avg_prize=("prize", "mean"),
    max_prize=("prize", "max"),
    total_prize=("prize", "sum")
).reset_index()

# 複数列でgroup
df.groupby(["race_course", "distance"])["prize"].mean()

# pivot_table
pivot = df.pivot_table(
    values="prize",
    index="race_course",
    columns="race_year",
    aggfunc="mean"
)
```

---

## 結合（merge）

```python
# INNER JOIN
result = pd.merge(races_df, horses_df, on="horse_id", how="inner")

# LEFT JOIN
result = pd.merge(races_df, horses_df, on="horse_id", how="left")

# 異なる列名で結合
result = pd.merge(
    races_df,
    horses_df,
    left_on="horse_id",
    right_on="id",
    how="left"
)

# 縦に積む（UNION ALL）
combined = pd.concat([df_2022, df_2023, df_2024], ignore_index=True)
```

---

## apply / map / lambda

```python
# apply: 各行または列に関数を適用
df["distance_cat"] = df["distance"].apply(
    lambda x: "長距離" if x >= 2400
              else "中距離" if x >= 1800
              else "短距離"
)

# 複数列を使う場合
def calc_value(row):
    return row["prize"] / row["distance"]

df["prize_per_m"] = df.apply(calc_value, axis=1)

# map: Series の値を変換
course_map = {"Tokyo": "東京", "Osaka": "大阪", "Kyoto": "京都"}
df["course_jp"] = df["race_course"].map(course_map)
```

---

## 日付操作

```python
# 文字列 → datetime
df["race_date"] = pd.to_datetime(df["race_date"])

# 日付の属性を抽出
df["year"] = df["race_date"].dt.year
df["month"] = df["race_date"].dt.month
df["weekday"] = df["race_date"].dt.dayofweek   # 0=月〜6=日
df["quarter"] = df["race_date"].dt.quarter

# 日付の差分
df["days_since_last_race"] = (
    df.groupby("horse_id")["race_date"]
    .diff()
    .dt.days
)

# 月初・月末
df["month_start"] = df["race_date"].dt.to_period("M").dt.start_time
```

---

## 文字列操作

```python
# str アクセサ
df["horse_name"].str.upper()
df["horse_name"].str.strip()
df["horse_name"].str.contains("イク")
df["horse_name"].str.startswith("ア")
df["horse_name"].str.len()
df["horse_name"].str.replace("（", "(")

# 分割
df[["first", "last"]] = df["full_name"].str.split(" ", expand=True)
```

---

## ウィンドウ操作

```python
# 移動平均
df["prize_ma3"] = (
    df.groupby("horse_name")["prize"]
    .transform(lambda x: x.rolling(3).mean())
)

# 累積和
df["cumulative_prize"] = (
    df.sort_values("race_date")
    .groupby("horse_name")["prize"]
    .cumsum()
)

# ランク
df["rank_in_course"] = (
    df.groupby("race_course")["prize"]
    .rank(ascending=False, method="min")
)
```

---

## データ出力

```python
# CSV
df.to_csv("output.csv", index=False, encoding="utf-8-sig")

# Excel
df.to_excel("output.xlsx", index=False, sheet_name="races")

# Parquet（推奨: 軽い・型保持）
df.to_parquet("output.parquet")

# SQLデータベースへ
from sqlalchemy import create_engine
engine = create_engine("postgresql://user:pass@host/db")
df.to_sql("races", engine, if_exists="replace", index=False)
```

---

## Pandas ↔ PySpark 変換

```python
# PySpark DataFrame → Pandas
pandas_df = spark_df.toPandas()  # 注意: 全データをドライバに集める

# Pandas → PySpark DataFrame
spark_df = spark.createDataFrame(pandas_df)

# 大量データの場合（Pandas on Spark）
import pyspark.pandas as ps
ps_df = ps.from_pandas(pandas_df)  # Spark上で動くPandas
```
