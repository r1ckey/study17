# Databricks アーキテクチャ

## Sparkの仕組み

```
Driver（司令塔）
  └── Task を分割して配布
  
Executor × N台（作業員）
  └── Task を並列実行
  └── 結果をDriverに返す
```

**重要**: Sparkは「分散処理」が前提。データを複数のマシンに分割して同時に処理する。

---

## RDD → DataFrame → Dataset

| 世代 | 概要 | 現在の使用 |
|------|------|-----------|
| RDD | 低レベルAPI。柔軟だが遅い | 非推奨（特殊用途のみ）|
| DataFrame | 列指向の分散データ構造 | **メインで使う** |
| Dataset | 型安全なDataFrame（Scala/Java）| Pythonでは使わない |

!!! tip "結論"
    PythonのDE業務では **DataFrame API** だけ使えればOK。

---

## Transformations と Actions

Sparkの最重要概念。**遅延評価（Lazy Evaluation）** が鍵。

=== "Transformations（変換）"
    - 新しいDataFrameを返す
    - **実行されない**（定義するだけ）
    - `filter()`, `select()`, `groupBy()`, `join()`, `withColumn()`
    
    ```python
    # この段階では何も実行されない
    df2 = df.filter(F.col("distance") > 2000)
    df3 = df2.select("horse_name", "prize")
    ```

=== "Actions（実行）"
    - 結果を返す or 書き込む
    - **ここで初めて実行される**
    - `show()`, `count()`, `collect()`, `save()`, `write()`
    
    ```python
    # ここで初めてSparkが動く
    df3.show()   # Action → 実行開始
    ```

**なぜ遅延評価か？** → Sparkが最適な実行計画（クエリプラン）を作るため。

---

## Shuffle（シャッフル）

DEが最も意識すべきコンセプト。

```
Shuffle = データを全ノードにまたがって再分配する処理
```

**Shuffleが発生する操作:**
- `groupBy()` → 同じキーを同じノードに集める必要がある
- `join()` → 同じキーを同じノードに集める必要がある
- `orderBy()` → グローバルソートのため全データを並び替える
- `distinct()` → 重複排除のため

**Shuffleが遅い理由:**
- ネットワーク通信が発生する
- ディスクI/Oが発生する

```python
# Shuffleを確認
df.explain()  # ← "Exchange" が出たらShuffleあり
```

---

## パーティション

DataFrameはパーティション（小さいデータの塊）に分割されている。

```python
# パーティション数確認
df.rdd.getNumPartitions()

# 推奨: CPUコア数 × 2〜4 程度
# Databricksのクラスターでは200がデフォルト（spark.sql.shuffle.partitions）
```

```python
# パーティション調整
df.repartition(10)  # Shuffleあり・増減どちらも可
df.coalesce(1)      # Shuffleなし・削減のみ
```

---

## Adaptive Query Execution (AQE)

Spark 3.x以降のデフォルト機能。実行中にクエリプランを最適化する。

```python
# 確認
spark.conf.get("spark.sql.adaptive.enabled")  # true

# AQEが自動でやること
# - Skew Join の自動修正
# - Shuffle パーティション数の自動調整
# - ブロードキャストJoinへの自動変換
```

---

## Broadcast Join

小さいテーブルを全ノードにコピーして、Shuffleを避ける。

```python
from pyspark.sql import functions as F

# 小さいテーブルをブロードキャスト
result = large_df.join(
    F.broadcast(small_df),
    on="horse_id",
    how="left"
)
```

**目安**: 小さいテーブルが数百MB以下ならbroadcastを検討。

---

## Data Skew（データの偏り）

特定のキーにデータが集中すると一部のノードだけ重くなる問題。

```python
# 偏りの確認
df.groupBy("race_course").count().orderBy("count", ascending=False).show()

# 対策例：Salt（ランダムキーを追加して分散）
import random

df_salted = df.withColumn(
    "salt",
    (F.rand() * 10).cast("int")
).withColumn(
    "salted_key",
    F.concat(F.col("skewed_key"), F.lit("_"), F.col("salt"))
)
```

---

## 試験で問われるポイント

!!! question "よく出る問題"
    Q: TransformationsとActionsの違いは？
    
    A: Transformationsは新しいDataFrameを返す操作で実行されない（遅延評価）。
    Actionsは実際に計算を実行し結果を返す or 書き込む操作。

!!! question "よく出る問題"
    Q: Broadcast Joinはどのような場合に有効か？
    
    A: 一方のテーブルが小さい（数百MB以下）場合。Shuffleを回避できるため高速。

!!! question "よく出る問題"
    Q: repartitionとcoalesceの違いは？
    
    A: repartitionはShuffleありで増減両方可能。coalesceはShuffleなしで削減のみ。
