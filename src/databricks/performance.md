# パフォーマンス最適化

## 基本原則

```
1. まず計測する（explain / Spark UI で確認）
2. 問題を特定する（Shuffle？ Skew？ スキャン量？）
3. 最小限の変更で改善する
```

---

## Spark UI の見方

```python
# 実行計画を確認
df.explain()        # 簡易版
df.explain(True)    # 詳細版（Physical Plan・Logical Plan）
```

**チェックポイント**

| 表示 | 意味 | 対策 |
|------|------|------|
| `Exchange` | Shuffle発生 | Broadcast Joinや再設計 |
| `Sort` | ソート発生 | 必要なければ削除 |
| `FileScan` の行数が多い | フルスキャン | フィルタ・パーティション確認 |
| `SkewedJoin` | データ偏り | Salt・AQEに任せる |

---

## キャッシュ戦略

```python
# 繰り返し使うDataFrameはキャッシュ
df = spark.table("races").filter(F.col("year") == 2024)
df.cache()   # または df.persist()

# 最初のActionで実際にキャッシュされる
df.count()

# キャッシュ解放（メモリ節約）
df.unpersist()
```

**いつキャッシュするか:**
- 同じDataFrameを3回以上使う場合
- 計算コストが高い中間結果
- 大規模なJoinの結果

**キャッシュしないほうがよい場合:**
- 1〜2回しか使わない
- メモリが少ない環境
- ストリーミング処理

---

## Broadcast Join の使い方

```python
from pyspark.sql import functions as F

# 小テーブルをブロードキャスト（Shuffleを回避）
result = large_races.join(
    F.broadcast(small_horse_master),
    on="horse_id",
    how="left"
)

# 自動ブロードキャストの閾値確認
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
# デフォルト: 10MB

# 閾値を変更
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 50 * 1024 * 1024)  # 50MB
```

---

## パーティション最適化

```python
# シャッフル後のパーティション数設定
spark.conf.set("spark.sql.shuffle.partitions", "200")  # デフォルト

# データ量に応じた目安
# 〜1GB   → 8〜16
# 1〜10GB → 100〜200
# 10GB〜  → 200〜400
```

```python
# 書き込み前に最適なパーティション数に調整
df.repartition(10).write.format("delta").save("/path/")

# ファイルサイズが大きすぎる場合は増やす
# ファイルが小さすぎる場合は減らす（スモールファイル問題）
```

---

## スモールファイル問題

Delta Lakeでよく起きる問題。小さいファイルが大量にできてスキャンが遅くなる。

```sql
-- OPTIMIZEでファイルを結合（推奨: 定期実行）
OPTIMIZE races;

-- Z-Orderと組み合わせる
OPTIMIZE races ZORDER BY (race_date, race_course);
```

**自動最適化（Databricksのみ）**

```sql
-- テーブルプロパティで自動最適化を有効化
ALTER TABLE races SET TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

---

## Predicate Pushdown（述語プッシュダウン）

フィルタをできるだけ早い段階（データソース）で適用して、読み込むデータ量を減らす。

```python
# これはDeltaが自動でPredicate Pushdownを適用する
df = spark.read.format("delta").load("/path/") \
    .filter(F.col("race_date") >= "2024-01-01")  # ← 読み込み時点でフィルタ

# explainで確認
df.explain()  # PushedFilters が表示されたらOK
```

---

## よくあるアンチパターン

```python
# NG: collect() で全データをDriverに引き込む
all_data = df.collect()  # メモリ不足の原因

# OK: 必要な件数だけ取る
sample = df.limit(100).collect()

# NG: ループ内でDataFrame操作
for i in range(100):
    df = df.withColumn(f"col_{i}", ...)  # 毎回新しいDFが作られる

# OK: まとめて処理するか、SQLで書く

# NG: UDFを使いすぎる
# OK: 組み込み関数（F.xxx）を使う
```

---

## 試験で問われるポイント

!!! question "よく出る問題"
    Q: `spark.sql.shuffle.partitions` のデフォルト値は？
    
    A: 200

!!! question "よく出る問題"
    Q: Broadcast Joinが有効な条件は？
    
    A: 一方のテーブルが `spark.sql.autoBroadcastJoinThreshold`（デフォルト10MB）以下の場合。

!!! question "よく出る問題"
    Q: スモールファイル問題への対処法は？
    
    A: `OPTIMIZE` コマンドでファイルを結合する。自動最適化プロパティを設定することもできる。
