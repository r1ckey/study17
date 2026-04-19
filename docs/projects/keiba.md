# 競馬研究プロジェクト

## プロジェクト概要

習得したDEスキルの実践場。ポートフォリオの核。

```
DE（パイプライン）× BI（可視化）× 統計（分析・予測）
```

---

## 技術スタック

| レイヤー | 技術 | 役割 |
|---------|------|------|
| 収集 | Python（requests）| JRAサイト・APIからデータ取得 |
| 蓄積 | Delta Lake on Databricks | 生データの保管 |
| 変換 | PySpark + dbt | データクレンジング・特徴量作成 |
| 管理 | Airflow（将来）| パイプラインスケジュール |
| 可視化 | Power BI / Streamlit | ダッシュボード |
| 分析 | 統計・ML（将来）| 予測モデル |

---

## データアーキテクチャ（Medalion Architecture）

```
Bronze（生データ）
  └── JRAから取得したそのままのデータ
  └── スキーマ: race_id, race_date, race_course, horse_name, ...

Silver（クレンジング済み）
  └── 型変換・欠損処理・正規化
  └── dbtで管理

Gold（集計・分析用）
  └── 馬別成績・コース別傾向・騎手別分析
  └── BI直結テーブル
```

---

## フェーズ別実装計画

### フェーズ1：データ収集（Python習得後）

```python
import requests
from bs4 import BeautifulSoup
import time

def fetch_race_results(race_date: str) -> list[dict]:
    """指定日のレース結果を取得"""
    # URLは実際のデータソースに応じて設定
    url = f"https://example.com/races/{race_date}"
    
    headers = {"User-Agent": "Mozilla/5.0"}
    response = requests.get(url, headers=headers)
    
    # レート制限（サーバーに負荷をかけない）
    time.sleep(1)
    
    # HTMLパース
    soup = BeautifulSoup(response.text, "html.parser")
    results = []
    # ... パース処理
    
    return results
```

### フェーズ2：Delta Lake蓄積（Databricks習得後）

```python
from pyspark.sql import functions as F
from pyspark.sql.types import *
from datetime import datetime

def save_to_bronze(race_data: list[dict], spark) -> None:
    """生データをBronzeレイヤーに保存"""
    schema = StructType([
        StructField("race_id", StringType(), False),
        StructField("race_date", StringType(), True),
        StructField("race_course", StringType(), True),
        StructField("race_name", StringType(), True),
        StructField("distance", IntegerType(), True),
        StructField("horse_name", StringType(), True),
        StructField("finish_position", IntegerType(), True),
        StructField("odds", DoubleType(), True),
        StructField("prize", LongType(), True),
        StructField("ingested_at", TimestampType(), True),
    ])
    
    df = spark.createDataFrame(race_data, schema) \
        .withColumn("ingested_at", F.current_timestamp())
    
    # MERGE（重複防止）
    df.createOrReplaceTempView("new_races")
    spark.sql("""
        MERGE INTO bronze.races AS target
        USING new_races AS source
        ON target.race_id = source.race_id
           AND target.horse_name = source.horse_name
        WHEN NOT MATCHED THEN INSERT *
    """)
```

### フェーズ3：dbtで変換（dbt習得後）

```sql
-- models/silver/stg_races.sql
WITH source AS (
    SELECT * FROM {{ source('bronze', 'races') }}
),

cleaned AS (
    SELECT
        race_id,
        TO_DATE(race_date, 'yyyyMMdd') AS race_date,
        TRIM(race_course)              AS race_course,
        TRIM(horse_name)               AS horse_name,
        finish_position,
        odds,
        prize,
        ingested_at
    FROM source
    WHERE race_id IS NOT NULL
      AND horse_name IS NOT NULL
)

SELECT * FROM cleaned
```

```sql
-- models/gold/horse_performance.sql
WITH races AS (
    SELECT * FROM {{ ref('stg_races') }}
),

performance AS (
    SELECT
        horse_name,
        COUNT(*)                                          AS total_races,
        SUM(CASE WHEN finish_position = 1 THEN 1 END)    AS wins,
        SUM(CASE WHEN finish_position <= 3 THEN 1 END)   AS top3,
        ROUND(AVG(odds), 2)                              AS avg_odds,
        SUM(prize)                                        AS total_prize
    FROM races
    GROUP BY horse_name
)

SELECT
    *,
    ROUND(wins / total_races, 3)  AS win_rate,
    ROUND(top3 / total_races, 3)  AS top3_rate
FROM performance
WHERE total_races >= 5
ORDER BY win_rate DESC
```

### フェーズ4：分析・可視化（BI習得後）

**分析アイデア**

```
コース別傾向分析
  → 距離・馬場状態・コース形状による勝率変化

血統分析
  → 父馬・母父別の成績傾向

騎手分析
  → 騎手×コース×距離の組み合わせ勝率

期待値計算
  → オッズ × 勝率 で理論的な期待値を算出
  → プラス期待値のレースを見つける
```

**Streamlitダッシュボード（Pythonで作れる）**

```python
import streamlit as st
import pandas as pd

st.title("競馬分析ダッシュボード")

# サイドバーでフィルタ
course = st.sidebar.selectbox("コース", ["東京", "中山", "阪神", "京都"])
min_races = st.sidebar.slider("最低出走数", 1, 50, 10)

# データ読み込み（Databricks接続または CSV）
df = pd.read_csv("horse_performance.csv")
filtered = df[
    (df["race_course"] == course) &
    (df["total_races"] >= min_races)
]

st.dataframe(filtered.sort_values("win_rate", ascending=False))
st.bar_chart(filtered.set_index("horse_name")["win_rate"])
```

---

## GitHubリポジトリ構成

```
keiba-de-project/
├── README.md
├── data_collection/
│   ├── fetch_races.py
│   └── utils.py
├── notebooks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_transform.py
│   └── 03_gold_aggregation.py
├── dbt_project/
│   ├── models/
│   │   ├── bronze/
│   │   ├── silver/
│   │   └── gold/
│   └── tests/
├── dashboard/
│   └── app.py
└── requirements.txt
```

---

## 履歴書・ポートフォリオへの書き方

```
【個人プロジェクト】競馬データ分析基盤の構築
期間: 2024年〜継続中
技術: Python / PySpark / Databricks / Delta Lake / dbt / Power BI

概要:
JRAのレースデータを自動収集し、Databricks上にデータ基盤を構築。
Medalionアーキテクチャ（Bronze/Silver/Gold）で設計し、
dbtによるデータモデリングとPower BIによる可視化を実現。
馬別・コース別の勝率分析から期待値計算モデルまで実装。
```

!!! tip "ポイント"
    「趣味の競馬」ではなく「技術の実践場」として説明する。
    使った技術スタックを具体的に列挙することが重要。
