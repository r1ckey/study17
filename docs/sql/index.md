# SQL for Data Engineering

データエンジニアとして必須のSQL。SELECT文の書き方から分析関数・パフォーマンスまで。

---

## 基本構文

```sql
SELECT
    column1,
    column2,
    COUNT(*) AS cnt
FROM table_name
WHERE condition
GROUP BY column1, column2
HAVING COUNT(*) > 10
ORDER BY cnt DESC
LIMIT 100;
```

**実行順序（書く順と違う！）**

```
FROM → WHERE → GROUP BY → HAVING → SELECT → ORDER BY → LIMIT
```

!!! tip "重要"
    SELECTはFROMより後に評価される。
    WHERE句でSELECTの別名（alias）は使えない。

---

## JOIN

```sql
-- INNER JOIN（両方に存在する行のみ）
SELECT r.race_id, r.race_course, h.horse_name
FROM races r
INNER JOIN horses h ON r.horse_id = h.horse_id;

-- LEFT JOIN（左のテーブルは全件、右は一致する場合のみ）
SELECT r.race_id, h.horse_name
FROM races r
LEFT JOIN horses h ON r.horse_id = h.horse_id;

-- 複数条件でJOIN
SELECT *
FROM race_results r
JOIN race_entries e
  ON r.race_id = e.race_id
  AND r.horse_id = e.horse_id;
```

### JOINの種類まとめ

| 種類 | 説明 |
|------|------|
| INNER JOIN | 両方にある行のみ |
| LEFT JOIN | 左は全件 + 右は一致のみ（一致しない場合NULL）|
| RIGHT JOIN | 右は全件 + 左は一致のみ |
| FULL OUTER JOIN | 両方全件（一致しない側はNULL）|
| CROSS JOIN | 全組み合わせ（件数×件数）|

---

## 集計関数

```sql
SELECT
    race_course,
    COUNT(*)              AS race_count,       -- 行数
    COUNT(DISTINCT horse_id) AS unique_horses, -- ユニーク数
    SUM(prize)            AS total_prize,
    AVG(prize)            AS avg_prize,
    MAX(prize)            AS max_prize,
    MIN(distance)         AS min_distance,
    STDDEV(odds)          AS odds_stddev
FROM races
GROUP BY race_course;
```

---

## ウィンドウ関数（分析関数）

DEで最重要。GROUP BYと違い、**行を残したまま集計**できる。

```sql
SELECT
    horse_name,
    race_date,
    prize,
    -- 順位（同率あり）
    RANK()       OVER (PARTITION BY race_course ORDER BY prize DESC) AS rank_with_gap,
    -- 順位（同率なし・連番）
    ROW_NUMBER() OVER (PARTITION BY race_course ORDER BY prize DESC) AS row_num,
    -- 連続順位（同率でもスキップなし）
    DENSE_RANK() OVER (PARTITION BY race_course ORDER BY prize DESC) AS dense_rank,
    -- 累積合計
    SUM(prize)   OVER (PARTITION BY horse_name ORDER BY race_date) AS cumulative_prize,
    -- 移動平均（直近3レース）
    AVG(prize)   OVER (PARTITION BY horse_name ORDER BY race_date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS moving_avg_3,
    -- 前の行の値
    LAG(prize, 1) OVER (PARTITION BY horse_name ORDER BY race_date)  AS prev_prize,
    -- 次の行の値
    LEAD(prize, 1) OVER (PARTITION BY horse_name ORDER BY race_date) AS next_prize
FROM race_results;
```

### PARTITION BY と ORDER BY

```
PARTITION BY = グループ分け（GROUP BYに相当）
ORDER BY     = グループ内での順序
```

```sql
-- 馬ごとに、レース日順に「それまでの通算賞金」を計算
SELECT
    horse_name,
    race_date,
    prize,
    SUM(prize) OVER (
        PARTITION BY horse_name  -- 馬ごとに
        ORDER BY race_date       -- 日付順に
        ROWS UNBOUNDED PRECEDING -- 最初の行から現在行まで
    ) AS running_total
FROM race_results;
```

---

## CTE（Common Table Expression）

サブクエリを名前付きで書ける。可読性が大幅に上がる。

```sql
-- WITH句でCTEを定義
WITH
-- 1. 馬ごとの通算成績
horse_stats AS (
    SELECT
        horse_id,
        horse_name,
        COUNT(*) AS total_races,
        SUM(CASE WHEN finish_position = 1 THEN 1 ELSE 0 END) AS wins
    FROM race_results
    GROUP BY horse_id, horse_name
),

-- 2. 勝率を計算
horse_win_rate AS (
    SELECT
        horse_id,
        horse_name,
        total_races,
        wins,
        ROUND(wins * 1.0 / total_races, 3) AS win_rate
    FROM horse_stats
    WHERE total_races >= 5  -- 5戦以上に絞る
)

-- 3. 最終的なSELECT
SELECT *
FROM horse_win_rate
ORDER BY win_rate DESC
LIMIT 20;
```

!!! tip "CTEのメリット"
    - 読みやすい（上から下に処理が流れる）
    - 再利用できる（同じCTEを複数回参照可能）
    - デバッグしやすい（段階的に確認できる）

---

## CASE WHEN

```sql
SELECT
    horse_name,
    finish_position,
    -- シンプルなCASE
    CASE finish_position
        WHEN 1 THEN '1着'
        WHEN 2 THEN '2着'
        WHEN 3 THEN '3着'
        ELSE '着外'
    END AS result_label,
    
    -- 条件式のCASE
    CASE
        WHEN distance >= 2400 THEN '長距離'
        WHEN distance >= 1800 THEN '中距離'
        WHEN distance >= 1400 THEN '短中距離'
        ELSE '短距離'
    END AS distance_category
FROM race_results;
```

---

## サブクエリ

```sql
-- WHERE句のサブクエリ
SELECT * FROM horses
WHERE horse_id IN (
    SELECT horse_id FROM race_results WHERE finish_position = 1
);

-- FROM句のサブクエリ（インラインビュー）
SELECT avg_prize_by_course.*
FROM (
    SELECT race_course, AVG(prize) AS avg_prize
    FROM races
    GROUP BY race_course
) AS avg_prize_by_course
WHERE avg_prize > 1000000;

-- EXISTS（存在確認）
SELECT * FROM horses h
WHERE EXISTS (
    SELECT 1 FROM race_results r
    WHERE r.horse_id = h.horse_id
    AND r.finish_position = 1
);
```

---

## 文字列・日付操作

```sql
-- 文字列
UPPER(name)                          -- 大文字
LOWER(name)                          -- 小文字
TRIM(name)                           -- 前後空白削除
SUBSTRING(name, 1, 5)               -- 部分抽出
CONCAT(first_name, ' ', last_name)  -- 結合
REPLACE(name, '（', '(')            -- 置換
LENGTH(name)                         -- 文字数
LIKE '%pattern%'                     -- パターンマッチ

-- 日付（Spark SQL / BigQuery）
CURRENT_DATE()
DATE_ADD(race_date, 7)               -- 7日後
DATEDIFF(end_date, start_date)       -- 日数差
DATE_FORMAT(race_date, 'yyyy-MM')   -- フォーマット変換
YEAR(race_date)                      -- 年
MONTH(race_date)                     -- 月
DAYOFWEEK(race_date)                 -- 曜日（1=日〜7=土）
```

---

## NULL の扱い

```sql
-- NULLの比較は = ではなく IS NULL / IS NOT NULL
SELECT * FROM races WHERE prize IS NULL;
SELECT * FROM races WHERE prize IS NOT NULL;

-- COALESCE: 最初のNULL以外の値を返す
SELECT COALESCE(prize, 0) AS prize  -- NULLなら0
FROM races;

-- NULLIF: 指定値と同じならNULLを返す（0除算防止に便利）
SELECT wins / NULLIF(total_races, 0) AS win_rate
FROM horse_stats;
```

---

## パフォーマンスのポイント

```sql
-- NG: SELECT * は使わない（必要な列だけ選ぶ）
SELECT * FROM huge_table;

-- OK
SELECT race_id, horse_name, prize FROM huge_table;

-- フィルタは早めにかける
-- NG: JOIN後にWHERE
SELECT * FROM races r
JOIN results res ON r.race_id = res.race_id
WHERE r.race_year = 2024;

-- OK: サブクエリでフィルタ後にJOIN
SELECT * FROM (
    SELECT * FROM races WHERE race_year = 2024
) r
JOIN results res ON r.race_id = res.race_id;
```

---

## よく出る問題パターン

### N番目の順位を取得

```sql
-- 各競馬場で最も高い賞金レースを取得
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY race_course ORDER BY prize DESC) AS rn
    FROM races
) ranked
WHERE rn = 1;
```

### 前期比・前年比

```sql
SELECT
    race_year,
    race_month,
    SUM(prize) AS monthly_prize,
    LAG(SUM(prize), 1) OVER (ORDER BY race_year, race_month) AS prev_month_prize,
    SUM(prize) - LAG(SUM(prize), 1) OVER (ORDER BY race_year, race_month) AS diff
FROM races
GROUP BY race_year, race_month;
```

### 重複排除（最新1件）

```sql
SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY horse_id ORDER BY updated_at DESC) AS rn
    FROM horse_master
) deduped
WHERE rn = 1;
```
