# dbt（data build tool）

## dbtとは

SQLでデータ変換パイプラインを書くツール。
「データウェアハウス内の変換」に特化。

```
dbtがやること: Transform（変換）のみ
Extract・Load は別ツール（Fivetran・ADF等）が担う
```

---

## なぜdbtか（日本市場での立ち位置）

- 2024〜2025年にかけて日本のDE市場で急速に普及
- Analytics Engineerというロールの中核ツール
- Databricks・Snowflake・BigQueryすべてに対応
- SQL中心なので既存スキルを活かせる
- 「知ってるだけで差別化」になる段階

---

## dbtの核心概念

### モデル（Model）

dbtの基本単位。1つのSQLファイル = 1つのモデル。

```sql
-- models/marts/races_summary.sql

WITH source AS (
    SELECT * FROM {{ ref('stg_races') }}   -- 他のモデルを参照
),

aggregated AS (
    SELECT
        race_course,
        race_year,
        COUNT(*)        AS race_count,
        AVG(prize)      AS avg_prize,
        MAX(prize)      AS max_prize
    FROM source
    GROUP BY 1, 2
)

SELECT * FROM aggregated
```

**`{{ ref('モデル名') }}`** が重要。依存関係をdbtが自動解決する。

### マテリアライゼーション（Materialization）

モデルをどの形式でDBに保存するか。

| 種類 | 説明 | 使いどころ |
|------|------|-----------|
| `view` | ビューとして保存（デフォルト）| 軽い変換・開発中 |
| `table` | テーブルとして保存 | 集計済みの重いクエリ |
| `incremental` | 差分のみ更新 | 大量データ・ログ系 |
| `ephemeral` | 保存しない（CTE扱い）| 中間処理のみ |

```sql
-- モデルの先頭に設定を書く
{{ config(
    materialized='incremental',
    unique_key='race_id'
) }}

SELECT * FROM {{ ref('stg_races') }}

{% if is_incremental() %}
WHERE race_date > (SELECT MAX(race_date) FROM {{ this }})
{% endif %}
```

---

## ディレクトリ構成（ベストプラクティス）

```
models/
├── staging/          # 生データをそのまま整形（型変換・名前統一）
│   ├── _sources.yml  # ソース定義
│   ├── stg_races.sql
│   └── stg_horses.sql
├── intermediate/     # ステージング同士を結合・加工
│   └── int_race_results.sql
└── marts/            # ビジネスロジック・最終的なテーブル
    ├── races_summary.sql
    └── horse_performance.sql
```

**命名規則**

| ディレクトリ | プレフィックス | 例 |
|-------------|-------------|---|
| staging | `stg_` | `stg_races` |
| intermediate | `int_` | `int_race_results` |
| marts | なし（わかりやすい名前）| `races_summary` |

---

## テスト

dbtの強力な機能。データ品質をSQLで担保。

```yaml
# models/staging/_stg_races.yml
version: 2

models:
  - name: stg_races
    columns:
      - name: race_id
        tests:
          - unique           # 重複なし
          - not_null         # NULLなし
      - name: race_course
        tests:
          - accepted_values: # 値が決まったリストに含まれるか
              values: ['Tokyo', 'Osaka', 'Kyoto', 'Nakayama']
      - name: distance
        tests:
          - not_null
```

```bash
# テスト実行
dbt test
dbt test --select stg_races  # 特定モデルのみ
```

---

## ドキュメント自動生成

```bash
# ドキュメント生成
dbt docs generate

# ブラウザで確認（DAGも見れる）
dbt docs serve
```

---

## 基本コマンド

```bash
# プロジェクト初期化
dbt init my_project

# 実行（全モデル）
dbt run

# 特定モデルのみ実行
dbt run --select races_summary

# 依存するモデルも含めて実行
dbt run --select +races_summary

# テスト実行
dbt test

# ドキュメント生成・確認
dbt docs generate
dbt docs serve

# デバッグ
dbt debug
dbt compile  # SQLの生成結果を確認
```

---

## Databricksとdbtのセットアップ

```yaml
# profiles.yml（~/.dbt/profiles.yml）
my_project:
  target: dev
  outputs:
    dev:
      type: databricks
      host: <workspace-host>
      http_path: <sql-warehouse-path>
      token: <personal-access-token>
      schema: my_schema
      threads: 4
```

---

## Databricks + dbt の学習ステップ

```
Step1: dbt Core をローカルインストール
       pip install dbt-databricks

Step2: profiles.yml でDatabricksに接続

Step3: dbt init でプロジェクト作成

Step4: stagingモデルを1つ作ってdbt run

Step5: テストを追加してdbt test

Step6: incrementalモデルを試す
```

!!! tip "最初の一歩"
    まず `stg_races.sql` を1本書いて `dbt run` が通ることを確認する。
    概念より手を動かすほうが早く理解できる。
