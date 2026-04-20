# Unity Catalog

Databricksのデータガバナンス基盤。2023年以降の案件では必須知識。

---

## Unity Catalogとは

```
旧来: Hive Metastore（ワークスペースごとに分離・管理が煩雑）
     ↓
Unity Catalog: 組織全体でデータを一元管理するガバナンスレイヤー
```

**できること**
- データへのアクセス制御（テーブル・列・行レベル）
- データリネージ（どこからきたデータか追跡）
- 監査ログ（誰がいつアクセスしたか）
- Delta Sharing（組織外とのデータ共有）

---

## 3層の名前空間

```
Catalog（カタログ）
  └── Schema（スキーマ）= データベース
        └── Table / View / Volume / Function
```

```sql
-- フルパスでアクセス
SELECT * FROM my_catalog.my_schema.my_table;

-- デフォルトのカタログ・スキーマを設定
USE CATALOG my_catalog;
USE SCHEMA my_schema;
SELECT * FROM my_table;  -- 以後省略可
```

```python
# PySpark でも同様
df = spark.table("my_catalog.my_schema.my_table")

spark.sql("USE CATALOG my_catalog")
```

---

## 主なオブジェクト

| オブジェクト | 説明 |
|------------|------|
| Catalog | 最上位の名前空間 |
| Schema | テーブルの入れ物（旧: Database）|
| Table | データ本体（Managed / External）|
| View | 仮想テーブル（クエリの定義）|
| Volume | ファイル（非構造化データ）の管理領域 |
| Function | UDF・集計関数の登録 |

### Managed Table vs External Table

```
Managed Table:
  - Databricksがデータの場所を管理
  - DROP TABLE するとデータも削除される
  - Unity Catalog推奨の形式

External Table:
  - データの場所（S3/ADLS等）を自分で指定
  - DROP TABLE してもデータは残る
  - 既存のデータレイクを参照する場合に使う
```

```sql
-- Managed Table の作成
CREATE TABLE my_catalog.my_schema.races (
    race_id STRING,
    race_date DATE,
    race_course STRING,
    prize BIGINT
) USING DELTA;

-- External Table の作成
CREATE TABLE my_catalog.my_schema.races_external
USING DELTA
LOCATION 'abfss://container@storage.dfs.core.windows.net/races/';
```

---

## アクセス制御

### GRANT / REVOKE

```sql
-- スキーマ全体の読み取り権限を付与
GRANT USE SCHEMA, SELECT
ON SCHEMA my_catalog.my_schema
TO `user@company.com`;

-- グループに権限付与
GRANT SELECT ON TABLE my_catalog.my_schema.races TO `data_analysts`;

-- 権限確認
SHOW GRANTS ON TABLE my_catalog.my_schema.races;

-- 権限剥奪
REVOKE SELECT ON TABLE my_catalog.my_schema.races FROM `data_analysts`;
```

### 権限の種類

| 権限 | 説明 |
|------|------|
| `SELECT` | データの読み取り |
| `MODIFY` | INSERT / UPDATE / DELETE |
| `CREATE TABLE` | テーブル作成 |
| `USE CATALOG` | カタログへのアクセス |
| `USE SCHEMA` | スキーマへのアクセス |
| `ALL PRIVILEGES` | 全権限 |

!!! warning "権限の注意"
    テーブルへのSELECTを付与するだけではアクセスできない。
    `USE CATALOG` と `USE SCHEMA` も付与する必要がある。

---

## 列レベルセキュリティ（Column Masking）

機密情報（個人情報など）を特定ユーザーにはマスキング。

```sql
-- マスキング関数を定義
CREATE FUNCTION my_catalog.my_schema.mask_email(email STRING)
RETURNS STRING
RETURN IF(IS_MEMBER('data_admins'), email, '***@***.***');

-- テーブルに適用
ALTER TABLE my_catalog.my_schema.customers
ALTER COLUMN email SET MASK my_catalog.my_schema.mask_email;
```

---

## 行レベルセキュリティ（Row Filter）

ユーザーのグループによって見える行を制限。

```sql
-- フィルタ関数を定義（自分の担当エリアだけ見える）
CREATE FUNCTION my_catalog.my_schema.region_filter(region STRING)
RETURNS BOOLEAN
RETURN IS_MEMBER('admin') OR region = current_user();

-- テーブルに適用
ALTER TABLE my_catalog.my_schema.sales
SET ROW FILTER my_catalog.my_schema.region_filter ON (region);
```

---

## データリネージ

Unity Catalogは自動でリネージを追跡する。

```
sources → my_catalog.bronze.races_raw
            ↓（Spark処理）
          my_catalog.silver.races_cleaned
            ↓（dbt変換）
          my_catalog.gold.races_summary
```

Databricks UI の「Catalog Explorer」から視覚的に確認できる。

---

## Delta Sharing

Unity CatalogのデータをリードオンリーでOrg外と共有する機能。

```python
# Providerがシェアを作成
spark.sql("""
    CREATE SHARE keiba_share;
    ALTER SHARE keiba_share ADD TABLE my_catalog.gold.races_summary;
""")

# Recipientを作成（外部組織）
spark.sql("""
    CREATE RECIPIENT external_partner
    COMMENT '外部パートナー向けシェア';
""")

spark.sql("GRANT SELECT ON SHARE keiba_share TO RECIPIENT external_partner;")
```

---

## よく出る試験ポイント

!!! question "Q: Managed TableとExternal Tableの違いは？"
    A: Managed Tableはデータの場所をDatabricksが管理し、DROP TABLEでデータも削除される。
    External Tableはデータの場所をユーザーが指定し、DROP TABLEしてもデータは残る。

!!! question "Q: Unity Catalogの3層名前空間は？"
    A: Catalog → Schema → Table/View/Volume/Function。
    `catalog.schema.table` の形式でアクセスする。

!!! question "Q: テーブルへのSELECT権限だけで十分か？"
    A: 不十分。USE CATALOG と USE SCHEMA も付与する必要がある。
