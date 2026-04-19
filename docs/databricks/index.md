# Databricks 概要

## Databricksとは

ApacheSparkをベースにした統合データ分析プラットフォーム。
データエンジニアリング・データサイエンス・機械学習を1つのプラットフォームで実現。

**クラウド対応**: AWS / Azure / GCP すべてで動く。資格はクラウド問わず通用。

---

## Lakehouse アーキテクチャ

```
従来のアーキテクチャ
Data Lake（安い・生データ）  +  Data Warehouse（高い・整形済み）

Lakehouse（Databricksが提唱）
= Data Lake の柔軟性・コスト
+ Data Warehouse のパフォーマンス・信頼性
→ Delta Lake が実現する
```

---

## 主要コンポーネント

| コンポーネント | 役割 |
|--------------|------|
| **Workspace** | チームでノートブック・ジョブを管理する場所 |
| **Cluster** | Sparkを実行するコンピューティングリソース |
| **Notebook** | コードを書いて実行する場所（Python・SQL・Scala対応）|
| **Delta Lake** | データストレージ形式（ACIDトランザクション対応）|
| **Delta Live Tables** | パイプラインを宣言的に定義する仕組み |
| **Unity Catalog** | データガバナンス・アクセス管理 |
| **MLflow** | 機械学習実験管理 |
| **Jobs** | ノートブックをスケジュール実行する仕組み |

---

## クラスターの種類

=== "All-Purpose Cluster"
    - 対話的な探索・開発用
    - ノートブックから直接接続
    - 起動に数分かかる
    - コストが高め

=== "Job Cluster"
    - ジョブ実行専用
    - ジョブ開始時に起動・終了時に削除
    - コスト効率が良い
    - 本番パイプラインに使う

=== "SQL Warehouse"
    - SQL専用のコンピューティング
    - Databricks SQL で使用
    - BIツール接続に使う

---

## Databricksのデータ階層

```
Unity Catalog（メタデータ管理）
└── Catalog（最上位）
    └── Schema（Database）
        └── Table / View
```

```sql
-- Unity Catalogを使った参照
SELECT * FROM catalog_name.schema_name.table_name

-- 現在のCatalog確認
SELECT current_catalog(), current_schema()
```

---

## ノートブックの基本

```python
# マジックコマンド（%で始まる）
%python   # Pythonで書く（デフォルト）
%sql      # SQLで書く
%scala    # Scalaで書く
%md       # Markdownで書く
%sh       # シェルコマンド
%fs       # DBFS（Databricksファイルシステム）操作

# DBFSの確認
%fs ls /

# ファイルの内容確認
%fs head /path/to/file
```

```python
# セル間でのデータ受け渡し
# Python → SQL
df = spark.sql("SELECT * FROM races")
df.createOrReplaceTempView("races_view")

# SQLセルで参照
# %sql
# SELECT * FROM races_view
```

---

## DBFS（Databricks File System）

```python
# ファイル一覧
dbutils.fs.ls("/")

# ファイルコピー
dbutils.fs.cp("/source/path", "/dest/path")

# ファイル削除
dbutils.fs.rm("/path/to/file", recurse=True)

# ファイル内容確認
dbutils.fs.head("/path/to/file")
```

---

## Databricksで学習する順番

```
1. ノートブックでPySpark操作に慣れる
   ↓
2. Delta Lakeの読み書きを覚える
   ↓
3. Jobs でスケジュール実行を理解する
   ↓
4. Delta Live Tables でパイプラインを組む
   ↓
5. Unity Catalog でガバナンスを理解する
```

---

## 次のステップ

- [アーキテクチャ詳細](architecture.md) を理解する
- [Delta Lake](delta_lake.md) を深める
- [パフォーマンス最適化](performance.md) を学ぶ
- [Databricks Associate 試験対策](../certifications/databricks_associate.md) へ
