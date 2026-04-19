# Python 概要・環境構築

## なぜPythonか

データエンジニアの主武器。PySpark・dbt・Airflow、すべてPythonで書く。
SQLは「使えて当然」のベースライン。Pythonが「差別化の武器」になる。

---

## 環境構築

### Databricks上（最速）

Databricksのノートブックを開けばすぐPython・PySpark環境が使える。
ローカル環境構築より先にDatabricksで慣れるのがおすすめ。

### ローカル環境

```bash
# pyenvでPythonバージョン管理
pyenv install 3.11.0
pyenv global 3.11.0

# 仮想環境作成
python -m venv .venv
source .venv/bin/activate  # Mac/Linux
.venv\Scripts\activate     # Windows

# 必要ライブラリ
pip install pandas pyspark jupyter
```

---

## Python基礎：データエンジニアが使う頻出パターン

### リスト内包表記

```python
# 基本
numbers = [1, 2, 3, 4, 5]
doubled = [x * 2 for x in numbers]
# [2, 4, 6, 8, 10]

# 条件付き
evens = [x for x in numbers if x % 2 == 0]
# [2, 4]
```

### 辞書操作

```python
record = {"id": 1, "name": "競馬太郎", "age": 30}

# 値の取得（KeyError回避）
name = record.get("name", "unknown")

# 辞書内包表記
upper = {k: v.upper() if isinstance(v, str) else v for k, v in record.items()}
```

### ファイル・CSV操作

```python
import pandas as pd

# CSV読み込み
df = pd.read_csv("races.csv")

# 基本操作
df.head()           # 先頭5行
df.info()           # 型・欠損確認
df.describe()       # 統計サマリ
df.shape            # (行数, 列数)

# フィルタ
df[df["distance"] > 2000]

# 集計
df.groupby("race_course")["prize"].mean()
```

### 関数定義

```python
def clean_horse_name(name: str) -> str:
    """馬名を正規化する"""
    return name.strip().upper()

# 型ヒント付き（推奨）
def calculate_odds(win_count: int, total: int) -> float:
    if total == 0:
        return 0.0
    return win_count / total
```

---

## pandasとPySparkの対応表

| 操作 | pandas | PySpark |
|------|--------|---------|
| 読み込み | `pd.read_csv()` | `spark.read.csv()` |
| フィルタ | `df[df["col"] > 1]` | `df.filter(col("col") > 1)` |
| 選択 | `df[["col1","col2"]]` | `df.select("col1","col2")` |
| 集計 | `df.groupby().mean()` | `df.groupBy().avg()` |
| 結合 | `df.merge()` | `df.join()` |
| ソート | `df.sort_values()` | `df.orderBy()` |
| 追加 | `df["new"] = ...` | `df.withColumn("new", ...)` |

!!! tip "学習の進め方"
    pandasで操作に慣れてからPySparkに移行するとスムーズ。
    概念は同じで、文法が少し違うだけ。

---

## 次のステップ

- [PySpark基礎](pyspark.md) へ進む
- pandasが慣れてきたら[Databricks](../databricks/index.md)上で実践する
