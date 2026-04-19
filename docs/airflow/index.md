# Apache Airflow

## Airflowとは

データパイプラインをスケジュール・管理するワークフローオーケストレーターツール。

```
Airflowの役割: 「いつ・何を・どの順で実行するか」を管理する
実際の処理: SparkやdbtやAPIコールなど別ツールに任せる
```

---

## なぜAirflowか

データエンジニアリング案件の多くで使われているスタンダードツール。
「パイプラインを管理する語彙を持てる」が最初の目標。

Databricks案件では **Databricks Workflows** が代替になることも多いが、
Airflowを知っていればどこでも通用する。

---

## 核心概念

### DAG（Directed Acyclic Graph）

Airflowの基本単位。「有向非巡回グラフ」= 矢印が一方向でループしない処理の流れ。

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

# DAG定義
with DAG(
    dag_id="keiba_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 6 * * *",  # 毎朝6時（cron形式）
    catchup=False,          # 過去分を遡って実行しない
    tags=["keiba", "daily"],
) as dag:
    pass
```

### Task と Operator

DAGの中の1つの処理単位がTask。Operatorが処理の種類を定義する。

```python
# Python処理
def fetch_race_data():
    print("レースデータ取得中...")

fetch_task = PythonOperator(
    task_id="fetch_race_data",
    python_callable=fetch_race_data,
)

# Shell処理
run_dbt = BashOperator(
    task_id="run_dbt_transform",
    bash_command="dbt run --select races_summary",
)

# 依存関係の定義
fetch_task >> run_dbt  # fetch_taskが終わったらrun_dbtを実行
```

---

## よく使うOperator

| Operator | 用途 |
|----------|------|
| `PythonOperator` | Python関数を実行 |
| `BashOperator` | シェルコマンドを実行 |
| `DatabricksRunNowOperator` | Databricks Jobを実行 |
| `DatabricksSubmitRunOperator` | Databricksにnotebookを投げる |
| `HttpSensor` | APIが返ってくるまで待つ |
| `FileSensor` | ファイルが届くまで待つ |
| `BranchPythonOperator` | 条件分岐 |
| `TriggerDagRunOperator` | 別のDAGをトリガー |

---

## DAGの完全な例

```python
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import requests

def fetch_race_results(**context):
    """JRAからレース結果を取得"""
    race_date = context["ds"]  # 実行日 (YYYY-MM-DD)
    # APIコール等の処理
    print(f"{race_date} のレースデータを取得")

def check_data_quality(**context):
    """データ品質チェック"""
    # 取得件数が0ならスキップ
    count = 10  # 実際はDBから取得
    return "transform_data" if count > 0 else "skip"

def transform_data(**context):
    """データ変換"""
    print("データ変換中...")

with DAG(
    dag_id="keiba_daily_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="0 20 * * 0,6",  # 土日の20時
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": 60,  # 秒
    }
) as dag:

    start = EmptyOperator(task_id="start")

    fetch = PythonOperator(
        task_id="fetch_race_results",
        python_callable=fetch_race_results,
    )

    quality_check = BranchPythonOperator(
        task_id="check_data_quality",
        python_callable=check_data_quality,
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="dbt run --select +races_summary",
    )

    skip = EmptyOperator(task_id="skip")
    end = EmptyOperator(task_id="end", trigger_rule="none_failed")

    # 依存関係
    start >> fetch >> quality_check
    quality_check >> [transform, skip]
    transform >> dbt_run >> end
    skip >> end
```

---

## Cron式の読み方

```
分 時 日 月 曜日

0 6 * * *       毎日6時
0 6 * * 1       毎週月曜6時
0 6 1 * *       毎月1日6時
0 */2 * * *     2時間おき
0 0,6,12,18 * * *  1日4回（0時・6時・12時・18時）
```

---

## XCom（タスク間のデータ受け渡し）

```python
def push_value(**context):
    # 値をXComにプッシュ
    context["ti"].xcom_push(key="race_count", value=100)

def pull_value(**context):
    # 他のタスクの値を取得
    count = context["ti"].xcom_pull(
        task_ids="push_value",
        key="race_count"
    )
    print(f"レース数: {count}")
```

!!! warning "XComの注意"
    XComは大量データの受け渡しには向かない。
    件数・フラグ・パスなど小さいデータのみに使う。
    大量データはDBやストレージ経由で渡す。

---

## Databricks × Airflow

```python
from airflow.providers.databricks.operators.databricks import (
    DatabricksRunNowOperator,
    DatabricksSubmitRunOperator,
)

# 既存のDatabricks Jobを実行
run_spark_job = DatabricksRunNowOperator(
    task_id="run_spark_job",
    databricks_conn_id="databricks_default",
    job_id=12345,
)

# ノートブックをその場で実行
run_notebook = DatabricksSubmitRunOperator(
    task_id="run_notebook",
    databricks_conn_id="databricks_default",
    new_cluster={
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "Standard_DS3_v2",
        "num_workers": 2,
    },
    notebook_task={
        "notebook_path": "/Users/you/keiba_etl",
    },
)
```

---

## 最初の学習ステップ

```
Step1: Airflowの概念（DAG・Task・Operator）を理解
Step2: ローカルでAirflow起動（Docker推奨）
       docker compose up airflow-init && docker compose up
Step3: シンプルなDAGを1本書いて動かす
Step4: PythonOperatorとBashOperatorを使いこなす
Step5: Databricksと接続する
```

!!! tip "Databricksで代替する場合"
    Databricks Workflowsを使えばAirflowなしでジョブ管理できる。
    どちらも概念は同じ（DAG・タスク・スケジュール）なので
    どちらかを覚えればもう一方も理解しやすい。
