# Airflow 応用

基礎のDAG・Operator・XComに加えて、現場で必要な応用知識。

---

## TaskFlow API（推奨スタイル）

Airflow 2.0以降の書き方。デコレータでシンプルに書ける。

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    dag_id="keiba_taskflow",
    start_date=datetime(2024, 1, 1),
    schedule="0 20 * * 0,6",
    catchup=False,
)
def keiba_pipeline():

    @task
    def fetch_race_data(race_date: str) -> dict:
        """レースデータを取得してdictで返す"""
        # 実際はAPI呼び出し等
        return {
            "race_date": race_date,
            "race_count": 12,
            "data_path": f"/data/races/{race_date}/"
        }

    @task
    def validate_data(data: dict) -> dict:
        """データ品質チェック"""
        if data["race_count"] == 0:
            raise ValueError(f"{data['race_date']} のレースデータがありません")
        return data

    @task.branch
    def check_weekend(data: dict) -> str:
        """土日かどうかで分岐"""
        from datetime import datetime
        dt = datetime.strptime(data["race_date"], "%Y-%m-%d")
        if dt.weekday() >= 5:  # 土=5, 日=6
            return "run_full_transform"
        return "run_light_transform"

    @task
    def run_full_transform(data: dict):
        print(f"本番レース変換: {data['data_path']}")

    @task
    def run_light_transform(data: dict):
        print(f"軽量変換: {data['data_path']}")

    # タスク実行の流れを定義
    data = fetch_race_data("{{ ds }}")
    validated = validate_data(data)
    check_weekend(validated) >> [run_full_transform(validated), run_light_transform(validated)]

keiba_pipeline()
```

---

## Variables と Connections

### Variables（環境変数の代替）

```python
from airflow.models import Variable

# UIまたはCLIで設定した変数を取得
api_key = Variable.get("JRA_API_KEY")
config = Variable.get("keiba_config", deserialize_json=True)
# {"start_year": 2020, "max_races": 1000}

# タスク内で使用
@task
def fetch_data():
    api_key = Variable.get("JRA_API_KEY")
    # ...
```

```bash
# CLI でVariableを設定
airflow variables set JRA_API_KEY "your-api-key"
airflow variables set keiba_config '{"start_year": 2020}'
```

### Connections（DB・API接続情報）

```python
from airflow.hooks.base import BaseHook

# UIで登録したConnectionを取得
conn = BaseHook.get_connection("databricks_default")
host = conn.host
token = conn.password

# HTTPOperatorと組み合わせ
from airflow.providers.http.operators.http import SimpleHttpOperator

fetch_api = SimpleHttpOperator(
    task_id="fetch_jra_api",
    http_conn_id="jra_api",        # UIで登録したConnection
    method="GET",
    endpoint="/api/v1/races",
    headers={"Authorization": "Bearer {{ conn.jra_api.password }}"},
)
```

---

## Sensors（待機タスク）

条件が満たされるまでポーリングし続けるタスク。

```python
from airflow.sensors.filesystem import FileSensor
from airflow.sensors.python import PythonSensor

# ファイルが届くまで待つ
wait_for_file = FileSensor(
    task_id="wait_for_race_file",
    filepath="/data/landing/races_{{ ds }}.csv",
    poke_interval=300,    # 5分おきにチェック
    timeout=7200,         # 2時間でタイムアウト
    mode="reschedule",    # タスクスロットを解放して待つ（推奨）
)

# カスタム条件
def check_api_ready(**context):
    import requests
    r = requests.get("https://api.jra.jp/health")
    return r.status_code == 200

wait_for_api = PythonSensor(
    task_id="wait_for_api",
    python_callable=check_api_ready,
    poke_interval=60,
    timeout=3600,
    mode="reschedule",
)
```

---

## エラーハンドリング・リトライ

```python
from datetime import timedelta

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,   # 指数バックオフ
    "max_retry_delay": timedelta(hours=1),
    "email_on_failure": True,
    "email": ["data-team@company.com"],
}

@task(
    retries=3,
    retry_delay=timedelta(minutes=2),
    execution_timeout=timedelta(hours=2),  # 2時間でタイムアウト
)
def risky_task():
    pass
```

---

## テンプレート変数（Jinja）

```python
@task
def show_context(**context):
    # よく使うテンプレート変数
    print(context["ds"])           # 実行日 "2024-01-15"
    print(context["ds_nodash"])    # "20240115"
    print(context["ts"])           # タイムスタンプ
    print(context["prev_ds"])      # 前回の実行日
    print(context["next_ds"])      # 次の実行日
    print(context["dag_run"].conf) # 手動実行時のパラメータ

# テンプレートをそのまま使う場合
bash_task = BashOperator(
    task_id="echo_date",
    bash_command="echo {{ ds }} && echo {{ dag.dag_id }}",
)
```

---

## DAGの依存関係パターン

```python
# 直列
a >> b >> c >> d

# 並列（bとcが同時に動く）
a >> [b, c] >> d

# 複雑な依存関係
a >> b
a >> c
b >> d
c >> d
d >> e

# trigger_rule: 前のタスクの状態で分岐
from airflow.utils.trigger_rule import TriggerRule

finish = EmptyOperator(
    task_id="finish",
    trigger_rule=TriggerRule.NONE_FAILED,  # 失敗がなければ実行
    # ALL_SUCCESS: 全部成功
    # ALL_DONE: 全部完了（失敗含む）
    # ONE_FAILED: 1つでも失敗
)
```

---

## モニタリング・デバッグ

```bash
# DAGのリスト確認
airflow dags list

# 特定DAGのタスク確認
airflow tasks list keiba_daily_pipeline

# 手動でDAGを実行
airflow dags trigger keiba_daily_pipeline

# パラメータ付きで実行
airflow dags trigger keiba_daily_pipeline --conf '{"race_date": "2024-01-15"}'

# タスクのログ確認
airflow tasks logs keiba_daily_pipeline fetch_race_data 2024-01-15

# タスクをローカル実行（デバッグ用）
airflow tasks test keiba_daily_pipeline fetch_race_data 2024-01-15
```

---

## ベストプラクティス

```python
# NG: DAGファイルにビジネスロジックを書かない
@task
def process():
    # 100行のビジネスロジック...  ← NG
    pass

# OK: ロジックは別ファイルに切り出す
from keiba.transform import process_race_data

@task
def process():
    process_race_data()  # 別モジュールに委譲
```

```python
# NG: catchup=True（デフォルト）のまま使わない
# 過去の全実行日分が一気に動いてしまう

with DAG(
    dag_id="example",
    start_date=datetime(2020, 1, 1),
    # catchup=True ← NG
    catchup=False,  # ← 必ずFalseにする
):
    pass
```

```python
# NG: XComで大量データを渡す
@task
def produce():
    return [大量のデータ]  # XComに入れるのはNG

# OK: パスやIDだけ渡す
@task
def produce():
    data_path = "/data/output/races_2024.parquet"
    save_data(data_path)
    return data_path  # パスだけ渡す

@task
def consume(data_path: str):
    df = load_data(data_path)  # パスからロード
```

---

## Dockerでのローカル起動

```yaml
# docker-compose.yaml（公式から取得）
# curl -LfO https://airflow.apache.org/docs/apache-airflow/stable/docker-compose.yaml

# 初期化
docker compose up airflow-init

# 起動
docker compose up -d

# UI: http://localhost:8080
# ユーザー: airflow / パスワード: airflow
```
