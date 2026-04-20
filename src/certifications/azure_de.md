# Azure Data Engineer Associate（DP-203）

## 試験概要

| 項目 | 内容 |
|------|------|
| 試験時間 | 100〜120分 |
| 問題数 | 40〜60問 |
| 合格ライン | 700/1000 |
| 有効期限 | 1年（無料オンライン更新あり）|
| 受験料 | ¥20,655（日本）|

!!! info "2025年の動向"
    DP-203に加えてDP-700（Microsoft Fabric）も登場。
    Azure Databricksを使う案件ではDP-203が直結。

---

## 出題範囲

| セクション | 配点 |
|-----------|------|
| データストレージの設計と実装 | 15-20% |
| データ処理の設計と開発 | 40-45% |
| データセキュリティの設計と実装 | 10-15% |
| データの監視と最適化 | 10-15% |
| データの保存と提供 | 15-20% |

---

## 重要サービス一覧

### データ取り込み・統合

| サービス | 役割 | 覚えるポイント |
|---------|------|-------------|
| **Azure Data Factory** | ETL/ELT・パイプライン | リンクサービス・データセット・アクティビティ |
| **Azure Event Hubs** | ストリーミング取り込み | パーティション・コンシューマグループ |
| **Azure IoT Hub** | IoTデバイスからの取り込み | デバイス認証 |

### データストレージ

| サービス | 役割 | 覚えるポイント |
|---------|------|-------------|
| **Azure Data Lake Storage Gen2** | 大規模データ湖 | 階層型名前空間・ACL |
| **Azure SQL Database** | マネージドRDB | DTU vs vCore |
| **Azure Synapse Analytics** | DWH + 分析統合 | dedicated/serverless SQLプール |
| **Azure Cosmos DB** | グローバル分散NoSQL | 一貫性レベル5種類 |
| **Azure Blob Storage** | オブジェクトストレージ | アクセス層 |

### データ処理

| サービス | 役割 |
|---------|------|
| **Azure Databricks** | Spark + Lakehouse |
| **Azure Stream Analytics** | リアルタイムストリーム処理 |
| **Azure HDInsight** | オープンソース（Hadoop/Spark/Hive）|

---

## Azure Data Factory（ADF）

### 主要コンポーネント

```
Pipeline（パイプライン）
└── Activity（アクティビティ）× N
    └── Dataset（データセット）← どのデータか
        └── Linked Service（リンクサービス）← どこに接続するか
```

### よく使うアクティビティ

| アクティビティ | 用途 |
|-------------|------|
| Copy | データのコピー（最も基本）|
| Mapping Data Flow | ノーコードでデータ変換 |
| Execute Databricks Notebook | Databricksを呼び出す |
| Web | REST APIを呼び出す |
| Get Metadata | ファイル情報を取得 |
| If Condition | 条件分岐 |
| ForEach | ループ処理 |

### トリガーの種類

| トリガー | 説明 |
|---------|------|
| Schedule | Cron式でスケジュール実行 |
| Tumbling Window | 連続する時間枠でデータを処理 |
| Event-based | ストレージにファイルが届いたら実行 |
| Manual | 手動実行 |

---

## ADLS Gen2（Azure Data Lake Storage Gen2）

### 階層型名前空間

```
Storage Account
└── Container（= ファイルシステム）
    └── Directory
        └── File
```

### アクセス制御

| 方式 | 説明 |
|------|------|
| RBAC | サブスクリプション・リソースグループ・ストレージアカウントレベル |
| ACL | ファイル・ディレクトリレベルの細かい制御 |
| SAS | 一時的なアクセスキー |

```python
# Databricksからのアクセス設定
spark.conf.set(
    "fs.azure.account.key.storageaccount.dfs.core.windows.net",
    "storage_account_key"
)

# またはサービスプリンシパル
spark.conf.set("fs.azure.account.auth.type", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type",
    "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
```

---

## Azure Synapse Analytics

### SQLプールの種類

| 種類 | 特徴 | 使いどころ |
|------|------|----------|
| Dedicated SQL Pool | 専用リソース・高パフォーマンス | 本番DWH・定期レポート |
| Serverless SQL Pool | オンデマンド・ADLS上のファイルをSQL検索 | データ探索・アドホック分析 |

### PolyBase と COPY INTO

```sql
-- 外部テーブルでADLSのデータを直接クエリ
CREATE EXTERNAL TABLE ext_races
WITH (
    LOCATION = '/races/',
    DATA_SOURCE = my_data_lake,
    FILE_FORMAT = csv_format
)
AS SELECT * FROM OPENROWSET(...)

-- COPY INTO（推奨・高速）
COPY INTO races
FROM 'https://account.dfs.core.windows.net/container/races/'
WITH (
    FILE_TYPE = 'PARQUET',
    COMPRESSION = 'Snappy'
)
```

---

## セキュリティ

### よく出る概念

| 概念 | 説明 |
|------|------|
| **Azure Active Directory** | 認証基盤 |
| **Managed Identity** | サービス間の認証（パスワード不要）|
| **Key Vault** | シークレット・キー・証明書の管理 |
| **Private Endpoint** | パブリックインターネットを通さないアクセス |
| **Purview** | データガバナンス・データカタログ |

### Managed Identity（重要）

```
ADFからDatabricksにアクセスする場合:
→ ADFのManaged Identityにロールを付与する
→ 接続文字列にパスワードを書かなくて済む
→ セキュリティのベストプラクティス
```

---

## 頻出問題パターン

!!! question "Q: ADLSへのアクセスに最もセキュアな方法は？"
??? success "A"
    **Managed Identity** を使う。
    ストレージアカウントキーやSASトークンよりも安全。

!!! question "Q: Synapse Analyticsのサーバーレスプールで使えるのはどのストレージ？"
??? success "A"
    **ADLS Gen2**や**Azure Blob Storage**上のファイル（Parquet・CSV・JSON）を直接クエリ可能。

!!! question "Q: ADF Pipelineで条件分岐・ループを実現するアクティビティは？"
??? success "A"
    - 条件分岐 → **If Condition**
    - ループ → **ForEach**
    - 失敗時の処理 → **On Failure** 接続

!!! question "Q: PolyBaseとCOPY INTOの違いは？"
??? success "A"
    COPY INTOの方が新しく高速。新規開発では**COPY INTO**を推奨。
    PolyBaseは外部テーブルとして永続的なスキーマが必要。
