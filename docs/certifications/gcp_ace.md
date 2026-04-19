# Google Cloud Associate Cloud Engineer

## 試験概要

| 項目 | 内容 |
|------|------|
| 試験時間 | 120分 |
| 問題数 | 50問 |
| 合格ライン | 非公開（70%目安） |
| 有効期限 | 2年 |
| 受験料 | $200 |

---

## 出題範囲

| セクション | 配点 |
|-----------|------|
| クラウドソリューション環境の設定 | 17.5% |
| クラウドソリューションの計画と構成 | 17.5% |
| クラウドソリューションのデプロイと実装 | 17.5% |
| クラウドソリューションの運用の成功確認 | 20% |
| アクセスとセキュリティの構成 | 27.5% |

---

## 重要サービス一覧

### コンピューティング

| サービス | 用途 | 覚えるポイント |
|---------|------|-------------|
| **Compute Engine** | VM（IaaS）| マシンタイプ・プリエンプティブVM |
| **GKE** | Kubernetes | ノードプール・Autopilot |
| **Cloud Run** | コンテナ（サーバーレス）| リクエストベース課金 |
| **App Engine** | PaaS | Standard vs Flexible |
| **Cloud Functions** | 関数（サーバーレス）| イベント駆動 |

### ストレージ

| サービス | 用途 | 覚えるポイント |
|---------|------|-------------|
| **Cloud Storage** | オブジェクトストレージ | ストレージクラス（Standard/Nearline/Coldline/Archive）|
| **Persistent Disk** | ブロックストレージ | Compute Engineに接続 |
| **Filestore** | ファイルストレージ（NFS）| 共有ファイルシステム |

### データベース

| サービス | 用途 |
|---------|------|
| **Cloud SQL** | マネージドRDB（MySQL/PostgreSQL/SQL Server）|
| **Cloud Spanner** | グローバル分散RDB |
| **Firestore** | NoSQL（ドキュメント型）|
| **Bigtable** | NoSQL（大規模・低レイテンシ）|
| **BigQuery** | データウェアハウス |
| **Memorystore** | Redis/Memcachedマネージド |

### ネットワーク

| サービス | 用途 |
|---------|------|
| **VPC** | 仮想プライベートクラウド |
| **Cloud Load Balancing** | ロードバランサ |
| **Cloud CDN** | コンテンツ配信 |
| **Cloud VPN** | VPN接続 |
| **Cloud Interconnect** | 専用線接続 |

---

## IAM（Identity and Access Management）

### 役割の種類

| 種類 | 説明 |
|------|------|
| 基本ロール | Owner / Editor / Viewer（粒度が粗い・本番非推奨）|
| 事前定義ロール | サービス別の細かい権限（推奨）|
| カスタムロール | 独自に定義する権限 |

### 最小権限の原則

```
必要最小限の権限のみ付与する
Owner / Editor は基本的に使わない
サービスアカウントを適切に使う
```

### よく出るIAMコマンド

```bash
# プロジェクトのIAMポリシー確認
gcloud projects get-iam-policy PROJECT_ID

# ロール付与
gcloud projects add-iam-policy-binding PROJECT_ID \
    --member="user:user@example.com" \
    --role="roles/storage.objectViewer"
```

---

## gcloud コマンド基礎

```bash
# プロジェクト設定
gcloud config set project PROJECT_ID
gcloud config get-value project

# Compute Engine
gcloud compute instances list
gcloud compute instances create INSTANCE_NAME \
    --machine-type=e2-medium \
    --zone=asia-northeast1-a

# Cloud Storage
gsutil ls gs://BUCKET_NAME
gsutil cp file.txt gs://BUCKET_NAME/
gsutil mb -l asia-northeast1 gs://BUCKET_NAME

# GKE
gcloud container clusters create CLUSTER_NAME --zone=asia-northeast1-a
gcloud container clusters get-credentials CLUSTER_NAME --zone=asia-northeast1-a
kubectl get nodes
```

---

## Cloud Storage のストレージクラス

| クラス | アクセス頻度 | 最小保存期間 | 用途 |
|-------|------------|------------|------|
| Standard | 高頻度 | なし | 頻繁にアクセスするデータ |
| Nearline | 月1回以下 | 30日 | バックアップ |
| Coldline | 四半期1回以下 | 90日 | 災害復旧 |
| Archive | 年1回以下 | 365日 | 長期アーカイブ |

---

## 試験のコツ

!!! tip "GCP ACEの特徴"
    - コマンドラインの問題が多い（gcloud・gsutil）
    - IAMと権限管理は必ず出る
    - コスト最適化の問題（プリエンプティブVM・ストレージクラス選択）
    - 「最小権限の原則」を常に意識する

!!! tip "合格戦略"
    模擬試験を繰り返して80%安定したら受験。
    わからない問題は「コスト最適化」と「最小権限」の観点で選ぶと正解率が上がる。

---

## 頻出問題パターン

!!! question "Q: 数TBのデータを定期的にGCSにバックアップする最もコスト効率の良いストレージクラスは？"
??? success "A"
    **Nearline**（月1回程度のアクセスならNearline。
    四半期1回ならColdline、年1回ならArchive）

!!! question "Q: VMを一時的に使いたい場合のコスト削減方法は？"
??? success "A"
    **プリエンプティブルVM**（最大80%割引。ただし24時間で停止される可能性あり）

!!! question "Q: Compute EngineインスタンスにCloud Storage へのアクセス権を与える正しい方法は？"
??? success "A"
    **サービスアカウント**をインスタンスにアタッチし、
    サービスアカウントに必要なIAMロールを付与する。
    （インスタンスに直接IAMを付与するのではない）
