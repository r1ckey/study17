# セットアップ手順

## 1. ローカル確認

```bash
# 依存関係インストール
pip install -r requirements.txt

# ローカルで確認
mkdocs serve
# → http://127.0.0.1:8000 で確認
```

## 2. GitHubリポジトリ作成

1. GitHubで新しいリポジトリを作成（例: `de-learning-journey`）
2. Publicに設定

## 3. mkdocs.yml の修正

```yaml
site_url: https://YOUR_GITHUB_USERNAME.github.io/de-learning-journey/
```

`YOUR_GITHUB_USERNAME` を自分のGitHubユーザー名に変更。

## 4. GitHubにプッシュ

```bash
git init
git add .
git commit -m "feat: initial learning site"
git branch -M main
git remote add origin https://github.com/YOUR_USERNAME/de-learning-journey.git
git push -u origin main
```

## 5. GitHub Pages の有効化

1. リポジトリの Settings → Pages
2. Source: `gh-pages` ブランチを選択
3. GitHub Actionsが自動でデプロイ（mainへのpushで発火）

## 6. 完成

`https://YOUR_USERNAME.github.io/de-learning-journey/` でアクセス可能。

---

## ブログ記事の書き方

```
docs/blog/posts/ フォルダに新しい .md ファイルを作成。

---
date: 2024-04-20
---

# 今日学んだこと

内容...
```

mainにpushするたびに自動で更新されます。
