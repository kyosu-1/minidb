# minidb 技術ドキュメント

minidb の各コンポーネントの設計と実装を解説するドキュメント集です。

各ドキュメントは**概念の説明（なぜ必要か）→ データ構造 → アルゴリズム → minidb での実装**の構成で、要素技術ごとに独立して読めるように書かれています。

---

## 目次

| ドキュメント | 内容 |
|---|---|
| [ストレージエンジン](storage.md) | スロットページ、ディスクマネージャ、バッファプール、テーブルヒープ、カタログ |
| [WAL と ARIES リカバリ](wal-and-recovery.md) | Write-Ahead Logging のプロトコル、ログレコード形式、ARIES 3 フェーズリカバリ |
| [トランザクションと MVCC](transactions-and-mvcc.md) | トランザクション管理、スナップショット分離、MVCC 可視性ルール |
| [B-Tree インデックス](btree-index.md) | B-Tree の基礎、ノードフォーマット、検索・挿入・分割アルゴリズム |
| [SQL パーサーと実行エンジン](sql.md) | 字句解析、再帰下降パーサー、各 SQL 文の実行フロー |

---

## 対応ソースコード

```
internal/
├── storage/
│   ├── page.go     → ストレージエンジン
│   ├── disk.go     → ストレージエンジン
│   ├── buffer.go   → ストレージエンジン
│   └── heap.go     → ストレージエンジン（テーブルヒープ & カタログ）
├── wal/
│   ├── log.go      → WAL と ARIES リカバリ
│   ├── writer.go   → WAL と ARIES リカバリ
│   └── recovery.go → WAL と ARIES リカバリ
├── txn/
│   ├── transaction.go → トランザクションと MVCC
│   └── mvcc.go        → トランザクションと MVCC
├── index/
│   └── btree.go    → B-Tree インデックス
└── sql/
    ├── lexer.go    → SQL パーサーと実行エンジン
    ├── parser.go   → SQL パーサーと実行エンジン
    └── executor.go → SQL パーサーと実行エンジン
```
