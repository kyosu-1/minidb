# MiniDB

教育目的で作成されたディスクベースのデータベースエンジン。以下の機能を実装しています：

- **ディスクベースストレージ** - ページ構造、バッファプール（LRU）
- **WAL (Write-Ahead Logging)** - クラッシュリカバリのためのログ先行書き込み
- **ARIES Recovery** - 3フェーズリカバリ（Analysis → Redo → Undo）
- **MVCC** - スナップショット分離による並行制御
- **B-Treeインデックス** - 高速検索
- **SQLパーサー** - CREATE, INSERT, SELECT, UPDATE, DELETE

---

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────────────┐
│                          SQL Layer                               │
│   ┌──────────┐    ┌──────────┐    ┌──────────┐                  │
│   │  Lexer   │───▶│  Parser  │───▶│ Executor │                  │
│   └──────────┘    └──────────┘    └──────────┘                  │
└────────────────────────────────────────┬────────────────────────┘
                                         │
┌────────────────────────────────────────┼────────────────────────┐
│                    Transaction Layer   │                         │
│   ┌──────────────────┐    ┌───────────┴──────────┐              │
│   │   TxnManager     │    │     MVCC Store       │              │
│   │ BEGIN/COMMIT/    │    │  Snapshot Isolation  │              │
│   │ ROLLBACK         │    │  Visibility Check    │              │
│   └──────────────────┘    └──────────────────────┘              │
└────────────────────────────────────────┬────────────────────────┘
                                         │
┌────────────────────────────────────────┼────────────────────────┐
│                    Storage Layer       │                         │
│   ┌──────────────┐  ┌─────────────┐  ┌┴────────────┐            │
│   │ Buffer Pool  │  │   Catalog   │  │  B-Tree     │            │
│   │   (LRU)      │  │  (Schema)   │  │  Index      │            │
│   └──────┬───────┘  └─────────────┘  └─────────────┘            │
│          │                                                       │
│   ┌──────┴───────┐  ┌─────────────┐                             │
│   │ DiskManager  │  │  TableHeap  │                             │
│   │  Page I/O    │  │  (Rows)     │                             │
│   └──────────────┘  └─────────────┘                             │
└────────────────────────────────────────┬────────────────────────┘
                                         │
┌────────────────────────────────────────┼────────────────────────┐
│                      WAL Layer         │                         │
│   ┌──────────────────┐    ┌───────────┴──────────┐              │
│   │   WAL Writer     │    │  RecoveryManager     │              │
│   │  Log + fsync()   │    │  ARIES Algorithm     │              │
│   └──────────────────┘    └──────────────────────┘              │
└────────────────────────────────────────┬────────────────────────┘
                                         │
                                         ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Disk Files                               │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────┐         │
│   │   data.db    │  │   wal.log    │  │ minidb.meta  │         │
│   │  (4KB pages) │  │  (log recs)  │  │ (catalog ID) │         │
│   └──────────────┘  └──────────────┘  └──────────────┘         │
└─────────────────────────────────────────────────────────────────┘
```

---

## ディレクトリ構造

```
minidb/
├── cmd/minidb/main.go           # エントリーポイント（REPL）
├── internal/
│   ├── engine/engine.go         # データベースエンジン
│   ├── storage/
│   │   ├── page.go              # ページ構造（4KB固定サイズ）
│   │   ├── disk.go              # ディスクマネージャー
│   │   ├── buffer.go            # バッファプール（LRU）
│   │   └── heap.go              # テーブルヒープ & カタログ
│   ├── index/
│   │   └── btree.go             # B-Treeインデックス
│   ├── sql/
│   │   ├── lexer.go             # 字句解析
│   │   ├── parser.go            # 構文解析
│   │   └── executor.go          # 実行エンジン
│   ├── txn/
│   │   ├── transaction.go       # トランザクション管理
│   │   └── mvcc.go              # MVCC実装
│   └── wal/
│       ├── log.go               # ログレコード定義
│       ├── writer.go            # ログ書き込み
│       └── recovery.go          # ARIESリカバリ
├── pkg/types/types.go           # 共通型定義
├── go.mod
└── README.md
```

---

## ビルド & 実行

```bash
# ビルド
cd minidb
go build -o minidb ./cmd/minidb

# 実行
./minidb -data ./mydata -buffer 1024

# オプション:
#   -data    データディレクトリ (default: ./minidb-data)
#   -buffer  バッファプールサイズ (default: 1024 pages = 4MB)
```

---

## 使用例

```sql
minidb> CREATE TABLE users (id INT, name TEXT, active BOOL)
CREATE TABLE users (id=1)

minidb> INSERT INTO users (id, name, active) VALUES (1, 'Alice', true)
INSERT 1 (page=1, slot=0)

minidb> INSERT INTO users (id, name, active) VALUES (2, 'Bob', false)
INSERT 1 (page=1, slot=1)

minidb> SELECT * FROM users
├──────┼───────┼────────┤
│ id   │ name  │ active │
├──────┼───────┼────────┤
│ 1    │ Alice │ true   │
│ 2    │ Bob   │ false  │
├──────┼───────┼────────┤
SELECT 2 rows

minidb> BEGIN
BEGIN (txn 5)

minidb> UPDATE users SET active = true WHERE id = 2
UPDATE 1

minidb> COMMIT
COMMIT (txn 5)

minidb> stats
╔══════════════════════════════════════════╗
║         Database Statistics              ║
╠══════════════════════════════════════════╣
║  WAL Current LSN:    15                  ║
║  WAL Flushed LSN:    14                  ║
║  Active Txns:        0                   ║
╠══════════════════════════════════════════╣
║  Disk Pages:         2                   ║
║  Tables:             1                   ║
╠══════════════════════════════════════════╣
║  Buffer Pool Hits:   12                  ║
║  Buffer Pool Misses: 2                   ║
║  Buffer Pool Cached: 2                   ║
║  Buffer Hit Rate:    85.7%               ║
╚══════════════════════════════════════════╝
```

---

## 主要コンポーネントの解説

### 1. ページ構造 (`storage/page.go`)

```
┌────────────────────────────────────────┐
│ Header (28 bytes)                      │
│   PageID(4) | Type(1) | Reserved(3)   │
│   LSN(8) | SlotCount(2)               │
│   FreeSpaceOffset(2) | FreeSpaceEnd(2)│
│   NextPageID(4) | Reserved(2)         │
├────────────────────────────────────────┤
│                                        │
│           Free Space                   │
│                                        │
├────────────────────────────────────────┤
│ ← Tuple Data (grows downward)          │
├────────────────────────────────────────┤
│ Slot Array (grows upward) →            │
│   [offset|len] [offset|len] ...        │
└────────────────────────────────────────┘
```

ヒープページは `NextPageID` フィールドによりリンクリストで連結されます（連続ページ番号を仮定しません）。

### 2. バッファプール (`storage/buffer.go`)

```go
type BufferPool struct {
    pages    map[PageID]*Page  // ページキャッシュ
    lruList  *list.List        // LRU順序
    capacity int               // 最大ページ数
}

// ページ取得: キャッシュにあればヒット、なければディスクから読む
func (bp *BufferPool) FetchPage(pageID) (*Page, error) {
    if page := bp.pages[pageID]; page != nil {
        bp.hits++
        bp.touchLRU(pageID)  // LRUを更新
        return page, nil
    }
    bp.misses++
    page := bp.diskManager.ReadPage(pageID)
    bp.evictIfNeeded()  // 必要ならLRUページを追い出し
    bp.pages[pageID] = page
    return page, nil
}
```

### 3. WAL書き込み (`wal/writer.go`)

```go
// コミット時の処理
func (w *Writer) LogCommit(txnID) (LSN, error) {
    // 1. COMMITレコードをバッファに追加
    lsn := w.Append(&LogRecord{Type: COMMIT, TxnID: txnID})
    
    // 2. 【重要】ディスクに強制書き込み（fsync）
    //    これにより、クラッシュしてもコミットが失われない
    w.Force(lsn)
    
    return lsn, nil
}
```

### 4. MVCC可視性 (`txn/mvcc.go`)

```go
func (s *Snapshot) IsVisible(tuple *Tuple) bool {
    // 作成トランザクションがコミット済みか？
    if !s.isTxnCommitted(tuple.XMin) {
        return false
    }
    
    // 削除されていないか？
    if tuple.XMax == 0 {
        return true  // 削除されていない
    }
    
    // 削除トランザクションがまだ見えない？
    if !s.isTxnCommitted(tuple.XMax) {
        return true  // まだ削除は見えない
    }
    
    return false  // 削除済み
}
```

### 5. B-Treeインデックス (`index/btree.go`)

```
            [30|60]              ← 内部ノード
           /   |   \
      [10|20] [40|50] [70|80]    ← リーフノード
         ↓      ↓       ↓
       RIDs   RIDs    RIDs      ← 行位置
```

### 6. ARIESリカバリ (`wal/recovery.go`)

```
Phase 1: Analysis
  └─ ログを走査してATT（アクティブTxn）とDPT（ダーティページ）を再構築

Phase 2: Redo
  └─ DPTの最小RecLSNから全ての変更を再適用（コミット済み/未コミット両方）

Phase 3: Undo
  └─ ATT内の未コミットTxnをPrevLSNチェーンを逆順に辿ってロールバック
```

---

## ディスクファイル形式

### data.db (ページファイル)

```
┌─────────────────────────────────────────┐
│ File Header (16 bytes)                  │
│   Magic: "MINIDBPD" | Version | NumPages│
├─────────────────────────────────────────┤
│ Page 0 (4096 bytes) - Catalog           │
├─────────────────────────────────────────┤
│ Page 1 (4096 bytes) - Table Data        │
├─────────────────────────────────────────┤
│ Page 2 (4096 bytes) - ...               │
└─────────────────────────────────────────┘
```

### wal.log (WALファイル)

```
┌─────────────────────────────────────────┐
│ File Header (16 bytes)                  │
│   Magic: "MINIDBWA" | Version           │
├─────────────────────────────────────────┤
│ LogRecord 1: [len][LSN|PrevLSN|TxnID|...│
├─────────────────────────────────────────┤
│ LogRecord 2: [len][LSN|PrevLSN|TxnID|...│
├─────────────────────────────────────────┤
│ ...                                     │
└─────────────────────────────────────────┘
```

---

## 学習リソース

- **ARIES論文**: "ARIES: A Transaction Recovery Method" (Mohan et al., 1992)
- **書籍**: "Database Internals" by Alex Petrov
- **PostgreSQLソース**: `src/backend/access/transam/`, `src/backend/storage/buffer/`
- **CMU 15-445**: Database Systems (Andy Pavlo)

---

## 今後の拡張案

| 機能 | 説明 |
|------|------|
| WAL圧縮 | 古いログの削除・圧縮 |
| 並列スキャン | 複数スレッドでのテーブルスキャン |
| クエリオプティマイザ | 実行計画の最適化 |
| JOIN | 複数テーブルの結合 |
| セカンダリインデックス | 任意カラムへのインデックス |
| VACUUM | 古いMVCCバージョンのガベージコレクション |

---

## ライセンス

MIT License
