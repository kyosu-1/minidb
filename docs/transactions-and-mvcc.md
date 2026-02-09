# トランザクションと MVCC

トランザクションの分離性と一貫した読み取りを実現する仕組み。各トランザクションは開始時点のスナップショットを通じてデータを見る。

対応ソース: `internal/txn/transaction.go`, `mvcc.go`, `internal/sql/executor.go`

---

## 1. トランザクション管理

### Begin / Commit / Rollback のライフサイクル

```mermaid
stateDiagram-v2
    [*] --> Running: BEGIN
    Running --> Committed: COMMIT
    Running --> Aborted: ROLLBACK
    Committed --> [*]
    Aborted --> [*]
```

### TxnID の割り当て

TxnID は `uint64` のアトミックカウンタで生成される。単調増加するため、TxnID の大小関係は時間順序を表す。

```go
txnID := types.TxnID(atomic.AddUint64(&m.nextTxnID, 1))
```

### Begin の処理

```mermaid
sequenceDiagram
    participant E as Executor
    participant TM as TxnManager
    participant W as WAL Writer

    E->>TM: Begin()
    TM->>TM: nextTxnID++ (atomic)
    TM->>TM: createSnapshot()
    TM->>TM: activeTxns[txnID] = txn
    TM->>TM: updateGlobalXmin()
    TM->>W: LogBegin(txnID)
    TM-->>E: Transaction
```

### Commit の処理

```mermaid
sequenceDiagram
    participant E as Executor
    participant TM as TxnManager
    participant W as WAL Writer
    participant BP as BufferPool

    E->>TM: Commit(txn)
    TM->>W: LogCommit(txnID)
    W->>W: Force(lsn) — fsync で永続化
    TM->>TM: status = Committed
    TM->>TM: activeTxns から削除
    TM->>TM: updateGlobalXmin()
    E->>BP: FlushAllPages()
```

**重要**: WAL の COMMIT レコードが Force された時点でコミットは確定する。データページのフラッシュはその後でよい（No-Force ポリシー）。

### Rollback の処理

```go
txn.Status = TxnStatusAborted
walWriter.LogAbort(txnID)
delete(activeTxns, txnID)
```

ロールバック時のデータの巻き戻しは、リカバリの Undo フェーズと同じメカニズムで行われる。

---

## 2. スナップショット分離

### Snapshot の構造

トランザクション開始時に、その時点で「見える」トランザクションの範囲を記録する。

```go
type Snapshot struct {
    Xmin       TxnID              // アクティブな最小 TxnID
    Xmax       TxnID              // 次に割り当てられる TxnID
    ActiveTxns map[TxnID]bool     // スナップショット時点のアクティブ TxnID
}
```

```mermaid
flowchart LR
    subgraph 可視["可視（コミット済み）"]
        T1["Txn 1"]
        T2["Txn 3"]
    end
    subgraph 不可視A["不可視（実行中）"]
        T3["Txn 5"]
        T4["Txn 7"]
    end
    subgraph 不可視B["不可視（未来）"]
        T5["Txn 9+"]
    end
    Xmin["Xmin=5"] ~~~ T3
    Xmax["Xmax=9"] ~~~ T5
```

- `TxnID < Xmin`: 確実にコミット済み → **可視**
- `TxnID >= Xmax`: スナップショット後に開始 → **不可視**
- `Xmin <= TxnID < Xmax` かつ `ActiveTxns` に含まれる → **不可視**（まだ実行中）
- `Xmin <= TxnID < Xmax` かつ `ActiveTxns` に含まれない → **可視**（コミット済み）

---

## 3. MVCC 可視性ルール

### タプルの MVCC メタデータ

```go
type Tuple struct {
    XMin    TxnID     // このバージョンを作成したトランザクション
    XMax    TxnID     // このバージョンを削除したトランザクション（0 = 生存中）
    Cid     CommandID // トランザクション内のコマンド順序
    TableID uint32
    RowID   uint64
    Data    []byte    // 実際の行データ（バイナリ）
}
```

### isTxnVisible — トランザクションの可視性判定

```mermaid
flowchart TD
    A["isTxnVisible(txnID)"] --> B{"txnID == InvalidTxnID (0)?"}
    B -- Yes --> C["return false"]
    B -- No --> D{"txnID >= Xmax?"}
    D -- Yes --> E["return false<br/>(未来のトランザクション)"]
    D -- No --> F{"ActiveTxns[txnID]?"}
    F -- Yes --> G["return false<br/>(まだ実行中)"]
    F -- No --> H["return true<br/>(コミット済み)"]
```

### IsVisible — タプルの可視性判定

```mermaid
flowchart TD
    A["IsVisible(tuple)"] --> B["isTxnVisible(tuple.XMin)"]
    B -- false --> C["return false<br/>(作成者が不可視)"]
    B -- true --> D{"tuple.XMax == InvalidTxnID?"}
    D -- Yes --> E["return true<br/>(削除されていない)"]
    D -- No --> F["isTxnVisible(tuple.XMax)"]
    F -- true --> G["return false<br/>(削除が可視 → タプルは見えない)"]
    F -- false --> H["return true<br/>(削除がまだ見えない → タプルは見える)"]
```

3 つのルールをまとめると：

| ルール | 条件 | 結果 |
|--------|------|------|
| 1 | 作成トランザクション（XMin）が不可視 | タプル不可視 |
| 2 | XMax が無効（未削除） | タプル可視 |
| 3 | 削除トランザクション（XMax）が可視 | タプル不可視 |

---

## 4. タプルのライフサイクル

### INSERT

```
Tuple { XMin=T1, XMax=0, Data=... }
```

T1 が INSERT すると、XMin=T1、XMax=0（InvalidTxnID）のタプルが作成される。T1 がコミットするまで、他のトランザクションからは見えない。

### DELETE

```
Before: Tuple { XMin=T1, XMax=0, Data=... }
After:  Tuple { XMin=T1, XMax=T2, Data=... }
```

T2 が DELETE すると、既存タプルの XMax を T2 に設定する。タプル自体は削除されず、T2 がコミットした後のスナップショットからは見えなくなる。

### UPDATE

UPDATE は「旧バージョンの DELETE + 新バージョンの INSERT」として実装される：

```
旧: Tuple { XMin=T1, XMax=T2, Data=old }  ← XMax を T2 に設定
新: Tuple { XMin=T2, XMax=0, Data=new }   ← 新タプルを挿入
```

```mermaid
sequenceDiagram
    participant E as Executor
    participant H as TableHeap

    Note over E: UPDATE users SET name='Bob' WHERE id=1
    E->>H: Scan() で旧タプルを取得
    E->>E: MVCC 可視性チェック + WHERE フィルタ
    E->>E: 旧タプルの XMax = 現在の TxnID
    E->>H: Update(pageID, slotNum, 旧タプル) — XMax を更新
    E->>E: 新タプルを作成 (XMin=TxnID, 新データ)
    E->>H: Insert(新タプル) — 新バージョンを挿入
    E->>E: WAL に LogUpdate(before, after)
```

---

## 5. 書込競合検出

### IsVisibleForUpdate

UPDATE/DELETE 時に、他のトランザクションが同じタプルを変更中でないかチェックする。

```go
func (s *Snapshot) IsVisibleForUpdate(tuple *Tuple, myTxnID TxnID) (visible bool, conflict TxnID) {
    // 基本的な可視性チェック
    if !s.IsVisible(tuple) {
        return false, InvalidTxnID
    }

    // 他のアクティブなトランザクションが XMax を設定している？
    if tuple.XMax != InvalidTxnID && tuple.XMax != myTxnID {
        if s.ActiveTxns[tuple.XMax] || tuple.XMax >= s.Xmax {
            // 書込-書込競合
            return false, tuple.XMax
        }
    }

    return true, InvalidTxnID
}
```

```mermaid
flowchart TD
    A["IsVisibleForUpdate(tuple, myTxnID)"] --> B{"IsVisible(tuple)?"}
    B -- No --> C["return (false, ∅)"]
    B -- Yes --> D{"XMax != 0 かつ XMax != myTxnID?"}
    D -- No --> E["return (true, ∅)<br/>(競合なし)"]
    D -- Yes --> F{"XMax がアクティブ or 未来?"}
    F -- Yes --> G["return (false, XMax)<br/>(書込競合!)"]
    F -- No --> E
```

---

## 6. VACUUM — デッドタプルのガベージコレクション

### 問題

UPDATE/DELETE は MVCC の仕組みにより旧バージョンの XMax を設定するだけで、物理的にはタプルを削除しない。これらの「デッドタプル」が蓄積し続けると、ストレージが肥大化し、Scan のパフォーマンスも劣化する。

### デッドタプルの判定条件

タプルが安全に回収可能な条件（3 つ全て満たす）:

| # | 条件 | 理由 |
|---|------|------|
| 1 | `XMax != InvalidTxnID` | 削除マークが付いている |
| 2 | `XMax < GlobalXmin` | 全アクティブトランザクションから不可視 |
| 3 | XMax のトランザクションがコミット済み | アボートされた DELETE/UPDATE は回収しない |

```mermaid
flowchart TD
    A["Scan() で全タプルを取得"] --> B{"XMax != 0?"}
    B -- No --> C["SKIP（生存中）"]
    B -- Yes --> D{"XMax < GlobalXmin?"}
    D -- No --> E["SKIP（まだ可視の可能性）"]
    D -- Yes --> F{"IsTxnCommitted(XMax)?"}
    F -- No --> G["SKIP（アボートされた変更）"]
    F -- Yes --> H["DeleteTuple(pageID, slotNum)<br/>物理削除"]
```

### アボートされたトランザクションの扱い

ROLLBACK 時、ヒープページ上の XMax 変更はランタイムでは戻されない（ARIES crash recovery の Undo でのみ復元される）。そのため、VACUUM がアボートされたトランザクションの XMax を持つタプルを誤って回収しないよう、`TxnManager` が `committedTxns` マップでコミット済みトランザクションを追跡する。

```go
// Commit 時に記録
m.committedTxns[txn.ID] = true

// VACUUM 時に確認
if m.IsTxnCommitted(xmaxTxnID) {
    // 安全に回収可能
}
```

### WAL ログ

VACUUM は WAL ログを書かない。VACUUM は冪等な操作であり、クラッシュ後に再実行しても同じ結果になるため、リカバリの対象にする必要がない。

### 使用例

```
minidb> DELETE FROM users WHERE id = 1
DELETE 1

minidb> vacuum
VACUUM: removed 1 dead tuples.
  users: scanned 3, removed 1

minidb> vacuum
VACUUM: removed 0 dead tuples.
```

---

## 7. Auto-Commit

### 単文の暗黙トランザクション

明示的な `BEGIN` なしで実行される SQL 文には、自動的にトランザクションが割り当てられる。

```go
func (e *Executor) getTransaction() (*txn.Transaction, bool) {
    if e.currentTxn != nil {
        return e.currentTxn, false   // 明示トランザクション
    }
    return e.txnManager.Begin(), true // Auto-Commit (autoCommit=true)
}
```

Auto-Commit の場合、SQL 文の実行後に自動的に `Commit()` が呼ばれる。これにより、単一の INSERT や SELECT でも MVCC の可視性ルールが正しく適用される。

```mermaid
flowchart TD
    A[SQL 文の実行] --> B{"currentTxn != nil?"}
    B -- Yes --> C[既存トランザクションを使用]
    B -- No --> D["Begin() で新規作成<br/>autoCommit = true"]
    D --> E[SQL 文を実行]
    E --> F["自動 Commit()"]
    F --> G["FlushAllPages()"]
```
