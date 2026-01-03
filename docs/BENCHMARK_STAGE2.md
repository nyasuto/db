# Stage 2 Benchmark Report

## 実行環境
- **OS**: macOS (darwin/arm64)
- **CPU**: Apple M4 Pro
- **Date**: 2026-01-03

## 概要
Stage 2 (削除機能・コンパクション実装) 完了時点でのベンチマーク結果です。
削除ロジック (Tombstone判定) が追加されましたが、Coreの読み書き性能には大きな影響がないことを確認しました。

## パフォーマンス・サマリー

### 書き込み性能 (Put)
| Payload Size | Latency (平均応答時間) | Throughput (推定処理能力) | Memory Allocations |
|--------------|------------------------|---------------------------|--------------------|
| **Small** (~8B) | **1.32 µs**            | **約 944,300 ops/sec**    | 218 B/op           |
| **1 KB**       | **1.88 µs**            | **約 688,866 ops/sec**    | 1,273 B/op         |

### 読み込み性能 (Get)
| Payload Size | Latency (平均応答時間) | Throughput (推定処理能力) | Memory Allocations |
|--------------|------------------------|---------------------------|--------------------|
| **Small** (~8B) | **0.71 µs**            | **約 1,665,549 ops/sec**  | 21 B/op            |
| **1 KB**       | **0.93 µs**            | **約 1,322,572 ops/sec**  | 2,193 B/op         |

### 考察
- **Impact Analysis**: Stage 1 と比較しても性能劣化は見られません。
    - Put: 条件分岐 (Tombstoneサイズチェック) のコストは無視できるレベルです。
    - Get: 変更なし (KeyDirからの直接Lookup) のため、高速性を維持しています。
- **Note**: `Merge` (コンパクション) 処理自体のベンチマークはこのレポートには含まれていませんが、機能テスト (`TestMerge`) にて動作の正確性は検証済みです。

---

## 生データ (Raw Output)
```text
go test -v -bench=. -benchmem ./internal/...
=== RUN   TestPutGet
--- PASS: TestPutGet (0.00s)
=== RUN   TestRecovery
--- PASS: TestRecovery (0.00s)
=== RUN   TestDelete
--- PASS: TestDelete (0.00s)
=== RUN   TestMerge
--- PASS: TestMerge (0.00s)
goos: darwin
goarch: arm64
pkg: bitcask-go/internal/storage
cpu: Apple M4 Pro
BenchmarkPut
BenchmarkPut-14           944300              1319 ns/op             218 B/op          5 allocs/op
BenchmarkPut1KB
BenchmarkPut1KB-14        688866              1883 ns/op            1273 B/op          4 allocs/op
BenchmarkGet
BenchmarkGet-14          1665549               711.3 ns/op            21 B/op          2 allocs/op
BenchmarkGet1KB
BenchmarkGet1KB-14       1322572               928.1 ns/op          2193 B/op          3 allocs/op
PASS
ok      bitcask-go/internal/storage     8.082s
```
