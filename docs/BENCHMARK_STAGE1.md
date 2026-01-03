# Stage 1 Benchmark Report

## 実行環境
- **OS**: macOS (darwin/arm64)
- **CPU**: Apple M4 Pro
- **Date**: 2026-01-03

## パフォーマンス・サマリー

### 書き込み性能 (Put)
| Payload Size | Latency (平均応答時間) | Throughput (推定処理能力) | Memory Allocations |
|--------------|------------------------|---------------------------|--------------------|
| **Small** (~8B) | **1.34 µs**            | **約 747,300 ops/sec**    | 189 B/op           |
| **1 KB**       | **1.99 µs**            | **約 503,000 ops/sec**    | 1,278 B/op         |

### 読み込み性能 (Get)
| Payload Size | Latency (平均応答時間) | Throughput (推定処理能力) | Memory Allocations |
|--------------|------------------------|---------------------------|--------------------|
| **Small** (~8B) | **0.73 µs**            | **約 1,373,000 ops/sec**  | 21 B/op            |
| **1 KB**       | **0.95 µs**            | **約 1,051,000 ops/sec**  | 2,193 B/op         |

### 評価と考察
1. **スケーラビリティ**:
   - データサイズが数バイトから1KBへ約100倍になっても、レイテンシの増加は **1.5倍程度** (Put: 1.34µs -> 1.99µs, Get: 0.73µs -> 0.95µs) に留まっています。
   - これは、ログ型構造（LSM/Bitcaskモデル）の強みである「シーケンシャル書き込み」と「ダイレクト読み込み」効率の良さを示しています。

2. **ボトルネック**:
   - 現在のボトルネックはディスクI/Oよりも、システムコールやメモリアロケーションのオーバーヘッドが支配的であると考えられます（1KB程度であればOSのページキャッシュに乗るため）。

---

## 生データ (Raw Output)
```text
=== RUN   TestPutGet
--- PASS: TestPutGet (0.00s)
=== RUN   TestRecovery
--- PASS: TestRecovery (0.00s)
goos: darwin
goarch: arm64
pkg: bitcask-go/internal/storage
cpu: Apple M4 Pro
BenchmarkPut
BenchmarkPut-14           897561              1338 ns/op             189 B/op          5 allocs/op
BenchmarkPut1KB
BenchmarkPut1KB-14        643522              1988 ns/op            1278 B/op          4 allocs/op
BenchmarkGet
BenchmarkGet-14          1690966               728.2 ns/op            21 B/op          2 allocs/op
BenchmarkGet1KB
BenchmarkGet1KB-14       1294184               951.3 ns/op          2193 B/op          3 allocs/op
PASS
ok      bitcask-go/internal/storage     7.241s
```
