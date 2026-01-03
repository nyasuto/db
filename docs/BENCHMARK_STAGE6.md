# Stage 6 Benchmark Report (Sharding)

## 実行環境
- **OS**: macOS (darwin/arm64)
- **CPU**: Apple M4 Pro (14 threads)
- **Date**: 2026-01-03

## 概要
Stage 6 "The Scaler" では、データベースの並行処理性能を向上させるため、**Sharding (パーティショニング)** を導入しました。
キーハッシュに基づいてデータベースを複数の独立した Shard に分割することで、単一のグローバルロック (`sync.RWMutex`) への競合を分散させました。

## パフォーマンス比較 (Parallel Execution)
同時実行数: RunParallel (GOMAXPROCS依存, ~14)

| Operation | Single DB (ns/op) | Sharded DB (16 Shards) | Improvement |
|-----------|-------------------|------------------------|-------------|
| **Put (Write)** | 1660 ns | **1508 ns** | **1.10x Faster** |
| **Get (Read)** | 2039 ns | **1568 ns** | **1.30x Faster** |

### 考察
- **Read Scalability**: `Get` 処理において 30% の高速化を達成しました。Sharding によりロックの粒度が細かくなり、読み取りスレッド間の待機時間が減少したためです。
- **Write Scalability**: `Put` 処理でも 10% の改善が見られました。I/O 帯域がボトルネックになりやすい書き込み処理においても、ロック競合の緩和がスループット向上に寄与しています。
- **Sharding Overhead**: Shard選択（ハッシュ計算）のコストは非常に小さく（数十ns程度）、メリットが大きく上回っています。

## 生データ (Raw Output)
```text
BenchmarkPutParallel
BenchmarkPutParallel-14                   714996              1660 ns/op
BenchmarkGetParallel
BenchmarkGetParallel-14                   576184              2039 ns/op

BenchmarkShardedPutParallel
BenchmarkShardedPutParallel-14            670310              1508 ns/op
BenchmarkShardedGetParallel
BenchmarkShardedGetParallel-14            677016              1568 ns/op
```
