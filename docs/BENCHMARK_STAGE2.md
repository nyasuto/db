# Stage 2 Benchmark Report (with Concurrency Analysis)

## 実行環境
- **OS**: macOS (darwin/arm64)
- **CPU**: Apple M4 Pro
- **Date**: 2026-01-03

## 概要
Stage 2 時点での性能評価レポートです。
通常のシーケンシャルアクセス性能に加え、並列アクセス (`RunParallel`) 時の挙動についても検証を行いました。

## パフォーマンス・サマリー

### 基本性能 (Sequential / Payload: Mixed)
| Payload Size | Latency (平均応答時間) | Throughput (推定処理能力) | Memory Allocations |
|--------------|------------------------|---------------------------|--------------------|
| **Put (Small)**| **1.21 µs**            | **約 824,000 ops/sec**    | 183 B/op           |
| **Put (1 KB)** | **1.81 µs**            | **約 552,000 ops/sec**    | 1,271 B/op         |
| **Get (Small)**| **0.71 µs**            | **約 1,410,000 ops/sec**  | 21 B/op            |
| **Get (1 KB)** | **0.89 µs**            | **約 1,120,000 ops/sec**  | 2,193 B/op         |

### 並列アクセス性能 (Concurrency)
`gomaxprocs` (CPUコア数) に応じた並列負荷をかけた場合の結果です。

| Operation | Sequential Latency | Parallel Latency | Throughput Check |
|-----------|--------------------|-------------------|------------------|
| **Put**   | 1.21 µs            | **1.63 µs**       | 📉 低下 (-25%)   |
| **Get**   | 0.71 µs            | **2.16 µs**       | 📉 低下 (-67%)   |

## 考察と分析

### 1. 書き込み (Put) の並列性
- **傾向**: 並列化により性能が低下しました。
- **原因**: Bitcaskモデル（LSMツリーのWAL部分と同様）は、単一ファイルへの追記型アーキテクチャを持ちます。
  `sync.Mutex` により書き込みが直列化されるため、並列数を増やしてもディスクI/Oのスループット向上は見込めず、むしろロック競合（Contention）とCPUコンテキストスイッチのオーバーヘッドが上乗せされる結果となりました。

### 2. 読み込み (Get) の並列性
- **傾向**: こちらもスループットが悪化する結果となりました。
- **原因**:
    - **ロック競合**: `sync.RWMutex.RLock` は共有ロックですが、高頻度な取得/解放はCPUキャッシュラインの競合を引き起こします。
    - **システムコール**: 小さなデータの `read` syscall を大量の並列度で発行した場合、OS側のファイルシステムレイヤーでのオーバーヘッドが支配的になります。
    - **ベンチマークの性質**: `fmt.Sprintf` 等の文字列操作によるメモリアロケーションが並列実行時に競合した可能性も含まれます。

### 結論
現状のアーキテクチャにおいて、**過度な並列化は逆効果**となる可能性があります。
将来的には、バッチ書き込み（Batch Commit）機能の導入や、コネクションプーリングによる同時実行数制御がパフォーマンス向上に有効であると考えられます。

---

## 生データ (Raw Output)
```text
go test -v -bench=. -benchmem ./internal/...
BenchmarkPut-14                   886838              1213 ns/op             183 B/op          5 allocs/op
BenchmarkPut1KB-14                701398              1810 ns/op            1271 B/op          4 allocs/op
BenchmarkGet-14                  1662559               707.1 ns/op            21 B/op          2 allocs/op
BenchmarkGet1KB-14               1332290               889.4 ns/op          2193 B/op          3 allocs/op
BenchmarkPutParallel-14           805942              1634 ns/op             106 B/op          5 allocs/op
BenchmarkGetParallel-14           537409              2155 ns/op             288 B/op          3 allocs/op
```
