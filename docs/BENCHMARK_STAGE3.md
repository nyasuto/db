# Stage 3 Benchmark Report (Segmentation & Compaction)

## 実行環境
- **OS**: macOS (darwin/arm64)
- **CPU**: Apple M4 Pro
- **Date**: 2026-01-03

## 概要
Stage 3 "The Cleaner" 完了時点での性能評価レポートです。
単一ファイル構成から、複数セグメントファイル（ディレクトリベース）への移行を行いました。

## 比較評価 (vs Stage 2)

| Operation | Stage 2 Latency | Stage 3 Latency | 変化率 | メモ |
|-----------|-----------------|-----------------|--------|------|
| **Put (Small)** | 1,213 ns        | **1,331 ns**    | -9.7%  | Active File ローテーション判定などのオーバーヘッド増 |
| **Put (1 KB)**  | 1,810 ns        | **2,027 ns**    | -11.9% | 同上 |
| **Get (Small)** | 707 ns          | **733 ns**      | -3.7%  | 軽微なオーバーヘッド。ファイルハンドルの解決コストなど |
| **Get (1 KB)**  | 889 ns          | **1,210 ns**    | -36.1% | 有意な低下。メモリアロケーション増加 (2kB -> 3kB) が影響か |

### 考察
- **オーバーヘッドの許容**: セグメンテーション導入により、書き込み・読み込み共に若干のレイテンシ増加が見られますが、機能拡張（無限の追記からの脱却）とのトレードオフとして許容範囲内と考えられます。
- **Get 1KB の低下**: アロケーション回数とサイズが増加しています。`Get` 処理内でファイル特定やパス結合などの処理が影響している可能性があります。Hint File やキャッシュ導入（Stage 4）での改善が期待されます。

## 生データ (Raw Output)
```text
goos: darwin
goarch: arm64
pkg: bitcask-go/internal/storage
cpu: Apple M4 Pro
BenchmarkPut-14                   948044              1331 ns/op             274 B/op          6 allocs/op
BenchmarkPut1KB-14                563701              2027 ns/op            1340 B/op          4 allocs/op
BenchmarkGet-14                  1631932               733.6 ns/op            53 B/op          3 allocs/op
BenchmarkGet1KB-14                956721              1210 ns/op            3346 B/op          4 allocs/op
BenchmarkPutParallel-14           783421              1753 ns/op             117 B/op          5 allocs/op
BenchmarkGetParallel-14           604608              1958 ns/op             448 B/op          4 allocs/op
PASS
ok      bitcask-go/internal/storage     9.539s
```
