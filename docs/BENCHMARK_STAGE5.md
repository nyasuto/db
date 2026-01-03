# Stage 5 Benchmark Report (mmap)

## 実行環境
- **OS**: macOS (darwin/arm64)
- **CPU**: Apple M4 Pro
- **Date**: 2026-01-03

## 概要
Stage 5 "The Zero-Copy" では、不変ファイル（`olderFiles`）へのアクセス最適化を行いました。
従来の `os.File.ReadAt`（システムコール + コピー）から、**`mmap` (Memory-Mapped Files)** を用いたダイレクトメモリアクセスに変更しました。

## パフォーマンス計測
ベンチマーク条件:
- データセット: 2,000レコード (1KB Value), Total ~2MB
- 測定対象: **古いセグメントファイル (`olderFiles`) からのランダムリード**
- ファイルサイズを小さく抑え強制的にローテーションさせ、古いファイルへのアクセスを発生させた。

| Metric | Latency / Throughput | Note |
|--------|----------------------|------|
| **Get Latency (Single)** | **461.6 ns/op** | 非常に高速 (Memory Access Speed) |
| **Get Latency (Parallel)** | **620.7 ns/op** | Lock Contention Overhead |

### 考察
- **ゼロコピー効果**: 461ns という数値は、通常のディスクI/O (数µs〜) では到達不可能な速度です。データ全体がOSのページキャッシュに乗っている状態とはいえ、`read` システムコールのオーバーヘッドさえも回避し、単なる `memcpy` 相当の速度でデータ取得ができています。
- **ロックのボトルネック**: 読み出しがあまりに高速化されたため、相対的に `sync.RWMutex` のロックコストが見え始めています（Parallelの方が遅い現象）。これは次の Stage 6 (Sharding) で解消すべき課題です。

## 生データ (Raw Output)
```text
BenchmarkGetOlder
BenchmarkGetOlder-14                     2634426               461.6 ns/op          3368 B/op          5 allocs/op
BenchmarkGetOlderParallel
BenchmarkGetOlderParallel-14             1866003               620.7 ns/op          3368 B/op          5 allocs/op
```
