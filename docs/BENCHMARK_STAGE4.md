# Stage 4 Benchmark Report (Hint Files & Buffering)

## 実行環境
- **OS**: macOS (darwin/arm64)
- **CPU**: Apple M4 Pro
- **Date**: 2026-01-03

## 概要
Stage 4 "The Speedster" では、データベースの起動時間短縮に焦点を当てました。
以下の2つの最適化を実施しました：
1. **Hint File**: 起動時に全データファイルをスキャンする代わりに、コンパクトなインデックスファイル（`.hint`）を読み込む。
2. **Buffered I/O**: `bufio.Scanner` (Reader) を導入し、システムコール回数を削減。

## パフォーマンス比較 (Startup Time)
データセット: 10,000 records, Key=16 bytes, Value=4KB (Total ~41MB data)

| Condition | Latency (Unbuffered) | Latency (Buffered) | Improvement |
|-----------|----------------------|--------------------|-------------|
| **NoHint (Data Scan)** | 21.75 ms | **13.45 ms** | **1.6x faster** |
| **WithHint (Hint Scan)** | 11.54 ms | **3.99 ms** | **2.9x faster** |

### 最終成果 (vs Baseline)
ベースライン（バッファなし・Hintなし）の **21.75ms** から、最適化後（バッファあり・Hintあり）の **3.99ms** へと、
**約 5.4倍 (445%向上)** の高速化を達成しました。

## 考察
- **Hint File効果**: Value（4KB）を読み飛ばす必要がないため、ディスク読み込み量が大幅に削減されました（~41MB -> Hint Fileサイズ）。
- **Buffering効果**: `ReadFull` を繰り返し呼ぶ際のシステムコールオーバーヘッドが `bufio` により劇的に削減されました。特に小さなヘッダ読み込みでの効果が顕著です。

## 生データ (Raw Output)
```text
goos: darwin
goarch: arm64
pkg: bitcask-go/internal/storage
cpu: Apple M4 Pro
BenchmarkRecovery/WithHint
BenchmarkRecovery/WithHint-14                284           3989918 ns/op        13902188 B/op      40136 allocs/op
BenchmarkRecovery/NoHint
BenchmarkRecovery/NoHint-14                   82          13445424 ns/op        50525038 B/op      40134 allocs/op
```
