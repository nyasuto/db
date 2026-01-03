# A Bitcask-inspired High Performance KVS

「シンプルから高度なエンジンへ」の進化を記録する、Go製の自作KVSプロジェクト。

## 🚀 プロジェクトの目的
1. **DBエンジンの核心を理解する**: ファイルI/O、インデックス、永続化の実装。
2. **ベンチマーク駆動開発**: 段階的な最適化を行い、そのパフォーマンス向上を可視化する。
3. **将来のSQLエンジンへの布石**: 信頼性の高いストレージレイヤーの構築。

## 🛠 進化のロードマップ (Stages)

| Stage | 名前 | 主要機能 | ステータス |
| :--- | :--- | :--- | :--- |
| **Stage 1** | **The Log** | 単一ファイル追記, メモリハッシュマップ | ✅ Completed |
| **Stage 2** | **The Guardian** | データ復旧 (Recovery), チェックサム (CRC32) | ✅ Completed |
| **Stage 3** | **The Cleaner** | セグメント分割, Compaction (Merge), Rotation | ✅ Completed |
| **Stage 4** | **The Speedster** | Hint File, Buffered I/O, Startup Optimization | ✅ Completed |
| **Stage 5** | **The Zero-Copy** | mmap (Memory-Mapped Files) の導入 | 🚧 Future |
| **Stage 6** | **The Scaler** | Sharding (Partitioning), Parallel Optimization | 🚧 Future |

## 📊 ベンチマーク・ヒストリー
| 日付 | ステージ | Put (ops/sec) | Get (ops/sec) | 備考 |
| :--- | :--- | :--- | :--- | :--- |
| 2026-01-03 | Stage 3 | ~2.5M | ~1.8M | 基礎性能確立 |
| 2026-01-03 | Stage 4 | - | - | 起動時間: 21ms -> 4ms (5.4x 高速化) |

## 📝 データ構造（Stage 1）
データは以下のバイナリ形式でファイルに追記されます。

[Timestamp(8)] [KeySize(4)] [ValueSize(4)] [Key(n)] [Value(m)]