# V3 着手前の監査

開始時は初回コミット前で、ソース・仕様・既存文書は未追跡。適用対象の AGENTS.md は見つからなかった。README、V1/V2 仕様、実装および既存結果を照合した。

| 領域 | 既存実装・証拠 | V3 着手時点の不足 |
| --- | --- | --- |
| V1 | 3 ポリシー、事前確保、MCAP、独立検証、停止処理。docs/v1-retrospective.md に測定 | 長時間の同期込み 1/2 GB/s は未確認 |
| V2 Source と所有権 | Synthetic、pool generation、遅延返却、予約・置換、仮想到着スケジュール、Planner/Scheduler | 個別ソース能力、CDR、fence、受理前の観測範囲 |
| V2 I/O | positional sync / 擬似非同期 / io_uring。short write、失敗同期、取消と Lease のテスト | 現セッションの実 io_uring は未検証。過去測定で 1 GB/s を短時間達成、持続および 2 GB/s は未確認 |
| V2 整合性 | commit group の offset と ID、loss JSONL、独立線形 MCAP Reader、CRC | group CRC、異常終了後の検証・repair、Chunk/Index、rotation |
| V2 運用 | plan・record・verify・benchmark、終了シグナルと drain | run directory 管理、個別ソース表示、クラッシュ検証 |
| 実機 | センサ・製品 SDK・ROS 2 publisher は今回利用不可 | 実 Adapter、SDK build、ハードウェア試験は別項目 |

着手前に fmt、clippy (all targets/features, -D warnings)、既存 34 テスト成功。release バイナリを target/v3-evidence/recorder-v2 に保存。基準実験は tools/bench.py --suite policy と recorder benchmark (v2-smoke)。
