# V3 後のリファクタリング

検証済みの範囲で外部動作の互換性を確認した。CLI help はバイナリ名を揃えて比較し、変更前・変更後それぞれの代表 MCAP と sidecar を両バイナリで読んだ。各ファイルの Verifier と Recovery の JSON はバイト単位で一致した。

## 責務の移動

1. `src/v2/capture.rs` に取得ループを移した。 `record.rs` が adapter を構築し、capture は共通契約を使う。ソース別の分岐は追加していない。
2. `src/v2/writer/group.rs` にグループの範囲・状態・commit membership の管理を移した。Writer は completion をグループ境界で渡す。Storage の所有権と MCAP layout は既存の境界を維持した。
3. `CommitJournal` が JSONL と checkpoint 完成時の同期を扱う。duration、lease ownership、警告、形式、同期順は既存の試験で照合した。unsafe ブロックを追加していない。

V3 完成済み領域に限定して進めた。ソースと Writer は一括で置換せず、取得分離、commit 分離、全体の順で fmt、clippy、テストを実行。初回コミット前の作業ツリーを保持し、各段階の検証ログ、バイナリと復元用ソースを target/v3-evidence に保存した。コミットは作成していない。

最終ゲートは fmt、clippy（workspace、all-targets/all-features、-D warnings）、43 テスト成功。追加した2試験で Mock pool 枯渇時の世代保護、重複の種類別報告と repair 拒否を確認した。復旧コピーの index は独立 Reader で190件すべてを seek した。V1 E0–E6 と V2 P1–P9 を最終バイナリで再実行し、全判定 PASS。

## 性能比較

[測定条件、変更前の数値とコマンド](refactor-baseline.md)

| 指標 | 変更前 | 変更後 |
| --- | ---: | ---: |
| Deadline-aware NVMe、2 s × 3、同期込み payload MB/s | 0.6738–0.6742 | 0.6738–0.6742 |
| 同、accepted→data sync の p99 / p99.9 / max | 各 102.165–102.266 ms | 各 102.148–102.216 ms |
| 同、submit→complete の p99 / p99.9 / max | 20.48–24.58 / 35.36–54.00 / 35.36–54.00 µs | 20.48–25.60 / 27.42–50.49 / 27.42–50.49 µs |
| 同、CPU / peak RSS / queue peak | 3.20–3.34% / 6.32–6.41 MiB / 2 | 3.22–3.35% / 6.36–6.41 MiB / 1–2 |
| 同、CopyOut / 既知欠損 / Unknown | 各回 0 B / 7 / 0 | 各回 0 B / 7 / 0 |
| Always Copy NVMe、2 s × 3、同期込み payload MB/s | 0.6742–0.6743 | 0.6740–0.6742 |
| 同、CopyOut p99 / p99.9 / max | 3.07–3.33 / 3.45–3.70 / 3.45–3.70 µs | 3.07–3.71 / 3.23–3.76 / 3.23–3.76 µs |
| 同、CopyOut bytes/GiB | 1,042,982,003 | 1,042,982,003 |
| Deadline-aware Null、2 s × 3、CPU / peak RSS / queue peak | 3.04–3.06% / 6.30–6.36 MiB / 1 | 2.99–3.03% / 6.36–6.42 MiB / 1 |
| Deadline-aware NVMe、各 1 × 30 s、同期込み payload MB/s | 0.674643 | 0.674634 |
| 同、submit→complete の p99 / p99.9 / max | 24.58 / 47.10 / 98.30 µs | 22.53 / 43.01 / 187.84 µs |
| 同、peak RSS / queue peak / 既知欠損 / Unknown | 6.34 MiB / 2 / 109 / 0 | 6.16 MiB / 2 / 109 / 0 |

比較は入力律速の試験。30 秒試行で最大書込み完了遅延が増加し、p99・p99.9 は低下した。各1回で最大値の再現性と原因は未確定。2 秒試行の帯域範囲は丸め後に一致した。コピー量・欠損は同数で、測定から一般的な性能改善や飽和時の互換性は判定できない。

各結果は target/v3-evidence/{v3-baseline-final,v3-after,v3-copy-before,v3-copy-after,v3-30s,v3-30s-after}/comparison.json。30 秒の変更前はフォーマット検証追加前の recorder-v3 で測定し、録画パスと同一構成を使った。

V1 E3 Adaptive は着手前と最終の双方 144/144、同期込み 7.8253 → 7.8246 MB/s。V2 P1 は双方 378/378、2.00919 → 2.00914 MB/s。V1 E0 の最高負荷は測定双方の Null 試行で1,152/1,152。V2 P10 と実 io_uring の再測定は今回未実施。V1/V2 の生データは target/v3-evidence/{v1-before,v1-after,v2-before,v2-after}/comparison.json。

同一 V3 プール予算で四方式を試行した。Always Copy は 1,310,720 B を退避し、Deadline-aware と Deadline-aware Reserve はこの入力では CopyOut なし。Always Borrow は Planner が固定期限の保守的な返却上限で拒否し、明示 opt-in の短時間試行は 633/640 を検証した。各方式とも設定どおりの UDP 欠損7件。予約の動的効果は V2 P5 で比較し、V3 のスモークでは観測していない。

## 未達

V3 のライブローテーション、通常録画の Chunk/Index、全故障境界、実機対応、長時間高帯域は未達。残る技術課題は、実 SDK と共通契約を接続するための所有権・返却 ACK の拡張、セグメント管理と回復証拠の統合。実機別の状態は [V3 報告](v3-report.md) を参照。
