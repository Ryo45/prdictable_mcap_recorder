# Writer と Durability Coordinator

既定は **Barrier**。Overlap は `[durability] mode = "overlap"` を指定した実験モード。非チャンク・非圧縮 MCAP、DataEnd の CRC、対象ソースの Prefix CRC、Commit Journal と Loss Ledger のレコード形式は共通。

## 所有権とデータ経路

| モジュール | 責務 |
|---|---|
| `writer.rs` | Queue claim、Layout で CRC と Message を構築、位置指定 I/O、元 Completion 回収、Lease の解放。同期対象の境界を通知 |
| `durability.rs` と `durability/group.rs` | request ID・Frame ID・連続オフセット・サイズ・投入時刻・成否を追跡。Commit 条件、保留量、確定済みカウンタ、Unknown を管理 |
| `durability/worker.rs` と `durability/journal.rs` | Overlap では単一同期スレッドがファイルハンドルと Journal を所有。Barrier では Coordinator が同じスレッドで操作 |
| `integrity.rs` | Loss/Unknown の記録、Loss Ledger の同期、Monitor サンプル。互換用の `monitor.commit_log_synced` は Coordinator の結果をサマリーへ転記 |

Writer→Coordinator の通知は `Submitted`、`completed(request, frame, success)`、`seal(offset, prefix_crc, finalized)`。Coordinator グループとメールボックスのどちらにも Frame、Lease、ペイロードの参照は入らない。Writer は元 Completion を取得した直後に Lease を解放する。実際の Device 再利用可能時刻には既存の返却遅延が残る。

投入時に request ID と実ファイル位置が隣接していることを検証する。Completion は順不同でよく、重複、不明 ID、Frame ID 不一致はエラー。Seal はグループ内の全件が成功し、末尾が Writer 側の Layout の実オフセットと一致する場合だけ受理する。最終 Seal には Footer と DataEnd の長さも含む。Prefix CRC は Seal ごとのスナップショット。

## 状態遷移

- **Barrier** は全 I/O 完了 → MCAP `fdatasync` → Commit Journal のグループ・成功連番を追記 → flush と `fdatasync` → 確定の順。次のグループは最後の段階まで待機する。旧 Writer は Monitor の Journal 同期を待たず次へ進んだため、待機順は意図的に強化されている。従来は中間同期を省略していた synthetic の Journal も、Barrier ではグループごとに同期する。
- **Overlap** は I/O 完了済みのグループを単一ワーカーへ FIFO で渡し、Writer は次のグループを形成する。位置指定 I/O と同期用の別ハンドルを使う。Linux の `fdatasync` はファイル全体に作用する。並行書込みが同期に含まれることはあるが、Journal に記録する確定範囲は Seal の境界まで。
- グループのデータ同期、Journal の書込み受理、Journal 同期、最終確定はサマリーで別のカウンタ。成功フレームと committed bytes の進捗は Journal 同期成功を受け取ってから進める。

## 設定と上限

旧 `[record]` の `commit_bytes`、`commit_messages`、`commit_interval_ms` を引き続き読める。新欄に値がある場合は項目ごとに都度上書きし、Plan／Verifier 向けの `settings.record` に有効値を保存する。新しいポリシーはサマリー直下の `durability_policy` に保存するため、従来の PC 上の Verifier でも新しい録画を検査できる。モード未指定では Barrier。例の閾値は実運用の推奨変更ではなく、比較時の値。

```toml
[durability]
mode = "overlap"
commit_bytes = 16777216
commit_messages = 16
commit_interval_ms = 80
max_pending_bytes = 134217728
max_pending_groups = 2
event_queue_capacity = 2
```

- Seal 済み未確定数は、同期中の1件を含め `min(max_pending_groups, event_queue_capacity)` 以下。形成中のグループ1件が別にある。イベントと結果のメールボックスは同じ有限容量。形成中＋Seal 済みのメッセージの wire bytes（ヘッダーとペイロード）は `max_pending_bytes` 以下。固定サイズの MCAP preamble と tail はこの上限の外。
- 次の claim 前に最大メッセージ1件分を確保できるか確認する。足りなければ現グループを早めに区切る。Seal 枠待ちでも Completion を回収し続け、イベント経路への送信は非ブロッキング。停止したワーカーと通信できなければ Unknown を記録し、成功の継続を止める。
- メモリ見積りには保留グループと形成中グループのメタデータを含める。容量のゼロ値や最大メッセージより小さい Overlap のバイト上限は起動前に拒否する。

## 失敗と終了

元 I/O の失敗を含む範囲は Seal できない。同期・Journal に失敗したグループより後ろを確定しない。Journal の追記や同期に失敗した場合は未確定追記を破棄し、直前の同期済みファイル長へ切り戻して同期する。切り戻しも失敗した場合は `rollback_failed` とエラーを報告する。その場合、Recovery は終了サマリーのこのフラグを確認した場合に、読める Journal が残っていても確定件数をゼロとして扱う。失敗の報告前にプロセスも停止した場合、フラグが媒体に残る保証はない。元の io_uring 輸送障害で I/O が止まったと確認できない場合の Lease 保持は Backend の従来処理を維持する。

終了では Queue と全 I/O を drain し、Layout が Footer を書いてから最終 Seal。Coordinator の全グループ回収後に Writer が終了し、その後 Monitor が残りの Loss Ledger を同期する。`finalized` は Footer 書込み成功、`final_sync` は最終データ同期成功、`commit_log_synced` と `loss_log_synced` はそれぞれの sidecar の成功。正常終了では全部を独立に確認する。電源断後の媒体上の永続性や実機DMAは今回の試験対象外。

## テスト

Rust の既存回帰に加え、順逆の完了・重複完了・穴のある境界、注入した同期中の Mock Async Completion と Lease 返却、保留上限待ち、Short Write、EIO、ENOSPC、同期失敗、Journal 失敗の切り戻し、同期ワーカー停止を検査した。小容量の正常ケースでは Recovery も照合。SIGKILL は既定 Barrier と Overlap の両方を検査した。実際の io_uring と物理電源断は未検証。

[性能評価](durability-performance.md)
