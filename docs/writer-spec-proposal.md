# Writer 最終仕様案

**既定は同期 split positional write、Data Section CRC 有効、対応ソースで Prefix CRC 有効、完全 Commit Journal、Group fdatasync。** 録画は非チャンクの MCAP を維持し、チャンク化・圧縮・インデックス構築はオフライン処理の責務とする。本提案は既定動作の変更を含まない。

## 構成の決定

| 項目 | 推奨仕様 |
|---|---|
| Writer transport | 現行の同期 split pwrite。pwritev は完全な証拠を維持する C0 で CPU と帯域の再現可能な改善が得られた場合の候補 |
| CRC | Data Section CRC を常時有効。Commit Group の Prefix CRC は現行と同じ対応ソースで保持し、異常終了時の検証に使う。CRC無効は診断用 |
| Commit Journal | 成功グループのバイト範囲、時刻、メッセージ数、確定の根拠となる Prefix CRC、ソース別連番範囲を残す |
| Loss-only | 実験・明示 opt-in の候補。LossLedger は既存形式のまま確保。成功グループ／連番メンバーシップを保存しない。正常終了時は同期済み MCAP と LossLedger を完了サマリーに照合。異常終了時は完全に読めるフレームも永続化 Unknown |
| データ同期 | グループのバイト数・件数・経過時間のいずれかで開始し、元 I/O 完了を待ってから fdatasync。ファイル終了時も同期する。現行の既定閾値は変更しない |
| 低レイテンシの候補 | 4 MiB / 4件 / 80 ms。1 MiB × 2系統、約1 GB/sではキュー最大11→7、観測同期最大11.7→7.6 ms、同期12→46回。導入前に長時間、異なるフレームサイズ、混在負荷で再確認 |
| 無同期 | ページキャッシュへの投入を測る診断専用。確定済みフレーム数は0。運用の永続化プロファイルとして公開しない |

## Commit Group の最小状態

- グループ ID、直前の確定オフセット、現在の末尾、開始時刻、バイト数、件数、先頭 request ID、および同期結果。
- 各 in-flight request の Frame ID、BufferLease と書込み済み位置。元 I/O の成否確定まで解放禁止。
- 同期前のフレームについては、エラー・キャンセル・同期失敗から正しい Unknown 範囲を生成できる ID と状態を保持する。空間は commit 件数上限と in-flight 上限で拘束する。
- 完全 journal 用のソース別連続 sequence 範囲、最終バイト境界の Prefix CRC。帰属に必要な境界を維持できた場合に限って圧縮表現に置き換える。今回の C1/C2 では in-memory のフレーム単位状態を保持しており、その削減費用は測っていない。
- キュー滞留／コミット遅延のサンプル時刻は観測用。削減する場合も、同期失敗時に必要な状態と一緒に消さない。

## 性能監視

通常の集計に per-source の planned / generated / accepted / submitted / written / committed / lost / Unknown、最終同期までの実効帯域、CPU秒/GiB、Queue／in-flight／未同期ペイロードの最大値、CopyOutバイト、RSS、同期回数と p50/p95/p99/max を含める。詳細な CRC、投入・完了、Group、journal 送出、mutex の計時は opt-in。データ同期・commit journal同期・返却 ACK を別の指標として扱う。

## 比較と採択

- **最高性能だった診断構成は C2＋pwritev＋16 MiB同期。** 約2.084 GB/sの短い入力で、現行同等 C0 は1.598 GB/s・0.734秒/GiB、C2＋pwritev は1.629 GB/s・0.688秒/GiB。中央値で帯域約+2%、CPU約−6%。900 MiB の約1 GB/s入力では帯域差がほぼなく、CPU差は約−4%。集計上の欠損・期限違反は0だが、異常終了時の確定証拠と保存後の汎用CRC検査を失うため採用しない。
- **最もバランスがよい構成は C0 の同期 Writer、両CRC、完全 journal。** 正常時に同等の帯域を保ち、試験した SIGKILL 条件で8件の確定証拠を維持した。C1 は同等の速度で8件すべてが Unknown になった。完全保証のままベクトル I/O だけ変更した試験にも安定した利得はなかった。
- **最適化順序**は、同期バリアと未同期量の調整、次に同じ保証を保つ positional I/O の測定、最後に CRC や Group 内部の見直し。観測では Writer の同期壁時計が最長で、計時した CPU は I/O 投入が CRC より大きかった。実ストレージの長時間持続上限、同期 syscall 内部、DRAM帯域、io_uring は未測定。

## io_uring の保留事項

Writev の iovec、ヘッダー、Borrowed/Spill は元リクエストの CQE まで割り当てを固定する。短い完了は残りの iovec とオフセットだけを再送する。取消要求だけで Lease を解放せず、元 CQE または輸送の完全な停止を確認する。複数 in-flight、順序逆転、EINTR、同期境界、partial send、transport failure の回帰と実 NVMe の比較を行う。今回は EPERM のため採用保留。

正常終了・異常終了・Verifier の境界は [Writer の完全性契約](writer-integrity-contract.md)。実測値と再現コマンドは [Writer の機能別評価](writer-performance-ablation.md)。
