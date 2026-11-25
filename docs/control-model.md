# Phase 1 離散イベントモデル

Python 標準ライブラリだけで動作する。Rust の Writer と Scheduler は基点のまま。`python3 -m unittest discover -s tools/control_lab/tests -v` で決定的な再現、所有権、故障分類を検査する。

| 構造 | フィールドと単位 |
|---|---|
| Scenario | name、frames、pools、spill（バイト数と同時枠数の組）、queue（枠数）、service、stalls（開始・終了 ns の昇順非重複区間）、sync_fail（1始まりの同期番号）、public_arrivals、wakes（ns） |
| Frame | source（0始まり）、arrival、hold（到着から ACK 期限まで、任意）、ack（返却から再利用まで）、ready（到着からフェンス解除まで）、size（bytes）、borrowed。時間は整数 ns |
| Pool | slots、reserve、minimum。借用到着直前に available が minimum 以上であれば1枠を取得 |
| Service | writer_ns_per_mib、copy_ns_per_mib、sync_ns、group（フレーム数）、certified、overlap_penalty_ns、assumed_copy_ns_per_mib（実際のコピーと異なる契約を注入する場合） |
| Trace event | ns、event、frame（配列 index、非フレームイベントは -1）。返却系イベントに [slot, generation]、writer_claim に borrowed、copy_claim に spill_class、fdatasync に count と success、loss に reason |

同時刻の完了・ACKを到着より先に処理し、到着を入力順に処理してから次の制御判断を行う。Writer は同一ソース内 FIFO、ソース間 RR。フェンスまたは CopyOut で先頭が止まったソースはスキップする。Copy Worker は1本で非プリエンプティブ。コピー中はキューに残り、Spill は I/O 完了で解放する。借用は ACK で再利用可能になり、データ同期完了は別イベント。有限の登録済み予備は空きに一方向で移る。終端でコミット、既知欠損、Unknown を独立に照合し、全資源の回収を検査する。MCAP は Phase 0 の独立 Verifier で照合した。モデルは MCAP を生成しない。

M1 の3件は Legacy で ACK 7、9、11 ms、Global で 3、5、7 ms。4件版は必要コピーだけで8 msのため期限7 msに収まらない。M4 の固定期限と空き需要は同一フレームに重複配賦しない。M5 の長いコピーと短い後着、M7 の不足クラス、M8 の低速コピー、M9 の Writer 保持を同じ状態機械で再現する。M9 は同期 Writer の I/O のみ。

モデルの Legacy/Global は概念比較用。実機側のプール予測、各種マージン、ロック再試行、バリアのバイト・時刻条件、損失ジャーナル、返却ドライバ、非同期 Writer は省略した。Pool 需要算出に公開シナリオの到着を使う場合は到着列が既知という解析条件になる。Writer の選択は自動で、新制御器から Writer の順序を予約できない。干渉試験は開始時にもう一方が稼働中なら加算する簡略モデル。

Phase 0 の B3 から I/O と同期の小さい p50 および大きい p99 を別プロファイルとして読み込む。予測感度の入力であり、保証ステータスは Unknown。どちらも個々の実行トレースの再生や WCET には使わない。
