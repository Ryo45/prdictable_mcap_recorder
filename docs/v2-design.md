# V2 の実装と運用

V2 は設定ファイルで複数の疑似入力を動かし、Planner、実行中の契約監視、収録後の検証を行います。 `--config` を指定した `record` が V2 の入口です。V1 の既存 CLI とファイルは引き続き使用できます。

## 起動

```sh
cargo build --release --locked
target/release/recorder plan --config configs/v2-smoke.toml --duration-sec 0.6
target/release/recorder record --config configs/v2-smoke.toml --duration-sec 0.6 --output run.mcap
target/release/recorder verify --input run.mcap --report run-summary.json
```

デフォルトのサイドカーは `run-summary.json`、`run-metrics.csv`、`run-loss.jsonl`、`run-commits.jsonl` です。別の場所へ移動した場合は `verify --loss-log … --commit-log …` で指定できます。 `--profile configs/v2-profile.json` は TOML の `storage_profile` と `copy_profile` 全体を置き換えます。 `--duration-sec` は秒で小数第9位まで指定でき、予定時刻は収録開始を含む半開区間に入ります。

Planner は未測定の値または不明な入力包絡線を `unknown`、既知の仮定に対する不足を `rejected` とします。どちらも実験として動かすには `--allow-unverified` が必要です。メモリ予算、出力バイト上限、既存出力、実行環境で確認できる空きメモリ・ディスク容量は、このオプションを指定しても検査します。見積もりが256 MiBを超える MCAP には `--allow-large-output` に加え、TOML の `max_output_bytes` の設定が必要です。警告のある実行や Planner の拒否を明示して動かした実行でも、欠損ジャーナルとの照合が成立すれば Verifier は成功できます。 `within_assumed_contract` は Planner 合格と、実行が観測した時間・損失・期限の条件を区別して報告します。

## 設定の単位と意味

| 節 | 設定 | 意味 |
| --- | --- | --- |
| `record` | `policy` | `always-copy`、`always-borrow`、`v1-threshold`、`deadline-aware`、`deadline-aware-reserve` |
| `record` | `writer` | `sync`、`mock-async`、`io-uring`、`null` |
| `record` | `queue_capacity`、`in_flight`、`max_borrowed_in_flight` | 全入力共通の待機 Job 数、I/O 数、うち借用可能な I/O 数。同期と Null の I/O 上限は1 |
| `record` | `commit_bytes`、`commit_interval_ms`、`commit_messages` | グループ同期を開始する MCAP バイト数、最初の submit からの ms、Message 数。レコードと I/O は途中で分割して commit しないので、時間・バイトは開始閾値 |
| `record` | `memory_budget_bytes`、`max_output_bytes` | 確保と出力の上限見積もりに適用するバイト数 |
| `record` | `metrics_interval_ms`、`loss_queue_capacity`、`seed` | CSV 周期、欠損と commit を監視スレッドへ渡す固定チャネルの件数、疑似データとジッタの seed |
| `storage_profile` | `sustained_bytes_per_sec`、`max_stall_ms`、`max_io_us`、`max_sync_ms` | 通常のグループ同期を含む持続バイト毎秒の下限、追加停止 ms、submit から completion の µs、1回の同期 ms |
| `storage_profile` | `min_stall_period_ms` | 追加停止間の最小周期。未指定なら Planner は単発停止を仮定 |
| `copy_profile` | `assumed_min_bytes_per_sec`、`return_margin_us`、`scheduling_margin_us`、`max_capture_delay_us` | コピーサービス下限、返却と実行の余裕、収録スレッドの到着遅延上限。後3項は µs |
| `sources` | `id`、`model` | 識別名と `fixed-deadline`、`pool`、`replaceable`、`owned` のいずれか |
| `sources` | `frame_bytes`、`fps`、`burst`、`phase_us`、`jitter_us`、`arrival_bound_known` | フレーム内ID込みバイト数、周期イベント毎秒、同時発生フレーム数、位相と seed による非負ジッタの最大 µs、到着上限の可観測性 |
| `sources` | `dma_buffers`、`capture_reserve`、`return_delay_us`、`max_hold_ms` | 有効プール数、保持したい最小空き数、擬似デバイスの再利用通知遅延 µs、固定期限 ms |
| `sources` | `reserve_buffers`、`replacement` | 初期確保する予備数、`unsupported`、`pre-registered-pool`、`explicit-replacement` の能力 |
| `spill` | `frame_bytes`、`buffers` | サイズクラスの1枠のバイト数と数量。クラスは小さい順に指定 |
| `faults` | `stall_at_ms`、`stall_period_ms`、`stall_duration_ms`、`writer_bytes_per_sec` | 開始からの単発停止、周期停止（初回は1周期後）、継続時間、ペイロード送出の上限 |
| `faults` | `copy_delay_us`、`copy_fail_attempt`、`mock_delay_us` | コピー待機、失敗を注入する0起点のコピー試行、擬似非同期完了遅延 |
| `faults` | `io_short_bytes`、`io_error_request`、`io_error`、`sync_fail_group` | 1回の書込み上限、0起点 I/O 要求、`eio` または `enospc`、失敗させる0起点同期グループ |

各周期の発生時刻は位相と `ceil(group × 1e9 / fps)` ns とジッタの和です。同じ周期の `burst` フレームには連続した sequence を付けます。ジッタは周期未満です。先行周期の処理が次の周期に間に合わない場合、過ぎた連番は `source_deadline_miss` になります。疑似ソースは全予定 ID を観測でき、取得時に全バイトを初期化してから Core に渡します。

## Planner のモデル

入力ごとに、区間 `h` 内の到着個数上限を `B_i(h) = burst_i × (1 + ceil(fps_i × (h + jitter_i + capture_delay) / 1e9))` とします。1 Message のワイヤーサイズは `payload + 31` バイト。到着包絡線 `α_i(h) = b_i + r_i × h` の切片 `b_i` は `B_i(0)` にワイヤーサイズを掛け、傾き `r_i` は平均ワイヤーレートです。

持続サービスレートを `R` とし、周期停止を仮定する場合は `R × (1 - max_stall / min_period)` で割り引きます。 `T = max_stall + max_io + max_sync` の rate-latency モデルで、集約到着レートが有効レート未満のとき、最大バイト滞留を `Σb_i + Σr_i × T` と置きます。同期コスト込みの継続レートは実際に採用した同期設定で測定する必要があります。キューとプールはバイト数に加えて端数込みの個数で検査します。JSON の `stall_tolerance_ns` はキュー容量から計算した余裕の目安です。プールや期限については源別の必要個数・遅延予算と拒否理由を併読します。有限の仮想時刻シミュレーションは位相・ジッタ・同時バーストを再生し、最大滞留と同時到着数を補助情報として示します。 [Network Calculus](https://leboudec.github.io/netcal/)

コピー可能な全入力が退避を要する保守的な場合を使い、その入力がコピー下限未満か検査します。バースト量に最大フレーム1個分の非プリエンプティブなブロックを加え、コピー下限とコピー入力の差分レートで割り、コピー滞留時間の見積もりを得ます。源ごとに再利用通知とスケジューリング余裕、最大借用 in-flight 数を加えて DMA 最低個数と固定期限を検査します。Legacy 借用方式では書込み側の停止時間を含む個数・期限も検査します。退避先は最小適合クラスに固定するため、別クラスに空きがあっても枯渇する場合があります。予備は全方式のメモリ見積もりに含めます。Planner は予備の動的な有利さを成立条件に加算しない保守的な判定です。

Planner の判定は指定した入力・サービス・コピー等の限界値に依存します。Linux の実行時間についての無条件の期限証明ではありません。小規模ベンチマークのプロファイルは実験の契約値です。SSD や制御機器の保証値として転用する場合は実測で更新する必要があります。

## 所有権、スケジューリング、可観測性

論理フレーム ID は `source + session_epoch + sequence`、物理バッファ ID は `pool + buffer + generation` です。固定プールから取得するたびに generation を増やします。複製できない Lease を move し、登録表で返却先と generation を検査します。 `available + held + returning + reserve = total` はプール操作で確認します。コピー時の元 Lease と退避先は1つの Copy Worker が所有します。コピー完了後に元 Lease を解放し、書込み中は元 I/O の完了まで保持します。疑似デバイスの返却要求から再利用通知までの間は Returning であり空き数に入りません。停止後は疑似時刻上の未完了返却を精算します。

短時間のキューロックで Pending から Copy Worker または Writer にだけ payload を move します。コピー中はキュー位置を保持し、コピー完了後に同じ位置へ Owned として戻します。コピー中や Writing 中の Lease を第三者が回収する操作はありません。コピーは1件ずつでプリエンプトしません。各 stream の FIFO と、ストリーム間の Writer ラウンドロビンは EDF の退避順序から独立しています。

固定期限を与えられた Job は、期限からコピー、返却、余裕を引いて退避の判断時刻を求めます。プールでは次の到着列と空き数 `F` に基づいて `F - A(t,h) < R` となる最初のイベントの時刻と、そのバースト全体に必要な返却数を計算します。再利用時刻が既知の疑似返却は、その時刻まで空きには数えず、期限までに通知が来る場合の退避判断でのみ考慮します。固定期限とプール要求の早い方を優先し、開始時刻を過ぎた候補から EDF で選びます。Writer の予想滞留と固定期限 slack が薄い場合に警告や追加退避を行います。

`deadline-aware-reserve` は、初期登録済みプールでは待機中の予備を有効化します。明示置換の場合は保持中の物理 ID を確認して代替スロットを渡し、元スロットの再利用通知後は予備枠へ戻します。両方とも初期確保数が上限です。能力を宣言しないソースには適用しません。

CSV は源別の生成・受理・書込み・同期件数、プール状態と slack、必要返却数、キュー、コピー、in-flight、書込み・同期帯域、RSS、警告を含みます。短い周期で取得する各値は観測スナップショットです。サマリは CPU time、ピーク RSS、生成・コピー・キュー・I/O 完了・同期のヒストグラムと固定資源の回収結果を保持します。分位は固定ヒストグラムによる近似です。

| 警告 | 意味 |
| --- | --- |
| `AT_RISK` | 予想 Writer 滞留、期限 slack または退避先不足に基づく先行警告 |
| `CONTRACT_VIOLATION` | 入力遅延、停止、コピー、I/O、同期、継続滞留時の書込みサービスが仮定を逸脱 |
| `LOSS_DETECTED` | 既知の欠損が発生 |
| `DURABILITY_UNKNOWN` | 部分書込みや同期失敗で保存結果が未確定 |

サマリは警告の初回・最終時刻、初回 `AT_RISK` と初回欠損の差、欠損を伴わなかったリスク区間数を記録します。欠損がなかった警告には、退避が機能して欠損を回避したケースも含まれます。 `false_risk_episodes` は、そのうち退避・予備投入や予約割れ・期限超過も観測しなかった区間数です。空きゼロの最長時間もプールごとに記録します。実期限超過は再利用時に数え、欠損は別に計数します。

## MCAP、非同期 I/O、グループ同期

MCAP のヘッダー、Schema、Channel は公式 Rust Writer で生成します。Message の31バイトのヘッダーとオフセットをレイアウト層で割り当て、ペイロードをコピーせず Backend に渡します。Chunk と Summary は省略し、DataEnd と Footer の CRC を付けます。各 stream の `log_time` は取得予定枠内の時刻、`publish_time` は予定時刻です。Channel に source ID・epoch・seed を持ち、ペイロード形式は V1 と同じ先頭10バイトと決定的パターンです。 [MCAP 仕様](https://mcap.dev/spec)

同期方式は固定位置への `write_all_at`、実非同期方式は `io_uring` の `Writev` を使います。擬似非同期方式は同じ契約で保持した要求を書き、所定時刻に順不同で完了させます。実 I/O はメモリと iovec を動かさず、CQE を受けるまで所有します。短い完了は同じ要求の未消費 suffix を再投入します。停止時は新規取得を止めて通常の完了を待ち、擬似取消しテストでは取消し要求だけで Lease を返さないことを確認します。致命的な ring 制御エラーで参照終了を確認できない場合はプロセス終了まで該当メモリを保持し、異常終了します。 `null` のグループ・完了数は実験用の論理値で、SSD の耐久性には使用できません。 [Linux io_uring_enter(2)](https://man7.org/linux/man-pages/man2/io_uring_enter.2.html)

グループの sync 前に全対象 I/O の完了を確認し、次のグループの submit を止めて `sync_data` を行います。成功したグループの ID、ファイル位置、stream ごとの連番範囲を記録します。write 完了で Lease を返せることと、sync 完了で Committed と記録できることは別です。グループ同期に失敗した場合はそのグループを Unknown、未投入が確定したキューを Lost とし、新規取得を止めます。JSONL の報告自体が失敗した場合はサマリでカバレッジ未確定を示し、非ゼロで終了します。

Verifier は公式 MCAP Linear Reader で1レコードずつ読み、連番・サイズ・順序・全ペイロード・CRC を検査します。物理オフセットと各グループの連番集合を照合し、源ごとに予定、受理、コミット済みかつ検査済み、申告欠損を突合します。異常終了時は検証できる以前のグループを報告し、ファイル全体の成功は返しません。サイドカーと MCAP は同一故障で失われ得ます。V2 の group commit は通常実行の診断・検証を対象とします。 [End-to-End Arguments in System Design](https://web.mit.edu/saltzer/www/publications/endtoend/endtoend.pdf)

## V3 に向けた確認事項

- proFRAME の DMA 割当、通知、再利用・欠損カウンタと、NvSIPL の事前登録、AddRef/Release、EOF fence、stride・レイアウトを実機 SDK で検証する。
- ROS 2 等の連番可観測性、時刻基準、入力前欠損、Viewer/rosbag 互換性を入力ごとに定義する。
- 電源断からの再開に必要な意図／完了の永続メタデータ、既知のファイル位置での復旧、不確定範囲の照合を設計する。
- SSD の定常帯域、同期の停止時間、DRAM・NUMA・CPU/GPU 競合を計測し、必要に応じて Direct I/O、Chunk/Index、実機の非同期返却を評価する。
