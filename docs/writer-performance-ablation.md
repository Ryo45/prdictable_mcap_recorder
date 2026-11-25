# Writer 性能の機能別評価

基点は GitHub master と同じ `060f5b8a25a790560ab32abc5ffea3181967be0e`。アーカイブした実験ビルドでのみ機能を切り替えた。本番の Writer、Global Scheduler、非チャンク MCAP の既定動作は変更していない。比較は Intel Core Ultra 7 265KFの同一ホスト、CPU 0〜7、ext4 の KIOXIA EXCERIA PLUS G3（ELFA01.2）、Buffered I/O で実施。開始順序をローテーションし3反復。GB/s は10億 bytes/s、MiB と GiB は2の累乗。CPU/GiB は録画プロセスの CPU 秒を I/O 完了ペイロードで割った値。

通常マトリクスは試行あたり最大180 MiB、W0 は512 MiB、W1 は32 MiB。延長比較は試行あたり最大900 MiB。バッチごとの出力上限10 GiBに対し、開始時の空きは約127.85 GBと117.65 GB。計温した NVMe は通常マトリクスで25.85〜27.85°C。65°C で停止する設定。詳細フレームトレースは無効、詳細計時は C0-profile のみ。過去の8 GiB・1.62 GB/sは[前回試験](writer-profile-and-model-validation.md)の値であり、ここでの中央値には含めない。長時間のキャッシュ枯渇、熱平衡、持続書込み上限は未測定。

## A — 分離計測

基準の実装 C0 を、1 MiB × 2系統 × 500 fps、0.18秒、16 MiB / 16メッセージ / 80 ms で計測した。ここでの16 MiBは実験用設定。リポジトリの設定既定値は64 MiB / 4,096件 / 500 ms。各行の時間は3試行の範囲。I/O と CRC にはスレッド CPU 時計も使った。

| 計測点 | 結果 |
|---|---|
| MCAP メッセージヘッダー・オフセット | 合計76–81 µs。起動メタデータと終了フッターは計時対象外 |
| ヘッダー CRC | p50 57–61 ns、p95 103–159 ns、p99 351–495 ns、最大1.02–1.33 µs |
| ペイロード CRC | p50 30.7 µs、p95 53.2–57.3 µs、p99 139–156 µs、最大139–156 µs。ヘッダーと合わせた CRC 区間の CPU 合計5.86–5.95 ms |
| Backend submit | p50 139–147 µs、p95 205–328 µs、p99 426–524 µs、最大0.64–3.30 ms。区間の CPU 合計26.18–32.04 ms |
| Writer claim→I/O 完了観測 | p50 164–180 µs、p95 262–426 µs、p99 590–721 µs、最大0.82–3.34 ms。CRC・投入を含む。同期 syscall 復帰→完了回収の合計33–37 µs |
| Completion 取り出し・Lease Drop | 合計42–48 µs。Device の遅延 ACK は含まない |
| Group submit / complete / commit | 合計3.1–3.2 / 4.8–5.4 / 19.7–21.7 µs。commit にはイベント送出を含む |
| Writer の MCAP fdatasync | 12回、合計74.5–78.6 ms、最大7.50–11.30 ms |
| Commit journal イベント送出 | 合計12.3–14.0 µs。チャンネルへの受け渡しまで |
| Monitor の journal 書込み / flush / fdatasync | 合計6.9–11.0 µs / 21.5–24.9 µs / 9.9–10.7 ms |
| Queue 滞留 / アイドル待機 / claim mutex 獲得 | 合計229–308 ms / 147–159 ms / 11–14 µs。Queue 滞留はフレームごとの重複区間を含む |

Writer スレッド CPU は37.5–43.9 ms。同期呼出しは最長の実作業の壁時計区間で、CPU は当該入力では positional write のほうが CRC より大きい。計時 OFF→ON は帯域中央値1.0240→1.0237 GB/s、プロセス CPU/GiB 0.680→0.674秒。3反復の範囲が重なり、計時による悪化は識別できなかった。同期 syscall 内部のページキャッシュ処理・カーネル待ち・デバイスサービスは分離していない。io_uring の完了待ちも未測定。

## B/C — 機能比較と性能限界

| 構成 | データ同期込み GB/s 中央値［範囲］ | CPU 秒/GiB 中央値 | 最大キュー | 欠損 / Unknown（全反復） |
|---|---:|---:|---:|---:|
| C0 現行 CRC＋完全 journal、split pwrite | 1.024［0.993–1.026］ | 0.680 | 11 | 0 / 0 |
| C1 Data CRC＋Loss-only、split | 1.023［0.997–1.024］ | 0.690 | 11 | 0 / 0 |
| C2 CRCなし＋Loss-only、split | 1.023［1.023–1.026］ | 0.673 | 12 | 0 / 0 |
| C0＋pwritev | 1.024［1.024–1.026］ | 0.681 | 12 | 0 / 0 |
| C2＋pwritev | 1.025［1.025–1.026］ | 0.656 | 11 | 0 / 0 |
| C0・小さい同期単位 | 1.035［1.035–1.036］ | 0.671 | 7 | 0 / 0 |
| C0・大きい同期単位 | 0.930［0.928–0.954］ | 0.673 | 26 | 0 / 0 |
| C0・30 ms同期 | 0.986［0.977–1.001］ | 0.688 | 21 | 0 / 0 |
| W2 Owned・Capture＋Writer | 1.025［1.024–1.025］ | 0.681 | 11 | 0 / 0 |
| W3 Borrowed・強制CopyOut＋Writer | 1.021［0.538–1.025］ | 0.774 | 32 | 17 / 0 |
| C0 高ペース | 1.598［1.464–1.735］ | 0.734 | 63 | 0 / 0 |
| C0＋pwritev 高ペース | 1.534［1.179–1.535］ | 0.741 | 59 | 0 / 0 |
| C2＋pwritev 高ペース | 1.629［1.620–1.681］ | 0.688 | 39 | 0 / 0 |

通常入力は実生成1.048 GB/s、高ペースは2.084 GB/s。高ペースは0.09秒の入力を終了後に drain する条件で、持続能力は測っていない。I/O 完了までの高ペース中央値は C0 1.625、C0＋pwritev 1.558、C2＋pwritev 1.655 GB/s。同期 Writer の in-flight 最大はいずれも1。実録画の欠損は W3 の一試行に集中し、内訳はすべて `spill_pool_exhausted`。W3 のコピーは各試行163〜180 MiB。同期録画の Verifier は全件通過し、未申告欠損・重複・破損・返却期限違反は0。録画側の RSS はおおむね165 MiB。

**CRC と journal。** C0 の commit journal は基準試行1回あたり約3.45 kB、Loss-only は空。C1 で省いたのは成功グループ・span の送出、Prefix CRC、journal のグループ同期。Data CRC の更新は共通で、C0→C1 の CPU/帯域の改善は確認できなかった。C1→C2 ではユーザー空間 CRC が読む180 MiBのペイロード1パスを省略。実DRAM帯域・キャッシュミスは未測定。別の `crc32fast` マイクロベンチは64 MiBのバッファで512 MiB分を処理し **28.6–30.7 GB/s、CPU 17.5–18.8 ms**。Recorder 全体の改善率に換算する際は Capture や I/O と共有するキャッシュ状態を考慮する。

**同期。** 実際の最大未同期ペイロード、同期回数、単独同期の最大値は、16 MiBで **16 MiB / 12回 / 11.7 ms**、4 MiBで **4 MiB / 46回 / 7.6 ms**、64 MiBで **64 MiB / 3回 / 26.4 ms**、30 ms主導で **45–51 MiB / 5回 / 20.4 ms**。4 MiB構成はこの入力でキューとテールを縮小したが、同期回数は3.8倍。30 ms構成は直前の同期を待ってから新しいグループを形成するため、30 msは取得から永続化までの上限ではない。同期なしは I/O 完了1.053 GB/s、試行ごとに180 MiB全体が未同期で180件 Unknown。永続化帯域は計上しない。

**I/O。** `pwritev` はヘッダー・借用ペイロードを1回の syscall に集約する。短い書込み、両スライス境界をまたぐ完了、EINTR、WriteZero、offset 更新を試験した。C0 の同一保証比較では性能改善が再現せず、高ペースで中央値が約4%低下した。io_uring は同じホストで **EPERM**。帯域と CPU は未測定。

**W0/W1。** fio が未導入のため W0 は通常ファイルに4 MiB単位で書き、64 MiB単位で同期。3反復で **1.320［1.223–1.362］GB/s、CPU 0.456秒/GiB**。CPU は短いファイル検証を含む。W1 は事前生成済みバッファを使う positional vector I/O マイクロベンチで **1.762［1.729–1.799］GB/s、CPU 0.184秒/GiB**。W1 は現行 Group、CRC、Commit Journal を含まない。32 MiBで開始遅延の影響も異なるため、W1−W2の差を Capture 単独のコストとして扱えない。デバイス統計は NVMe 全体で他プロセスを含み、W0 の512 MiBに対し物理デバイスの差分は約512〜630 MiBだった。

**延長比較。** 0.90秒・900 MiBの3反復で、C0 / C0＋pwritev / C2＋pwritev の帯域中央値は **1.044 / 1.043 / 1.044 GB/s**、CPU秒/GiBは **0.333 / 0.339 / 0.321**。全試行欠損・期限違反・Unknown 0。C2＋pwritev の C0 比 CPU 減は約4%だが、入力制約下で帯域差はなく、クラッシュ時の確定能力を失う。最速構成の評価は短い高ペース条件に限定する。

## D — 故障と整合性

C0/C1/C2 × split/pwritev の6構成で、短い書込み、意図的なペイロード改変、EIO、データ同期失敗、QueueFull、DMA Pool 枯渇、SIGKILL後の Recovery を試験した。清浄終了は独立 Reader/Verifier と LossLedger で照合。クラッシュ試験では全構成に読み出せる8件があり、C0 は8件確定、Loss-only は **0件確定・8件 Unknown**。CRCなしの改変は Mock の期待値比較が検出した。任意の実センサの不透明ペイロードに同じ方法は使えない。

隔離ビルドの fmt、clippy、Rust 55回帰テスト、別途 CRC 測定用テストが通過。既存の回帰には元I/O完了までのLease保持、世代付きPool保全、故障・Recoveryを含む。追加テストでベクトルI/Oの部分完了とEINTR、読み取り専用sidecarの失敗を確認した。実機DMA、電源断、io_uring の実機故障は範囲外。

## 再現

[通常マトリクス CSV](writer-ablation-evidence/matrix.csv) · [詳細 JSON](writer-ablation-evidence/matrix.json) · [延長 CSV](writer-ablation-evidence/extended.csv) · [延長 JSON](writer-ablation-evidence/extended.json) · [故障試験](writer-ablation-evidence/faults.json) · [ハッシュ](writer-ablation-evidence/manifest.json)

```sh
mkdir -p target
archive="$(mktemp -d "$PWD/target/writer-ablation.XXXXXX")"
git archive 060f5b8 | tar -x -C "$archive"
patch -p1 -d "$archive" < docs/writer-ablation-evidence/experiment.patch
(cd "$archive" && cargo fmt --check && cargo clippy --all-targets --locked -- -D warnings && cargo test --locked && cargo build --release --locked)
(cd "$archive" && taskset -c 0-7 cargo test --release --locked --test crc_cost -- --ignored --nocapture)
PYTHONDONTWRITEBYTECODE=1 python3 tools/check_writer_ablation.py --recorder "$archive/target/release/recorder" --out "$archive/faults"
PYTHONDONTWRITEBYTECODE=1 python3 tools/bench_writer_ablation.py --recorder "$archive/target/release/recorder" --out "$archive/smoke" --smoke
# 各バッチの上限10 GiB、新規フォルダと空き20 GiB以上が必要
PYTHONDONTWRITEBYTECODE=1 python3 tools/bench_writer_ablation.py --recorder "$archive/target/release/recorder" --out "$archive/matrix"
PYTHONDONTWRITEBYTECODE=1 python3 tools/bench_writer_ablation.py --recorder "$archive/target/release/recorder" --out "$archive/extended" --extended
```

通常マトリクスのバイナリは後で削除した空の条件分岐を含む。延長・故障・回帰は保存したパッチの版で実行。各バイナリの SHA-256 は対応する JSON に記録。元の設定、録画、Verifier、台帳は `target/writer-ablation` 以下。
