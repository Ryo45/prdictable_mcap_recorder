# Predictable Recorder

## V3 Mock-first

Linux と Rust でビルドできます。標準ビルドには製品 SDK や ROS 2 の依存がありません。サンプルは Synthetic、ROS 2 serialized、UDP、SocketCAN、V4L2、proFRAME、NvSIPL 相当の入力を同時に録画します。

```sh
cargo build --release --locked
target/release/recorder list-sources
target/release/recorder inspect-source --config configs/v3-smoke.toml --source drive
target/release/recorder plan --config configs/v3-smoke.toml
target/release/recorder record --config configs/v3-smoke.toml --output-dir runs/demo
target/release/recorder verify --run-dir runs/demo
target/release/recorder recover --run-dir runs/demo --dry-run
target/release/recorder benchmark --suite v3-smoke --output-dir target/v3-smoke-new
```

先に `mkdir -p runs` で親ディレクトリを作ります。録画は新規ディレクトリ内の `run.partial` に開始し、正常終了時に `run.mcap` を公開します。従来の `--output` と `verify --input … --report …` も使えます。Planner が Accepted 以外の場合は `--allow-unverified` が必要です。サンプルは観測できる UDP 欠損を意図的に注入します。

録画は非圧縮・非 Chunk です。復旧時は元ファイルと journal を読み、CRC と commit membership で確認できる範囲を報告します。同期証拠のない可読データは未確定です。プロセスのメモリにしか残らなかったイベントや、送信側の連番・観測情報が提供されない事前欠損には、独立した照合元が必要です。復旧コピーは非圧縮 Chunk/Index 付きです。ライブローテーション、実 Adapter、製品 SDK とハードウェアの試験は未実施です。

- [V3 検証結果、未達、実機対応状況](docs/v3-report.md)
- [Deadline-aware BufferLease の比較試験](docs/deadline-aware-bufferlease.md)
- [複数ソースの Deadline-aware スケジューリング](docs/multi-source-deadline.md)
- [アーキテクチャ](docs/architecture.md) と [不変条件](docs/invariants.md)
- [復旧操作](docs/recovery.md) と [同期境界](docs/storage-durability.md)
- [性能測定方法](docs/performance-methodology.md) と [リファクタリング結果](docs/refactor-report.md)

## V2

V2 は複数の疑似デバイスを同時に録画し、Planner、動的 CopyOut、予備枠、同期と非同期の I/O、グループ同期を比較できます。

```sh
cargo build --release --locked
target/release/recorder plan --config configs/v2-smoke.toml
target/release/recorder record --config configs/v2-smoke.toml --output run.mcap
target/release/recorder verify --input run.mcap --report run-summary.json
```

- [V2 の設定・数理モデル・所有権設計](docs/v2-design.md)
- [V1 の振り返りと P0 基準試験](docs/v1-retrospective.md)
- [V2 の P1〜P10 比較と再現手順](docs/v2-experiments.md)

## V1 基準実装

Linux 上で疑似 DMA 入力を録画する Rust CLI です。固定容量のバッファプールとキューを使い、非圧縮 MCAP、欠損ジャーナル、性能メトリクスを出力します。V1 の入力は単一の疑似センサです。設計と受け入れ条件は [robot-recorder-v1-spec.md](robot-recorder-v1-spec.md) にあります。

### ビルドと実行

Rust 1.91.1 と Linux で検証しました。

```sh
cargo build --release --locked
target/release/recorder record --output run.mcap
target/release/recorder verify \
  --input run.mcap --report run-summary.json --loss-log run-loss.jsonl
```

既定値では 64 KiB × 60 fps を1秒間録画します。出力は `run.mcap`、`run-summary.json`、`run-metrics.csv`、`run-loss.jsonl` です。すべて新規作成し、既存パスがあればエラーになります。出力先の親ディレクトリは作成済みである必要があります。開始前に予定フレーム数、予定ペイロード量、入力帯域、バッファ確保量を表示し、MCAP の見積もりを出力先の空き容量と照合します。単一の MCAP が見積もりで 256 MiB を超える場合は `--allow-large-output` が必要です。

```sh
target/release/recorder record --help
target/release/recorder verify --help

target/release/recorder record --policy adaptive --sink mcap --output stall.mcap \
  --frame-bytes 65536 --fps 120 --duration-sec 1.2 --seed 42 \
  --dma-buffers 8 --capture-reserve 2 --spill-buffers 32 --queue-capacity 40 \
  --max-hold-ms 120 --metrics-interval-ms 10 --stall-at-ms 320 --stall-duration-ms 170
target/release/recorder verify \
  --input stall.mcap --report stall-summary.json --loss-log stall-loss.jsonl
```

- `copy` は毎回退避します。`borrow` は DMA の Lease を Writer に渡します。`adaptive` は取得直後の空き数が `capture-reserve` 以下の場合に退避し、退避先が枯渇した場合は欠損を記録します。
- `--sink null` は Writer の実データ I/O を省き、サイドカーだけを出力します。アプリケーション内の生成・転送能力の計測に使います。MCAP の `verify` は `--sink mcap` で記録したファイルに適用します。
- `--stall-at-ms` と `--stall-period-ms` は取得開始からの単発・周期停止です。周期停止の初回は1周期後で、両設定は同時に使えます。停止区間中は Writer のキュー消費を止めます。実行中の同期書込みは中断せず、復帰時に残っている停止時間を適用します。停止はソフトウェア注入です。
- `--writer-bytes-per-sec` は Writer が1フレームを所有してから、設定されたバイトレート相当の時間を待って書き込みます。書込み時間も加わるため実測帯域は上限値以下になります。Lease の返却期限は監視用で、期限を超えても Writer の参照が終了するまで保持します。

### データ形式と検証

予定連番は0起点の `u64` です。単調時計の絶対スケジュールで、連番 `n` の開始は `ceil(n × 1e9 / fps)` ns です。録画時間の範囲には開始時刻を含み、終了時刻を含みません。取得が遅れて通過したスロットは `source_deadline_miss` として記録し、追いつくためのバースト生成は行いません。実行開始時に取得した Unix 時刻を原点として、MCAP の `publish_time` に予定時刻、`log_time` に取得ループが処理を開始した時刻を格納します。

MCAP のトピックは `/synthetic/dma`、スキーマは `recorder.SyntheticFrameV1`、メッセージ encoding は `recorder-dma-v1` です。1フレームの最小サイズは10バイトです。先頭8バイトは連番、続く2バイトはストリーム ID（V1 は0）で、両方ともリトルエンディアンです。MCAP ヘッダーの32ビット sequence は連番の下位32ビットです。ボディーには以下で作る256バイトを繰り返し、最後はフレームのサイズに合わせて切り詰めます。

1. 64ビットの初期状態を `seed XOR rotate_left(sequence, 21) XOR (stream_id << 48)` とします。
2. 状態に `0x9e3779b97f4a7c15` を加え、得られた値を `z` とします。次に `z = (z XOR (z >> 30)) × 0xbf58476d1ce4e5b9`、`z = (z XOR (z >> 27)) × 0x94d049bb133111eb` の順で計算します。加算と乗算は64ビットで周回します。
3. `z XOR (z >> 31)` をリトルエンディアンで出力し、手順2から32語を得ます。seed、サイズ、ストリーム ID、実行開始時刻は Channel の metadata に入ります。

公式 MCAP Rust ライブラリで Message を直接 `File` に書きます。Chunk、圧縮、Summary、シーク用インデックスは作成しません。非圧縮 Message の本体は借用スライスで渡し、ライブラリはアプリケーション側のフレームサイズの中間バッファを使わず、CRC 計算と同期 `write_all` を行います。通常のファイル I/O ではカーネルのページキャッシュが関与します。コピー量はアプリケーションの CopyOut のみを数えます。 [MCAP 仕様](https://mcap.dev/spec)

正常終了時はキューを drain し、Footer と末尾 magic を書いた上で flush、`sync_data` を実行します。ジャーナルも flush と `sync_data` を実行します。Lease または退避バッファは Writer がフレームを参照し終えた時点で返却されます。書込みエラーの場合はそのレコードを全体再試行せず、既知の失敗を `writer_error`、処理の確定できなかった範囲を `unknown` として記録します。同期失敗時は過去の書込みも含めて耐久性が未確定です。サマリの `uncertain_ranges` はこの範囲を保守的に示します。

欠損ジャーナルは半開区間 `[start_sequence, end_sequence_exclusive)` の JSONL です。取得側と Writer 側は連続する同一理由をそれぞれ1範囲にまとめ、固定容量のチャネル（既定1024イベント）で監視スレッドに渡します。原因は `source_deadline_miss`、`dma_pool_exhausted`、`spill_pool_exhausted`、`queue_full`、`writer_error`、`unknown` です。ジャーナルのチャネル溢れや書込み失敗では実行をエラー扱いにします。SIGINT・SIGTERM では取得を終了し、人工的な待機を打ち切って受理済みキューを drain します。停止時は `cancelled`、未試行の予定連番は `unknown` として、非ゼロで終了します。

`verify` は独立した公式 MCAP Linear Reader で逐次読出しし、DataEnd、Footer、両端の magic、CRC、Schema、Channel、全ペイロード、連番、時刻、欠損ジャーナルを照合します。保持するのは1レコード分のバッファと連番の連続区間です。区間数は欠損や不正な順序により増えることがあります。帳尻の合った申告済み欠損だけなら成功します。未申告・誤申告・重複・破損・不確定状態を検出した場合は JSON に結果を出し、非ゼロで終了します。完全性の判定には録画の正常終了、ファイル同期成功、`verify` 成功が必要です。

### 計測の読み方

- サマリに入力・生成・受理・書込み・各欠損の件数、確保済みペイロード量、プールごとの実測ピーク使用量、キュー深さ、コピー回数とバイト数を記録します。プールは開始時に全ページを初期化します。
- 遅延の単位は ns です。ヒストグラムは固定容量で `count`、`min`、`mean`、`p50`、`p95`、`p99`、`p99_9`、`max` を報告します。分位値はヒストグラム区間の上限による近似です。短い試験ではサンプル数とあわせて読み取ります。
- CSV には既定100 ms間隔の帯域、空き・保持バッファ数、キュー深さ、累積コピー・欠損、RSS、警告フラグがあります。警告は DMA 空きが予約数以下、退避空きが1以下、最も早い DMA 期限の残りが `max-hold-ms` の20%以下のときに立ちます。期限超過は全 Lease の返却時に計数するため、警告のサンプリング間隔より短い超過もサマリに残ります。
- `writer_bytes_per_sec` は予定開始からキュー排出完了までのペイロード帯域です。 `synced_payload_bytes_per_sec` は MCAP の最終同期までを分母に取ります。CSV の入出力帯域は各サンプリング区間の差分です。
- プロセス CPU time と RSS・ピーク RSS、監視スレッドの CPU time、計測とジャーナル I/O にかかった壁時計時間を分けて報告します。監視 CPU time は監視スレッド内の計測値です。取得・Writer のカウンター更新に要する CPU time はプロセス CPU に含まれます。
- OS 側の測定を併用する場合は `findmnt -T 出力先` でデバイスを確認し、`iostat -dx 1`、`pidstat -urd 1`、または `perf stat -- target/release/recorder record ...` で比較できます。ブロックデバイス統計には同一デバイスへの別プロセスの I/O も入ります。

### 検証と再現

```sh
cargo fmt --check
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo build --release --locked
python3 tools/bench.py --out target/bench-v1-new
```

`tools/bench.py` は E0〜E7 の録画、MCAP の全量検証、期待する欠損・回復条件の判定を行います。どの試行も個別ディレクトリに残り、実際の引数、サマリ、CSV、欠損ジャーナル、Verifier の JSON を比較できます。 `--repeats N` で個別結果を保持して反復できます。 `--suite policy` は E0〜E6、 `--suite ssd` は SSD ベースラインと E7 を実行します。SSD ベースラインには GNU `dd` の direct I/O と最終 `fdatasync` を使い、レコーダー自体は通常の同期ファイル I/O を使います。

#### 実測結果

Linux 7.0.0、Core Ultra 7 265KF、KIOXIA EXCERIA PLUS G3 NVMe、ext4 上で release ビルドを測定しました。E1〜E6 は方式ごとに同じ seed、144予定フレーム、64 KiB × 120 fps、DMA 8、退避32、キュー40です。

| 実験 | 条件 | 欠損 Copy / Borrow / Adaptive | Adaptive の CopyOut |
| --- | --- | ---: | ---: |
| E1 | 停止なし | 0 / 0 / 0 | 0 MiB |
| E2 | 30 ms 停止 | 0 / 0 / 0 | 0 MiB |
| E3 | 170 ms 停止 | 0 / 12 / 0 | 0.9375 MiB |
| E4 | 450 ms 停止 | 22 / 46 / 17 | 2 MiB |
| E5 | 350 ms ごとに170 ms 停止 | 0 / 36 / 0 | 2.8125 MiB |
| E6 | Writer を3 MiB/sに制限 | 56 / 80 / 51 | 4.4375 MiB |

E0 の NullSink は 4 MiB × 1,920 fps（約8.05 GB/s）の0.3秒試行で576/576フレームを処理しました。3,840 fps では全1,152予定フレーム中4件が生成期限超過でした。高速 NullSink は書込み処理を省略するため、これらは疑似入力とメモリ処理の値です。

E3 の Copy は9 MiBを退避しました。Adaptive の退避量は15フレーム分で、Copy より89.6%少なく、Borrow の欠損はすべて DMA 枯渇でした。E4 と E6 の欠損は全件ジャーナルと一致しました。E5 の Adaptive は周期停止ごとにキューが空まで回復し、DMA 期限接近の警告24サンプルと、15件の返却期限超過を記録しました。全 MCAP 試行で未申告欠損、誤申告、重複、ペイロード破損は0件でした。すべての試行でプールは全返却されました。

E7 は 4 MiB × 240 fps（1.007 GB/s）の0.2秒試行で、各方式48/48フレーム（192 MiB）を独立検証できました。設定したペイロードプールは全方式640 MiB、ピーク RSS は約644 MiBでした。

| 方式 | CopyOut | Writer 排出まで | 最終同期時間 | 同期込みペイロード帯域 | プロセス CPU time |
| --- | ---: | ---: | ---: | ---: | ---: |
| Copy | 192 MiB | 1,006 MB/s | 316 ms | 390.1 MB/s | 455 ms |
| Borrow | 0 MiB | 1,006 MB/s | 316 ms | 390.4 MB/s | 420 ms |
| Adaptive | 0 MiB | 1,006 MB/s | 243 ms | 454.3 MB/s | 364 ms |

同じ出力先の192 MiB direct I/O ベースラインは、最終同期込みで639 MB/sでした。デバイス統計ではベースラインの書込み区間で約202 MB、E7 各録画で約201〜204 MB のブロック書込みを観測しました。キューの最終排出は E7 全方式で7 µs未満で、最終同期には243〜316 msかかりました。この試行で同期込みのスループットを制約したのは主にストレージへの同期です。各方式1回の測定であり、方式間の同期時間差を CopyOut の効果とは判定できません。SSD 内部キャッシュ枯渇後の定常帯域はこの容量では確認できていません。

別の容量制限付き試行では、Adaptive を 4 MiB × 24 fps で2秒間動かし、48/48フレームを検証しました。取得中の Writer 帯域は100.7 MB/s、最終同期は73.6 ms、同期込み帯域は97.1 MB/sでした。Linux のファイルサイズ制限に達する障害も実ファイルで注入しました。書込みエラー1件と未確定14件を記録し、全バッファを返却して録画は非ゼロ終了しました。Verifier も未完了ファイルを拒否しました。

開発環境での個別実測は `target/bench-v1-20261122/comparison.json` と同ディレクトリ内の試行結果に保存しました。追加の2秒試行は `ssd-2s/`、ファイルサイズ制限の試行は `write-error/` にあります。長時間試験を行う場合は、対象 SSD の空き容量、システム負荷、記録領域を確認の上、以下のように明示的に実行できます。例の保存量はベースラインと3方式を合わせて約24 GBです。1 GB/s で録画を1分続ける場合は1方式で約60 GBの領域を消費します。

```sh
df -h target
python3 tools/bench.py --suite ssd --out target/bench-v1-sustained \
  --high-seconds 5 --disk-baseline-mib 8192 --allow-large-output
```

### Live UDP・ROS 2・SocketCAN

`record-live` は静的に設定した実入力を1つの非チャンク MCAP に記録します。ROS 2 は実行時の raw subscription bridge を使うため、SDKなしで Rust をビルドできます。

- [実行手順と検証結果](docs/real-source-e2e.md)
- [Ingress・Transform・Codec の仕様](docs/real-source-architecture.md)
