# 実入力 Recorder

基点は master `5491984`。実入力は `record-live`、検証は `verify-live`、バッファと出力の見積りは `plan-live`。固定周期の Mock と live は別の入力経路を使い、Writer、Queue、Owned Lease、Durability Coordinator、CRC、Commit Journal、Loss Ledger は共有する。1つの live config に UDP・ROS 2・SocketCAN を並べると、1つの非チャンク・非圧縮 MCAP に出力する。live と Mock の同一ファイルへの混在は今回の構成に含まれない。

## 責務と所有権

- `live/network.rs` と `live/ros.rs` は OS/Middleware の受信と正規化を担当する。Source ごとのスレッドが非ブロッキング受信し、入力がないときは fd の readiness を待つ。最大読取り待ち10 msで停止条件を再確認する。
- 共通 Ingress は観測連番と受信時刻を付与する。受信済み bytes をスレッド専用 scratch から Source 別の事前確保済み Owned Lease に格納する。Identity の UDP/CAN は scratch→Lease へ一度格納する。Zstd では Lease へ直接圧縮する。ROS 2 は Middleware と pipe でコピーが生じるが、CDR のデシリアライズ・再生成・JSON変換は行わない。
- `transform.rs` は入力と出力の slice と再利用可能なコンテキストを扱う。1入力→1出力。変換結果の長さを Codec が記録し、変換失敗では成功レコードを生成せずロス台帳に記録する。`codec.rs` はエンベロープ、Encoding、解釈を担当する。余分な変換は単一の設定段で拒否する。ROS 2 の `cdr` は identity のみ。
- 変換は同一 Source の受信スレッド内で行い、Source 内順序を維持する。バッファ数、scratch サイズ、Queue サイズ、Groupメタデータ、MCAP 最大出力量を制限する。Queue が満杯のときは `queue_full`、空き Owned Lease がないときは既存の `dma_pool_exhausted` 理由を再利用する。同期・コピーの別スレッドを待つ第2の Transform Queue は導入していない。
- 入力スレッドを停止して処理中の変換を完了し、Queue を閉じる。Writer が元の I/O Completion まで Lease を保持し、Coordinator が最終データと Journal を同期、Monitor が Loss Ledger を同期してから Summary を確定する。入力側 socket や DDS に残っていて Recorder がまだ観測していないメッセージは drain の保証範囲外。

## 時刻・連番・上流欠損

`sequence` は Source ごとに0から始まる Recorder 観測連番。観測後に切詰め・変換失敗・QueueFull があれば台帳上の穴になる。MCAP の32 bit連番を周回させず、枯渇時は入力エラーにする。Source 間の順序は Writer の実際の並べ方で、全体を時刻ソートしない。

`log_time` は開始時点の Unix ns に `Instant` の経過を加えた受信時刻。これは Recorder が受信を処理した時刻で、Kernel の物理受信精度や NTP 同期精度は保証しない。ROS 2 Middleware が正の送信時刻を渡した場合は `publish_time` に使い、送信側時計の精度は未確定。取得できない場合は `log_time` を代用し、Channel の `publish_time_origin` に出所を記録する。UDP/CAN には送信時計を推定しない。

UDP の `SO_RXQ_OVFL` は、カーネルが後続の受信に付与したソケット累積値だけを別カウンタに集計する。未取得分の sequence は作らない。付与が観測できなければ値は null。実受信API/Bridge のエラーは `receive_error` として記録し、正常終了として扱わない。既存 Writer の書込み失敗・同期失敗は Unknown。入力が有限の到着包絡線を持つという前提を置かず、`plan-live` の判定は Unknown。Mock の fps や予定件数を設定する必要はない。

## Encoding v1

MCAP プロファイルは `predictable-recorder-live-v1`。ROS 2 Channel は `cdr` と `ros2msg` Schema を持ち、本文がそのまま CDR となる。ROS Bridge は静的に指定した Topic 型を解決し、インストールされた `.msg` と依存型を収集してから起動確認を返す。型や定義が不足する場合は MCAP ファイルを作成しない。QoS は `sensor-data`（ROS既定の sensor data）または `reliable`（keep-last 10）。ネイティブ SDK を Rust のビルド条件から分離するため、別プロセスの [rclpy raw subscription](https://raw.githubusercontent.com/ros2/rclpy/jazzy/rclpy/rclpy/node.py) を採用した。型定義とフレームは上限付き pipe を通り、フレーム先頭の12 byteは little endian の長さ u32 と [Middleware の送信時刻 u64](https://raw.githubusercontent.com/ros2/rclpy/jazzy/rclpy/src/rclpy/subscription.cpp)。ROS bag 用 profile は付与しない。[MCAP Encoding Registry](https://mcap.dev/spec/registry#ros2msg)

UDP は `recorder-udp-v1`、CAN は `recorder-socketcan-v1`。本文は**固定56 byteヘッダー＋可変長本文**。数値は little endian。未使用 byte は0。Transform は0=identity、1=Zstd level 1。MCAP Channel にも方式、バージョン、最大入力サイズを記録する。

| Offset | 長さ | 共通・UDP | CAN |
|---|---:|---|---|
| 0 | 1 | Codec バージョン 1 | 同左 |
| 1 | 1 | Transform ID | 同左 |
| 2–3 | 2 | 送信元・宛先 family。4=IPv4、6=IPv6、宛先0は未取得 | 2 は0=Classical、1=FD。3 は Linux FD Flags（BRS、ESI 等） |
| 4 | 4 | 変換前の実 Payload 長 | 同左 |
| 8 | 4 | 受信 Interface index。0は未取得 | Interface index |
| 12–15 | 4 | 送信元・宛先 Port（u16×2） | Linux raw CAN ID（EFF/RTR/ERRを含む） |
| 16–19 | 4 | IPv6送信元 scope ID | 16は Classical DLC（len8_dlc が有効なら保持）。FD の生 DLC 未取得は255 |
| 24–39 | 16 | 送信元 IP octets。IPv4 は先頭4 byte | 予約 |
| 40–55 | 16 | 宛先 IP octets。IPv4 は先頭4 byte | 予約 |
| 56〜 | 可変 | Datagram本文（0 byteも可）または Zstd frame | データ本文。RTR の要求長はDLCに保存 |

UDP は `recvmsg` の pktinfo と `MSG_TRUNC` を使う。CAN は Linux CAN_RAW、FD とエラーフレームの受信は設定で選択し、Interface名を Channel に保存する。独立の `tools/decode_live.py` は Rust の Codec を使わず、Payload とメタデータを復元し、CAN の candump 風テキストも出力する。

## 永続化と検証

`verify-live` は独立の MCAP 読取りと Codec 復元、Data Section CRC、Prefix CRC、グループ境界、成功フレーム集合、Loss Ledger と Source 別の観測範囲を照合する。センサが送信した内容との比較は E2E ドライバーが別に実行する。Recovery は live profile に対しても確定済み prefix を検査する。両コマンドの現在の全体スキャンは **128 MiBかつ262,144メッセージ以下**。大容量録画の独立整合性検証と ROS bag ツール互換の資格試験は別途必要。

[実行と検証結果](real-source-e2e.md)
