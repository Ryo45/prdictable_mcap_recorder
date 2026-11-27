# 実入力の実行・検証

## 実行コマンド

Rust ビルドに ROS 2 SDK は不要。ROS 2 を使うときは ROS の環境を読み込み、Python の `rclpy`・`ament_index_python`・`rosidl_runtime_py`・対象メッセージを利用できる状態で起動する。独立 Python Decoder の Zstd 復元は Linux の `libzstd.so.1` を使用する。

```sh
cargo build --release --locked
mkdir -p target
target/release/recorder plan-live --config configs/live-udp.toml
target/release/recorder record-live --config configs/live-udp.toml \
  --duration-sec 10 --output target/live-udp.mcap
target/release/recorder verify-live --input target/live-udp.mcap
target/release/recorder recover --input target/live-udp.mcap --dry-run
python3 -B tools/decode_live.py target/live-udp.mcap
```

上の手動録画の UDP 宛先は localhost の2368・2369。次の試験プログラムは送信側も起動し、100 Hz、空 Datagramを含む可変長データ、意図的な切詰め、宛先・Interface、identity/Zstd 復元を照合する。出力先は毎回新規パスとする。

```sh
python3 -B tools/test_real_sources.py --mode udp --out target/udp-test --seconds 3
python3 -B tools/test_real_sources.py --mode udp --out target/udp-ring --seconds 3 --writer io-uring
python3 -B tools/test_real_sources.py --mode udp --out target/udp-stop --seconds 3 --stop-early
```

## 実 CAN / ROS 2 の再現

管理者が準備した未使用 vcan Interface を用いる。新規作成の例（実 CAN バスへは送信しない）。

```sh
sudo modprobe vcan
sudo ip link add dev vcan0 type vcan
sudo ip link set dev vcan0 mtu 72
sudo ip link set dev vcan0 up
python3 -B tools/test_real_sources.py --mode can --interface vcan0 --out target/can-test
python3 -B tools/decode_live.py target/can-test/run.mcap --candump
```

ROS 2の環境 setup を読み込んだシェルで実行する。試験はテスト Topic に String と Image を配信し、別プロセスで生 CDR を受信する。送信側で生成した CDR と Record Payload を比較する。混在版では UDP・CANも同時に送り、1ファイルの各 Channel・Payload・観測連番・CRC・Journal・最終同期を照合する。

```sh
python3 -B tools/test_real_sources.py --mode ros --out target/ros-test --seconds 10
python3 -B tools/test_real_sources.py --mode mixed --interface vcan0 \
  --out target/mixed-test --seconds 12
target/release/recorder record-live --config configs/live-mixed.toml \
  --duration-sec 12 --output target/live-mixed.mcap
```

## 検証結果

Linux 7.0.0-30-generic。native UDP と io_uring は、ソケット作成を制限する Codex サンドボックスの外で実行した。Rust の fmt、clippy、67テストが SDK なしで通過。回帰には V1/V2/V3、Mock Async、Barrier/Overlap、EIO、ENOSPC、Journal障害、SIGKILL、Recovery を含む。

| 試験 | 結果 |
|---|---|
| 実 UDP + sync、2 Channel | 100＋100件・Payload とアドレス/ポート/Interface一致。意図した切詰め2件のみ。独立 Python Decoder、Rust Verifier、Recovery 合格 |
| 実 UDP + io_uring、2 Channel | 同じく200件＋意図した切詰め2件。独立 Decoder、Verifier、Recovery 合格 |
| 実 UDP + SIGINT | 200件を drain・最終同期し、意図した切詰め2件を照合。Verifier と Recovery 合格 |
| ROS raw Bridge の手前のIPC fixture | CDRバイト列と送信時刻、汎用 Rust MCAP Reader、CRC改変拒否、Recovery、Schema起動失敗/受信エラー分類、圧縮設定拒否を検証 |
| SocketCAN ABI fixture | Classical DLC と FDの長さ／FDフラグの単体テスト合格 |
| vcan の実受信 | **未検証**。対象 Interface が存在せず、独立したネットワーク名前空間は権限で拒否された |
| ROS 2 String / Image の実 Middleware、3系統混在 | **未検証**。この環境に ROS 2 がない。混在試験の送信・照合コマンドは提供 |
| ROS 2 標準 bag ツール、実 CAN 車載バス、電源断 | **未検証** |

3秒の UDP 試験は両 Writer とも最大キュー2、MCAP 25,678 bytes。プロセス CPU は sync 約33 ms、io_uring 約37 ms、両方のピーク RSS は約5.7 MiB。この小入力から高帯域時の性能は導かない。カーネルの受信キュー溢れカウンタは未観測。切詰めの分類は実データで確認した。

### 既存 Mock の軽量比較

master と今回のビルドを同一 NVMe、CPU 0–7、2系統×1 MiB、各3反復で比較した。通常ペースの帯域中央値は **1.017 → 1.016 GB/s**、Queue最大は双方13、欠損0。短時間過負荷は **1.432 → 1.416 GB/s**、Queue最大63→64、追加ビルドの1試行で3件の申告済み欠損。全試行が独立 Verifier 合格、Unknown 0。過負荷域で無退行を確認したとは扱わない。トレースなしの小標本なので差の原因は特定していない。

```sh
python3 -B tools/bench_durability.py --before /path/to/master/recorder \
  --recorder target/release/recorder --out target/mock-live-regression
```

[入力と形式の仕様](real-source-architecture.md) · [機械可読の実測結果](real-source-evidence)
