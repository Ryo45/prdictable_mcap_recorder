# 1 GB/s 級の持続入力と Writer 飽和試験

対象は `dc543fb` の release ビルド。Linux 上の Core Ultra 7 265KF と KIOXIA EXCERIA PLUS G3 NVMe（ext4）を使用し、録画を CPU 2–9 に固定した。8系統のカメラ Mock（V4L2、proFRAME、NvSIPL 相当）を各 4 MiB × 32 fps、位相をずらして入力した。ペイロードの合計は 1.073742 GB/s。各バッファの準備完了・返却遅延は100 µs、キュー上限は96、ペイロードの事前確保は544 MiB。同期 Writer と128 MiB / 200 ms のグループ設定を使った。GB・GB/s は十進、MiB は二進。

## 15秒の持続入力

16.106 GBを記録し、最終データ同期までの平均は **1.070621 GB/s**。3,840件すべてがコミットされ、独立 Verifier で照合できた。既知欠損、受理後欠損、Unknown、未申告欠損、CopyOut は各0。キュー最大17/96、RSS 最大549 MiB、受理からデータ同期の p99 は176.2 ms、最大189.6 ms。契約違反警告は14件。

## 30秒の持続入力

32.212 GBを記録し、最終データ同期までの平均は **1.072486 GB/s**。7,680件すべてを独立 Verifier で照合し、欠損、Unknown、重複、破損は各0。コミット時刻を5秒窓で集計すると、0–5秒は1.047 GB/s、5–30秒の各窓は1.074 GB/s。最後の32件は30秒境界の35 ms後までに同期された。

キュー最大14/96、CopyOut 0、RSS 最大549 MiB。受理からデータ同期の p99 は176.2 ms、最大179.7 ms。データ同期単体の p99 は54.5 ms、最大56.7 ms。契約違反警告は3件。NVMe の温度サンプルは27.85–29.85 °C。帯域は入力ペースで制限されているため、この測定から SSD の最大帯域や長時間熱平衡の性能は判定できない。

## Writer の意図的な飽和

入力を維持し、Writer に450 MB/s のソフトウェア上限と、開始1.8秒で120 ms の停止を注入。6秒間に1,536件を生成し、約0.60秒でキューが96/96に達した。最初のリスク警告は141 ms、最初の欠損は586 ms（約445 ms の先行警告）。受付前に908件（59.1%）が `queue_full` となり、受理した628件は全件同期・照合できた。受理後欠損、Unknown、未申告欠損、重複、破損は各0。

- 同期込みペイロード帯域は373.1 MB/s。記録終了後の drain と最終データ同期を含めて7.059秒。
- CopyOut は493件・2.068 GB。受理からデータ同期の p99 は1.611秒、最大1.683秒。
- 628件のコミットと908件の欠損を独立 Verifier と loss journal で照合した。450 MB/s は注入したペース制限。実SSDの飽和限界はこの結果から算出できない。

## 判定条件と再現

Planner は3試行とも Rejected。持続試験では全コピーを前提とした需要1.074 GB/sが設定上のコピー下限1.000 GB/sを上回った。飽和試験ではさらに、ストレージ上限と停止時間が契約外。明示的に `--allow-unverified` を使用した。帯域の分母は予定開始から最終データ同期まで。journal 自体も同期するが、その完了待ちは帯域の分母に含めていない。実センサ、電源断耐性、SSD内部キャッシュの枯渇は別の検証項目。

設定、Planner 出力、MCAP、履歴 CSV、欠損・コミット journal、Verifier JSON は `target/writer-load.f2qPef/` に保存。既存の結果を上書きしない出力先を指定する。30秒試行には約32.3 GBの空きが必要。

```sh
root=target/writer-load.f2qPef
cargo build --release --locked
taskset -c 2-9 target/release/recorder record --config "$root/sustained-30s.toml" \
  --duration-sec 30 --allow-large-output --allow-unverified --output-dir "$root/repeat-30s"
target/release/recorder verify --run-dir "$root/repeat-30s"
taskset -c 2-9 target/release/recorder record --config "$root/saturated.toml" \
  --duration-sec 6 --allow-large-output --allow-unverified --output-dir "$root/repeat-saturated"
target/release/recorder verify --run-dir "$root/repeat-saturated"
```
