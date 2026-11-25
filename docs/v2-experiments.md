# V2 の検証結果

Linux 7.0.0、Core Ultra 7 265KF、KIOXIA EXCERIA PLUS G3 NVMe、ext4、Rust 1.91.1 の release ビルドで実行しました。機能用の契約値は `configs/v2-smoke.toml`、各ケースの差分は出力の `run.toml`、実際の引数は `commands.json` に保存しています。記載した帯域は十進 MB/s、ファイル・コピーサイズの KiB / MiB は二進単位です。

## 再現

```sh
cargo fmt --check
cargo clippy --all-targets -- -D warnings
cargo test
cargo build --release --locked

python3 tools/bench.py --suite policy --out target/v1-regression-new
target/release/recorder benchmark --output-dir target/v2-smoke-new
target/release/recorder benchmark --suite ssd --output-dir target/v2-ssd-new
```

V2 の比較には Python 3.11 以降を使います。SSD スイートは GNU `dd` の direct I/O と `fdatasync` をベースラインに使い、レコーダーは通常の buffered I/O を使います。各出力ディレクトリは新規に作成します。 `--include-io-uring` を付けると実非同期の比較と short completion の検証も実行します。通常の隔離環境で `io_uring_setup` は EPERM、ホスト上では NOP、実ファイル書込み、short completion と全量検証に成功しました。実行権限の確認には `python3 tools/probe_io_uring.py` を使えます。

## P0〜P9

P0 は V1 の E0〜E6 を V2 着手前と最終ビルドで再実行しました。3方式の同条件と独立 Verifier を維持しました。 [P0 の条件と基準値](v1-retrospective.md)

| 実験 | 結果 |
| --- | --- |
| P1 | 固定期限、プール、置換可能、Owned の4入力。サイズ192〜8192 B、周期、位相、ジッタ、同時バーストが異なる378/378フレームを検証。Planner 合格、実行時の契約逸脱0 |
| P2 | 58 ms 停止を注入し、固定期限・プール要求を同時に観測。378/378、8 KiB CopyOut、固定期限超過0 |
| P3 | 170 ms 停止で全5方式60/60。固定期限超過は V1 閾値方式11件、Deadline-aware 0件。Always Copy は240 KiB、Deadline-aware は52 KiB |
| P4 | 160 ms ごとに60 ms停止。472/472、キューピーク39。4回とも停止終了後の約10.1 msのサンプルでキューが空に戻った |
| P5 | 同じ初期確保量で両方式96/96。CopyOut は50 KiBから8 KiBへ84%減少。事前登録予備の有効化5回、明示置換5回 |
| P6 | 72 ms のコピー遅延注入で期限超過37件、欠損11件。全欠損が一致し契約逸脱を検出。コピー失敗注入の1件も照合。別の単一CPU上の32 MiBメモリコピー競合試験では60/60 |
| P7 | Planner が起動を拒否。明示指定で動かし、378件中277件を同期・検証、残る101件はジャーナルに一致。最初のリスク警告は最初の欠損の約261 ms前 |
| P8 | 同期、擬似非同期、実 `io_uring` の全方式で378/378。擬似非同期は順不同完了を発生させ、全方式で stream 内の物理順序を保持 |
| P9 | 同期と実 `io_uring` の短い書込みをそれぞれ378/378検証。EIO、ENOSPC、同期失敗では先行の同期済み3件を検証し、残り375件を Unknown と分類。録画と全体 Verifier は非ゼロ終了 |

P3 はキュー内の古い Lease への退避が必要な事例です。新規フレームを受け取る時点では空き数が閾値を割らず、V1 閾値方式はコピーしませんでした。Deadline-aware は13件を退避し、Always Copy の全60件に対しコピー量を78.3%減らしました。P6 では大きいコピーが実行中なら、後から来た期限の早いコピーをプリエンプトできないことも、仮想時刻のテストで確認しました。

P8 の値は同じ低帯域入力とグループ設定での比較です。擬似バックエンドには要求ごとの待機を注入しています。

| Backend | 最大 in-flight | submit→completion p99 / p99.9 | プロセス CPU time |
| --- | ---: | ---: | ---: |
| 同期 | 1 | 27.6 / 37.6 µs | 22.6 ms |
| 擬似非同期 | 6 | 5.17 / 5.17 ms | 41.2 ms |
| 実 `io_uring` | 3 | 180.2 / 230.8 µs | 34.2 ms |

P1〜P9 の全ケースでプール・キュー・in-flight の終了時回収を検査しました。ソフトウェアで注入した停止や遅延は、デバイス内部の停止と区別しています。Rust の試験には保持ハンドルの一回返却・世代識別、Copy と Writer の競合、返却通知遅延、取消し要求後の元 I/O の所有権、SIGTERM 中の drain、誤った commit membership と payload 改変の検出を含めました。Loom と Miri は未実施です。

## P10 と性能目標

同一 SSD の192 MiB direct I/O は最終同期込み2.342 GB/sでした。約86 msの短い基準測定です。P10 の設定にはこの値を Planner の暫定入力として保存しました。V1 時の同サイズ基準値は639 MB/sで、ストレージ条件に変動があります。両測定とも、SSDの内部キャッシュ枯渇後の値は得ていません。

| 入力 | Backend | コミット回数 | 検証済み | 同期込みペイロード帯域 |
| --- | --- | ---: | ---: | ---: |
| 8 × 4 MiB × 30 fpsを0.2秒 | 同期 | 4 | 48/48 | 892.4 MB/s |
| 同上 | 実 `io_uring` | 4 | 48/48 | 891.2 MB/s |
| 8 × 4 MiB × 3 fpsを2秒 | 同期 | 8 | 48/48 | 100.5 MB/s |

1.007 GB/s 入力の試行は、保守的な全コピー仮定でコピー下限と退避枠が不足するため Planner は拒否しました。 `--allow-unverified` で試験し、実際の CopyOut と欠損は0でした。同期方式・実非同期方式とも、約226 msの試行のうち約71 msがグループ同期です。同期中は新規投入を止めます。実非同期では4要求を並行化し CPU time は329 msから313 msに減りましたが、この設定の同期込み帯域はほぼ同じでした。2秒の試行は Planner 合格かつ契約内でした。

追加試行では 8 × 4 MiB × 60 fps（2.013 GB/s）の0.1秒入力を実 `io_uring` で受け、いずれも全48フレームを同期・検証しました。同じ入力・同じメモリ予算で同期グループだけを変えました。

| グループ閾値 | コミット回数 | 最大同期時間 | 同期込みペイロード帯域 |
| --- | ---: | ---: | ---: |
| 64 MiB | 3 | 22.8 ms | 1.353 GB/s |
| 128 MiB | 2 | 48.1 ms | 1.235 GB/s |

一時的には同期込み1 GB/sを超え、グループ拡大で最大停止とキューピークが増える事例も観測しました。1 GB/sの長時間持続と同期込み2 GB/sは未確認です。2 GB/s を評価するには、実データでの定常サービス下限、同時コピー下限、ストレージ空き容量を測定し直す必要があります。現実の同期停止に対する耐性と、同期境界における安全な並行I/Oの設計が次の比較候補です。

短い追加試験の設定は P10 の `P10-io-uring/run.toml` を基に、全源 `fps = 60`、 `phase_us = floor(index × 1e6 / 480)`、 `commit_bytes = 67108864` または `134217728` とし、 `record --duration-sec 0.1 --allow-unverified` で再現できます。

長時間の高帯域試験は空き容量を確認し、以下のように明示的に実行します。例は同期と実非同期の2方式で約14 GBの記録領域を使用します。 `--high-fps-per-source 60` を指定すると入力目標が約2.013 GB/sとなり、MCAP の必要容量はおよそ倍になります。

```sh
df -h target
target/release/recorder benchmark --suite ssd --output-dir target/v2-ssd-long \
  --include-io-uring --high-seconds 5 --allow-large-output
```

開発環境の生データは `target/v2-p0-v1-regression-final/`、`target/v2-p1-p9-final/`、`target/v2-p10-final/`、`target/v2-p10-tuning/` に保存しました。
