# リファクタリング前の V3 ベースライン

V3 の実装段階で 41 テスト、fmt、all-targets/all-features の clippy (-D warnings) が成功。8 チャンネルの MCAP（Synthetic、6 系統 Mock、ROS 2 の追加型）、欠損 journal、コミット journal、CLI の help/能力表示、末尾破損と SIGKILL からの復旧を確認した。末尾切断、journal 切断、コミット CRC 改変、データ sync 失敗のテストを変更前に追加した。

着手前の V1 E0–E6 と V2 P1–P9 の再実行は全判定 PASS。スモークの生データは target/v3-evidence/v1-before と target/v3-evidence/v2-before。V3 バイナリを target/v3-evidence/recorder-v3-final に、ソースを v3-source-before-refactor.tar.gz に保存した。target/v3-evidence/v3-baseline-final/sync-1 に全チャンネルの MCAP、verify.json、recover.json と各 sidecar がある。

## 測定条件と結果

Core Ultra 7 265KF、Linux 7.0.0-30-generic、Rust 1.91.1 release、KIOXIA EXCERIA PLUS G3、ext4。CPU 2–5 に pinning。SSD 温度・キャッシュ枯渇は未測定。入力は configs/v3-smoke.toml の 8 チャンネル、合計 320 frames/s、MCAP Message ヘッダー込み 685,280 B/s。同期はグループ毎の data fdatasync、後続の journal fdatasync と終了時同期。Null は書込み先のある測定と分けて集計。RSS と CPU は recorder process の値。

| モード | 試行 | 同期込み MB/s | accepted→data sync p99 / p99.9 / max | submit→complete p99 / p99.9 / max | CopyOut | CPU | peak RSS | queue peak | 既知欠損 / Unknown |
| --- | ---: | ---: | --- | --- | --- | --- | --- | --- | --- |
| NVMe sync | 3 × 2 s | 0.673801–0.674224 | 各 102.17–102.27 ms | 20.48–24.58 / 35.36–54.00 / 35.36–54.00 µs | 0 B/GiB、サンプルなし | 3.20–3.34% | 6.32–6.41 MiB | 2 | 各 7 / 0 |
| Null | 3 × 2 s | 該当なし | 該当なし | 9.22–9.73 / 9.88–11.63 / 9.88–11.63 µs | 0 B | 3.04–3.06% | 6.30–6.37 MiB | 1 | 各 7 / 0 |
| NVMe sync | 1 × 30 s | 0.674643 | 各 102.29 ms | 24.58 / 47.10 / 98.30 µs | 0 B/GiB、サンプルなし | 3.23% | 6.34 MiB | 2 | 109 / 0 |

2 秒の試行は 633/640、30 秒は 9,491/9,600。欠損は設定にある Mock UDP の事前欠損で、受理済み欠損と未申告欠損は 0。帯域は入力ペースで制限される。1 GB/s 持続と 2 GB/s は未評価。

```sh
taskset -c 2-5 target/v3-evidence/recorder-v3-final benchmark --suite v3-smoke --output-dir target/v3-evidence/v3-baseline-final
taskset -c 2-5 python3 tools/bench_v3.py --recorder target/v3-evidence/recorder-v3 --out target/v3-evidence/v3-30s --seconds 30 --repeats 1
```

Always Copy の補助試行も 3 × 2 s。同期込み 0.6742–0.6743 MB/s、CopyOut は毎回 1,310,720 B（約 0.971 GiB/GiB）、CopyOut p99 3.07–3.33 µs、p99.9 と max は 3.45–3.70 µs。結果は v3-copy-before/comparison.json に保存した。
