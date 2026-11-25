# 複数ソース Deadline-aware の検証結果

[試行別 CSV](multi-source-results.csv) · [全比較 JSON](multi-source-results.json) · [設計・再現手順](multi-source-deadline.md)

## 基点と回帰

GitHub master と一致した `ca246e7` で fmt、clippy、43テストが成功。基点バイナリで M1 を3回録画し、毎回28/28件と期限超過6件を再現した。新バイナリでその MCAP と sidecar を検証し、旧バイナリでも新 legacy の録画を検証した。

最終の `cargo fmt --check`、`cargo clippy --locked --all-targets -- -D warnings`、`cargo test --locked` は成功。52テスト。V1 E0–E6、V2 P1–P9、V3 smoke、単一センサ Deadline-aware の6反復を再実行し、全判定に合格。V2 P10 と実 io_uring は未再試験。既存の30秒1 GB/s試験は初期実装の結果で、本変更はH0の短時間で比較した。

## M1〜M10

低容量3反復、計45試行。各競合ケースでデバイス別プール、Spill、Writer、同期、入力とseedを揃え、実行順をローテーション。M3 は seed で位相とjitterを変えた。全試行で破損、重複、未申告欠損、Unknownは0。

| ID | 録画と観測 |
| --- | --- |
| M1 | 2ソースと同時burst。全方式28/28。基点/legacyの期限超過6、Always Borrow 8、Always Copy/global 0。globalとlegacyは32 KiB、Always Copyは112 KiB。globalは全件保存で71.4%削減 |
| M2 | 同位相8ソースで56/56、期限超過0。仮想時刻で同一期限8件の全処理を確認 |
| M3 | 位相・jitter・burstを変えた8ソースで105/105、期限超過0 |
| M4 | 固定期限とpoolの混在で35/35。仮想時刻で二重計上回避と需要2件の選択を確認 |
| M5 | 4 MiB / 256 KiBで18/18、期限超過0。仮想時刻で先行blockingとEDFの反例を確認 |
| M6 | 事前登録予備を全4枠使用した後もCopyOut。70/70を維持し、予備なしglobalの60 KiBから44 KiB |
| M7 | Spill 1枠で25/28、適合クラスなしで20/28。受理後の3件／8件は全件spill枯渇。警告と期限超過を観測 |
| M8 | 25 msのコピー遅延に対し22 ms期限。Planner Rejected、実行時に成立不能1区間と契約違反。全28件を保存したが期限超過も28件 |
| M9 | 13 msの擬似非同期I/Oで全I/O peak 4、borrow deferral 12件、28/28。Writer先行借用下で期限超過7件が残った |
| M10 | 20 fps × burst 2 と5 fpsで16/16、期限超過0、両ソースの順序を確認 |

M1〜M5の計画と順序、M6の予備とACK、M8の計算限界、M9の所有権、M10の同着順には決定的テストを用いた。M7の実行時結果は通常録画での実時計試験。M1のglobalは全コピーを前提にした保守的予算ではRejected。無条件の飢餓防止は未検証。

## 実 NVMe H0〜H6

Core Ultra 7 265KF、KIOXIA EXCERIA PLUS G3、ext4、Linux 7.0.0-30-generic、CPU 2–9。各ケースの4モードを `--ssd` と `--seconds 0.8` で明示指定。H5はjitterとburstを実行し、計32試行。SSD温度の前後サンプルは25.85〜28.85 °C。ページキャッシュ関連の `/proc/meminfo` を各試行の前後で保存。buffered I/O、128 MiB/200 msグループ、データ fdatasync。入力は約1.074 GB/s、H4の大小混在は約1.007 GB/s。Writer停止・コピー遅延はソフトウェア注入。

表は **legacy → global**。帯域は録画開始から最終データ同期までのpayload、I/O p99はsubmit→completion。「—」は欠損がなく先行時間が定義されない場合。Always Copy/Borrowを含む全モードと同期時間はCSV/JSONを参照。

| ID | 照合済み | 欠損 | 期限超過 | CopyOut MiB | CPU ms | I/O p99 ms | 帯域 MB/s | AT_RISK先行 ms |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| H0 無停止・分散 | 205 → 205 | 0 → 0 | 0 → 0 | 0 → 0 | 839 → 651 | 6.82 → 4.19 | 1036.6 → 1029.1 | — |
| H1 100 ms・分散 | 205 → 205 | 0 → 0 | 0 → 0 | 36 → 116 | 665 → 687 | 3.28 → 3.54 | 1026.5 → 1052.3 | — |
| H2 200 ms・同位相 | 208 → 208 | 0 → 0 | 0 → 0 | 368 → 432 | 711 → 707 | 3.01 → 3.28 | 1040.3 → 1041.5 | — |
| H3 500 ms・容量制限 | 139 → 141 | 66 → 64 | 34 → 34 | 300 → 308 | 559 → 550 | 3.41 → 3.54 | 610.7 → 615.8 | 377 → 377 |
| H4 200 ms・サイズ混在 | 456 → 458 | 24 → 22 | 0 → 3 | 262 → 291 | 593 → 604 | 3.01 → 2.62 | 917.1 → 922.0 | 332 → 460 |
| H5 200 ms・jitter | 208 → 208 | 0 → 0 | 0 → 0 | 384 → 452 | 733 → 728 | 3.28 → 4.06 | 1042.1 → 1035.9 | — |
| H5 200 ms・burst | 208 → 208 | 0 → 0 | 0 → 0 | 420 → 504 | 720 → 724 | 4.06 → 4.46 | 1038.5 → 1046.6 | — |
| H6 200 ms＋10 msコピー遅延 | 203 → 205 | 2 → 0 | 139 → 138 | 556 → 564 | 721 → 768 | 2.75 → 3.80 | 471.4 → 472.4 | 667 → — |

H2 のglobalは Always Copyの832 MiBに対して432 MiBで48.1%削減。双方208件を保存し期限超過0。Always Borrowも208件保存したが期限超過83件。受理→データ同期 p99 はlegacy 282.4 ms、global 277.8 ms、Always Copy 291.2 ms、Always Borrow 290.8 ms。

H1では同一保存件数でもglobalのコピー量が36→116 MiB。H4は欠損が2件減る一方、期限超過が3件増えた。H6は2件のDMA枯渇を防いだが、138件の期限超過が残った。H3のglobalは64件のキュー満杯を報告した。全高負荷試行で未申告欠損、Unknown、破損、重複は0。

高負荷の各条件は1反復なので、CPU・p99・2件差の再現性は未確認。実センサ、共有物理プール、実SSD停止、DRAM競合、30秒持続・熱平衡は未検証。生データ・コマンド・設定は `target/multi-source-qualification/` に約26 GiB。JSONの `git_sha` は基点で、試行時の作業ツリー状態と計測バイナリのSHA-256を併記した。
