# Deadline-aware BufferLease の比較検証

検証する命題は、期限内に CopyOut と返却を完了できる条件で、Deadline-aware が Always Copy と同じデータをより少ないアプリケーション内コピーで保存し、Always Borrow が同じ条件で起こす返却期限超過や取得欠損を避けられること。効率の主指標は **同期・照合済みの同量データに対する CopyOut bytes**。CPU、帯域、滞留は副指標として別に記録する。

## 成り立つ条件

フレーム `i` の個別返却期限を `D_i`、このフレームの返却で満たす必要があるプール側の期限を `P_i` とする。該当する制約がない場合の期限は無限大。CopyOut 開始を `S_i`、コピー時間を `C_i`、CopyOut 完了後の再利用可能までの時間を `R_i` とすると、

```text
S_i + C_i + R_i <= min(D_i, P_i)
```

を満たせば、その退避元は期限までに再利用可能になる。プールから `k` 個の返却が必要なら、必要な時刻までに `k` 個がこの条件を満たす必要がある。実装は見積もったコピー・返却時間と余裕を期限から引いて CopyOut を起動する。これは、フレームが準備済みでキューにあり、退避枠があり、先に始めたコピーやスケジューリング遅延が開始時刻の余裕に収まるときの十分条件。すでに I/O が参照している Lease は、元 I/O の完了まで保持される。

Always Borrow では返却が Writer 完了に依存する。115–285 ms の停止に対し、毎10 msの入力が116 msから17件到着するモデルを考える。期限は各取得から60 ms。停止終了まで一度も返却できない場合、32スロットなら最初の11件の期限が285 ms以前に来る。10スロットなら初期空きが全10枠あっても停止中の17件のうち少なくとも7件を取得できない。停止境界とスレッド実行時刻の差は実測で確認する。

Always Copy が全件を退避すると、80件 × 256 KiB = 20 MiB をコピーする。Deadline-aware が一部だけを退避し、同じ80件を保存できれば、その差がアプリケーション内で省けたコピー量になる。一般のディスク帯域やカーネル内部のコピー回数はこの式に含まれない。

## 実験設計

Core Ultra 7 265KF、KIOXIA EXCERIA PLUS G3 NVMe / ext4。release バイナリ、CPU affinity 2–9、同期ファイル Writer、Mock V4L2 固定期限、100 fps、256 KiB、0.8秒。カメラ側は fence 100 µs、返却後の再利用遅延250 µs、期限60 ms。各ケース内で確保する DMA と spill の合計、キュー上限48、コミット設定、障害条件と seed を揃えた。予約スロットの動的な追加は使わない。

| ケース | Writer 停止 | DMA + spill | 判定 |
| --- | --- | --- | --- |
| 無停止の対照 | なし | 10 + 32、10.5 MiB | Borrow と Deadline-aware が無欠損・期限内。Deadline-aware は Copy より少ないコピー |
| 期限超過 | 115 msから170 ms | 32 + 32、16 MiB | Borrow は返却超過、Copy と Deadline-aware は全件同期・期限内 |
| プール枯渇 | 同上 | 10 + 32、10.5 MiB | Borrow は取得欠損と返却超過、Copy と Deadline-aware は全件同期・期限内 |
| 共有プール | 同上、個別返却期限なし | 10 + 32、10.5 MiB | Borrow は取得欠損、Deadline-aware はプール側の返却期限で退避 |
| 条件外の反証対照 | 同上、コピーごとに72 ms追加 | 10 + 32、10.5 MiB | Deadline-aware でも期限を超え、契約違反を報告 |

固定期限の各主要ケースで12組、共有プールは6組。同じ seed の3方式を比較し、実行順は6通りを同数回使用。seed はペイロードを変え、到着条件は固定した。条件外の対照は3回。試験の合否には独立 Verifier の結果、コミットと欠損の合計、Unknown・重複・破損・未申告欠損、リソースの全回収を含める。Planner が起動を拒否する条件も比較するため、明示 opt-in で実行する。

```sh
cargo build --release --locked
PYTHONDONTWRITEBYTECODE=1 taskset -c 2-9 python3 tools/bench_deadline_lease.py \
  --repeats 12 --out target/deadline-lease-repeat
PYTHONDONTWRITEBYTECODE=1 taskset -c 2-9 python3 tools/bench_deadline_lease.py \
  --suite shared-pool --repeats 6 --out target/deadline-lease-shared-repeat
```

## 結果

各行の件数は1試行あたり。表に示す件数とコピー量は各ケースの全反復で一致した。

| ケース | Always Copy | Always Borrow | Deadline-aware |
| --- | --- | --- | --- |
| 無停止、12組 | 80/80、期限超過0、20 MiB | 80/80、期限超過0、0 MiB | **80/80、期限超過0、0 MiB** |
| 固定期限・32枠、12組 | 80/80、期限超過0、20 MiB | 80/80、**期限超過11**、0 MiB | **80/80、期限超過0、3 MiB** |
| 固定期限・10枠、12組 | 80/80、期限超過0、20 MiB | **73/80、期限超過10**、0 MiB | **80/80、期限超過0、3 MiB** |
| 共有プール・10枠、6組 | 80/80、20 MiB | **73/80**、0 MiB | **80/80、2.25 MiB** |

固定期限の停止ありでは Always Copy より **85.0%**、共有プールでは **88.75%**、CopyOut が少ない。固定期限の退避は12件、共有プールの退避は9件で、実行時カウンターが示す退避理由もケースの返却制約と一致した。Always Borrow の7欠損はすべて `dma_pool_exhausted`。共有プールには個別の返却期限がないため、表では保存件数とコピー量を比較した。各ケースで Unknown、未申告欠損、重複、破損は0。全リソースを回収した。

Always Copy と比較した試行ごとのプロセス CPU time の減少率を取り、その中央値は無停止14.7%、固定期限・32枠10.3%、固定期限・10枠12.0%、共有プール13.5%。42組すべてで CPU time は減少した。保存に成功した両方式の同期込み帯域は約26 MB/sで、入力レートに制限された。固定期限・停止ありの受理から同期の p99 の中央値も両方式とも約373 ms。コピー量と CPU time の改善を観測した一方、Writer 停止中の滞留を短縮する効果はこの試験では確認していない。

### 条件が崩れた場合

1回のコピーに72 msを追加すると、60 msの返却期限内にそのコピーを完了する条件が崩れる。Deadline-aware は3試行のすべてで30/80件のみ保存、18件の期限超過、50件の `dma_pool_exhausted` を報告した。独立 Verifier が全欠損を照合した。Planner は設定されたコピー下限を前提に Accepted と判定し、実行時 Monitor は契約違反を報告した。この差は、Planner の受理判定と実行中の前提監視の両方が必要なことを示す。

比較結果が支持するのは、上記の返却・容量・コピー条件を満たす Mock ワークロードでの安全性と効率。無停止では Always Borrow も同じ80件を期限内に保存した。実 DMA、別の入力包絡線、コピー帯域不足、長時間のサービス停止は個別に評価が必要。

## 証跡

対象レコーダーは `e45ba6804fd8b47ce31c4d3b5f5002d938b17d07` の release ビルド。主結果の129試行は [試行別 CSV](deadline-aware-bufferlease-results.csv) に収録した。設定、コマンド、Planner、MCAP、journal、Verifier と集計 JSON はローカルの `target/deadline-lease-evidence/` と `target/deadline-lease-shared/` に保存した。既存の Rust 実装は変更せず、fmt、clippy、43テストを再実行した。

最終版スクリプトで固定期限スイートを6組ずつ再実行し、54比較試行と3条件外試行でも同じ合否を確認した。補足の出力は `target/deadline-lease-repeat-check/`。
