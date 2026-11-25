# Writer の返却見込みを使った CopyOut 選別

試作は `[record] copy_scheduling = "selective"` で選ぶ。従来の global と legacy は比較基準として残す。

同期 Writer に上限がある場合だけ、キューのラウンドロビン順に各 stream の先頭を1巡する。既知の停止と次の commit 境界、I/O 上限、返却 ACK を含めて予測枠を使い、一つの枠を複数回数えない。実際に Writer が claim した分はキューロック内で差し引く。期限の近いプール需要、強制退避、予測切れの in-flight がある場合は該当する予測を使わない。予測に載せる一群について、最後の予測返却時刻に先行コピー1件を加えても CopyOut の作業量で回復できる場合だけ、その回のコピー計画から外す。各計画で再判定する。Spill 容量の予約や、将来の到着・I/O 上限逸脱を含む期限保証は提供しない。Writer に claim された Lease の所有権は既存の完了経路に従う。

## 受け入れ判定

同一設定の global / selective を4組、実行順を交替して比較。H1 と H4 は各0.8秒、実 NVMe で同期。独立 Verifier で全試行を照合し、未申告欠損・Unknown・重複・破損は0。fmt、clippy、Rust 53テスト通過。

| ケース | global | selective |
| --- | --- | --- |
| M1 保存・期限 | 全回28/28・期限超過0 | 全回28/28・期限超過0 |
| M1 コピー | 全回32 KiB | 全回32 KiB |
| H1 コピー MiB（seed順） | 96、76、120、124 | 104、128、88、124 |
| H1 保存・期限 | 全回205/205・期限超過0 | 全回205/205・期限超過0 |
| H4 コピー MiB | 287、278、286、280 | 292、320、291、288 |
| H4 期限超過 | 0、3、2、0 | 2、22、2、0 |
| H4 既知欠損 | 22、22、23、25 | 22、26、22、22 |

H1 は中央値108→114 MiB。最終試作は4組のうち1組で削減、1組で同量、2組で増加した。H4 は CopyOut の時間に予備があっても、サイズクラス別の Spill 空きを確保できるとは限らず、期限超過が悪化する試行が残った。H4 の各 Lease の時系列は未計測で、今回の22件すべてを Spill に帰属させる根拠はない。H1 の削減基準を満たさず、H4 で悪化を観測したため公開ブランチへ反映しない。

```sh
cargo build --release --locked
PYTHONDONTWRITEBYTECODE=1 taskset -c 2-9 python3 tools/bench_multi_source.py \
  --compare-selective --repeats 4 --out target/selective-m1-repeat
PYTHONDONTWRITEBYTECODE=1 taskset -c 2-9 python3 tools/bench_multi_source.py \
  --compare-selective --repeats 4 --ssd H1 --seconds 0.8 --out target/selective-h1-repeat
PYTHONDONTWRITEBYTECODE=1 taskset -c 2-9 python3 tools/bench_multi_source.py \
  --compare-selective --repeats 4 --ssd H4 --seconds 0.8 --out target/selective-h4-repeat
```

生データ・比較CSV/JSONは `target/selective-final/`。

## 追加診断と対策

`RECORDER_COPY_TRACE=1` のときだけ、録画ファイルと同じ場所に `<stem>-copy-trace.jsonl` を新規作成する。epoch・stream・sequence でコピー判断、Spill 拒否、CopyOut 完了、Writer への投入と完了を結べる。固定期限、予測累積作業時間、Writer 停止見込み、ACK 推定時刻も記録する。32,768イベントに制限し、末尾の `trace_end` に件数・省略・I/O失敗を記録する。末尾欠落や省略のあるトレースは全量照合の根拠に使わない。ACK はソフトウェアの推定時刻。試作のみ、Writer 枠への委譲と取り消しの遷移も記録した。トレースの同期・JSON整形はスケジュールを変え得るため、対策の採否はトレースなしで判定した。

### H1 でコピーが増えた経路

100 ms停止区間のコピーは、トレースありの全3組で両方式とも11件。差は停止解除後に発生した。例えば seed 1 では解除後のコピーが global 14件、試作30件。試作の追加16件はいずれも global では Writer で処理されたフレーム。最初の組の `camera_0 / sequence 13` は、global では期限約496.30 msに対し460.10 msに Writer 返却見込み。試作では464.65 msに、残りの Writer 停止見込み約39 msとコピー候補15件・作業85.4 msを観測してCopyOutした。4 MiBの1件あたりの契約上のコピー作業量は約5.69 ms。同期の進行やキューの状態が異なる実行間の比較なので、トレースだけで同期の並びが変わった最初の原因まで確定できない。

試作で記録した委譲取り消しは、H1 の3試行で12・6・8回。取り消しを一度でも記録したフレームが、同じ試行で最終的にコピーされた事例は0。コピーが増えた主な経路として観測したのは、**委譲されなかったフレームも含む全コピー前提の累積作業が大きいまま、再開後の同期境界で前倒しされたこと**。委譲を取り消して同じフレームをコピーしたために増えたとする説明は、このトレースとは一致しない。

### H4 の期限超過と対策の再評価

H4 のトレースでは、推定ACKが期限を超えた CopyOut はすべて 1 MiB フレーム。例えば global の seed 1 の `camera_4 / sequence 59` は、同じ1 MiBクラスで46回退避先取得に失敗し、推定ACKが期限を約4.07 ms超えた。全フレームの遅延を同じ経路に帰属させるものではない。

試作に、Writer に任せる候補の**最小適合クラスに空きがあり、同じクラスの委譲候補と先行コピー1件を収容できる**場合だけ委譲するガードを追加した。空きの観測であり、物理予約ではない。前回の同予算の配分変更 `80×1 MiB + 60×4 MiB → 112×1 MiB + 52×4 MiB` は別対策で、前回の3組では期限超過5・23・15件が0・0・0件になった。どちらもキュー容量の不足を直接解消しない。

| トレースなし、各4組 | global | Spill ガード付き試作 |
| --- | --- | --- |
| M1 | 全回28/28、期限超過0、コピー32 KiB | 同じ |
| H1 コピー中央値 | 112 MiB | 84 MiB |
| H1 保存・期限 | 全回205/205、期限超過0 | 同じ |
| H4 コピー中央値 | 299.5 MiB | 282 MiB |
| H4 期限超過、seed順 | 0・0・8・6件 | 0・0・0・0件 |
| H4 既知欠損、seed順 | 19・22・23・25件 | 24・24・24・26件 |

H1 は3組で削減、1組で増加。H4 は期限超過がなくなったが、キュー満杯による既知欠損が全4組で増えた。全36試行を独立 Verifier で照合し、未申告欠損・Unknown・重複・破損は0。欠損増加を許容してコピー量だけで採用を決めることは避け、トレースと本レポートを公開し、選別と Spill ガードは試作に留めた。

公開されたトレースは例えば次で使える。試作比較の設定、コマンド、JSON/CSV、トレース原本はローカルの `target/selective-diagnostics/`。

```sh
RECORDER_COPY_TRACE=1 target/release/recorder record \
  --config configs/multi-source-ci.toml --duration-sec 0.35 \
  --allow-unverified --output target/copy-trace-demo.mcap
```
