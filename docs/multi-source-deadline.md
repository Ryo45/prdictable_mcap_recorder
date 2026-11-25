# 複数ソースの返却需要を扱う Deadline-aware BufferLease

基点は GitHub の `master` とローカルで一致した `ca246e7d596af62537d2e25ee50690ce467b05ee`。基点で fmt、clippy、43テストを確認し、release バイナリを `target/multi-source-baseline/recorder` に保存した。適用対象の AGENTS.md は見つからなかった。既存のプール、Queue、Scheduler、Planner、Writer、監視、ベンチ、設計と測定を監査した。基点の Scheduler は pool の必要返却数を観測していたが、コピー開始には候補1件のコストを使っていた。実行中の CopyOut は1件、Writer 内の Lease はコピー候補外だった。

## 再現する失敗

期限7 ms、CopyOut 各2 ms、返却ACKまで0.5 msの3要求が同時に Pending になったとする。個別方式は4.5 msから開始し、最後のACKは11 ms。単一 worker でも、0.5 msから連続でコピーすれば最後のACKは7 ms。4要求を時刻0から処理すると最後のACKは8.5 msで、同じ期限には物理的に間に合わない。先に数値例を固定し、実装後は Planner に渡すコピー計画と同期ファイル録画の両方で比較した。

## 集約手順

`[record] copy_scheduling = "global"` で有効。未指定は `legacy`。どちらのモードも MCAP、commit/loss journal、recovery のデータ形式、Frame と Lease の型、Writer の完了条件を維持する。

1. 各デバイスの有効空き数、次の到着時刻、同時バーストと捕捉余裕から、pool obligation の期限と必要返却数を求める。Mock Pool で再利用時刻が確定した Returning のうち、その期限までに戻るものを控除する。
2. Queue ロック内の Pending Borrowed だけを対象にする。固定期限が pool 期限以前の候補を一度だけ両方に計上し、不足分の個数だけ同じプールの古い Pending を追加する。この対応付けは現在の計画にのみ有効。Writer の未完了I/Oと将来到着は候補に含めない。
3. CopyOut のコスト `C` は payload とコピー下限の切り上げにスケジューリング余裕を加えた値。返却のコスト `R` は再利用遅延と返却余裕。個別期限と割り当てた pool 期限のうち早い期限 `D` から `R` を引き、コピー完了限界でEDF順に並べる。同じ限界では短いコピー、古い到着、slot IDの順。
4. 先頭からの作業時間の和 `sum(C[0..k])` に対し、全 prefix の `(D[k] - R[k]) - sum(C[0..k])` の最小が先頭の最遅開始。開始と実行可能性の計算は別に行い、既知の先行 blocking と retry の release 時刻を前向き検査に含める。実運用のコピーは同じ worker が同期実行し、実行中は次の計画を作らない。完了時の実時刻で作り直す。
5. 選ばれた1件だけを Queue から move する。Spill は最小適合クラスから事前確保済みの1枠を取得できた場合に使用する。通常の候補は枠不足時に警告して再試行。既存の強制 CopyOut は枠不足時に `spill_pool_exhausted` を報告する。事前登録予備は既存の有限 `activate` 経由で使い、global では固定期限のある候補に予備を適用しない。

Writer は既存の短い固定 slack の borrow guard と最大 borrowed in-flight を維持する。global では guard による deferral を数える。CopyOut が job を保持中の slot は Writer から見えず、Owned になって元の FIFO 位置に戻る。Writer 内の Lease は completion まで保持され、sync や取消要求を借用終了として扱わない。Writer の stream 間ラウンドロビンは EDF 順から独立。

## 前提と限界

- 1つの Copy Worker、独立したデバイスプール、共通の事前確保 Spill。各プールの ID は現在ソースに一対一で対応する。複数 Source が同じ物理プールから取得するための到着包絡線集約は未実装。
- 候補数と pool 需要は別。プール空きは次の計画でも再計算する。独立した時点で取得した Pool と Queue のスナップショットにはずれがあり、保守的な早期コピーや一時的な予測警告が出得る。
- EDF は調停ヒューリスティック。例えば時刻8まで再試行できない2単位の仕事（期限11）と、今から実行できる7単位の仕事（期限12）は、前者を待つEDFでは後者が遅れる。後者から始めれば両方が間に合う。未知の将来到着、先に始めた長いコピーによる blocking、retry とサイズクラス別の枯渇を含めた最適化や無条件の飢餓保証は対象外。
- 計画は Queue の有限スロット数までの事前確保 scratch を使用する。走査・ソートを Queue ロック内で行うため、大きな常時バックログは CPU と Writer の claim 遅延を増やし得る。
- Static Planner は全 eligible frame がCopyOutを要する上限を保持する。global はさらに1秒あたりのコピー作業、バースト量、最大1件の先行 blocking を予算に入れる。不足や既知の過大なコピー遅延を拒否し、不明な入力・サービス境界は Unknown。計画が Accepted でも Linux の WCET が得られるわけではない。

## 観測と実行

global のサマリに比較用の作業量ピーク、観測した最早の未処理期限、成立しないと判断した区間の回数、copy worker busy/queue wait、サイズクラス拒否、borrow deferral、ソース別の Pending Borrowed ピークとコピー理由を追加した。既存 CSV からプール空き、Returning、必要返却数、slack、spill 使用量を取得する。返却期限超過、台帳に記録した欠損、永続化の Unknown は別々に照合する。ストレージの submit→completion と受理→data fdatasync は別の分位。commit journal も同期するが、journal の同期完了を含む per-frame 遅延は計測していない。

```sh
cargo build --release --locked
PYTHONDONTWRITEBYTECODE=1 taskset -c 2-9 python3 tools/bench_multi_source.py \
  --repeats 3 --out target/multi-source-repeat
```

低容量ケースは15試行/反復、各0.35秒、全比較の出力上限の見積りは約40 MB/反復。実SSD試験は、ケースと秒数を明示する場合に実行する。SSD設定は約1.074 GB/s、1秒の4方式比較なら約4.4 GBとヘッドルーム。既存出力への上書きは禁止。SSDの観測可能なNVMeデバイス、出力量の上限と空き容量を確認し、温度とページキャッシュの観測値を保存する。

```sh
PYTHONDONTWRITEBYTECODE=1 taskset -c 2-9 python3 tools/bench_multi_source.py \
  --ssd H2 --seconds 0.8 --out target/multi-source-h2-repeat
```

[評価結果](multi-source-results.md)
