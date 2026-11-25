# Writer の分離計測と Rust トレースによるモデル検証

**次は Writer のデータ同期経路を優先する。** 制御の評価に使うモデルは、Rust の借用 admission とコピー再試行を再現するよう修正する。新方式の Rust 移植は、修正済みモデルと既存 Global の実トレースで採用条件を確認してから判断する。

対象は `060f5b8` のアーカイブ。診断専用パッチと本番ツリーを分離した。Global の判定ロジック、キューの所有権遷移、MCAP と台帳のフォーマットは変えていない。診断パッチそのものは計時・記録を挟むため、トレース採取時には競合のタイミングが変わり得る。既存の作業ツリーの試作は保持した。

## 1. Writer の処理時間

Mock V4L2 2系統、各 1 MiB × 400 fps × 0.27秒、16 MiB / 16メッセージ / 80 ms の同期条件。同期 Writer で計時の有無を2回ずつ、無→有→有→無の順に試した。全回216/216を独立 Verifier で確認。コピー0、最大キュー9、最終データ同期まで0.827〜0.828 GB/s。プロセス壁時計は計時なし396.1/400.1 ms、あり397.4/399.4 ms。両組に同程度の揺れがあり、計時のコストはこの試行数から確定できない。下表はトレースを書き出さない計時試行2回の合計時間の範囲。

| 区間 | 合計時間 / 試行 | 境界 |
|---|---:|---|
| MCAP メッセージ生成 | 0.010〜0.011 ms | ヘッダーとファイル位置。起動メタデータとフッターを除く |
| メッセージ CRC | 22.82〜23.60 ms | ヘッダー・ペイロードの CRC 更新 |
| I/O 投入 | 35.87〜39.43 ms | 同期 backend の positional write 呼出し。カーネル内の書込み・ブロック時間を含む |
| 投入復帰から完了回収 | 0.038 ms | 同期 Writer の投入復帰から完了通知を回収するまで。poll 呼出しは別計器で約0.016 ms、区間は一部重複 |
| MCAP の fdatasync | 88.95〜90.55 ms | 14回、最大11.34〜12.07 ms |
| Commit journal の flush / fdatasync | 0.034〜0.039 / 11.39〜11.71 ms | Monitor スレッドで15回。グループ同期と終了時の同期を含む |
| Queue 滞留 | 248.68〜265.32 ms | フレームごとの enqueue→Writer claim の合計。並行する滞留を含み、Writer 壁時計へ加算できない |
| Writer のアイドル Queue 待ち | 201.64〜202.70 ms | in-flight が0の時の condvar 呼出し区間。入力待ちを含む |
| Writer claim の mutex 獲得 | 0.014〜0.015 ms | Queue の claim 時のみ。全スレッド・全ロックの待ち時間ではない |

測ったアクティブな Writer 区間ではデータ同期が最大で、約90 msは投入・CRCの合計約59〜62 msを上回った。アイドル待ちが大きく、この入力では Writer の飽和点を測っていない。fsync と競合する実ストレージ・カーネル内部の待ち、非同期 I/O の完了待ち、MCAP 起動・終了組み立て、他スレッドのロック待ちは未分離。試験実績から、先に同期バリアの内訳を調べ、次に positional I/O と CRC を検討する。

## 2. 容量制限付きファイル書込み

同じ ext4 の `/dev/nvme1n1p2`、KIOXIA EXCERIA PLUS G3、ELFA01.2。開始前の空き150.58 GB。新規通常ファイルだけに書込み、上限 **8 GiB**、時間の停止判定 **20秒**、対象 NVMe の温度停止 **65°C**。温度と時間は各データ同期後に確認するため、1グループの I/O が長い場合は判定も遅れる。4 MiB単位で buffered write、64 MiBごとに fdatasync。長さと先頭・中央・末尾のサンプルを照合した。

- **8 GiB / 5.298秒、1.621 GB/s**。約1秒の完了境界窓は **1.32・1.59・1.73・1.71・1.72 GB/s**。
- データ同期128回、中央値 **21.21 ms**、p99 **28.67 ms**、最大 **29.48 ms**。
- 温度 **24.85 → 26.85°C**。デバイスの書込みカウンタ差 **8.597 GB**、busy 時間差 **2.959秒**。デバイス全体の統計であり、他プロセス・メタデータの寄与を含む。SSD内部のNAND書込み量は未測定。

32 MiB より長い区間で同期込みの性能を確認した。8 GiB上限で終了したため、SSD内部キャッシュ枯渇、熱平衡、継続飽和点は未確認。64 MiBごとに同期する通常ファイル試験と、16 MiBごとに同期してMCAP・ジャーナルを生成する録画の帯域を直接比較して性能差を帰属させることはできない。

## 3. Rust → Python リプレイ

フレーム ID と pool/buffer/generation で結合し、Rust の Queue 投入直前イベントを Python へ入力した。実トレースと同じ終了条件・フレームサイズ・Pool/Spill 容量・注入停止を利用。Phase 0 のタイミングを固定した予測、同じ試行のサービス時間を与える条件付き診断、それに Rust の copy/return マージンを加えた診断を実行した。後二者は後知恵を使う誤差の切り分けで、オンライン予測のスコアには使わない。Spill 占有はトークンの取得→再利用可能時刻の区間で算出。時刻誤差の符号は予測−観測。

| Rust 側のケース | Phase 0 プロファイルのまま | 同一試行サービスと Rust マージンを反映 |
|---|---|---|
| 同位相2系統、28/28保存、期限違反0 | コピー対象8/8一致。開始誤差中央値 **+8.66 ms**。誤って期限違反8件を予測。Writer claim の順序逆転0/378 | コピー開始絶対誤差中央値 **0.069 ms**、模擬期限違反0。Writer claim の絶対誤差中央値 **0.044 ms**。返却可能時刻は中央値 **0.049 ms**、最大 **1.424 ms** の絶対誤差 |
| 混在4系統、76/90保存、欠損14、期限違反7 | CopyOut 実測6 MiBに対し予測4 MiB。コピー開始絶対誤差中央値 **0.831 ms**。Writer claim の順序逆転 **14/2,850** ペア | コピー開始誤差中央値 **0.177 ms**。実装だけがコピーした各1 MiBの2件は **両方 forced**。Writer claim 誤差中央値 **0.021 ms**、順序逆転14ペア。返却可能時刻の絶対誤差中央値 **0.026 ms**、最大 **1.576 ms**（76件で照合） |

同位相の4 KiB Spill はピーク8枠で一致。占有積分は実測354.44 slot-ms、補正前283.58、補正後354.07。混在のピークは 1 MiB が2枠、256 KiB が3枠で一致。補正後の積分は実測に対しそれぞれ約 **+2.9%・+3.0%**。混在では欠損14・期限違反7の総数は一致したが、Rust の Spill 失敗27回と Python の261回は異なるイベントを数えている。総数が一致しても同じ機序を再現したとは判定しない。

| モデルと Rust の差 | 予測への影響 |
|---|---|
| サイズ比例のコピー時間。Rust はコピーと返却に別々のマージンを持つ | 同位相のコピー開始を最大9 ms以上遅く予測。マージン差を除くと大半が解消 |
| Rust Writer は borrow の残余時間が足りないと claim を見送り forced copy を付ける。モデルは同一ソース FIFO/RR を自動実行 | 混在で2件・2 MiBの CopyOut を取りこぼし、claim 順にも差 |
| Rust は最初に適合する Spill クラスと retry 時刻を使う。モデルは空きのある適合クラスを選び、wait ごとに不足を計数 | クラスの代替や再試行で挙動が分岐。失敗回数を同じメトリクスとして扱えない |
| モデルは enqueue を取得時刻として扱い、返却可能時刻で即解放。Rust は fence・生成・Queue claim 競合を経て、プール操作時に reap | Rust の reap 対比で返却誤差の中央値が約0.62〜0.67 ms。Shutdown の仮想 settle を含む。実DMAドライバの ACK ではない |
| モデルは同期 Writer、メッセージ数でバリア。同一試行サービスの再利用も固定近似 | バイト/時刻バリア、実際の停止重畳、journal の独立進行、DRAM干渉、io_uring、スケジューラ競合は再現範囲外 |
| Python への入力は Queue 投入直前のフレームだけ | この2ケースでは予定フレームを全件観測。取得できず欠損したフレームを含むケースのモデル検証は別途必要 |

## 4. 採用判定

Safe-Greedy、Safe-DPP、Bounded-MPC に実機での安全保証は付与しない。採用には、同じ入力列・メモリ予算・Writer/同期条件で既存 Global とペアにして、独立 Verifier 合格と台帳の完全照合を前提にする。各評価ケースで既知欠損、欠損の未申告、Unknown、期限違反を個別に増やさず、同じ保存件数でコピー削減が複数試行・複数 seed にわたって再現することを必須とする。正常条件と負荷超過条件を両方含める。今回の実測は Global に限り、新方式はいずれも採用保留。

## 再現

[Rust診断パッチ](control-evidence/writer-followup/diagnostic.patch) · [Writer計時](control-evidence/writer-followup/writer.json) · [SSD時系列](control-evidence/writer-followup/ssd.json) · [リプレイ誤差](control-evidence/writer-followup/replay.json) · [ハッシュとローカル証跡の一覧](control-evidence/writer-followup/manifest.json)

```sh
archive="$(mktemp -d "$PWD/target/writer-diagnostics.XXXXXX")"
git archive 060f5b8 | tar -x -C "$archive"
patch -p1 -d "$archive" < docs/control-evidence/writer-followup/diagnostic.patch
(cd "$archive" && cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings && cargo test --locked && cargo build --release --locked)

python3 tools/bench_writer_trace.py --recorder "$archive/target/release/recorder" --out "$archive/runs"
PYTHONDONTWRITEBYTECODE=1 python3 -m tools.control_lab.replay_rust \
  "$archive/runs/aligned-1-profile-trace" "$archive/runs/mixed-2-profile-trace" --out "$archive/replay.json"

# 容量と時間を明示した別の opt-in コマンド
python3 tools/bench_bounded_ssd.py --out "$archive/ssd" --mib 8192 --seconds 20
```

診断版の fmt、clippy、Rust 53テスト、Python 16テストが通過。実録画6回を独立 Verifier で照合。詳細ファイルは `target/control-followup`。
