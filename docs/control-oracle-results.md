# Phase 2 制御方式と Writer の優先順位

**同期 Writer のサービス遅延を先に調査し、Global を維持する。** Safe-Greedy は、将来の Writer サービスと到着を契約で拘束できる条件で再評価する第一候補。Safe-DPP と Bounded-MPC の本番移植は保留する。

## 方式と評価条件

- **Oracle** は実際の到着・停止・障害を知る。Copy、有限 Reserve、Wait を同じシミュレータ内で列挙し、辞書式の単調なコスト下界で枝刈りする。厳密性の範囲は、離散イベント、明示 wake、latest-start 境界で判断できる固定同期 Writer モデル。任意時刻・任意手順の Writer を含む連続時間の最適値ではない。6フレームの枝刈りなし全列挙と照合した。時間・ノードで打ち切った解には最適性を付けない。
- **Safety Shield** は共有。宣言済みの到着・停止と契約付きサービスを使って、候補の直後から全ジョブが回収されるまでに欠損・期限超過・同期不明が発生しない実行列を探す。計画の続きには概念 Global を使用。遮蔽された停止、非公開 jitter、実測分位のみのサービス、有限ホライズン内で終端に到達しないケースは Unknown。単独の返却ですでに期限を守れないことを下界で示せる場合は Infeasible。Unknown/Infeasible 時は Global の最善努力へ戻る。オンライン側に Writer を任意に選ぶ操作はない。
- **Safe-Greedy** は証明付き候補を予測コピー量の少ない順、同量なら Wait 優先で選ぶ。**Safe-DPP** は同じ候補をイベント境界ごとの正規化プール・Spill の占有二乗の差と、MiB 単位のコピー罰則で順位付けする。Spill の重みは0.25、イベント重みは1。Lyapunov の理論的安定性は主張しない。**Bounded-MPC** は同じ候補から深さ4・ビーム幅6で探索し、計算予算超過時に Safe-Greedy へ戻る。共有 Shield のノード上限は384。Python プロセスの実行時間は測定値であり、リアルタイム上界ではない。
- 学習 seed は11・23。固定ケースで V と MPC のホライズン／上限の候補を比較し、別の101・103・107を評価に固定した。選択は V=2 と MPC の25 ms／32ノード。同じシナリオでも公開された位相・入力計画のケースと、非公開 jitter のケースを区別した。固定ケースは3 seedで同じ入力になる場合がある。

## 結果

各方式114試行。コピー集計は **両方式とも欠損・期限超過・永続化 Unknown が0** のペアのみ。単位は MiB。Python の判断時間は各試行の p99 の中央値。既知欠損はモデルでの受理前欠損。

| 方式 | 正常試行 / 114 | 正常ペア コピー Global → 方式 | コピー削減ペア | 既知欠損 / 期限超過の総数 | 判断 p99 |
|---|---:|---:|---:|---:|---:|
| Global | 59 | 71 → 71 | 0 | 132 / 163 | — |
| Legacy | 44 | 20 → 6 | 8 | 135 / 334 | — |
| Always Borrow | 42 | 12 → 0 | 6 | 147 / 460 | — |
| Always Copy | 60 | 50 → 686 | 0 | 138 / 135 | — |
| Safe-Greedy | 63 | 71 → 69 | 6 | 132 / 159 | 約1.2 ms |
| Safe-DPP | 63 | 71 → 64 | 6 | 132 / 159 | 約1.2 ms |
| Bounded-MPC | 63 | 71 → 69 | 6 | 132 / 159 | 約1.8 ms |

同期失敗の Unknown は全方式で計12件。欠損と分けて記録した。3新方式はこの評価で Global より悪い欠損・期限・Unknown のケースを増やさなかった。Safe-DPP は正常ペアの合計を約10%削減した。各新方式に、両者が安全でも Global よりコピーが多いペアが5件あった。MPC は Safe-Greedy からコピー量を下げず、探索時間と fallback が増えた。

| 反例・条件 | Global | Safe-Greedy | Safe-DPP | Bounded-MPC | Oracle |
|---|---:|---:|---:|---:|---:|
| H1 を模した停止復帰、コピー MiB（全8件保存） | 5 | 3 | 2 | 3 | 1 |
| H4 を模した混在サイズ、QueueFull / 期限超過 | 3 / 4 | 3 / 4 | 3 / 4 | 3 / 4 | 打ち切り時 3 / 3 |
| 同一期限3件、コピー MiB（全3件保存） | 3 | 3 | 3 | 3 | 3 |
| 同一期限4件、期限超過 | 1 | 1 | 1 | 1 | 1 |
| 予備枠を使い切る条件、Pool枯渇 | 4 | 4 | 4 | 4 | 2 |
| Copy と Writer の競合モデル、コピー MiB（全3件保存） | 1 | 0 | 0 | 0 | 0 |

Oracle 45件の内訳は最適18、実行不能の証明18、打ち切り Unknown 9。同一期限4件は実行不能を証明した。H1 の Safe-Greedy には Oracle 比で2 MiBの改善余地。H4 では退避クラス不足と QueueFull が同時に観測され、期限だけを目的にしても解決できない。H4 の Oracle は実行可能性を証明できなかった。長い停止は100、200、500 msのすべてでキューが先に埋まり、24件中10件の QueueFull。短い入力列が停止中に到着し終わるため、停止長を伸ばした差はこの入力列では識別できない。これらはモデルの観測であり、過去の H1/H4 の実SSD値とは別に扱う。

## Writer の感度と採否

実機低容量の B3 の高ペースでは生成約1.67 GB/s、最終データ同期まで約1.25 GB/s。Null と Sync のキュー最大は1と10、Sync の I/O p99 は0.54 ms、同期 p99 は5.10 ms。Phase 0 の範囲では同期サービス経路が先に調べるべき箇所と判断する。fdatasync、グループ境界、MCAP書込み、ジャーナルの個別寄与と物理SSDの飽和点は未確定。

- 解析プロファイルで入力1.0 GB/s、Writer 0.8 GB/s の seed 101 では Global が8 MiBコピーして期限超過1件。Safe-Greedy は10 MiBで期限超過0件。Writer 1.25 GB/s では Global のままコピーも期限超過も0。
- 入力1.5 GB/s、Writer 1.25 GB/sでは Global が4 MiB、Safe-Greedy が6 MiB。Writer 2.0 GB/sでは双方0。入力2.1 GB/sで Writer を1.25から2.0 GB/sにした場合、Global は QueueFull 1件／コピー14 MiBから両方0。
- 実測分位を当てた1.5 GB/sでは、低い p50 プロファイルは Global のコピー・期限違反とも0、高い p99 プロファイルは10 MiB・1件。実測プロファイルは Shield にとって Unknown のため、新方式は Global へ戻る。これらは実ストレージでの能力予測や保証の値ではない。

サービス条件の差は、新方式の安全なコピー削減を上回る場合があり、H4 型の QueueFull にも直接影響する。Phase 3 の前に同期サービスの内訳を計測し、改善後の Writer で同じペア比較を行う。実センサの到着包絡線、DMA ACK、実SSDの持続書込み、非同期 Writer、クリティカル区間の実行上界は未検証。

## 再現と証拠

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s tools/control_lab/tests -v
PYTHONDONTWRITEBYTECODE=1 python3 -m tools.control_lab.compare --out target/control-trial
```

ペアごとの [CSV](control-evidence/control-comparison.csv) と [証拠マニフェスト](control-evidence/control-manifest.json) を保存した。詳細は `target/control-baseline/phase2-final/` の `comparison.csv`、`comparison.json`、`scenarios.json`、`tuning.json`、ケース別 trace。全843試行でモデルの台帳・終端資源を照合した。実ファイルの独立 Verifier とプロセス kill、故障注入、Recovery は凍結版の Phase 0 / Rust 回帰で確認した。fmt・clippy・Rust 53テストと Python 13テストが通過。[Phase 0 の計測](writer-capacity-baseline.md) · [状態モデル](control-model.md)
