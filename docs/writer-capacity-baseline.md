# Phase 0 Writer 性能の切り分け

1 MiB × 32フレーム、8 MiB単位のデータ同期。B1/B2 は逐次プッシュ型、B3/B4 は 500/1,600 fps のペース入力。試行順を反転した。録画は同じキュー・メモリ・同期設定。B4 は Mock V4L2、Global と 10 ms のソフトウェア停止を使い CopyOut を発生させた。

| 層 | 試行1 GB/s | 試行2 GB/s | 試行2 I/O p99 | 試行2 sync p99 |
|---|---:|---:|---:|---:|
| B1 通常ファイル、buffered pwritev/fdatasync | 1.188 | 1.108 | 2.233 ms | 3.942 ms |
| B2 I/O backend microbenchmark、ヘッダー付き pwritev/fdatasync | 1.330 | 1.436 | 3.383 ms | 4.034 ms |
| B3 Owned Capture + 同期 Writer | 0.499 | 1.245 | 0.540 ms | 5.098 ms |
| B4 Borrowed + Global + 停止 | 0.501 | 0.935 | 10.241 ms | 5.074 ms |

分母は最終データ同期まで。B1/B2 は長さ・SHA-256照合、B3/B4 は独立 Verifier を実行。高ペースの生成は両試行で約1.67 GB/s。B3 の生成 p99 は139 µs、キュー最大10、Null のキュー最大1。B4 は30 MiBをコピーし、キュー最大23。B4 の 10.241 ms には注入した停止が入り、B3 との差からコピー単体のコストは推定できない。

短時間の高ペース試行で最初に切り分けるべき箇所は同期 Writer のサービス経路。B3 の I/O p99 より同期 p99 が約9.4倍長く、Null との差は生成能力だけでは説明できない。B2 には本番の MCAP、ジャーナル、CRC処理を含まない。B1/B2 の順位は試行ごとにファイル・キャッシュ条件が異なるため帰属に使わない。fio は未導入。io_uring は EPERM。物理転送量・温度・ロック時間は未測定。物理SSDの持続上限は inconclusive。

```sh
python3 tools/bench_writer_capacity.py --recorder "$archive/target/release/recorder" \
  --base-sha 3ce438cb3c61f563a531e75f63bfdef4838b80b8 \
  --out target/new-writer-capacity --allow-unverified
```

短時間専用。予定出力の予算は512 MiB、空き容量は1 GiB以上、新規ディレクトリ必須。Planner Unknown を明示して実行し、検証を行った。release SHA-256 は `4781537bbb324f5a5ead185450963d2c27c56d8fccc0220ba4b4a227ba9df4ea`。機械可読結果は [writer-capacity.json](control-evidence/writer-capacity.json)。詳細 CSV、環境、設定、ログ、検証結果は `target/control-baseline/capacity-evidence`。
