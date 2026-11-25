# Phase 0 録画・整合性ベースライン

GitHub master とローカルの基点は `3ce438cb3c61f563a531e75f63bfdef4838b80b8`。この SHA を `git archive` で別ディレクトリへ展開してビルドした。既存のローカル試作は作業ツリーに残した。

| 検証 | 結果 |
|---|---|
| cargo fmt --check、cargo clippy --locked --all-targets -- -D warnings、cargo test --locked | PASS |
| V1 既定設定 record + verify | 60/60 |
| V2 0.35 秒 record + verify | PASS |
| V3 0.35 秒 sync/null、sync の verify と recover --dry-run | PASS |
| M1〜M10 0.35 秒、seed 1 | 全ケースと台帳・資源照合 PASS |

環境と未コミット差分一覧は `target/control-baseline/{capacity-evidence/environment.json,status.txt}`。各録画の設定、ログ、Verifier、ジャーナルは `target/control-baseline/{v1,v2,v3,matrix,capacity-evidence}`。再実行時は新しい出力ディレクトリを使う。

```sh
git archive 3ce438cb3c61f563a531e75f63bfdef4838b80b8 | tar -x -C "$archive"
(cd "$archive" && cargo fmt --check && cargo clippy --locked --all-targets -- -D warnings && cargo test --locked && cargo build --release --locked)
(cd "$archive" && python3 tools/bench_multi_source.py --out "$evidence/matrix")
(cd "$archive" && python3 tools/bench_v3.py --recorder target/release/recorder --seconds 0.35 --repeats 1 --out "$evidence/v3")
```

過去の30秒試験は 1.072486 GB/s、7,680件照合済み。H1 の選別試作はコピー中央値108→114 MiB。H4 の Spill ガードは期限超過を減らしたが、全4組で既知欠損を増やした。今回はこの3試験を再実行していない。[Writer負荷の記録](writer-load-experiments.md) · [コピー選別試験](selective-copy-experiment.md)
