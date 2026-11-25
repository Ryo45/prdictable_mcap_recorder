# V1 の振り返り

V2 着手前に V1 の仕様、README、Cargo 構成、録画・Verifier・テストを確認し、同じ release ビルドで P0 基準試験を再実行した。

```sh
cargo build --release --locked
python3 tools/bench.py --suite policy --out target/v2-p0-v1-baseline
```

E0〜E6 の全判定が通過した。E1 と E2 は全方式 144/144。170 ms 停止の E3 では Borrow が DMA 枯渇で12件を失い、Copy と Adaptive は全件保存した。CopyOut は Copy が9 MiB、Adaptive が0.9375 MiB。450 ms 停止の E4 の欠損は順に22、46、17件。定期停止の E5 は Adaptive が全件保存し、各停止後にキューが空まで回復した。E6 の欠損は順に56、80、51件で、いずれもジャーナルと一致した。個別の結果は `target/v2-p0-v1-baseline/comparison.json` と試行ディレクトリに保存した。

既存の E7 は、KIOXIA EXCERIA PLUS G3 NVMe 上で 4 MiB × 240 fps を0.2秒間入力し、全方式で48/48を検証した。Writer 排出まで約1.006 GB/s、最終同期込みで390〜454 MB/s、同期区間は243〜316 msだった。192 MiB の direct I/O ベースラインは同期込み639 MB/s。SSD 内部キャッシュ枯渇後の定常帯域と 1 GB/s 持続は未確認で、短時間の見かけの Writer 帯域だけでは判定できない。`target/bench-v1-20261122/comparison.json`

固定閾値 Adaptive は DMA 枯渇を抑えたが、E3 で5件、E5 で15件の返却期限を超過した。取得済みの古い Lease は固定閾値の CopyOut 対象外だった。V2 ではプール残量、期限、Writer 停滞に応じて Pending の古い Lease を退避する。

V1 のコマンド、Writer、Verifier と比較スクリプトは回帰基準に残す。V2 では決定的なペイロード生成、ヒストグラム、OS 計測、公式 MCAP の記録・読出し API、停止処理、上書き防止を再利用する。

V2 統合後も同じ試験を `target/v2-p0-v1-regression-final/` に再実行しました。E1〜E6 は全18試行で欠損数とコピー量が一致し、E3 の Adaptive の同期込み帯域は両試行とも丸めて7.8 MB/sでした。E0 の最高負荷（4 MiB × 3,840 fps）では着手前に1,152/1,152、統合後に1,147/1,152でした。統合後の5件は生成期限超過で、Writer や DMA 枯渇は観測されていません。この高負荷点はホストのスケジューリングと生成時間の影響を受けます。
