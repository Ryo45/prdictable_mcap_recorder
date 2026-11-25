# ストレージ同期とコミット証拠

1. Writer は要求を submit し、I/O 完了を回収して Written に進める。短い I/O は既存バックエンドで残りの範囲を完了する。
2. グループの I/O 完了後、MCAP ファイルを `sync_data` する。成功後に Committed を数える。
3. V3 Mock のファイル出力では、同期に対応する prefix CRC、ファイル境界、ID 範囲を JSONL に出す。Monitor はグループと全メンバーを受け取ったあと commit journal を flush、`sync_data` する。Monitor のエラーは実行の失敗になる。
4. run directory は新規作成し、親を同期する。記録開始前にファイルのディレクトリエントリを同期し、完了時に `run.partial` と同一 inode を `run.mcap` として公開する。ディレクトリを同期し、partial 名を解除して再同期する。

性能の accepted→committed は手順 2 まで。手順 3 の完了を含む応答遅延は未計測。電源断時の NVMe コントローラーと媒体の保証は別途評価が必要。journal のない末尾、破損した checkpoint、書込み後の同期失敗は、可読でも耐久状態が未確定。
