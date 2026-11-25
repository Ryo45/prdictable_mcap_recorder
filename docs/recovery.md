# 復旧

録画プロセスが終了したファイルを指定する。run directory の場合は正常公開名、次に partial 名を読み取る。

```sh
target/release/recorder recover --run-dir runs/demo --dry-run
target/release/recorder recover --input runs/demo/run.partial --apply --output runs/rebuilt.mcap
```

MCAP のレコード境界、型登録、内容、連番、CRC と commit journal の先頭から連続する範囲を検査する。 `committed_with_evidence` は checkpoint のある可読メッセージ、 `recovered_but_unconfirmed` はその他の可読メッセージ。切断と破損はエラー欄、同一内容と異なる内容の重複は別カウンターに出す。書込み前に消えたイベントの総数は、この走査では確定できない。既知の loss の全量集計には、正常なレコード・summary・loss journal の独立照合を用いる。

復旧コピーは MCAP Rust Reader で全メッセージと Summary を再検証し、新しい名前で公開する。output に `.recovery.json` を付けた報告には原本パスと証拠件数が残る。処理が途中で失敗した場合は `.partial` が残り得る。既存ファイルは上書きしない。現在の上限は各入力128 MiB、262,144 メッセージ。通常の recorder unchunked 形式を扱う。古い V2 journal には prefix CRC がなく、復旧時は未確定になる。
