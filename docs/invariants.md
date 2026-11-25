# 所有権・完全性の不変条件

- 物理スロットは pool、slot、generation で識別する。Lease はムーブ専用で、利用中にコピー退避や書込みが競合しないよう Queue が claim を管理する。
- Mock の準備完了以前、待機中の Lease は Adapter に留まる。取得終了時は pending fence を drain する。返却後も遅延中のスロットは Returning であり、空き数に含まれない。
- Positional I/O は request に Frame を保持する。元の I/O の completion を回収するまで借用を継続する。io_uring の submission queue に渡す stable slot が iovec と参照先を所有する。リングが不明状態で teardown する経路は storage 実装の契約に従う。
- キュー、プール、in-flight、commit group、journal の通信キューは設定上限を持つ。journal queue 溢れは実行失敗として扱う。Verifier の連番区間数は欠損・不正順序の数に従って増え得る。
- MCAP 内の source ID・epoch・64-bit sequence・時刻・payload を照合する。MCAP ヘッダーの sequence は下位32ビット。正常終了時は受理数・書込み数・commit membership・欠損を別々に照合する。
- sync 失敗は Unknown。Recovery の可読レコードは証拠のある prefix と未確定部分に分かれる。二重 ID が見つかった場合は件数を報告し、修復コピーの公開前に停止する。
