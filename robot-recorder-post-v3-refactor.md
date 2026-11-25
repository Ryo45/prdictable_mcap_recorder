# 高帯域 MCAP Recorder — V3後のリファクタリング指示書（Codex向け）

目的: **V1→V2→V3の試行錯誤で蓄積したコードを、実測性能・所有権・完全性・復旧可能性を保ったまま再設計する。** V3はMock-firstとして実装し、実センサ・製品SDKは不要。 新機能を追加する指示ではない。安全性が確認できない場合は大規模リライトを禁止する。

## 0. 実行開始条件

- まず `AGENTS.md`、V1/V2/V3仕様（V3のMock-first変更を含む）、README、architecture docs、最新のテスト結果とperformance reportを確認。
- V3が未完了なら未完成事項をリスト化し、リファクタリングで「実装済み」に偽装しない。完成済み領域から着手する。
- 既存のコード、公開API、CLI、ファイル形式、出力サンプル、テスト、計測ログを保護する。
- 現状のgit状態を記録する。未コミットのユーザー変更は削除・上書きしない。大規模修正前に復元可能なコミット/ブランチを準備する（必要な許可/環境に従う）。
- 今回の第一目的は保守性と検証容易性。性能最適化を行う場合は明示的なベンチと測定結果を伴わせる。

## 1. 最初にリファクタリング計画を作る

`docs/refactor-audit.md` に以下を記載する。

1. 依存グラフ（Source, BufferLease, Pool, Scheduler, Planner, CopyOut, Writer, Storage, Commit, Integrity, Monitor, Recovery, CLI）。
2. 各モジュールの責務、循環依存、不自然な型変換、重複実装、API漏れ。
3. パフォーマンス上のホットパス（source acquire→lease→queue→copy/borrow→write completion）。
4. `unsafe` / FFI / dma mapping / `io_uring` で保護すべき生存期間と同期契約。
5. 全てのエラー経路（short write、EIO、ENOSPC、OOM、drop、timeout、cancel、shutdown、crash）。
6. 既存テストで保護できていない不変条件、観測不足、今後の変更の危険度。
7. 優先順位つき作業一覧。可能なら短いコミットに分割。

最初に `docs/refactor-baseline.md` を作り、変更前の機能・性能ベースラインを記録する。実測のない項目は未測定と明記。

## 2. 絶対に壊してはいけない契約

### 2.1 バッファとLease

- 各物理バッファは generation を含め一意。二重返却、返却漏れ、use-after-returnを起こさない。
- `BufferLease` は安易に`Clone`できない。必要なら明示的な参照所有権のプロトコルがある。
- DMA fence 完了前にはCPUアクセスしない。CopyOut完了前に元バッファ返却しない。
- Async writeがバッファを参照中に、返却/再利用/解放しない。cancel依頼だけで完了と扱わない。
- 共有プールの「必要な返却数」と個々のバッファdeadlineを混同しない。
- Owned ROS2/UDP payload と device-borrowed payload の寿命が混線しない。

### 2.2 書込みと整合性

- logical record ID, stream sequence, original timestamp, payload bytes, source metadataが保たれる。
- Written（カーネルから成功）とCommitted（同期成功）を区別し、Unknownを誤ってLost/Committedに変換しない。
- MCAP正規形式/索引/ローテーション境界/復旧後の検証可能性を保つ。
- 障害注入時に未申告欠損・二重ID・破損を見逃さない。
- Sidecar/manifestの整合性・sync順序を暗黙に変えない。

### 2.3 リソース・性能

- bounded memory: queue / spill / inflight / buffer pool / journalに上限を維持。
- 性能やlatencyを、アルゴリズムが良さそうという理由だけで「改善」と書かない。
- 監視はホットパスを不必要にblockさせず、警告・欠損検出が失われない。
- 新しいグローバルロックや全payloadコピーをひそかに追加しない。

## 3. 固定すべき回帰テスト（コード変更の前に）

1. Golden MCAP: 合成入力の既知byte列、channel/schema、sequence、timestamp、rotation跨ぎの独立Reader検証。
2. Lease state machine: move/drop、CopyOut、double-return、generation更新、shutdown、in-flight完了順入替。
3. Planner: accepted/rejected/unknown、容量不足、arrival envelope逸脱、未知のデバイス能力。
4. Scheduler: AlwaysCopy/AlwaysBorrow/Threshold/Deadline-Aware/Reserve、期限切れ・複数source競合。
5. Fault matrix: partial write、EIO、ENOSPC、fsync失敗、copy失敗、DMA返却遅延、kill/rotation/journal更新途中。
6. Recovery: 正常終了、不完全MCAP、index不在、commit境界、Unknown、duplicate、read-only dry-run。
7. Adapter contract: synthetic, Mock ROS2 serialized, Mock UDP, Mock CAN, Mock V4L2, Mock proFRAME, Mock NvSIPL の全Mockを標準CIで検査。**同一のSourceAdapter契約を保ち、CoreにMock専用分岐がない**ことを確認する。実機試験はスコープ外。
8. 性能ベースライン: 同じ入力rate、CPU pinning、queue/pool設定、FS、SSD、sync方式、試験時間、温度で throughput/p99/p99.9/max、copy bytes、RSS、CPUを記録。

必ず **まずテストを追加してから** 既存実装を入れ替える。テスト自体の期待値を、新コードに合わせて安易に緩めない。

## 4. 望ましいモジュール境界（仮説。過剰な抽象化は禁止）

```text
recorder-core/
  source/          # Source contract, capabilities, metadata
  buffer/          # Owned/borrowed payload, lease, pool, generation
  scheduler/       # Admission, return obligation, CopyOut policies
  planner/         # Static feasibility and budgets
  pipeline/        # bounded queues, cancellation and shutdown orchestration
  mcap/            # MCAP layout, schema/channel management, chunk/index
  storage/         # sync/async IO, completion, durable barrier
  integrity/       # loss, unknown, ledger, verifier, recovery
  metrics/         # structured events, counters, histograms
adapters/
  synthetic/
  ros2/
  udp/
  socketcan/
  v4l2/
  proframe/        # mock必須、実SDK adapterは任意/feature-gated
  drive/           # mock必須、実SDK adapterは任意/feature-gated
recorder-cli/
benchmarks/
tests/
docs/
```

物理ディレクトリを無理にこの形にする必要はない。まず論理的な依存方向が `Adapter -> Core contract`, `Core -> Storage contract`, `CLI -> application composition` になるように整理する。WriterがデバイスSDKをimportしない。SourceがMCAP recordを組み立てない。

**抽象化の採用基準**: 異なる実装が二つ以上ある、またはテスト容易性/安全性を明確に高める場合だけtraitにする。インターフェースのためだけのtrait、巨大なgenericパラメータ、早すぎる動的ディスパッチは避ける。

## 5. 作業順序（フェーズごとに小さくコミット）

### R0: 現状調査・ベースライン

- `cargo fmt --check`, `cargo clippy --all-targets`（実行可能features）, `cargo test`、既存integration testsを実行。
- CLIスナップショット、出力MCAP golden files、事故時のrecovery report、ベンチ設定/実測を保存。
- 未完の機能やflaky testsを記録。性能計測ができない環境では「未測定」とする。

### R1: データモデルと所有権の整理

- `RecordId`, `SourceId`, `BufferId`, `Generation`, `SessionEpoch`, `ClockDomain`, `CommitState`などの概念を単純なnewtypeやenumで表現。型の増殖ではなく、不正な取り違えを防ぐことが目的。
- Payloadの所有権を表現する。Owned, DeviceBorrowed, InFlight の区別がAPI上必要なら表現し、不要なら状態機械で管理する。
- `Drop` の暗黙のデバイス返却による二重解放や失敗の握り潰しを見直す。device return ackが必要なら明示する。
- `unsafe`をごく小さなモジュールに閉じ、安全性前提を`// SAFETY:`で記載する。

### R2: Source/Buffer/Scheduler間の依存削減

- SourceからSchedulerへの具体型依存を除く。capabilityとacquire eventの契約を整理。
- Schedulerは返却義務とコストモデルだけを扱い、device APIやMCAPの内容を知らない。
- CopyOut、Reserve、Borrowの選択理由を構造化イベントで出せるようにする。
- Publisher/RMW/device側の観測できないlossを、内部lossと混同しない。

### R3: Writer/Storage/Commitの分離

- MCAPのlayout/offset決定を単一の責務として明確化。同期WriterとAsync backendの共通化で重複削減を図る。
- `submit`,`complete`,`flush/sync`, `commit`の契約とIDを整理。write completionとdurabilityを分離。
- `writev`/`io_uring`のiovecs・memory ownershipを明示。pending I/Oが残っているのにdropされないこと。
- Rotation/manifest/recoveryの順序制御を別モジュールへ。失敗時の状態遷移は表形式でテストする。

### R4: 観測・エラー処理・ライフサイクルの統一

- エラーに`source`, `operation`, `record/segment ID`, `layer`, `retriable`を必要十分な形で持たせる。過剰な文字列連結をHot Pathで行わない。
- shutdownは `StopAcquisition -> Drain -> Sync -> Finalize -> Report` を明示。各失敗経路をテスト。
- metrics / logs / warningsの命名、単位と意味を統一。表示専用の分岐をCoreに散らさない。
- Configのバリデーション、default、単位、legacy CLI互換を整理。

### R5: 根拠ある最適化（必要な場合のみ）

- ベンチで特定したホットスポットに限りメモリ確保、チャネル、ロック、コピー、syscall回数を改善。
- `unsafe`, FFI, raw pointersで速くする場合はsafe版との差分テスト、Miri/Loomなど可能な限りの検証を追加。
- 性能の優劣を同じNVMe/FS/CPU/温度/計測時間で比較し、達成条件と分散を示す。誤差範囲なら改善と断言しない。

### R6: 最終統合・文書化

- 全回帰テストとV1/V2/V3の代表実験を実施。
- `docs/architecture.md`, `docs/invariants.md`, `docs/source-adapter-contract.md`, `docs/storage-durability.md`, `docs/recovery.md`, `docs/performance-methodology.md` を最新実装に合わせ更新。
- 不要なコード・feature flags・依存ライブラリを削減。ただし旧実験を再現する手段は残す。

## 6. 性能回帰の扱い

- リファクタリング前後で少なくとも次の値を比較: accepted→committed latency、sustained durable throughput、p99/p99.9 write/CopyOut、copy bytes per GiB、CPU %、RSS/max queued、loss/unknown count。
- 新版が性能で劣る場合、影響が測定誤差内か検証する。再現可能な悪化は計測と原因を残し、修正するかリファクタ対象を戻す。
- 数値閾値を一律に「5%劣化なら失敗」等と固定する前に環境の測定分散を確認する。ユーザーが性能上限を明示した場合はそれを厳守。
- Linuxページキャッシュへのwriteの速度をdurable throughputと同列に比較しない。

## 7. レビューとテストのゲート

各フェーズの完了時に以下を確認してから次へ進む。

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
# 利用可能なfeaturesでintegration tests / fault tests / property tests
# SDK依存featuresは利用可能な環境でのみbuild/testし、未実施を記録
```

加えて、golden MCAP、Verifier、Loss Ledger、復旧レポート、各sourceのcapability試験、短時間の性能スモークテストを実施する。結果は「実行済・成功」「実行済・失敗」「環境がないため未実施」に分ける。

## 8. 禁止すること

- テストがない状態で全面リライトすること。
- 性能改善を確認していないのに、コピーや同期の削除を正当化すること。
- `Committed` を増やすため `fdatasync` を省略すること。
- エラーを握り潰す、UnknownをLost/成功に置き換える、欠損検出を無効化すること。
- 期限切れLeaseを`unsafe`で強制返却して参照中のDMAバッファを再利用させること。
- 実機SDKなしでproFRAME/NvSIPLの **実機対応** を「実装済・動作確認済」とすること（Mock契約対応済みは別の状態として正しく報告する）。
- モックを本物のSSDや実DMAと同じ性能保証に使うこと。
- Mock Adapter を後で実Adapterへ差し替える際、Core/Writer/Plannerに実装固有の分岐を追加しないと動かない構造へリファクタリングすること。
- ユーザーのファイル、未コミット変更、既存計測結果を消すこと。

## 9. 最終成果物とレビュー報告

- `docs/refactor-audit.md`（変更前の問題点、優先順位、作業方針）
- `docs/refactor-baseline.md`（機能/ベンチ/障害注入の変更前結果）
- `docs/refactor-report.md`（修正一覧、削減した重複、依存方向、残る技術的負債）
- `docs/architecture.md`、`docs/invariants.md`など最新ドキュメント
- テスト追加・差分、性能Before/Afterの比較データ、未実施理由
- モジュール別の変更コミット/PR説明（1段階で全部混ぜない）

終了報告は以下の形式にする:

```text
1. 結論: 振る舞いの互換性を保てたか
2. 変更した責務の境界と、その理由
3. 削除した重複・曖昧さ・unsafeの範囲
4. 所有権/永続化/欠損の不変条件に対するテスト結果
5. 性能比較（計測環境・回数・実測値。未実施は未実施）
6. 実機別ビルド/動作確認状態
7. 残課題と、今後の最適化候補
```

**完成像は、短いコードではなく、異なるカメラ/ROS2/OSストレージを載せても安全性の理由が追えて、性能の限界を測定できるコード。**
