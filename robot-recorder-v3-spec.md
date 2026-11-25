# 高帯域 MCAP Recorder V3 — Mock-first 実装・耐障害化・運用実装指示書（Codex向け）

対象: Linux / Rust 中心。必要な場合のみ C/C++ FFI。前提: `robot-recorder-v1-spec.md`、`robot-recorder-v2-spec.md`、実際のリポジトリ。  
本書は **V3 の実装指示** であり、現時点で V1/V2 が完了したと仮定しない。Codex は必ず現物を調査してから作業する。

**2026-11-25 要求変更：ユーザーの手元に実センサ・proFRAME・DRIVE AGXはない。V3の完了に実センサ接続、製品SDK、実際のROS 2 publisherを要求しない。** V1/V2と同様に実際の Adapter と同一の **レコーダー内部公開契約（SourceAdapter / SourceCapabilities / BufferLease / ReturnAck / SyncEvent 等）** を実装する Mock Adapter を主経路とし、実機実装へ差し替えられる構造を作る。実SDKの未確認の関数シグネチャやABIまで同一と主張しないこと。

## 0. Codex への最重要指示

1. 最初に `AGENTS.md`、`README.md`、Cargo workspace、直近の実装、既存テスト、V1/V2 の結果を読んで、実装済み／未実装／未検証を `docs/v3-gap-analysis.md` に記録する。既存成果を破棄しない。実装していない能力を実装済みと書かない。
2. V3 は **実機Adapterと同じ内部契約で動くMockによる統合・MCAP互換性・障害時に嘘をつかない記録・運用可能性** に重点を置く。最適化はV1/V2ベースラインと比較可能に維持する。
3. 実センサ・専用SDKは **ないことを前提** に進める。Fakeではなく契約を検証できる Mock Adapter を必須実装とする。実SDK依存はfeature分離し、存在しないSDK関数を捏造しない。実機向け手動試験手順は文書として残せばよい。実機未検証はV3の未完了理由にしない。
4. 全データパスで bounded memory と ownership/lease の不変条件を維持する。OOM、未申告欠損、二重保存、期限超過中のバッファ再利用を隠蔽しない。
5. 機能単位で実装→テスト→記録。大きな一括書き換えをしない。V3完了後のリファクタリングは **別指示書** `robot-recorder-post-v3-refactor.md` に従って実施する。
6. Linux上で通常ユーザー権限・安全なディレクトリに対してテストする。root権限、カーネル変更、大量ディスク占有、ハードウェア操作は明示的な許可なしに実行しない。

## 1. 目的と非目標

V3で目指すもの:

- 既存の Planner、BufferLease/Pool、Deadline Scheduler、Writer、Commit Tracker、Monitor、Verifier を再利用し、実機Adapterと同じ内部契約を持つMock入力をエンドツーエンドで記録できる。
- ROS 2 のシリアライズ済みCDRデータや型/メタデータをMockからそのまま扱う。V4L2 DMA、proFRAME、NvSIPLを模したMockで、異なる所有権・同期・プール制約の状態機械を検証する。
- MCAP が規格準拠であり、既存 MCAP Reader、想定する ROS 2 ツールで読める。
- 書込み完了／永続化済み／未確定／欠損を区別し、停止やクラッシュ後の復旧範囲を **証拠に基づいて** 再構成できる。
- 実用的な設定、ローテーション、ヘルス監視、再実行可能な負荷試験を持つ。

保証の表現は厳格にする。**無条件の exactly once、物理SSDの書込み回数の一意性、すべてのデバイスでの欠損ゼロは主張しない。** ターゲットは「モデル化・測定された条件下での無欠損」と「受理済みデータの永続化状態の検証可能性」。クラッシュ前に観測していない入力は、送信側の連番やドライバ統計がない限り完全には把握できない。

V3で実装しないもの: 実センサ接続の必須化、実機SDKの推測による互換実装、独自カーネルドライバ、すべてのDRIVE OS/SDKを跨ぐ互換ラッパ、全RMWにおける内部コピーゼロの保証、SPDK/DPDK導入、マルチSSD冗長化、デバイス破壊時に外部への欠損記録を必ず残すという保証、GUI、形式だけの抽象化。

## 2. V3の成果物と段階

| Stage | 主成果物 | 完了条件 |
|---|---|---|
| 0 | 現状監査と基準値 | `v3-gap-analysis.md`、V1/V2回帰試験・計測コマンド |
| 1 | 複数 Mock Adapter と契約 | ROS 2 serialized / UDP / SocketCAN / V4L2 / proFRAME / NvSIPL相当のMockが同じSourceAdapter契約を満たす |
| 2 | DMA相当のMock統合 | fence、プール枯渇、期限・返却、事前登録プール、許容されるReplaceをイベント駆動で再現。実機接続不要 |
| 3 | MCAP実用性 | 型/時刻/メタデータ、非圧縮Chunk/Index、ローテーション、互換性検証 |
| 4 | Recovery | crash-safeな範囲追跡、scan/repair/verify、Unknown の明示 |
| 5 | Ops & validation | 設定、ヘルス監視、決定的な障害注入、Mockを使った実SSDへの持続ベンチマーク、運用手順 |

Stage 0 が未達の状態で Stage 1 以降に進んでもよいのは、阻害要因が明確でログに残せる場合だけ。

## 3. アーキテクチャの境界（変更は必要な範囲に限定）

```text
Mock Source(s) [ROS2 / UDP / SocketCAN / V4L2 / proFRAME / NvSIPL]
    -> Source Adapter (same contract as future real adapter: capability, timestamps, sequence, errors, buffer access)
    -> BufferLease Registry / Owned Payload
    -> Deadline Scheduler + Planner + Monitor
    -> bounded Ready Queue
    -> MCAP Layout/Encoder
    -> Storage Backend (sync/async)
    -> Completion / Commit Ledger
    -> Rotation / Recovery Metadata
    -> Verifier / Health Reporting
```

- `SourceAdapter` は通知とバッファ寿命の共通契約を扱う。実AdapterはOS/デバイス固有通知を吸収し、Mockは同じ契約のイベントを再現する。いずれもMCAPのレイアウトは知らない。
- `Recorder Core` はセンサ固有APIもMockか否かも知らず、型情報・時刻・payload・観測された連番/ロス情報を受ける。
- `Storage Backend` は書込み結果、部分書込み、エラー、同期の完了を報告する。`write()` 成功を durable としない。
- `Verifier` は Writer の内部カウンタに頼らず、ファイルとジャーナルを別経路で検査する。
- V3では必ず synthetic / replay / Mock Adapter / fault-sink で同じ状態機械のテストを動かす。後から本物のSourceAdapterをDIで差し込むだけでCoreを変更せず利用できる設計とする。

### 3.1 Source Capability の最低限の項目

- `source_id`, `source_kind`, `payload_format`, `max_payload_bytes`, `stream_metadata`
- `OwnershipModel`: `Owned` / `PerBufferDeadline` / `SharedPool`
- `CopyAccess`: CPU可読、fence待ち要否、stride/plane、メモリのmapping可否
- `Replenishment`: 事前登録プール / 実行中追加可能 / 不可（SDKで確認した事実のみ）
- `Observability`: sequence有無、生成時刻・到着時刻、ドロップカウンタ、source-side lossの完全性
- `ClockDomain`: wall / monotonic / device clock と同期情報
- `Lifecycle`: start/stop/drain、取得停止・故障・復旧などの状態

能力は明示的に「不明」を許容する。未確認ならプランナーで保証可能と扱わない。

## 3.2 Mock-first 共通 Adapter 契約（V3必須）

**ここでいう「実機と同じインターフェース」は、SDK ABIではなく、Recorder Core が利用する境界の同一性を指す。** `SourceAdapter` trait、`SourceCapabilities`、`Frame` / payload handle、`acquire()`・`return()`、完了イベントとエラー通知、起動・停止・drain が **Mock と将来の実機実装で同じ** であること。Coreに `if mock { ... }` の特別経路を作らない。

最低限、以下のMockを実装する（内部構造の共通化は適度に行ってよい）。

| Mock | 再現する契約・異常 |
|---|---|
| `MockV4l2Source` | queue/dequeue、frame ready、buffer再利用禁止、streamoff、QBUF遅延 |
| `MockProframeSource` | PCIe capture相当のプール、完了通知、返却要求/ACK、デバイス内drop計数、キャプチャ中の新規登録が不可の場合 |
| `MockNvSiplSource` | 事前登録画像プール、AddRef/Release相当の寿命、EOF fence、prefence相当の利用可能時点、空き数枯渇、block/pitch/strideメタデータ |
| `MockRos2SerializedSource` | CDR bytes、topic/type/schema、publisher GID/sequence、timestamp/QoS、送信側gap/再起動、受信キュー過負荷 |
| `MockUdpSource` / `MockCanSource` | datagram/frame境界、時刻、連番可観測性、burst、kernel受信前drop相当の通知、異常サイズ |

- `MockProframeSource` と `MockNvSiplSource` は**名前も仕様もあくまで想定能力の再現**であり、実機SDKの型、実際のバッファ数上限、通知順序の完全互換を保証しない。未知の能力は `Unknown/Unsupported` で表す。
- 模擬では実際のDMA転送をせず、事前確保RAMにデータを生成して **device-borrowed の寿命契約** を模擬する。バッファの再利用可能時点やfence完了はイベントで管理する。返却が受理された時点と、実際に空きとして数えられる時点を区別する。
- 可変のサイズ・周期・位相・ジッタ・バースト・pool count・reserve count・コピー時間・write stall・fence遅延・返却遅延・drop・device reset・断線を seed 指定の仮想時刻イベントで注入する。
- Coreに渡す前の損失は source side の真値をMockが保持し、Recorderが観測できる情報（連番、ドロップ統計など）だけでどこまで推定できるかを独立Verifierで検証する。Mock内部真値を実録画側へ裏口で渡さない。
- 同一の負荷/seed/契約でAlways Copy・Borrow・Deadline-aware・Reserveを比較し、**同一の総バッファ予算**を保つ。MockではPCIe、IOMMU、キャッシュ整合、ISP/GPU負荷、実ドライバのタイミングは保証できないと明記する。
- 構造上の契約同一性をコンパイル時に検証し、Mock → Recorder Core → MCAP Writer → 実ファイル → 独立VerifierまでのE2Eテストを必須とする。SDKなし環境でMockを含む全default featuresをビルド・テストできること。

## 4. ROS 2 Serialized Adapter契約とMock（優先度: 高）

1. 原則としてメッセージをデシリアライズしない。型情報・CDR bytes・publish/receive timestamps・publisher GID/sequence（RMWが提供する場合）を保存する。
2. **V3必須はMockRos2SerializedSource**。CDR形式の既知fixture（または仕様に沿って生成したbyte列）を実際のROS2 Adapterと同じ内部契約で出力する。`rcl_take_serialized_message()` や rclcpp GenericSubscription を使う実Adapterは将来の任意実装。ROS 2 distribution/RMWがない開発機で導入しない。
3. `rclrs` に raw API が存在すると推測しない。rclrsへのOSS寄与は別作業とし、本体の完成条件に依存させない。
4. 事前確保バッファを活用する。rcl/RMW側で再確保・コピー・デシリアライズが起きない保証は **実装と計測がある場合だけ** 主張する。容量超過時の再確保回数または拒否を監視する。
5. ROS 2 メッセージをMCAPの適切な Schema/Channel と `message_encoding`（一般に`cdr`）で保持。型名、schemaの入手元、ROS 2 distribution、QoSを記録する。実際のReaderで復元できることを確認。
6. Mockのトピック増減・型変更・publisher再起動をイベントで再現する。実Discoveryは対象外。型が不明/非対応なら勝手にデータを別の型として保存しない。
7. QoSを明示し、`reliable`/`best_effort`、depth等を設定・ログに残す。ROS 2内の未受信メッセージ数は、publisher連番・RMWイベント・外部カウンタの可観測性に応じて`known loss`と`unknown`を区別する。
8. テスト: Mock既知CDR payload→録画→再読込、複数型・大/小データ、burst、publisher途中再起動、受信キュー過負荷、QoS不一致相当、シャットダウン。RMW実装の個別挙動は未検証とする。

ROS 2環境がないことを標準とする。`ros2-real` featureは任意で独立させ、**Mockを通す標準ビルドにROS2依存を持ち込まない**。実接続試験は`NOT TESTED`、V3のMock完了判定とは別枠にする。

## 5. UDP / SocketCAN Adapter契約とMock（優先度: 高～中）

V3必須は `MockUdpSource` / `MockCanSource` の完結した記録と検証。実socket/`vcan`/ネットワークループバック実装は設備と権限がある場合のみ任意。

- UDP は `recvmmsg`/非ブロッキングソケット等を評価し、既存epoll/reactor方針に合わせる。per-source sequenceがないデータグラムは受信前ロスの完全検出不可とする。
- `SO_RXQ_OVFL` や socket drop counters等は利用できるとき取得し、計測可能範囲とリセット/オーバーフローを明記する。UDPパケット分割、巨大ペイロードをロスレスで再構成すると勝手に仮定しない。
- SocketCANを有効にできる環境ではCAN/CAN FDを正しいDLCとタイムスタンプで記録する。CANの低帯域データでWriterの優先制御を壊さないことを確認。
- それぞれ Writer/Plannerにはバッファ容量・到着包絡線とLoss Observabilityを提示。bounded queue満杯時は明示的な欠損/受信停止理由を記録する。
- テスト: loopback/vcanがある場合と無い場合を区別。想定以上の特権/ネットワーク設定は要求しない。

## 6. カメラ・DMA相当のMock Adapter（実機接続なし）

### 6.1 共通の実装契約

- フレームの準備完了（EOF fence / driver completion等）以前にCPU読み出し・CopyOut・書込みを開始しない。
- Pixel format、planes、stride、height、pixel order、padding、device timestampをメタデータとして残す。`bytesused`とallocation sizeを混同しない。
- Device bufferとOwned spill bufferを別型で表現。コピー後に元Leaseを返しても、コピー済みpayloadは有効であること。
- `Release` / requeue は厳密に一度。非同期I/Oの参照終了前にデバイスへ再利用を許可しない。
- CopyOut がPCIe/CPU/DRAMを圧迫する場合の実測（コピー開始/完了、DMAプール空き数、システム負荷）を残す。
- 共有プール型は「どのbufferをいつ返すか」より「次のキャプチャ前に何個使えるか」を基準にする。
- デバイスの契約を満たせず、期限安全性が不明なら `unsafe-to-borrow` とし早期CopyOutにフォールバックする。そもそもCPU読み出しできない場合は、そのまま保持できる別経路がない限り録画不能と明示。

### 6.2 Linux V4L2（Mock必須・実デバイス任意）

- `VIDIOC_REQBUFS`, `QUERYBUF`/`QBUF`/`DQBUF` 等の既存デバイスに応じたフローで、mmap、DMABUFのimport/export可否を確認する。全ドライバが動的交換可能と仮定しない。
- 出力側/入力側の buffer ownership と dequeue/queue の合法な状態遷移をテスト。poll/epollからのwake-up、EINTR/EAGAIN、stream-off/shutdownを実装。
- 返却期限をドライバの真の保証値と見なさず、プール空きと到着上限から安全側に導出する。
- 実デバイスがなければ v4l2loopback 等も候補だが、real DMA挙動と同一とは主張しない。機器テスト手順を提供する。

### 6.3 Solectrix proFRAME 3.x（Mock必須・SDK統合不要）

- まずユーザーが利用可能なバージョンのSolectrix公式SDK、headers、sample、ライセンス条件を確認。3.0と3.1は同一APIだと決めつけない。
- 調査対象: DMA memory owner、登録可能バッファ数と制限、capture中の追加登録、完了通知、返却操作、drop counter、PCIe経路、CPU map可否。
- 可能ならC/C++ thin shimでRustへ（opaque handle + lifecycle + capabilities + acquire/release/status）。SDK型をCoreへ漏らさない。API確認前の架空シンボルを実装しない。
- **SDKの存在を前提にしない。** `MockProframeSource` を標準テストに含める。`proframe-real` featureは任意。SDK能力の未確認点をリスト化し、Mock完了を実proFRAMEの対応済みと表示しない。

### 6.4 NVIDIA DRIVE AGX Orin / Thor（Mock必須・NvSIPL SDK統合不要）

- 導入済みのDriveOSの **正確なバージョン** と、サンプル・ヘッダで対応するAPIを調査する。OrinとThorの同一挙動を前提にしない。
- `NvSciBuf` の画像登録、`NvSIPL` のバッファ取得/参照管理、EOF fence と必要なprefence、CPUアクセス可否、block-linear/pitch-linear/planesの扱いを把握する。
- 事前登録プールを拡大する能力と、撮影中に無制限に別バッファを供給できる能力を混同しない。Replaceは事前確認できた能力の範囲で使う。
- **SDKがない前提**で `drive-real` featureを分離し、fence/Releaseの状態機械を模した `MockNvSiplSource` を標準テストする。実GPU/ISP/PCIe/DRAM競合のベンチ結果は実機のみで報告。
- Rust FFIは例外・所有権・キャッシュ同期・スレッド制約をC++ shim内で閉じ、API呼び出し成否とdevice error eventsを明示的に返す。

## 7. MCAPフォーマットとファイルローテーション

1. 既存の非圧縮MCAP方針を維持。インデックス付きChunk構成を既定にできるようにする。Chunk/index が追加のコピーやメモリを生むなら測定する。
2. ROS 2以外のデータはチャンネル名、schema/encoding、デバイス固有のメタ情報を一貫して定義する。例: Raw YUV422 frameを勝手にROS ImageのCDRと偽装しない。必要ならdocumented custom schema/metadataにする。
3. 変動するsource metadata、ROS Type、clock domain、timestamp mapping、session epoch、sequenceの由来を記録する。ファイルに含められないrun-level情報はmanifestへ。
4. Rotation は時間/サイズ上限、手動トリガで実装。新ファイルが正常に開けなかった場合の挙動、古いファイルのclose/sync、両ファイル間のseq gap/overlapを定義。
5. 進行中の未完成ファイル名と完了ファイル名を区別し、footer/indexが未完でも復旧可能な範囲をscanできるようにする。MCAP仕様にない仮レコードを書いて読めると主張しない。
6. 公式/独立MCAP ReaderによるSchema、Channel、Message、sequence、payload、Chunk/index、再生範囲を検査する。mcap_playerによる実用試験も推奨。
7. ディスク残量/ローテーション上限/許容ファイル数を事前確認。既存ファイルの上書きや削除は明示opt-in以外禁止。

## 8. 永続化・Exactly-onceに近づくためのRecovery

V2の `Submitted` / `Written` / `Committed` / `Lost` / `Unknown` を維持し、V3は実際に再起動後に判定可能な範囲を増やす。ここが最も重要な安全性機能の一つ。

### 8.1 IDとジャーナル

- 論理レコードID: `(session_epoch, source_id, source_sequence)`。sourceに元連番がない場合は **受理時点からのローカル連番** とし、受理前ロス検出を保証しない。
- `recording_run_id`, `segment_id`, `transaction_id` と、ファイル内のoffset range、CRC/hash、source sequence rangeを記録できるようにする。
- manifest/ledgerの更新は **データ先行永続化→ledger確定** の順序を規定し、失敗時は保守的に `Unknown` とする。`fdatasync`成功だけでSSDの不正な電源断耐性まで証明したと扱わない。
- ジャーナルの破損/末尾切れ/旧版は正しく検知する。副作用なしの検査モードをデフォルトにする。
- Manifestを置換する場合は一時ファイルを書いてsync、rename、必要なディレクトリsyncの扱いを明記。ジャーナル自身の同期順とMCAP本体の同期順を文書化する。

### 8.2 復旧アルゴリズム

```text
recover --dry-run:
  1. manifest / journal / MCAP segment候補を列挙
  2. CRC・レコード境界・ID・offset/length と既知のcommit情報を検証
  3. 最後の確実なdurable rangeを識別
  4. それ以降の可読データはRecoveredButUnconfirmed等の別状態で報告
  5. Lost / Unknown / duplicate / corrupted / unobserved を区別
  6. どの修復を行えるかプレビュー、元ファイルは変更しない

recover --apply --output-dir <別ディレクトリ>:
  1. 原本を保持し、解析済みの回復可能メッセージから新MCAPを組み直す
  2. 新MCAPを独立Readerで再検証
  3. 詳細なrecovery reportと元ファイルへの参照を出力
```

- **確認できないデータを勝手に `Lost` と断定しない。** filesystemに残る可読レコードが「クラッシュ以前に確実にdurableだった」ことと同値ではない。
- 同じIDが複数回現れたら、内容一致・不一致を報告し、黙っていずれかを残さない。インデックス修復は元データの証拠を保存する。
- `recover --apply` は破壊的なin-place編集をしない。実験としてin-place版を追加するなら別フラグ、別テスト、バックアップ要件が必要。
- 完成したファイルの「exactly-once」は論理IDによる重複の無さと、列挙可能な範囲の検証を意味する。物理層の書込み回数や、センサ生成からの無欠損を保証しない。
- 記録中の電源断ではユーザーランドの未永続化ジャーナルも失われ得るので、全損失の永続的な報告まで保証するには独立した耐障害ログ/上位送信元の照合が必要、と明記。

### 8.3 Fault Injection

- syscall: short write, `EINTR`, `EAGAIN`、`EIO`, `ENOSPC`, fsync/fdatasync failure、rename/folder sync失敗。
- Async: completion reorder、取消し競合、copy中失敗、返却遅延、stop/drain中異常。
- Process: write前/後、sync前/後、manifest更新前/後、rotation途中に外部プロセスからkill。子プロセスでのみ実施し、作業ファイルを保護する。
- Verifier: 欠損、重複、CRC不整合、書込み途中の切れたrecord、壊れたindex、未知の出力を検知。

## 9. 保証・監視・パフォーマンス

### 9.1 監視必須指標

- Source別: 入力bytes/s、frames/s、到着遅延、source内部drop、受理前gapの観測可否
- Pool別: total/available/held/pending-return、空き枯渇予測時間、返却 deadline slack
- Scheduler: policy選択数、CopyOut bytes、予約数、期限違反予告/実績、欠損開始までの警告先行時間
- Writer: accepted/submitted/completed/committed bytes、in-flight、queue、partial write、sync latency
- OS/Storage: page cacheに依存した見かけのwrite速度とdurable rateを分ける。RSS、CPU、disk utilization、温度/thermal throttling（取得可能な場合）
- Integrity: known lost/unknown、再起動後に回復した範囲、未申告gap、duplicate/corrupt

### 9.2 Runtime warnings

V2の `AT_RISK`, `CONTRACT_VIOLATION`, `LOSS_DETECTED`, `DURABILITY_UNKNOWN` を維持し、必要なら`SOURCE_DISCONNECTED`, `RECOVERY_NEEDED`, `CLOCK_SYNC_UNCERTAIN`を追加。出力はJSONLなど機械可読な形式を用意し、過剰なログ出力がHot Pathを止めないようにする。

### 9.3 事前検証

Static Plannerは計測済みのstorage sustained minimum、stall上限、copy負荷、source rate/burst、各poolの予約枠を使う。未知の値は`unknown`。profileの測定条件（機器、ファイルシステム、温度、メモリ競合、持続時間、sync方式）を必ず保存する。

## 10. CLI・設定・運用

既存の `plan`, `record`, `verify`, `benchmark` に加え、必要な範囲で下記を導入する。既存コマンドと互換であることを優先する。

```bash
recorder list-sources
recorder inspect-source --source mock_v4l2:camera_0
recorder plan --config config.toml --profile profile.json
recorder record --config config.toml --output-dir runs/run-001
recorder verify --run-dir runs/run-001 --report report.json
recorder recover --run-dir runs/run-001 --dry-run
recorder recover --run-dir runs/run-001 --apply --output-dir recovered/run-001
recorder benchmark --suite v3-smoke --output-dir results/
```

- CLI名は現物に合わせる。サンプルconfigは `synthetic`, `mock_ros2`, `mock_udp`, `mock_socketcan`, `mock_v4l2`, `mock_proframe`, `mock_drive` を **標準で** 提供し、実センサのないPCで完走させる。実機用 config は任意・feature-gated。
- 起動時、空き容量・ファイルアクセス・Source capability・planner結果を表示。`unknown` で起動するには明示 opt-in を維持。
- SIGINT/SIGTERM で停止受付→source停止→queue drain→sync/close→report。タイムアウト時はunknown/incompleteを記録し正常終了を偽装しない。
- SDKなし、権限なし、デバイス未接続、RMW型不一致等の失敗を利用者に具体的に伝える。
- process crash時にアプリ内で失われる最終メモリ内容は記録不可。その限界をREADMEに記載する。

## 11. テスト計画

| ID | テスト | 最低合格条件 |
|---|---|---|
| V3-0 | V1/V2回帰 | 旧合成ベンチ/Verifier/Plannerの機能不変 |
| V3-1 | Mock ROS2 serialized round-trip | 複数型のCDR/Schema/metadata一致、未申告gapなし（観測可能な範囲） |
| V3-2 | Mock UDP/CAN input | packet/frame境界、timestamp、drop/連番可観測性が正確 |
| V3-3 | Mock V4L2 lease | frame ready前アクセス禁止、dequeue/queue相当の状態遷移とcleanup正確 |
| V3-4 | Mock proFRAME/NvSIPL | 両Mockが本物と同一の内部SourceAdapter契約を満たし、fence/返却/プール枯渇を決定的に検証 |
| V3-5 | MCAP互換性 | 独立Readerで全チャネル・索引・rotation跨ぎの検証 |
| V3-6 | Recover normal | 正常終了後、不必要なrepairなし |
| V3-7 | Crash matrix | write/sync/ledger/rotation各境界で重複・破損・Unknown検出 |
| V3-8 | Device/SSD stall | 欠損ゼロの前提条件を満たす範囲で保護、逸脱時は警告と記録 |
| V3-9 | Sustained throughput | 1 GB/s目標、2 GB/s stretch; 未達は律速・測定条件を報告 |
| V3-10 | No unbounded memory | 長時間/障害下のRSS、pool、in-flightに上限 |

- 性能は本物のNVMeと`fdatasync`を含む測定を、模擬ストレージと別表で報告。`/dev/null`やtmpfsをdurabilityの根拠にしない。
- 10秒だけのベンチでSSDの持続書込みを主張しない。キャッシュ枯渇や熱制限のため長時間評価を別実験として定義。
- 実センサを用いた試験はV3スコープ外であり、**未実施でもMock-first V3の完了を妨げない**。ただし模擬完了を実機対応完了と書かない。

## 12. 完了条件・報告形式

V3完了の判断を **完成したCore＋全対象Mock AdapterのE2E動作** とし、実機への対応状況は別途明示する。

### Core完了条件

1. `cargo fmt --check`, `cargo clippy --all-targets --all-features`（SDK不在で構築不能なfeatureは分離して例外明記）, `cargo test` が通る。
2. Mock ROS2 / Mock UDP / Mock CAN / Mock V4L2 / Mock proFRAME / Mock NvSIPL のすべてについて、**本番SourceAdapterと同じ内部契約**によるE2E記録と独立Readerでの検証が可能。実入力経路は要求しない。
3. Crash/rotation/recoveryが動作し、`Committed` と `Unknown` を混同しない。
4. BufferLeaseの生存期間、bounded memory、未申告の受理済みデータ欠損ゼロの不変条件をテストしている。
5. 性能測定と機能試験は明確に区別。V1/V2との回帰差を示す。
6. インストール手順、設定例、観測できる/できない欠損、unsupportedデバイス、既知の制約がREADMEにある。

### ハードウェア完了状況

`ROS2`, `UDP`, `SocketCAN`, `V4L2`, `proFRAME`, `DRIVE Orin`, `DRIVE Thor`ごとに **Mock contract implemented / Mock runtime verified / real adapter implemented / SDK build verified / hardware verified / not available** を別列で報告する。実機確認なしで「完全対応」と主張しない。

### Codexが最終回答する内容

- 変更点の概要、コミット単位、主な設計判断。
- 実行したコマンドとテスト結果、できなかったテストの理由。
- 性能の実測値と測定環境（未実施なら未実施）。
- V1/V2回帰結果・V3-0〜V3-10の一覧。6系統MockのE2Eテストと失敗注入の結果。
- 認識している保証の限界、未解決点、次のリファクタリングで扱う技術的負債。

## 13. 参照資料（実装するバージョンで再検証すること）

- MCAP形式: https://mcap.dev/spec
- rcl C API: https://api.nav2.org/jazzy/html/subscription_8h.html
- NVIDIA DriveOS 7.0.3 Camera SIPL sample: https://developer.nvidia.com/docs/drive/drive-os/7.0.3/public/drive-os-linux-sdk/embedded-software-components/DRIVE_AGX_SoC/NvMedia/NvMedia_Sample_Applications/nvsipl_camera/SIPLNormalStream.html
- NVIDIA INvSIPLBuffer: https://developer.nvidia.com/docs/drive/drive-os/7.0.3/public/drive-os-linux-sdk-api-ref/classnvsipl_1_1INvSIPLClient_1_1INvSIPLBuffer.html
- Solectrix proFRAME: https://solectrix.de/en/products/proframe/
- Linux V4L2: https://docs.kernel.org/userspace-api/media/v4l/v4l2.html

**重要**: V3の実装後、挙動を変えずに全体を整理する作業は `robot-recorder-post-v3-refactor.md` で独立して行う。新機能とリファクタリングを一括で混ぜない。
