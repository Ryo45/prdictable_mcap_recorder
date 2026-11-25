# V3 実装・検証報告

V2 のデータパスに Synthetic と 6 系統の Mock を組み込み、既存 Planner、Queue、Lease、Scheduler、Writer、独立 Verifier を通した。ROS 2 は2つのスキーマを持つ CDR fixture、他の Mock は独自 encoding の fixture を出力する。カメラ系 Mock は RAM プール、準備完了イベント、返却遅延を持つ。DRIVE Mock は事前登録予備を設定可能。

データ同期成功後に MCAP prefix CRC・offset・ID 範囲を checkpoint に出す。読み取り可能なメッセージと、検証できる checkpoint があるメッセージを区別する。正常な run-dir の公開はファイル同期、ハードリンク、ディレクトリ同期の順で行う。復旧 dry-run は原本を読取り、新規出力への repair は独立 Reader で照合した非圧縮 Chunk/Index 付き MCAP を作る。

| ID | 状態と証拠 |
| --- | --- |
| V3-0 | PASS。V1 E0–E6、V2 P1–P9 の既存判定と CLI 回帰 |
| V3-1 | 部分。2種類の CDR、Schema、metadata のラウンドトリップを検証。動的な型変更、publisher 再起動、QoS 不一致、ROS 2 ツールの再生は未検証 |
| V3-2 | 部分。Mock UDP の事前欠損2件と UDP/CAN FD 相当の個別メッセージ境界・時刻を検証。連番なし、カーネルカウンター、可変 DLC と不正パケット系列は未実装 |
| V3-3 | 部分。仮想時刻で fence 前の取得不可、借用、遅延返却、drain、プール枯渇時の世代保護。stream-off 固有通知は未実装 |
| V3-4 | 部分。両系統の同一内部契約、fence と返却のテスト。個別 reset、二段 fence、複数 plane、実デバイス挙動は未検証または未実装。Pool 枯渇と予約の基本動作は V2 試験で検証 |
| V3-5 | 部分。通常録画の線形読取と、復旧コピーの非圧縮 Chunk/Index を検証し、全190件を index から seek。通常録画の Chunk/Index とライブ rotation は未実装 |
| V3-6 | PASS。正常な V3 MCAP の dry-run は 190 件すべてを checkpoint と照合し、修復不要を報告 |
| V3-7 | 部分。SIGKILL 1 境界、sync 失敗、CRC 改変、footer/journal 末尾切断、同一内容/異内容の重複。全境界への kill と rotation、電源断は未検証 |
| V3-8 | 部分。V2 の停止・部分書込み・EIO・ENOSPC・コピー故障と V3 の sync 失敗を検証。全 Mock を対象とする故障直積や実 SSD 障害は未検証 |
| V3-9 | 部分。実 NVMe/ext4 に 30 秒、同期込み 0.674643 MB/s（入力律速）。1/2 GB/s の持続性能は未検証 |
| V3-10 | 部分。30 秒試行の peak RSS 6.34 MiB・queue 2、回帰で事前確保と Lease 解放を検証。長時間高負荷と OS OOM は未検証 |

全 Mock の E2E では 192 予定、190 保存・同期・照合、2 件の送信側相当の既知欠損、Unknown・未申告欠損・重複 0。Shadow truth を持つ汎用 replay、実時間から独立した一式の storage simulation、ライブ rotation と run/segment manifest は未実装。Recovery は recorder の unchunked MCAP を対象とし、入力・journal は各128 MiB、最大262,144メッセージ。古い V2 checkpoint は CRC 証拠がないため、Recovery では状態未確定。未永続化の観測や受理イベントを復元する独立ログは未実装。

| 系統 | Mock 契約 | Mock E2E | 実 Adapter | SDK build | 実機 |
| --- | --- | --- | --- | --- | --- |
| ROS 2 | 実装 | 確認 | 未実装 | 対象外 | 未実施、ROS 2 publisher なし |
| UDP | 実装 | 確認 | 未実装 | 対象外 | loopback 未試験 |
| SocketCAN | 実装 | 確認 | 未実装 | 対象外 | vcan/CAN 未試験 |
| V4L2 | 実装 | 確認 | 未実装 | 対象外 | 機材なし |
| proFRAME | 想定能力を実装 | 確認 | 未実装 | SDK なし | 機材なし |
| DRIVE Orin | 共通 NvSIPL 想定能力を実装 | 共通 Mock を確認 | 未実装 | SDK なし | 機材なし |
| DRIVE Thor | 共通 NvSIPL 想定能力を実装 | 共通 Mock を確認 | 未実装 | SDK なし | 機材なし |

[Test・計測条件と再現方法](refactor-baseline.md) · [着手時の V1/V2 監査](v3-gap-analysis.md)
