# Source Adapter の内部契約

`SourceAdapter` は capabilities、次に処理する event 時刻、acquire、pool_status、drain を提供する。Ready は Frame と空きプール数を返す。CPU で読める時点まで Frame を公開しない。Owned と device-borrowed は別の Payload variant。Mock の return request は Lease の Drop、return ack 相当は Pool が Returning を Available に戻すイベントで表す。

V3 サンプルでは各 source の時刻を同じ monotonic timeline に置き、epoch で Unix ns に写像する。ROS 2 fixture は CDR をバイト列のまま保管する。型・QoS・模擬 publisher ID を Channel metadata に入れる。UDP/CAN/Camera は recorder 独自 fixture を使う。カメラ fixture は単一の pitch-linear バイト面。実機の pixel format、GPU/ISP、DMA cache coherency、SDK ABI は未検証。

実機を追加する際は SDK と build feature を別に準備する。対象バージョンと headers で取得、返却、バッファ登録と準備完了の各操作を確認する。契約試験で無負荷の E2E、取得停止、fence 未完了、返却遅延、プール枯渇、USB/PCIe/ネットワーク切断を順に確認し、Driver カウンターと recorder journal を照合する。次に実 I/O を同期込みで計測する。ROS 2 は配布版・RMW ごとの serialized bytes と型・時刻・QoS、UDP は kernel drops と datagram 境界、SocketCAN は DLC と timestamp、V4L2 は DQBUF/QBUF/stream-off、proFRAME は登録制限と返却 ack、NvSIPL は実機種別ごとの fence、mapping と Release を個別に確認する。
