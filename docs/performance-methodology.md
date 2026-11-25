# 比較方法

- V1 は `tools/bench.py --suite policy`、V2 は `recorder benchmark --suite v2-smoke`。V3 は `recorder benchmark --suite v3-smoke`。各出力先は新規ディレクトリ。
- V3 は同じ release ビルド設定、入力・seed・CPU affinity・プール・キュー・グループ同期閾値を使用。sync と Null は別集計。Null に物理ストレージ耐久性はない。
- `tools/bench_v3.py --recorder PATH --out PATH --policy always-copy` は CopyOut を測る。同一の総プール予算でポリシーを変えられる。`--seconds 30 --repeats 1` は低負荷の持続試行。
- sync 帯域は録画開始から最終データ同期までを分母にする。各試行の JSON に p99/p99.9/max、copy bytes/GiB、process CPU 使用率、peak RSS、queue peak、known loss、Unknown を残す。比較は試行ごとの範囲も見る。
- 短い入力律速試行では、NVMe の飽和帯域・キャッシュ枯渇・熱制御を評価できない。1/2 GB/s の持続評価には、入力と書込み量を確保した別試験が必要。V2 の短時間高負荷結果は [V2 計測](v2-experiments.md) にある。
