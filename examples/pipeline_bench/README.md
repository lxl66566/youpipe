# pipeline-bench 实测

## 场景

三阶段文档处理流水线（IO 读 → CPU 分析 → IO 写），文档尺寸服从对数正态
分布（重尾，P99 ≈ 259 KiB）。对比五种实现，各跑一次计时。

- youpipe mixed : stream().stage_async(read).stage(cpu).stage_async(write)
  IO 在 tokio 运行时 M:N 多路复用，CPU 在工作窃取池
- youpipe sync : stream().stage(read).stage(cpu).stage(write)
  三个阶段全部在 compute pool 上
- rayon : par_iter().for_each(read; cpu; write)，默认全局池
- tokio : tokio::spawn(async { read → spawn_blocking(cpu) → write })
  IO 用 tokio::time::sleep（M:N），CPU 用 spawn_blocking 池
- sequential : for 循环逐文档，阻塞 IO

## workload

- 2000 个文档，尺寸 log-normal（μ=9.0, σ=1.5），截断 256 B .. 2 MB
- CPU 工作：每文档 size × 3 轮 SipHash（std DefaultHasher），~5.2 ns/轮
- IO 模拟：读 = size × 30 ns（上限 5 ms），写 = size × 15 ns（上限 3 ms）
- 用时占比：IO 读 48% / CPU 26% / IO 写 26%（顺序执行实测）
- 无真实磁盘 IO——用 thread::sleep / tokio::time::sleep 模拟，零 SSD 写入

## 环境

- CPU : 32 核 (AMD EPYC)
- 工具链 : rustc 1.85+, youpipe 0.3.0, rayon 1 / tokio 1

## 结果

2000 文档，三次独立运行

以下是将三次测试结果整理为 Markdown 表格，每次运行单独一个表格，方便对比。

---

### 1

| 实现          | 耗时 (ms) | Docs/s | 加速比 |
| ------------- | --------- | ------ | ------ |
| youpipe mixed | 47.4      | 42194  | 59.7×  |
| tokio         | 63.5      | 31496  | 44.5×  |
| rayon         | 94.0      | 21277  | 30.1×  |
| youpipe sync  | 145.0     | 13793  | 19.5×  |
| sequential    | 2830      | 706    | 1.0×   |

---

### 2

| 实现          | 耗时 (ms) | Docs/s | 加速比 |
| ------------- | --------- | ------ | ------ |
| youpipe mixed | 46.6      | 42884  | 60.9×  |
| tokio         | 64.6      | 30950  | 43.9×  |
| rayon         | 99.5      | 20105  | 28.5×  |
| youpipe sync  | 144.8     | 13809  | 19.6×  |
| sequential    | 2840      | 705    | 1.0×   |

---

### 3

| 实现          | 耗时 (ms) | Docs/s | 加速比 |
| ------------- | --------- | ------ | ------ |
| youpipe mixed | 47.5      | 42066  | 59.5×  |
| tokio         | 64.3      | 31105  | 44.0×  |
| rayon         | 102.3     | 19555  | 27.7×  |
| youpipe sync  | 144.8     | 13811  | 19.6×  |
| sequential    | 2830      | 706    | 1.0×   |

排名在三次运行中完全稳定，不存在相邻名次翻转。

## 【复现】

`cd examples/pipeline_bench && cargo run --release`

环境变量：

N_DOCS=2000 文档数量（默认 2000）
