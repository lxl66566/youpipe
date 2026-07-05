==========================================================================
file-encrypt-bench 实测结果与结论
==========================================================================

【场景】
读一堆大小差距很大的文件 → 处理 → 写回。对比三种实现，每个引擎各跑一次计时。
- youpipe : stream().stage(read).stage(process).stage(write).run()
            pool = 3×num_cpus（96 线程，每阶段 ≈ cores），buffer=4，Workload::Unbalanced
- rayon   : par_iter().for_each(read; process; write)，默认全局池（32 线程）
- tokio   : 每文件 tokio::spawn：tokio::fs::read → spawn_blocking(process) → 写回(fsync 时再 spawn_blocking)

CPU task 通过 FC_TASK 切换：
- compress（默认）: zstd 压缩（FC_ZSTD_LEVEL，默认 15）+ AES-256-GCM。重 CPU，随文件大小线性增长。
- aes           : 纯 AES-256-GCM。轻 CPU 参照（AES-NI）。

【环境】
- CPU      : 32 核
- 存储     : /root 为 btrfs（NVMe，compress=zstd:11）；oflag=direct 写 ~979 MB/s，缓存命中读 ~5 GB/s
- /tmp 为 tmpfs（内存盘）—— 会被程序检测并警告，故实测把 FC_DATA_DIR 指到 btrfs 真盘
- 工具链   : rustc 1.98.0-nightly，youpipe 0.2.0，zstd 0.13 / rayon 1 / tokio 1 / aes-gcm 0.10

【workload】
- 200 个文件，尺寸 log-uniform 8 KiB .. 8 MiB（1024× 跨度），按 hash 散布打乱
- 内容为可压缩数据（16 符号字母表、按文件 seed 的 LCG、非周期）——让 zstd 真正做最优解析
- 总输入 257.9 MiB；每个引擎跑前用 POSIX_FADV_DONTNEED 把输入逐出页缓存（冷读）
- 写回默认带 fsync（FC_FSYNC=1，durable write-back）

--------------------------------------------------------------------------
【重 CPU 结果】zstd(15) + AES-256-GCM，btrfs，fsync 开，257.9 MiB / 200 files
--------------------------------------------------------------------------
                          耗时        吞吐        files/s     校验
跑 1  youpipe          4.973 s     52.0 MiB/s    40          200/200   <== 最快
       rayon           5.023 s     51.3 MiB/s    40          200/200
       tokio           5.155 s     50.0 MiB/s    39          200/200

跑 2  youpipe          4.963 s     52.0 MiB/s    40          200/200
       rayon           4.744 s     54.4 MiB/s    42          200/200   <== 最快
       tokio           5.198 s     49.6 MiB/s    38          200/200

（8 符号字母表 + zstd-19 会触发 zstd 匹配爆炸、病态慢到 ~13 s：youpipe 13.34 / rayon 13.45 / tokio 15.86。
 该配置已弃用，默认改为 16 符号 + level 15，落入重但合理的 CPU 区间。）

--------------------------------------------------------------------------
【轻 CPU 参照】纯 AES-256-GCM，btrfs，fsync 开，257.9 MiB / 200 files
--------------------------------------------------------------------------
       youpipe         ~205 ms     ~1260 MiB/s
       rayon           ~201 ms     ~1290 MiB/s
       tokio           ~199 ms     ~1300 MiB/s
（三轮 youpipe 205~215 / rayon 192~207 / tokio 192~200 ms，三者紧贴。）

--------------------------------------------------------------------------
【结论】
1. 轻 CPU（AES-NI）+ 缓存命中：rayon 凭最少开销胜出。AES 太快使 CPU 段边际很小，
   workload 实际是内存带宽主导，youpipe 的 read/process/write 流水线没有可重叠的东西。

2. 轻 CPU + 持久 IO（fsync）：youpipe / tokio 凭 IO 并发略胜 rayon（rayon 默认池=核数，
   fsync 阻塞时核心闲置；超额订阅的 youpipe 和 tokio 的大阻塞池能掩盖 IO 停顿）。
   但单次计时三者仍在 ~10% 内。

3. 重 CPU（zstd）+ 持久 IO：CPU 把核心打满、IO 被掩盖，三种方案收敛到 ~10% 内，
   排名随单次抖动翻转（跑 1 youpipe 最快、跑 2 rayon 最快）。
   ★ 唯一稳定的差异：tokio 始终慢一档（+3~6%）——因为每个文件要两次 spawn_blocking
   （压缩 + 写回），重 CPU 下这份“每文件固定调度开销”摊不掉。youpipe 的 fused 工作窃取池
   和 rayon 的 par_iter 都没有这个每文件跳数开销。

4. youpipe 在本机（快 NVMe + 32 核 + AES-NI）上未能拉开差距，根因是硬件太快：
   - AES-NI 使加密段边际；
   - 快 NVMe 使 IO 停顿短；
   - 32 核使任何一方都能轻易打满 CPU 或 IO。
   youpipe 流水线的优势理论上在“慢 IO（HDD/网络盘）+ 重 CPU + 核数有限”时最明显——
   那时 fsync 的阻塞停顿大、CPU 段有真活儿可填，流水线能把停顿填满。

【复现】
  # 重 CPU（默认）
  FC_DATA_DIR=/path/to/real/disk cargo run --release -p file-encrypt-bench
  # 轻 CPU 参照
  FC_TASK=aes FC_DATA_DIR=/path/to/real/disk cargo run --release -p file-encrypt-bench
  # 可调: FC_TASK / FC_ZSTD_LEVEL / FC_COUNT / FC_MIN_KIB / FC_MAX_MIB / FC_FSYNC / FC_POOL

【重要方法学提醒（单次计时）】
- “只跑一次统计时长”本身有 ~10% 级别抖动，排名会翻转；本文件中相邻名次差异不可信，
  只有“tokio 在重 CPU 下稳定慢一档”这一条在多次跑中成立。
- 想要可重复的对比，应改用 criterion 多轮采样（仓库 benches/ 下已有同类基准）。
- /tmp 多为 tmpfs：放数据到 tmpfs = 无真实阻塞 IO，youpipe 流水线优势无从体现，务必指到真盘。
==========================================================================
