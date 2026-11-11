# URMA RPC「元数据查询」时延诊断：70µs → 13~20µs

> 诊断对象：`get_buffer` 内对 `mooncake_master` 的元数据查询 RPC（`GetReplicaList`，80B 请求）
> 现象：perf top 显示两个进程分别烧在 `libc 0x83e18(44.92%)` 与 `do_timerfd_settime(19.85%)`；目标 avg 13~20µs，实测 70µs
> 代码基线：Mooncake `supercache-snapshot-930` / HEAD `63b0caf4`（含未提交改动）；yalantinglibs = **alibaba main 分支**，`URMA_RPC_EVENT_MODE=0` 被显式设置
> 证据标注：**【事实】**=代码或 perf 直接可见；**【推断】**=由代码+perf 交叉推理；**【估算】**=量级推算，未实机验证

---

## 0. TL;DR（先看这五条）

1. **【事实】两端都不是 event 模式，而是 timer 兜底忙轮询**：每个 URMA socket 一个 `asio::steady_timer`，**5µs** 周期自循环（`urma_socket.hpp:449-485,708`）。一次 tick 至少 4 个 syscall（`timerfd_settime`×2、`epoll_pwait`、`urma_poll_jfc`），**≈3.3–3.6µs CPU / 5µs 周期 ≈ 72% 一个核，每个 socket**。master 进程的 perf 里 **55–66% 的采样**都落在 timerfd/epoll/syscall 入口这一组函数上。

2. **【事实】真正贵的是"用 timerfd 定时"这件事，不是 URMA 轮询本身**：`urma_poll_jfc` 走的是用户态 `udma_u_poll_jfc`（perf 里只占 0.48%/4.23%），而 `do_timerfd_settime` 单项就 19.85%。**把 5µs timer 换成用户态自旋，CPU 可以降一个数量级，时延不变甚至更好**——这是本次最大的可拿收益（详见 §5 B1）。

3. **【推断】70µs 不是"请求排队"，而是"每请求串行成本"**：1000 qps × 70µs = 单核 7% 利用率，按负载排队解释不通（【事实】handler 同步跑在连接所属 io_context 线程，单连接串行，`coro_rpc_server.hpp:125-138,677-682`）。70µs 的构成应是：**库基线 ~13µs + reactor 线程被 timer 轮询抢占造成的调度延迟（≈3.6µs × 该线程上的 socket 数）+ 应用层每请求固定税**。

4. **【事实】两端每请求都在打 INFO 日志，且 `MC_LOG` 宏即使日志级别关闭也会先格式化**（`mooncake_logging.h:63-67` + `mooncake_logging.cpp:363-389`）：客户端每请求 2~3 条（`query_rpc_breakdown` 是**无条件**的，`master_client.cpp:1374-1385`；`master_rpc_client_breakdown`/`get_breakdown` 受 `MC_HIFREQ_LOG_SAMPLE_RATE` 控制，而**代码默认值是 1.0 全采样**，`mooncake_logging.cpp:44-45`）；master 每请求 1 条并附带 `QueryIp` 的 segment 表拷贝（`rpc_service.cpp:317-329` + `master_service.cpp:6856-6889`）。这是纯开销，删掉不影响任何功能。

5. **【待定案】客户端 44.92% 的 `libc 0x83e18` 是一条命令就能定案的未知量**（`perf report -g` 看调用者 / `nm -D` 找前一个导出符号 / `readelf` 找 IFUNC）。三大候选：glibc malloc 家族（`_int_malloc`/`_int_free`，无 dynsym → 裸地址）、ifunc 选中的 `__memcpy_*` 实现、`sem_wait`/cond 内部。**在它定案前，任何"客户端瓶颈在 X"的结论都不该写进优化计划。**

---

## 1. 先校准基线：13.11µs 是在什么条件下测出来的

【事实】yalantinglibs 自己的 URMA 基准（`docs/proposals/urma_transport_proposal.zh.md:79-92`，程序 `src/coro_rpc/examples/urma_benchmark/urma_benchmark.cpp`，latency 模式**只建 1 个 client、单线程 ping-pong**）：

| payload | 传输 | avg | min | p50 | p90 | p99 | QPS |
|:--:|:--:|--:|--:|--:|--:|--:|--:|
| 64B | TCP | 62.27 | 48.69 | 59.81 | 72.32 | 78.10 | 166,455 |
| **64B** | **URMA** | **13.11** | 10.00 | 12.00 | **20.00** | **20.00** | **641,904** |
| 1024B | URMA | 14.22 | 11.00 | 12.00 | 20.00 | 21.00 | 657,047 |

**这张表说明两件事**：
- 你的目标区间 **13~20µs 恰好就是"单连接、无竞争、纯 RPC"的库里实测值**，所以"URMA 传输慢"不成立，缺的是 **70 − 13 ≈ 57µs 的部署税**。
- 该基线自己就有 **p50 12 / p90 20** 的分布：说明 URMA 完成路径每次唤醒/轮询本身就带 10~20µs 量级的抖动。**任何"再加一层 5µs 定时器 + 每请求日志"的实现都只会在它上面继续加**。

---

## 2. 两份 perf 的定量解读

### 2.1 master（B，2420471）：timerfd 风暴算术，反解自洽

以 `do_timerfd_settime = 19.85%` 反推：单次成本 0.99µs → 迭代频率 ≈ 200k/s，**正好等于一个 5µs 定时器**。用同一组参数核对全表：

| 符号 | 占比 | 反解单次成本 | 判读 |
|---|--:|--:|---|
| `do_timerfd_settime.part.0` | 19.85% | 0.99µs | hrtimer cancel+start，符合 aarch64 典型值 |
| `el0_slow_syscall` | 11.00% | 0.55µs/迭代 ≈0.14µs/syscall（4 syscall/迭代） | syscall 入口，符合 |
| `timerfd_settime`(libc) | 4.40% | 0.22µs | 纯 wrapper |
| `timerfd_poll` | 2.99% | 0.15µs | epoll 投递 timerfd |
| `ep_start_scan`+`ep_done_scan` | 6.07% | 0.30µs | 就绪链扫描 |
| `fput`+`__fget_files`+`epi_fget` | 5.96% | 0.30µs | 每 tick 的 fd 引用获取/释放 |
| `ktime_get_coarse_real_ts64` | 2.76% | 0.14µs | timerfd 内核取时间 |
| `start_polling()` 自耗 | 2.26% | 0.11µs | 5µs 兜底循环的宿主函数 |

**合计 ≈ 66% 采样、≈3.3–3.6µs/tick**，周期只有 5µs ⇒ **该循环把自己卡在 ~72% 一个核**。

```
单 socket CPU 占比 ≈ 3.5µs / 5µs            ≈ 72%
某 io_context 线程的有效周期 = max(5µs, 3.5µs × N_socket)
请求被调度的额外延迟       ≈ 3.5µs × N_socket        （N=该线程上的 URMA 连接数）
```

**这就是 70µs 的主要来源**：N=2 时线程已饱和；N=4~8 时单次请求要多等 14~28µs；再加上客户端侧的同类开销，正好落在 70µs 量级。

> 注意：这里说的是**同一条 io_context 线程上的 socket 数**，不是总连接数。连接是 round-robin 分配到 16 个 io_context 线程（`coro_rpc_server.hpp:313-316` + `io_context_pool.hpp:302-304`），所以要乘的是"每线程几个连接"。

### 2.2 客户端（A，2987711）：形态完全不同

- **【事实】45% 集中在一个未知 libc 符号**（`0x83e18`），第二个裸地址 `0x7c400` 只有 0.51%；同 DSO 的 `pthread_mutex_lock 5.82%`、`pthread_spin_lock 1.70%`。
- **【推断】这是"每请求线性"的用户态热点**，不是常数开销：`do_timerfd_settime 3.54%` + `timerfd_settime 0.74%` 只对应 ≈35k 次/s。若按 70µs/请求单线程推算 **14.3k rps**，则 35k ÷ 14.3k ≈ **2.4 次/RPC**，与 `coro_rpc_client` 的**每请求 30s 超时定时器**（arm + cancel，`coro_rpc_client.hpp:1280-1296,1630-1637,1948`）严丝合缝——**说明 A 的 timerfd 不是 5µs 兜底循环**。
- **【事实】A 有 `uburma_jfe_poll`(4.23%)，B 完全没有**：该符号是 jfce 字符设备 fd 的 `.poll` 回调，只在 fd 挂进 epoll（即 event 模式）时被调用。⇒ **A 的观测形态更像 event 模式，B 是 timer 兜底**。
  ⚠️ 这与"客户端日志也报 `timer-based busy poller (fallback)`"**冲突**。请在目标机用 §6-P0B 的 grep 计数复核；无论哪边对，都不影响 B（master）的结论。

### 2.3 一句话判据

| 形态 | 判读 |
|---|---|
| 一个非导出函数占 30–50%，其余是 `pthread_mutex_lock`/`spin_lock`/小裸地址 | **用户态热点**，CPU 随请求量线性涨 → 查分配/拷贝/等待 |
| `[kernel]` 合计 > 50%，呈 `do_*`/`ep_*`/`el0_*`/`fput`/`timerfd_*` 扇形分布 | **内核 syscall 风暴**，CPU 是常数项（空闲也烧）→ 查轮询/定时器 |

---

## 3. 根因排序

### R1【事实】master 与客户端都用 5µs timer 兜底轮询，这是显式配置导致的

- `URMA_RPC_EVENT_MODE=0` → `config.event_mode=false`（`urma_rpc_env.hpp:146-147`，默认 true）→ `start_completion_watch()` 走 else 分支，日志打印 `URMA starting timer-based busy poller (fallback)`（`urma_socket.hpp:606-619`）。
- 循环实现：`poll_once()` 无完成 → `start_polling()` → `expires_after(5µs)` + `async_wait`（`urma_socket.hpp:449-457,468-471`）。
- **注意这个配置是进程级缓存**（`urma_rpc_env.hpp:203-207` 的 `static const`），改环境变量必须重启进程。

### R2【事实】每连接一份 state = 每连接一个 jfce/jfc/jfr/jetty + 一个 5µs timer

`urma_socket_t` 构造时 `make_shared<urma_socket_shared_state_t>`（`urma_socket.hpp:1030-1037`），服务端每个 accept 连接升级一次（`coro_rpc_server.hpp:719` → `update_to_urma`）。客户端连接池默认 `max_connection=100`、`idle_timeout=30s`（`client_pool.hpp:396-402`），Mooncake 又用 `MC_YLT_RPC_POOL_MAX_CONNECTION` 覆盖（`master_client.h:73-112`）。

⇒ **开销与连接数线性相关**，而 R1 决定了每个连接 ~72% 一个核。

### R3【待定案】客户端 44.92% 的 libc 热点

为什么 perf 只显示裸地址（**可确定**）：发行版 libc 只保留 `.dynsym`，strip 掉 `.symtab`；采样落在没有任何导出符号的本地函数体内时就打印偏移。**反证**：同 DSO 的 `pthread_mutex_lock`/`epoll_pwait`/`timerfd_settime` 都解析成功，说明符号表读到了，只是这个地址没有导出符号。

候选排序（详见 §6-P0C 的判读表）：

| 序 | 候选 | 支持 | 反对 |
|---|---|---|---|
| 1 | glibc malloc 家族（`_int_malloc`/`_int_free`/`malloc_consolidate`，均为 local 符号） | 同 DSO 的 mutex/spin_lock 占比与之自洽；profile 里**没有 futex**，说明锁走快路径而非阻塞；第二个裸地址 `0x7c400` 位置自洽（`_int_free` 在 `_int_malloc` 之前） | 45% : 0.5% 的 malloc/free 比例偏大 |
| 2 | ifunc 选中的 `__memcpy_aarch64_simd` / `__memset_*` / `__strlen_*`（导出名 `memcpy` 等是 IFUNC，真实现是 local 符号） | 单函数 45% + 一个很小的第二个裸地址，很像"拷贝 vs 清零"的不对称；`store.get()` 走 `pybind11::bytes((char*)ptr, size)` **每请求一次 value 全量 memcpy**（`mooncake-integration/store/store_py.cpp:519-520`） | 若压测用的是 `get_into` 则无此拷贝 |
| 3 | `sem_wait`/cond 内部（`syncAwait` 用 `std::binary_semaphore`，`async_simple/util/Condition.h:36-40`；`master_client.cpp:425`） | 每请求一次线程 handoff | 阻塞等待不烧 CPU（除非底层自旋），且 perf 无 futex |

### R4【事实】每请求 INFO 日志 + `MC_LOG` 宏"先格式化后判级别"

- `MC_LOG(severity)` 展开成 `AsyncLogMessage(...).stream()`，其中 `stream_` 是 `std::ostringstream` 成员（`mooncake_logging.h:63-67`、`mooncake_logging.cpp:363-369`）。**构造与 `<<` 链的格式化与 `enabled_` 无关**，只有入队被跳过（`mooncake_logging.cpp:371-386`）。⇒ 即使 `MC_LOG_LEVEL=ERROR`，热路径上一串 `<<` 的 ostringstream 格式化照付。
- 客户端：`MasterClient::GetReplicaList` **无条件**一条 `query_rpc_breakdown`（`master_client.cpp:1374-1385`，约 10 个字段），`invoke_rpc` 再来一条 `master_rpc_client_breakdown`（`:501-512`），`real_client` 的 `get_breakdown` 一条（`real_client.cpp:3228-3266`）。后两条受采样控制，但**代码默认采样率是 1.0（全采样）**（`mooncake_logging.cpp:44-52`；注意头文件注释写 0.1，`mooncake_logging.h:17-19`，**代码与注释不一致**）。
- master：`WrappedMasterService::GetReplicaList` 每请求一条 INFO，且字段里有 `ResolveClientHost(...)`（`rpc_service.cpp:317-329`）→ `QueryIp` 会 **加 segment 管理器锁、拷贝 `std::vector<Segment>`、构造 `unordered_set<string>`、拼字符串**（`master_service.cpp:6856-6889`）。

### R5【事实/估算】客户端每请求固定开销（与 RPC 无关的部分）

| 项 | 证据 | 量级【估算】 |
|---|---|---|
| `GetLocalEndpoints()`：全局 `mounted_segments_mutex_` + 复制全部 segment endpoint 到新 `unordered_set<string>` | `real_client.cpp:3189`、`client_service.h:765-772` | 1–3µs |
| 3~4 条采样日志（见 R4） | 同上 | 2–6µs |
| `make_shared<BufferHandle>` + slab 分配 | `real_client.cpp:3217,3225` | 0.2–0.5µs |
| 3 次 `steady_clock` + 2 次 metrics 原子 + PerfKey 打点 | `real_client.cpp:3149-3154` | 0.2–0.5µs |
| `switch_us` 路由（单 master 时直接返回） | `master_client.cpp:993-1006` | ≈0 |
| `pybind11::bytes` 全量 value 拷贝（仅 `store.get()` API） | `store_py.cpp:519-520` | 与 value 大小成正比 |

### R6【事实】若切回 event 模式，会踩到一个"注释与实现相反"的坑

`event_loop` 注释说"有 pending 回调时跳过自旋直接 rearm+sleep"（`urma_socket.hpp:569-575`），但实现是 `pending_budget = has_pending ? 64 : busy_poll_budget_`（`:576-578`）。ping-pong 场景 `recv_callback_` 常驻非空 ⇒ **每次事件后固定打满 64 次空 `urma_poll_jfc`**。这是 event 模式下每 RPC 的固定 CPU 税，也是**你之前把 EVENT_MODE 关掉的可能原因之一**。

---

## 4. 为什么"直接打开 event 模式"不是答案

【事实】你自己的设计文档已经记录了两条关键事实（`.worktrees/urma_poll_threadpool/docs/superpowers/specs/2026-07-25-urma-poll-threadpool-design.md:7-24`）：
- **per-socket JFCE 事件唤醒延迟 50–300µs** —— 与 70µs 现象同一量级；
- 早先 `wait_urma_write_completion` 里 **2000 次 `urma_poll_jfc` 内联自旋** 是主要 CPU 消耗。

再加上 R6 的 64 次空轮询：**event 模式把"每 5µs 的 syscall 税"换成了"每次唤醒 50–300µs 的延迟 + 64 次空轮询"**。所以正确方向不是"退回事件模式"，而是：

> **保留轮询（延迟必需），但把轮询做得便宜且可共享**——用用户态自旋替代 timerfd+epoll，用共享 JFC/固定 poll 线程替代"每连接一个 timer"。

---

## 5. 修复方案（按"收益/风险/落地成本"排序）

### 档 A：零代码，今天就能验证（用于**消因定位**，不全是最终解）

| 编号 | 改动 | 预期 | 风险/说明 |
|---|---|---|---|
| A1 | `MC_HIFREQ_LOG_SAMPLE_RATE=0` | 去掉客户端 `master_rpc_client_breakdown` + `get_breakdown`、master `GetReplicaList` INFO（含 `QueryIp`）。**预期每请求省几 µs，且无任何延迟风险** | `query_rpc_breakdown` 是**无条件**的，采样关不掉，仍需代码改（见 B5） |
| A2 | `MC_RPC_TIMEOUT_MS=-1` | 去掉每请求 30s 超时定时器（客户端 timerfd 归零），省 1 个 timer 对象 + 1 个 asio op + arm/cancel | 负数禁用是代码支持的行为（`master_client.h:96-101`）；超时保护消失，压测环境可接受 |
| A3 | `MC_YLT_RPC_POOL_MAX_CONNECTION=4~8`（客户端）；服务端确认没有多条连接 | 直接降 N_socket/线程：**每减一个连接 ≈ 释放 72% 一个核** | 并发受限；配合 A2/线程数一起调 |
| A4 | 确认构建层 `MOONCAKE_ENABLE_SPDIAG` | 若为 Layer1（ON），master 每请求 7 个 PerfPoint + 客户端 PerfKey 全生效，实测前必须固定构建层做 A/B | `execute_rpc` 里即使 Layer0 也有 1 次 `make_unique<PerfPoint>`（`rpc_helper.h:63-68`） |
| A5 | `URMA_RPC_POLL_INTERVAL` **调大**（如 50~200） | ⚠️ **不是修复**：它会把"完成发现延迟"直接抬到同量级，70µs 只会更差 | 仅作为**诊断**用：若调大后 master 的 `do_timerfd_settime` 掉到 <2% 而时延没改善，说明 B 的 CPU 不是延迟主因；若时延同步变差，则证明 5µs 轮询就是完成路径 |

### 档 B：代码级小改（yalantinglibs，1~2 天，收益最大）

| 编号 | 改动 | 位置 | 预期收益 | 风险 |
|---|---|---|---|---|
| **B1** | **兜底路径去掉 asio timer，改"有界用户态自旋 + 退让"**：`poll_once()` 连续 0 完成时**内联自旋** `poll_completion()` N 次（如 64~256，每次 ~0.1–0.3µs 的用户态 `udma_u_poll_jfc`），仍未完成才 `asio::post` 退让；**只有真正空闲若干轮后才 arm 一次定时器做长眠** | `urma_socket.hpp:459-485` | 单 socket 从 ~3.5µs/5µs（**72% 核 + 4 syscall/tick**）降到 ~0.3µs/5µs（**~6% 核 + 0 syscall**），**约 10× 降 CPU，时延不变或更好** | 中：需调自旋预算；无 pending 时长自旋会烧核，退让/退避逻辑要正确 |
| **B2** | event 模式下 `pending_budget` 64 → 0~2（让实现与注释一致） | `urma_socket.hpp:576-599` | 若切回 event 模式，每 RPC 省最多 64 次空 poll | 低；深 pipeline 下吞吐可能小降 |
| **B3** | 每请求 30s 超时定时器改懒创建 / 默认禁用 | `coro_rpc_client.hpp:285,1630-1637,1948` | 每 RPC 省 1 timer + 1 asio op + 1 协程帧 | 低（可用 `call_for` 显式超时） |
| ~~B4~~ | ~~共享 JFC + 固定 poll 线程池~~ **【已实测否决，勿重复】用户实测该分支性能很差** | `.worktrees/urma_poll_threadpool` | — | 见 §10：poll 线程与协程线程分离 ⇒ 每次完成跨核 resume，对 13µs 目标是结构性伤害 |
| **B5** | 客户端去掉每请求固定开销：`GetLocalEndpoints` 结果按版本号缓存、`BufferHandle`/`Slice` 复用、`query_rpc_breakdown` 改为采样/降级 | `real_client.cpp:3189`、`master_client.cpp:1374-1385`、`client_service.h:765-772` | 每 RPC 省 2–5µs | 低 |

### 档 C：Mooncake store 侧（与传输无关，但计入 70µs）

| 编号 | 改动 | 位置 | 说明 |
|---|---|---|---|
| C1 | master 的 `GetReplicaList` INFO 日志与 `QueryIp` 采样/删除 | `rpc_service.cpp:317-329` | 每请求一次 segment 表拷贝 + ostringstream，纯开销 |
| C2 | `GetReplicaListLocal` 的 `alive_clients` 快路径：无 disk 副本时不必拷贝全量 client 集合 | `master_service.cpp:7476-7490` | 每请求 N 个节点分配 + 哈希 |
| C3 | `Replica::Descriptor` 在 `IsReplicaReadable` + visit 里各拷贝一次（各含 2 个 string） | `master_service.cpp:7138-7159`、`7492-7500` | 单副本时 2 次；改 `const&` 或先判可读再取描述符 |
| C4 | 若压测用 `store.get()`：改 `get_into()` 避免 `pybind11::bytes` 的全量 memcpy | `store_py.cpp:519-520` | **这条直接决定客户端 44.92% 是否就是 value 拷贝** |
| C5 | 删/降级 master 侧 `make_unique<ScopedTraceId>`、`make_unique<PerfPoint>` | `rpc_service.cpp:292-296`、`rpc_helper.h:63-68` | 每请求 2 次堆分配，可栈上化 |

> **建议落地顺序（2026-11-05 修订，见 §10）**：A1+A2+A3 → §6 的 P0 取证 → **N1（JFC 事件合并实验，可能一步解决）** → 定案 `0x83e18` → B1/B2（若仍要轮询）→ 最后才考虑"合并 poll 线程与 io_context 线程"的线程池变体。
> **已否决**：poll 线程与协程线程分离的线程池（B4，实测很差）。

---

## 6. 实机取证清单（按"一句话定案"排序，可直接复制）

> 前提：root、`sysctl -w kernel.perf_event_paranoid=1`；先把 `A=2987711`（客户端）、`B=2420471`（master）记下来；**每次只改一个变量**。

### P0-A ⭐ 先看你已经有的分段日志（不需要额外工具）

客户端每请求已经打印了 `query_rpc_breakdown`（无条件），默认采样下还有 `master_rpc_client_breakdown` / `get_breakdown`：

```bash
grep -o "query_rpc_breakdown.*" client*.log | tail -20
grep -o "master_rpc_client_breakdown.*" client*.log | tail -10
grep -o "get_breakdown.*" client*.log | tail -10
```

**判读**（这一步就能把 70µs 拆成三块）：

| 观测 | 结论 | 下一步 |
|---|---|---|
| `total_us` ≈ 70，且 `rpc_first_us` ≈ 60~65 | 时间在 **RPC 往返**（传输 + master 调度） | 做 P0-B/P0-D/P1 |
| `switch_us` 占大头 | 在 **路由/建连**（`SwitchToSubmaster`） | 查 submaster 路由表与连接池 |
| `get_breakdown` 里 `read_us`/`wait_us` 占大头 | 在**数据面传输**，不是元数据 RPC | 转查 TE/URMA 数据路径 |
| `client_overhead_us` 占大头 | 在客户端**本地代码**（日志/分配/pybind） | 做 P0-C + A1 |

### P0-B ⭐ 90 秒确认"到底谁在 timer 兜底、有几个 socket"

```bash
# 模式判定（日志字符串来自 urma_socket.hpp:608/616）
for f in <master日志> <client日志>; do echo "== $f"; \
  grep -c "URMA starting event-driven completion loop" "$f"; \
  grep -c "URMA starting timer-based busy poller (fallback)" "$f"; \
  grep -c "urma_create_jfce failed" "$f"; \
  grep -c "fcntl(F_GETFL\|fcntl(F_SETFL\|create stream_descriptor" "$f"; done

# 每个 socket 一个 uburma fd → 近似 socket 数
ls -l /proc/$B/fd | grep -c uburma ; ls -l /proc/$A/fd | grep -c uburma
ulimit -n ; cat /proc/sys/fs/file-max
```

**判读**：`timer-based` 计数 > 0 且 `uburma` fd ≥ 1 → 确认兜底；若同时出现 `urma_create_jfce failed`，说明即使打开 event 模式也起不来（要先修驱动/fd 上限）。

### P0-C ⭐⭐ 定案客户端 44.92%（**不需要装任何包**）

```bash
LIBC=$(readlink -f "$(ldd /bin/ls | awk '/libc\.so\.6/{print $3}')"); echo "$LIBC"

# 1) 前一个"导出"符号 → 给出家族
nm -D -n --defined-only "$LIBC" | awk -v t=0000000000083e18 '
  { s=$1; while(length(s)<16) s="0" s;
    if (s<=t) p=$0; else { print "PREV: " p; print "NEXT: " $0; exit } }'

# 2) 指令指纹（stripped 也有 .text）
objdump -d --start-address=0x83d80 --stop-address=0x83f00 "$LIBC" | head -70
objdump -d --start-address=0x7c300 --stop-address=0x7c500 "$LIBC" | head -60

# 3) 确认哪些导出是 IFUNC（memcpy/memset 大概率是）
readelf -sW "$LIBC" | grep -E 'IFUNC' | head -20

# 4) 最强一招：看调用者（call graph 与符号解析无关）
perf record -F 999 -p $A -g --call-graph dwarf,16384 -o /tmp/perfa.data -- sleep 20
perf report -i /tmp/perfa.data --stdio --no-children --percent-limit 1 | head -40
perf report -i /tmp/perfa.data --stdio -g graph --percent-limit 0.3 | grep -n -B4 -A18 '83e18' | head -90
```

**判读表**：

| `PREV` / 调用者 | 结论 | 对应改法 |
|---|---|---|
| `malloc`/`free`/`realloc` 或调用者是 `operator new`/`make_shared`/`struct_pack` | 分配器热点 | C5/B5 + 对象池；可先试 `MALLOC_ARENA_MAX=2` |
| `memcpy`/`memmove`/`memset`（且 `readelf` 显示 IFUNC），调用者是 pybind / `bytes` / TransferEngine | 拷贝热点 | **C4（`get_into`）**；确认压测 value 大小与 API |
| `sem_wait`/cond 内部、调用者是 `syncAwait` | 线程 handoff | 客户端改单线程 event loop 或降低 handoff 次数 |
| 无法解析但调用者是 `asio::detail::*`/`start_polling` | timer 自循环 | B1 |

> 若允许装包：`apt-get install libc6-dbg` 或 `dnf install glibc-debuginfo` 后 `eu-addr2line -f -C -e "$LIBC" 0x83e18`，一步到位。

### P0-D ⭐ 空闲基线实验（5 分钟，区分"常数开销"与"每请求开销"）

```bash
# 让压测发 0 请求但进程/连接保持存活，然后：
perf top -p $B --stdio 2>/dev/null | head -25
perf top -p $A --stdio 2>/dev/null | head -25
```

**判读**：B 空闲时 `do_timerfd_settime` 仍 ≈20% → R1 确认（与业务无关的常数税）；A 空闲时 45% 那个符号消失 → A 是每请求热点（R3/R4/R5）。

### P0-E ⭐ bpftrace 把算术变成实测（可选但判决力最强）

```bash
# B 的真实 timer 循环频率
bpftrace -e 'tracepoint:syscalls:sys_enter_timerfd_settime /pid=='$B'/ { @[tid]=count(); } interval:s:1 { print(@); clear(@); }'
# A 的 ioctl 频率 ÷ RPC/s = 每次 RPC 轮询几次
bpftrace -e 'tracepoint:syscalls:sys_enter_ioctl /pid=='$A'/ { @[tid]=count(); } interval:s:1 { print(@); clear(@); }'
# 确认 ioctl 是 URMA poll 而非别的
bpftrace -e 'kprobe:uburma_jfe_poll /pid=='$A'/ { @[kstack]=count(); } interval:s:5 { print(@); exit(); }'
```

**判读**：`timerfd_settime/s ≈ 200k × N_socket`；`epoll_pwait/s ≫ timerfd_settime/s`（>3×）说明 asio 退化成非阻塞轮询（再多烧一个核）；`ioctl/s ÷ RPC/s ≈ 60~70` 则命中 R6 的 64 次空轮询。

### P1 线程/调度：70µs 里有多少是"等 CPU"

```bash
ps -TH -p $B -o pid,tid,comm,psr,pcpu,stat --sort=-pcpu | head -15
mpstat -P ALL 1 10                 # 单核 100%、其余空闲 = 自旋签名
pidstat -w -t -p $B 1 10           # cswch/s ≈ 200k → 每 tick 一次睡醒
perf sched timehist -p $B --duration 5 2>/dev/null | tail -30   # 唤醒延迟分布
cat /proc/$B/status | grep -E "voluntary|nonvoluntary|Threads"
```

**判读**：`mpstat` 单核 100% + `perf sched timehist` 唤醒延迟中位数接近 70µs ⇒ **70µs 主要是调度延迟**，直接指向 R1/R2。

### P2 消因 A/B（每次一个变量，跑完重做 P0-A/P0-D）

```bash
MC_HIFREQ_LOG_SAMPLE_RATE=0 <bench>            # A1：日志税
MC_RPC_TIMEOUT_MS=-1 <bench>                   # A2：每请求定时器
MC_YLT_RPC_POOL_MAX_CONNECTION=4 <bench>       # A3：连接数
URMA_RPC_EVENT_MODE=1 URMA_RPC_BUSY_POLL_BUDGET=2 ./mooncake_master   # 对照组：event 模式（预期唤醒延迟变差）
```

**验收标准**：`do_timerfd_settime` 从 19.85% → <2%、`mpstat` 不再出现单核 100%，且 `query_rpc_breakdown.rpc_first_us` 下降 → 说明命中 R1。

---

## 7. 判读决策树

| # | 观测 | 结论 | 改法 |
|---|---|---|---|
| 1 | 空闲时 B 仍 `do_timerfd_settime ≥15%`、`timerfd_settime/s ≈200k×N` | 5µs 兜底空转 | **B1**（用户态自旋替代 timer）/ B4 |
| 2 | 空闲时 timerfd ≈0，忙时随 QPS 线性涨 | 每请求超时定时器 | A2 / B3 |
| 3 | `query_rpc_breakdown`：`rpc_first_us` 占 85%+ | 往返被对端 CPU 抢占 | 先 B1，再 P1 分核 |
| 4 | `get_breakdown`：`read_us`/`wait_us` 占大头 | 数据面而非元数据 | 转 TE/URMA 数据路径 |
| 5 | `0x83e18` 调用者是 new/malloc | 分配器 | C4/C5/B5；`MALLOC_ARENA_MAX` |
| 6 | `0x83e18` 调用者是 memcpy + `store.get()` | pybind 全量拷贝 | **改 `get_into`** |
| 7 | `mpstat` 单核 100%，`perf sched` 唤醒延迟 ≈70µs | 调度延迟 | B1/B4 + `taskset`/`isolcpus` |
| 8 | `uburma` fd 数与连接数相等且 >4 | R2 放大 | A3 + B4 |
| 9 | 日志里 `urma_create_jfce failed` | 驱动不支持 JFCE，event 模式不可用 | 先修 fd/驱动，再谈 B2 |

---

## 8. 待你确认/补充（会改变结论权重）

1. **70µs 的准确来源**：是 `query_rpc_breakdown.total_us`、`get_breakdown`，还是外部脚本端到端？仓库里没有"单 key + 80B + 出 avg 延迟"的现成压测器（`master_bench` 只出 QPS，`batch_get_replica_bench` 走 batch，`store_kv_bench.py`/`stress_cluster_bench` 测整条链路），所以那次测试大概率是自写脚本或日志统计——**贴一行日志比什么都快**。
2. **客户端到底是不是 timer 兜底**：perf 算术（35k timerfd/s、有 `uburma_jfe_poll`）指向 event 模式，与你的答复冲突；用 P0-B 的 grep 计数 + P0-E 的 bpftrace 定案。
3. **压测 API 与 value 大小**：`store.get()`（`pybind11::bytes` 全量拷贝）还是 `get_into`/`batch_get_into`？value 是 80B/4KB/4MB？这直接决定 `0x83e18` 是不是 memcpy。
4. **构建层**：`MOONCAKE_ENABLE_SPDIAG=ON/OFF`？`MC_HIFREQ_LOG_SAMPLE_RATE` 实际值（代码默认 1.0，文档写 0.1）？
5. **连接数**：客户端线程数、`MC_YLT_RPC_POOL_MAX_CONNECTION`、master `--rpc_thread_num`（默认 `min(16, cores)`）。
6. **代码来源**：实机二进制是工作区构建还是 `63b0caf4` 构建？（工作区已把单 key `RecordAccess` 彻底移除并新增 `--io_pattern_enabled` 开关，`master_service.cpp:7569-7576`、`master.cpp:156`；`docs/analyze/io_pattern_latency_review.md` 的部分结论已过期。）
7. **`URMA_RPC_ENABLE` 是否为 1**：`docs/yh/deployment.md:376,424` 的 HA 脚本里是 `0`（元数据 RPC 走 TCP）；但你的 master perf 出现 URMA poll 符号 + 日志是 timer 兜底，说明本次确实是 1。请确认没有"部分连接升级失败退回 TCP"（日志 `URMA init urma connection failed`，`coro_rpc_server.hpp:723`）。

---

## 9. 证据索引（关键 file:line）

**yalantinglibs（alibaba main）**
- 5µs 兜底循环：`include/ylt/coro_io/urma/urma_socket.hpp:449-485,606-619,708`
- 每 socket 一份 state：`urma_socket.hpp:105-135,1030-1037`；jfce 创建/失败回退：`:206-217`
- event 模式 64 次空轮询（注释与实现矛盾）：`urma_socket.hpp:569-599`
- 环境变量与进程级缓存：`include/ylt/coro_io/urma/urma_rpc_env.hpp:146-154,203-207`
- 服务端从 env 自动启用 URMA：`include/ylt/coro_rpc/impl/coro_rpc_server.hpp:225-249,310-312,711-728`
- 客户端自动启用 + 每请求超时定时器：`include/ylt/coro_rpc/impl/coro_rpc_client.hpp:642-647,1280-1296,1630-1637,1948`
- 连接池默认值：`include/ylt/coro_io/client_pool.hpp:395-402`
- asio timerfd 实现：`include/ylt/thirdparty/asio/detail/impl/epoll_reactor.ipp:46,500-524,604-618`
- 性能基线（13.11µs / p99 20µs / QPS 641,904）：`docs/proposals/urma_transport_proposal.zh.md:60,77-92`
- poll 线程池分支设计（JFCE 唤醒 50–300µs、2000 次自旋）：`.worktrees/urma_poll_threadpool/docs/superpowers/specs/2026-07-25-urma-poll-threadpool-design.md:7-24`

**Mooncake（supercache-snapshot-930）**
- master handler 每请求 INFO + `QueryIp`：`mooncake-store/src/rpc_service.cpp:287-331`；`src/master_service.cpp:6856-6889`
- `execute_rpc` 每请求 2 次时钟 + `make_unique<PerfPoint>`：`mooncake-store/include/rpc_helper.h:57-98`
- 单 key 查询主体（锁/副本拷贝/租约）：`mooncake-store/src/master_service.cpp:7428-7580`（`alive_clients` 复制 `:7476-7490`，`GrantLeaseForGroup :6144-6185`，`EnrichVSegmentLayouts :7582-7602`）
- 客户端每请求日志与 `syncAwait`：`mooncake-store/src/master_client.cpp:403-515,993-1027,1309-1388`
- 客户端 get_buffer 固定开销：`mooncake-store/src/real_client.cpp:3135-3329`
- pybind `bytes` 全量拷贝：`mooncake-integration/store/store_py.cpp:478-520`
- `MC_LOG` 宏"先格式化后判级别"：`mooncake-common/include/mooncake_logging.h:63-67`；`mooncake-common/src/mooncake_logging.cpp:44-52,342-347,363-389`
- 服务端线程模型（N 个 io_context、handler 内联）：`mooncake-store/src/master.cpp:1938-1946`；`yalantinglibs/include/ylt/coro_rpc/impl/coro_rpc_server.hpp:125-138,677-682`
- io_pattern 现状（单 key 已移除 `RecordAccess`、已有总开关）：`mooncake-store/src/master_service.cpp:7569-7576,811-824`；`src/master.cpp:156`

---

## 10. 方案修订（2026-11-05）：poll 线程池已实测否决，改走"事件合并 / 同线程自旋"

> 结论先行：**poll 线程池分支（`.worktrees/urma_poll_threadpool`）不要再用**。它的设计目标（降空闲 CPU、连接数扩展）与"单次 RPC 13~20µs"在架构上冲突。替代路线见 §10.2 / §10.3。

### 10.1 为什么那个分支必然慢（机制，不是猜）

设计文档自己写清了关键一步（`.../specs/2026-07-25-urma-poll-threadpool-design.md:22`）：

> "Send coroutines drop the 2000-iter spin to a short local check of the handoff state, then **suspend and are resumed by the poll thread**."

也就是**完成处理从"socket 自己的 io_context 线程"搬到了"poll 线程"**，于是每个 completion 都是一次跨核 handoff：

1. 快路径不再是"poll 自己的 CQ"，而是 **32 次检查原子状态 + 可能加锁的 fallback deque**（spec:245-257；实现 `urma_completion_handoff.hpp:98-104,130-138,160-172`）。原来 2000 次自旋虽然费 CPU，但**检测延迟是 ns 级且不跨核**。
2. 一轮 RPC 至少 4 个 completion（client send / server recv / server send / client recv），每个 completion 都要 poll 线程 `resume()` 到另一个线程（`try_deliver` → `take_resume_handle` → 调用 resume）。ARM 上跨核唤醒 + 缓存行迁移常见 **5–20µs/次** ⇒ **+20–80µs**，正好把 13µs 变成"很差"。
3. 被 resume 的协程在 poll 线程上继续跑，后续若 `post` 回 io_context **又是一跳**；线程亲和、缓存全部丢失。
4. 空闲期 `BUSY_POLL_BUDGET=16` 之后才 `wait_jfc`，而默认 `POLL_THREADS=4` ⇒ **4 个线程在忙轮询**；有流量时 4 个 poll 线程持续 100%。
5. 单 group 内 dispatch 是串行的（spec:153），M=4 个 group 时高并发下还会成为新的串行点。

**可复用的结论**：`urma_poll_jfc` 本身便宜（用户态 `udma_u_poll_jfc`，perf 里 0.48%），**贵的是"把检测权交给别的线程"**。任何架构只要在 completion 路径上引入一次跨线程 resume，就不可能做到 13–20µs。

### 10.2 N1（新首选，先做这个）：给 JFC 关掉事件合并（moderation）

**证据链（全部可查）**：
- SDK 提供 JFC 事件合并属性：`urma_jfc_attr_t { uint32_t mask; uint16_t moderate_count; uint16_t moderate_period; /* µs */ }`，mask 位 `JFC_MODERATE_COUNT` / `JFC_MODERATE_PERIOD`（`include/ylt/urma/urma_types.h:432-441`；命令侧 `urma_cmd_modify_jfc` 同名结构 `urma_cmd.h:578-586`）。
- API 存在：`urma_status_t urma_modify_jfc(urma_jfc_t *jfc, urma_jfc_attr_t *attr)`（`include/ylt/urma/urma_api.h:128`）。
- **整个 yalantinglibs 从未调用它**（grep `modify_jfc|JFC_MODERATE|moderate_count` 在 `*.hpp` 零命中）⇒ 所有 JFC 跑在**驱动默认合并参数**下。
- 而你们实测"per-socket JFCE 事件唤醒延迟 **50–300µs**"（spec:11），`moderate_period` 的单位正是**微秒**——量级吻合。

**假设**：event 模式的 50–300µs 不是 URMA 硬件慢，而是**默认 CQ 事件合并**（攒够 `moderate_count` 个 CQE 或等满 `moderate_period` 才发事件）。把合并关掉：

```cpp
// urma_socket_shared_state_t::init() 里创建 jfc_ 之后（当前只设了 depth + jfce）
urma_jfc_attr_t attr{};
attr.mask = JFC_MODERATE_COUNT | JFC_MODERATE_PERIOD;
attr.moderate_count  = 1;   // 每个 CQE 都可触发
attr.moderate_period = 0;   // 不按时间攒批
if (urma_modify_jfc(jfc_.get(), &attr) != URMA_SUCCESS) { /* 降级：保持默认 */ }
```

配套运行参数：`URMA_RPC_EVENT_MODE=1 URMA_RPC_POLL_THREADS=0 URMA_RPC_BUSY_POLL_BUDGET=2`（走 legacy `event_loop`，并把 R6 的 64 次空轮询降到 2）。

**若成立，这是唯一同时满足低延迟与低 CPU 的路径**：每事件 1 次 `epoll_wait` + `rearm/ack`，空闲 CPU≈0，无 5µs 风暴、无自旋、无跨线程。

**判读**：event 模式 RTT 从 50–300µs 掉到 <20µs；`perf top` 里 `uburma_jfe_poll` 占比上升但**每 RPC syscall 数是常数**（不再有 20 万次/s 的 timerfd）；`mpstat` 无单核 100%。`urma_modify_jfc` 返回非 0 说明该固件/驱动不支持 → 转 §10.3。

### 10.3 N2（N1 不成立时）：同线程内联自旋，不引入任何跨线程

- **客户端（收益最大）**：`async_receive`/等响应时，在**当前 io_context 线程**里内联 spin `urma_poll_jfc`（有界 20–200µs），spin 期间用同线程的一个 bool 表示"CQ 由我独占"，让 timer/event 轮询让路（**同一个线程，无需原子/无锁/无 handoff**）。这就是官方 benchmark 拿 13.11µs 的做法。
  - 代价可算：客户端同步 one-outstanding，1000 qps × 70µs 等待 ≈ **7% 一个核**——非常便宜。
  - `urma_poll_jfc` 是用户态（`udma_u_poll_jfc` 在 perf 里 0.48%），单次 ns 级，自旋不产生 syscall。
  - 风险：必须保证 spin 期间无人并发 poll 同一 jfc（同线程 bool 即可）；超时回退原路径。
- **服务端**：不能无限自旋（会饿死同线程的 handler）。用 §5-B1 的"有界 post 链自旋 + 长间隔 timer 兜底"，并把 `--rpc_thread_num` 从 16 降到 **2–4**，把忙轮询核数控制在可接受范围（每线程 1 核 = 已知代价，但延迟 <1µs）。
- **不要**再把 poll 线程与 io_context 线程分开（§10.1）。

### 10.4 如果将来还要做线程池，唯一正确形态

**poll 线程 == io_context 线程**：每个 reactor 线程持有自己的 jfc group，`dispatch` 在本线程内直接 resume（零 handoff），该线程同时跑 `io_context` 的 handler 与 jfc 轮询（等价于 §10.3 的服务端形态）。这样"线程池"退化为"每核一个 poll+reactor 线程"，代价是每线程一个忙核，收益是无跨核、无 timerfd、无跨线程队列。`URMA_RPC_POLL_THREADS` 应理解为"reactor 线程数"而不是"额外 poll 线程数"。

### 10.5 三组对照实验（每组只改一个变量，各测延迟+CPU+syscall）

| 组 | 配置 | 预期延迟 | 预期 CPU | 判读 |
|---|---|---|---|---|
| G0 现状 | `EVENT_MODE=0`（5µs timer） | 70µs | master 单核 ~72%/socket，55–66% 采样在 timerfd/epoll | 基线 |
| G1 事件+关合并 | `EVENT_MODE=1 POLL_THREADS=0 BUSY_POLL_BUDGET=2` + `urma_modify_jfc(1,0)` | 若 <20µs → **命中 N1** | 空闲≈0 | 首选；成功则收工 |
| G2 同线程自旋 | 客户内联 spin（§10.3）+ 服务端 B1 + `rpc_thread_num=4` | 接近 13µs 基线 | 每个 reactor 线程 1 核 | G1 失败时的兜底 |

测量口径：`query_rpc_breakdown.total_us/rpc_first_us`（客户端已有）、`perf top -p`、`bpftrace timerfd_settime/s`、`mpstat`。**每组至少重复 3 次取中位数**，且固定 `MC_HIFREQ_LOG_SAMPLE_RATE=0`（否则 logs 税会淹没差异）。

### 10.6 仍需你补充的一条信息

线程池分支当时"很差"的**具体数字**（延迟从多少涨到多少？CPU 是更高还是更低？QPS 掉多少？）——用来确认 §10.1 的归因（跨核 handoff）而不是别的原因（比如 group 串行或 CQ 深度不足）。如果当时只是"CPU 高但延迟还行"，那结论会偏向"group 数/预算调参"，而不是彻底否决。

---

## 11. event_loop 里那个 64（`urma_socket.hpp:576-600`）到底有什么问题

代码（`include/ylt/coro_io/urma/urma_socket.hpp:563-600`）：

```cpp
int ev_cnt = urma_wait_jfc(jfce_.get(), 1, 0, &ev_jfc);   // 超时0 = 非阻塞
if (ev_cnt > 0 && ev_jfc) { urma_ack_jfc(&ev_jfc, &ack_cnt, 1); }
// 注释说：有 pending 回调 → 跳过自旋直接 rearm+sleep；无 pending → 自旋 busy_poll_budget_
bool has_pending = !send_callbacks_.empty() || recv_callback_;
std::size_t idle_spins = 0;
std::size_t pending_budget = has_pending ? 64 : busy_poll_budget_;   // ← 这里
while (!has_close_) {
  auto [poll_ec, n] = poll_completion();
  if (poll_ec) { fail_pending(poll_ec); close(); goto loop_end; }
  if (n == 0) { if (++idle_spins >= pending_budget) break; continue; }
  idle_spins = 0;
  bool now_pending = !send_callbacks_.empty() || recv_callback_;
  if (now_pending && !has_pending) { has_pending = true; pending_budget = 4; }
}
```

### 11.1 九个问题（按影响排序）

| # | 问题 | 证据 | 影响 |
|---|---|---|---|
| **1** | **注释与实现完全相反**：注释说"有 pending 时跳过自旋、直接 rearm+sleep"（`569-575`），代码给的是 `has_pending ? 64 : 16`——**有 pending 时反而自旋 4 倍** | `569-578` | 维护者按注释推理必然得出错误结论（本轮最初的 R6 就是这样被写进报告的） |
| **2** | **64 是硬编码，绕过配置**：`busy_poll_budget_` 可由 `URMA_RPC_BUSY_POLL_BUDGET` 调（`urma_rpc_env.hpp:148-149`），64 只能改代码 | `578` vs `707` | **实机把 BUSY_POLL_BUDGET 调到 1 也无效**：ping-pong 走的是 64 分支 |
| **3** | **目标场景恒命中 64，自适应逻辑是死代码**：客户端等响应时 `recv_callback_` 非空；服务端响应发出后 `send_callbacks_` 非空 ⇒ 每次事件都 `has_pending=true` ⇒ 64。而 `if (now_pending && !has_pending)`（`596-599`）在 `has_pending` 已为 true 时永不成立 ⇒ `pending_budget=4` 那条**在本该生效的场景里永远不执行** | `576,595-599` | 自适应机制只存在于代码里，不在运行路径上 |
| **4** | **方向也反了**：当 pending 全部服务完、`has_pending` 由 true 变 false 时，代码**不会把预算收窄**；只有"新出现 pending"才改 | `595-599` | 该省的时候不省 |
| **5** | **64 次空 poll 的真实代价是"占住 reactor 线程 3–13µs"**：`poll_completion()` 空队列时正好 1 次 `urma_poll_jfc`（`do{...}while(count==16)`，`377-386`）；用户态 `udma_u_poll_jfc` 单次约 50–200ns ⇒ 64×(50~200ns)=**3–13µs** | `377-386,579-589` | 13–20µs 目标里**每事件 3–13µs 固定税**，吃掉 1/4~1/2 |
| **6** | **这 64 次忙等买不到延迟**：CQE 到达时 epoll 本来就会唤醒；而且 `rearm` 在循环顶部（`526`），**自旋期间 JFC 没有 rearm**，内层退出后才回去 rearm+wait ⇒ 自旋只是把"回到等待"推迟，同时挡住同线程其它连接 handler/timer | `526,579-600` | **纯成本、零收益** |
| **7** | **`urma_wait_jfc(..., 0, ...)` 返回 0 时没有 ack**：API 文档明确 `timeout=0` 是"立即返回、即使没有事件"（`urma_api.h:946-948`）。epoll 说 fd 可读但 `wait_jfc` 返回 0（事件已被消费/信号中断/聚合）→ 不 ack → 下一次 `urma_rearm_jfc` 可能失败 → 走 `526-546` 的 drain+重试；连续失败 >16 次后靠 `asio::post` 让出一轮（`533-545`）【推断】 | `563-568,526-546` | 异常时序下退化成"低效自旋"，CPU 不可控；文档还要求检查 `errno==ERESTARTSYS(512)`（`urma_api.h:951-953`），这里没查 |
| **8** | **`idle_spins` 与动态预算交互不确定**：预算从 64 改成 4 时 `idle_spins` 不重置，是否立即 break 取决于历史累计值 | `587,591,598` | 同一负载下自旋次数抖动 → 时延尾部抖动 |
| **9** | **三种 "64/16" 并存、语义不同**：timer 路径 `max_active_poll_budget_=64`（`709`）表示"连续有完成的活跃窗口"；`busy_poll_budget_=16`（`707`）表示空闲突发的空轮询；event 路径硬编码 64（`578`）表示"连续空 poll" | `578,707,709` | 一个 env 调不了两条路径，无法用配置做 A/B |

### 11.2 改法（最小 patch）

```cpp
// urma_socket.hpp:576-600 —— 有 pending：drain 到空立刻回去 rearm+wait；预算统一走配置
bool has_pending = !send_callbacks_.empty() || recv_callback_;
std::size_t idle_spins = 0;
const std::size_t spin_budget = has_pending ? 0 : busy_poll_budget_;
while (!has_close_) {
  auto [poll_ec, n] = poll_completion();
  if (poll_ec) { fail_pending(poll_ec); close(); goto loop_end; }
  if (n > 0) {
    idle_spins = 0;
    has_pending = !send_callbacks_.empty() || recv_callback_;   // 每轮重估
    continue;
  }
  if (++idle_spins > spin_budget)
    break;
}
```

配套三处：
1. 新增 `URMA_RPC_PENDING_POLL_BUDGET`（默认 0~2）替代硬编码 64；空闲档继续用 `URMA_RPC_BUSY_POLL_BUDGET`。
2. 若确实要"抓突发"，用"自旋 N 次后 `asio::post` 让出再自旋"，而不是在内层 `while` 里死等。
3. `wait_jfc` 返回 0 时按文档检查 `errno`，并把"未 ack 就 rearm"收敛成单一状态机（先 drain，再 rearm）。

### 11.3 预期收益与前提

- 每个事件省 **3–13µs** 的 reactor 占用；ping-pong RPC 每端每 RPC 至少 1 个事件 ⇒ 直接减 3–13µs，且没有延迟风险（延迟由 epoll 唤醒决定，不由自旋决定）。
- **必须先修这个，再评估 event 模式**：否则测到的是"事件模式 + 64 次忙等"，会把 event 模式的真实上限（由 JFCE 唤醒延迟决定，见 §10.2 的 moderation 假设）低估。两件事是正交的：64 是纯浪费，moderation 才决定事件模式能不能用。
- 若目标仍是 13–20µs 而 moderation 不可调，则 event 模式的上限就是 **50–300µs 的 JFCE 唤醒**，此时应按 §10.3 走"同线程内联自旋"，而不是把 64 调大来"抢"延迟——那只是在用 CPU 换一个本来就抓不到的窗口。

### 11.4 补丁已落地（2026-11-05，yalantinglibs `opt_urma`，含一轮独立审查修订）

改动 5 个文件（+279 / −29）：

| 文件 | 改动 |
|---|---|
| `include/ylt/coro_io/urma/urma_socket.hpp` | ① 删除硬编码 64，改为每轮重估 `has_pending`：`pending_poll_budget_`（默认 **1**，覆盖"CQE 落在最后一次抽干与 rearm 之间"的窗口）/`busy_poll_budget_`（默认 16）两档，且**已交付过完成的事件不再额外自旋**（`delivered_any ? 0 : busy_poll_budget_`）；② 抽干加**有界轮数**（`kMaxDrainRounds=64`），防流水线对端独占 reactor 线程；③ `urma_wait_jfc` 按厂商约定处理 `errno==ERESTARTSYS(512)`（glibc 不定义，新增 `kUrmaErestartsys`，非阻塞重试 1 次、不 ack）；④ JFC moderation 独立成 `apply_jfc_moderation()`，**只在 `init_event_fd()` 成功后才应用**（落回 timer 路径的 socket 不会留下"每 CQE 一事件却无人消费"）；⑤ 启动日志改为按实际生效模式打印 |
| `include/ylt/coro_io/urma/urma_rpc_env.hpp` | 新增 `URMA_RPC_PENDING_POLL_BUDGET`（钳制 ≤4096 并 WARN）、`URMA_RPC_JFC_MODERATE_COUNT`（`<=0` 或 `>0xFFFF` 回退 `-1`）、`URMA_RPC_JFC_MODERATE_PERIOD`（`>0xFFFF` 回退 `-1`） |
| `cmake/config.cmake` | **configure 期用 `check_cxx_source_compiles` 探测** `urma_jfc_attr_t`/`JFC_MODERATE_COUNT`/`urma_modify_jfc`，缺失时自动定义 `YLT_URMA_NO_JFC_MODERATION`（默认构建不再依赖 UMDK 版本；手工强制关闭时宏**不带值**） |
| `src/coro_rpc/tests/test_urma_rpc.cpp` | 8 组用例：默认值、覆盖、控制臂（双 `-1` 跳过 modify）、count/period 越界、count=0、pending 钳制、pending 非数字/负数 |
| `docs/urma_rpc.md` | 环境变量表（默认 `pending=1`、`count=1`/`period=-1`、`budget+1` 语义、吞吐提示）+ 事件模式延迟排查三步 + 宏用法 |

**默认值最终取值与理由**：

| 项 | 默认 | 理由 |
|---|---|---|
| `pending_poll_budget` | **1** | 0 会让"抽干→rearm"窗口在每个事件都出现；1 次额外 poll ≈0.1µs 覆盖该窗口（审查 S2） |
| `jfc_moderate_count` | **1** | 每 CQE 一个事件，消除驱动合并带来的 50–300µs 唤醒延迟 |
| `jfc_moderate_period` | **-1**（不设 PERIOD 位） | `period=0` 在部分驱动上可能非法或等同默认，只设 COUNT 已足够（审查 S3） |
| `busy_poll_budget` | 16（不变） | 只在"没有在途回调"的真实空闲时生效 |

**已验证（本机，非 ARM 实机）**：
- `g++ 15 -std=c++20 -fsyntax-only`（仓库自带 UMDK 0.9 头 + 真实 asio/async_simple）：默认路径、`-DYLT_URMA_NO_JFC_MODERATION` 路径、改动后的测试文件，**三者零错误**；
- 运行时探针实测 env 解析：`defaults → pending=1 count=1 period=-1`、`overrides → 8 / -1 / 50`、`100000 → 4096`、`abc / -5 → 1`、`count=0 → -1`、`period=70000 → -1`；
- `clang-format --dry-run --Werror` 三文件零告警。

**仍未验证（需 ARM + URMA 实机）**：
1. `urma_modify_jfc(count=1, period=-1)` 在该驱动/固件上是否返回成功（看 `URMA JFC event moderation applied` vs `urma_modify_jfc failed`）；
2. 事件模式 RTT 是否从 50–300µs 掉到目标区间；
3. `count=1` 在极高 QPS 下是否引发事件风暴（高吞吐场景可用 `count=16 period=10` 折中，或双 `-1` 回到驱动默认）；
4. `pending_poll_budget=1` 是否足够覆盖"drain 后立刻 rearm"的时序窗口（构造压力测试确认无滞留尖刺）。

**建议的实机对照（同一负载、各 3 次取中位数）**：

```bash
# G0 基线：timer 兜底（当前跑法）
URMA_RPC_ENABLE=1 URMA_RPC_EVENT_MODE=0 ...
# G1 本补丁：event + 每 CQE 一个事件 + 有界抽干
URMA_RPC_ENABLE=1 URMA_RPC_EVENT_MODE=1 ...
# G2 对照：event 但保留驱动默认合并（隔离 moderation 的作用）
URMA_RPC_ENABLE=1 URMA_RPC_EVENT_MODE=1 \
  URMA_RPC_JFC_MODERATE_COUNT=-1 URMA_RPC_JFC_MODERATE_PERIOD=-1 ...
```
判读顺序：先看 G1 的启动日志确认 moderation 生效；再比 G1 vs G2 的延迟差（差值 = 合并造成的唤醒延迟）；最后比 G1 vs G0 的延迟与 CPU（`perf top` 里 `do_timerfd_settime` 应消失、`mpstat` 不再有单核 100%）。

### 11.5 timer 兜底路径已改（2026-11-05，commit `918cc501`）

§5-B1 的第一刀落地：timer 模式不再每个 5µs 为每个 socket arm 一次 `asio::steady_timer`，改为 **reactor 线程内连续轮询**（每 8 轮 `asio::post` 让出一次 io_context）。

| 项 | 改前 | 改后 |
|---|---|---|
| 每 tick 成本 | 2×`timerfd_settime` + `epoll_pwait` + `read(timerfd)` ≈ 3.5µs CPU / 5µs 周期 / socket | 1 次用户态 `udma_u_poll_jfc` ≈ 0.2–0.5µs |
| 完成检测延迟 | 最坏 5µs（timer 粒度） | 一次轮询（亚微秒） |
| CPU | ≈72% 一个核 × socket 数 | 每个含 socket 的 reactor 线程 1 个核（与 socket 数无关） |
| 开关 | `URMA_RPC_POLL_INTERVAL` | `URMA_RPC_POLL_SPIN`（默认 `1`；`0` 恢复旧 timer 节奏） |

**验证口径**：启动日志新增 `timer_poll=spin|timer`；`perf top -p <master>` 里 `do_timerfd_settime` / `timerfd_settime` / `timer_fd_poll` / `ep_*` 应基本消失，`uburma_jfe_poll`/`udma_u_poll_jfc` 相应上升；`mpstat` 应看到「少量核 100%、其余空闲」——这是自旋签名，属预期，用 `--rpc_thread_num` 限核。**仍未上机验证**。

> 注：`send_callbacks_` 的 fire-and-forget 优化（§见会话讨论）**本次未做**，按用户要求暂缓；它需要先把 `user_ctx` 改成 seq 并做发送失败注入验证。

### 11.6 profile 采样器改造（2026-11-06，commit `c24da32c` + `a607ffa0`）

`YLT_RPC_PROFILE_ENABLE=1` 之前只在 `atexit` 时打印，长跑进程或被 SIGKILL/SIGINT 杀掉时**完全没有输出**；另外它把 `urma.read_*`/`urma.write_*` 只作为 RPC 层阶段的子节点渲染，而 Mooncake 这条链路只埋了 URMA 层，所以即使打印也是空报告。

改动：

| 项 | 说明 |
|---|---|
| 周期打印线程 | 每 `YLT_RPC_PROFILE_INTERVAL_S` 秒（默认 **10**，`<=0` 关闭）把报告打到 **stderr**；`YLT_RPC_PROFILE_RESET=1` 则每次打印后清零，得到**窗口统计**（推荐排查时用） |
| 线程安全 | 记录线程每 1024 条或每 ~100ms（先到者）把本地样本 flush 到共享池；打印线程只读共享池，不再无锁读活跃线程的 vector |
| 退出顺序 | 共享池/锁/线程注册表改为进程级不析构（诊断器允许泄漏），否则析构会早于 join-atexit 与打印线程竞争（TSAN 已证实并修复） |
| 孤儿阶段 | 没有 RPC 层父节点时，额外输出 `[urma-stage]` 平铺段——**Mooncake 场景真正会有数据的就是这一段** |

用法：

```bash
export YLT_RPC_PROFILE_ENABLE=1
export YLT_RPC_PROFILE_SAMPLE_RATE=10     # 1/N 采样；先 10，要更细用 1
export YLT_RPC_PROFILE_INTERVAL_S=10      # 默认 10s
export YLT_RPC_PROFILE_RESET=1            # 每窗口清零，看窗口 p50/p99
./mooncake_client ... 2>profile.log
```

预期输出（每 10s 一段）：

```
[rpc_profile] enabled: interval=10s sample_rate=10 reset=1 (...)
[rpc_profile] window_end=10s interval=10s
[urma-stage] (no RPC-level parent recorded)
urma.read_wait_completion  n=... avg=... p50=... p99=... max=...
urma.read_copy             n=... avg=... p50=... p99=... max=...
urma.wait_send_completion  ...
urma.write_copy / urma.post_send ...
```

判读（正好覆盖"收包那块"）：

| 观测 | 结论 |
|---|---|
| `urma.read_wait_completion` 的 p50/p99 大（几十 µs） | 时间在**等收包完成**：event 模式 = JFCE 唤醒延迟；poll 模式 = 轮询/调度问题 |
| `urma.read_copy` 大 | 收到后拷进用户 buffer 的 memcpy 成本 |
| `urma.wait_send_completion` 大 | 发送完成等待（fire-and-forget 能省掉的就是这一段） |
| `urma.write_total` 远大于 copy+post+wait | 发送路径的调度/提交开销 |

注意：该 profiler 只覆盖 URMA 层；RPC 层的 `client.recv_header/recv_payload/deserialize`、`server.handler_execute` 枚举存在但主干未埋点（在 `urma_shared_cq` 分支），要更细需要合并那三个 commit 或按同框架补点。

### 11.7 共享 JFC（每 io_context 一个 JFC + 单 poll loop）已实现（commit `38447caa` + 修复 `16a57515`）

§10.4 里"poll 线程 == io_context 线程"的正确形态，现在以**可选开关**落地：同一 io_context 的所有 URMA socket 共用一个 JFC，一个 poll loop 抽干该组 CQE 并在**同一 reactor 线程内**按 `user_ctx` 编码分发给对应 socket。

| 项 | 实现 |
|---|---|
| 开关 | `URMA_RPC_SHARED_JFC=1`（默认 `0`，默认路径完全不变）；`URMA_RPC_GROUP_CQ_SIZE`（默认 2048，范围 64~65536，创建时再按设备 `max_jfc_depth` 钳制） |
| 适用模式 | 仅 poll 模式（`URMA_RPC_EVENT_MODE=0`）；event 模式自动回退 per-socket JFC |
| 完成归因 | send WR 的 `user_ctx = [op 4bit][sid 28bit][seq 32bit]`；send 完成按 `seq` 查 `seq_slot_ring<pending_send>`（替代 FIFO，解决多 socket CQE 交错错配）；recv 完成按 `sid` 派发；未知 sid / `user_ctx==0` / 非法 op 直接丢弃 |
| 轮询 | 组内单 tick：`urma_poll_jfc(jfc, 16)` × `io_rate` 轮 → dispatch → `asio::post` 让出；空组自停（避免 posted tick 让组永不析构） |
| 容量 | `attach` 按 `recv_buffer_cnt + send_buffer_cnt + 4` 记账，超出或超过 CQ 深度返回 `-2`，回退 per-socket JFC；已析构 socket 的额度在下次 attach 时回收 |
| 错误 | 组级 poll 失败 → 置 failed 并 `on_group_error` 扇出，组内所有 socket fail+close，之后新 socket 不再拿到该组（回退 per-socket JFC）；故障域从单连接扩大到整组，这是共享的代价 |
| 生命周期 | `close()`（含 peer-close 路径）恰好 detach 一次并结算在途 send；`release_resources()` 最后 reset `group_`，保证 JFC 晚于 jetty/jfr 销毁 |
| 关键正确性 | sid **单调递增不复用**（stale CQE 不可能投给新 socket）；send 槽位在 `urma_post_jetty_send_wr` **之前**占用，post 成功后绝不归还 buffer，post 失败回滚占用 |

**预期收益**（需实机验证）：每个 reactor 线程每轮 `urma_poll_jfc` 从 N 次降到 1 次；post 链从 N 条降到 1 条；JFC 资源从 N 个降到 1 个。对 master 单 reactor + 多连接是主要受益场景；client 每线程只有 1 个 socket 时基本无收益。

**实机验证清单**（未做则不应默认开启）：

1. 功能：`URMA_RPC_SHARED_JFC=1` 下多连接 RPC 全通，握手/建链不受影响；
2. 日志：出现 `URMA shared JFC created: depth=...`、`URMA using shared JFC: sid=...`，不出现 `attach failed` / `poll failed`；
3. 容量：故意把 `GROUP_CQ_SIZE` 设小，确认 `-2` 回退路径生效且不影响正确性；
4. 压力：突发 + 多连接并发，检查 RPC 全部成功、`buffer_pool` 使用量回到基线、stale completion 计数无异常；
5. 错误注入：断开一个连接、制造一次 group poll 失败，确认组内连接被正确关闭且无 UAF；
6. 性能：A/B `SHARED_JFC=0/1`，看 `query` avg/p99、`perf top` 里 `udma_u_poll_jfc`/`asio post` 占比、`mpstat` 核占用变化。

**测试覆盖**（已做，硬件无关）：`user_ctx` 编解码往返（含边界）、`seq_slot_ring` 乱序/回绕/占位拒绝、registry 的 dispatch/detach/未知 sid/CQ 容量/group error（mock socket）、env 默认值与钳制；另用独立运行时探针执行同一组逻辑并通过，TSAN 干净。**尚未在 URMA 硬件上验证。**
