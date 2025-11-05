# OpenSwoole/Swoole MySQL 协程压测工具

一份专为 OpenSwoole/Swoole 环境设计的 MySQL 协程压测工具，在通用压测能力之上，增强了面向协程与数据库场景的 QPS 打点、连接/查询耗时统计与失败分析能力。

## 1. 工具总览

### 1.1. 核心功能

- **协程并发模型**：基于 `OpenSwoole` 或 `Swoole` 协程，以指定的并发数持续对目标数据库执行 SQL 查询。
- **多驱动支持与自动回退**：
    - 优先使用原生协程客户端 `OpenSwoole\Coroutine\MySQL` 或 `Swoole\Coroutine\MySQL`。
    - 若协程客户端不可用，可自动或手动回退至 `PDO` 或 `mysqli` 驱动，并借助 `Runtime Hook` 实现协程化。
    - 针对 `mysqli` 回退场景，自动关闭 `mysqli_report` 的异常抛出，以脚本内错误处理逻辑为准。
- **精准的超时控制**：
    - **建连超时**：通过 `--timeout` 参数，支持 `ms` 或小数秒级别的精细化建连超时控制。在 `PDO/mysqli` 模式下，利用 `Runtime Hook` 的 `socket_connect_timeout` 保证亚秒级超时生效，并设置整数秒超时作为保底。
    - **查询超时**：通过 `--sql_exec_timeout` 与 `--sql_timeout_mode`，支持客户端和服务端两种查询超时模式。
- **详尽的统计指标**：提供 QPS、成功/失败计数、多维度耗时（连接、查询、生命周期）、退避策略等统计，并输出 P95 分位值。
- **失败日志与分析**：支持将失败请求的详细信息记录到日志文件，可选 `text` 或 `jsonl` 格式，便于问题定位。
- **网络时延探测**：内置独立的网络时延探测协程，周期性测量到数据库服务器的网络往返时延（RTT），并从最终统计中排除预热期数据。

### 1.2. 事件与状态模型

工具内部通过事件驱动模型来跟踪每个请求的生命周期，并维护一致的在途状态计数：

- **事件 (Events)**：
    - `connect_begin`: 尝试建立连接前触发。
    - `connect_success`: 连接成功时触发。
    - `connect` (fail): 连接失败时触发。
    - `query` (success/fail): 查询执行成功或失败时触发。
    - `backoff`: 因失败触发重试退避时触发。
    - `cleanup`: 优雅退出时，用于清理悬挂的在途计数。
- **在途状态 (Inflight States)**：
    - `inflight_connect`: 正在建立连接的协程数。
    - `inflight_query`: 已建立连接、正在执行查询的协程数。
    - `connected_current`: 当前已建立的连接总数，口径上约等于 `inflight_query`。
## 2. 安装与先决条件

- **PHP 环境**: PHP 8.0 或更高版本。
- **扩展**: 
    - `openswoole` 或 `swoole` 扩展（版本 >= 4.5）。
    - `pdo_mysql` (当使用 `pdo` 驱动时)。
    - `mysqli` (当使用 `mysqli` 驱动时)。
- **系统工具**:
    - `ping` (`iputils` 包): 当使用 ICMP 网络探测模式 (`--net_probe_mode=icmp`) 时需要。
- **权限**:
    - 脚本执行权限。
    - 对目标数据库的网络访问权限与相应的数据库用户凭证。

## 3. 快速开始

以下示例展示了如何快速启动压测。

### 3.1. 最小化压测

以 100 并发，执行 `SELECT 1`，建连超时为 500ms。

```bash
php tools/openswoole_bench.php \
    --host=127.0.0.1 \
    --port=3306 \
    --user=your_user \
    --password=your_password \
    --sql="SELECT 1" \
    --c=100 \
    --timeout=500ms
```

### 3.2. 高并发与失败日志

以 2000 并发压测，开启失败日志记录，并设置3次重试。

```bash
php tools/openswoole_bench.php \
    --host=db.example.com \
    --port=3306 \
    --user=test_user \
    --password=test_pass \
    --dbname=testdb \
    --sql="SELECT id, name FROM users WHERE id = ?" \
    --c=2000 \
    --timeout=2s \
    --retries=3 \
    --backoff-ms=100 \
    --fail-log-enable=1 \
    --fail-log-format=jsonl
```

默认情况下，失败日志会以 `bench_fail_YYYYMMDD_HHMMSS.jsonl` 的格式保存在当前目录。
## 4. 详细参数说明

### 4.1. 数据库连接

- `--driver`: 使用的客户端驱动。
    - `openmysql` (默认): 自动选择 `OpenSwoole\Coroutine\MySQL` 或 `Swoole\Coroutine\MySQL`。
    - `pdo`: 使用 `PDO` 客户端 (需 `Runtime Hook` 支持协程化)。
    - `mysqli`: 使用 `mysqli` 客户端 (需 `Runtime Hook` 支持协程化)。
- `--host`: 数据库主机名或 IP 地址。
- `--port`: 数据库端口 (默认: `3306`)。
- `--user`: 数据库用户名。
- `--password`: 数据库密码。
- `--dbname`: 数据库名称。
- `--sql`: 需要执行的 SQL 查询语句。

### 4.2. 并发与时序

- `--c`: 并发协程数，即同时发起的请求数。
- `--max_run_time`: 压测最大运行时长。
    - 支持 `s`, `m`, `h` 后缀，如 `30s`, `5m`, `1.5h`。纯数字表示秒。
    - 到达指定时长后，工具将触发优雅退出流程。

### 4.3. 超时控制

- `--timeout`: **建连超时**。支持 `ms` 和小数秒，如 `300ms`, `0.5s`, `2`。
    - 此超时仅作用于 TCP 连接建立阶段。
    - 在 `pdo`/`mysqli` 模式下，会优先通过 `Runtime Hook` 设置 `socket_connect_timeout` (浮点秒)，并额外设置整数秒 `ATTR_TIMEOUT`/`MYSQLI_OPT_CONNECT_TIMEOUT` 作为保底。
- `--sql_exec_timeout`: **查询执行超时** (单位：秒，默认: `0` 不限时)。
- `--sql_timeout_mode`: 查询超时模式 (默认: `client`)。
    - `client`: 客户端超时。对于协程 MySQL 客户端，是 `query` 方法的超时参数；对于 `mysqli`，是 `MYSQLI_OPT_READ_TIMEOUT`。`PDO` 不支持客户端查询超时，会自动回退到 `server` 模式。
    - `server`: 服务端超时。通过执行 `SET SESSION MAX_EXECUTION_TIME=...` 来让 MySQL 服务器中断超时查询。

### 4.4. 重试与退避

- `--retries`: 每个请求失败后的最大重试次数 (默认: `0`)。
- `--backoff-ms`: 每次重试前的退避等待时间 (单位：毫秒，默认: `200`)。

### 4.5. 日志

- `--fail-log-enable`: 是否启用失败日志 (默认: `false`)。
- `--fail-log`: 指定失败日志的输出路径。若不指定，则自动生成文件名如 `bench_fail_YYYYMMDD_HHMMSS.log`。
- `--fail-log-format`: 失败日志格式。
    - `text` (默认): 人类可读的文本格式。
    - `jsonl`: 每行一个 JSON 对象，便于程序解析。
- **日志轮转**: 当单个日志文件达到 `100MB` 时，会自动进行轮转，生成如 `...log.part01`, `...log.part02` 的分片文件。

### 4.6. 网络探测

- `--net_probe_mode`: 网络时延探测模式 (默认: `tcp`)。
    - `tcp`: 通过 TCP 连接到目标数据库端口的方式测量建连耗时。
    - `icmp`: 通过 `ping` 命令测量 ICMP RTT (需要系统安装 `ping` 工具)。
    - `none`: 禁用网络时延探测。
- `--net_probe_timeout`: ICMP 模式下的 `ping` 超时时间 (单位：秒，默认: `1.0`)。

### 4.7. 其它

- `--tz`: 设置脚本运行时区，如 `Asia/Shanghai` (默认) 或 `UTC`。
## 5. 事件模型与在途计数

工具通过一个中心化的聚合器（`StatsAgg`）处理来自所有工作协程的事件，以维护精确的系统状态。理解事件流和在途计数器有助于解读区间统计的动态变化。

- **事件流**: 
    - 一个典型的成功请求流：`connect_begin` -> `connect_success` -> `query(success)`。
    - 一个带重试的失败请求流：`connect_begin` -> `connect(fail)` -> `backoff` -> `connect_begin` -> ...

- **在途计数器 (Inflight Counters)**:
    - `inflight_connect`: 当一个协程开始尝试连接时 (`connect_begin` 事件)，该计数器加一；当连接成功 (`connect_success`) 或失败 (`connect` fail) 时，减一。
    - `inflight_query`: 当连接成功后，准备发查询时 (`connect_success` 事件)，该计数器加一；当查询成功或失败 (`query` success/fail) 时，减一。
    - `connected_current`: 该值与 `inflight_query` 保持同步，代表当前已建立连接并正在处理查询的连接数。
    - `cleanup` 事件在程序退出时触发，用于将所有在途计数强制归零，确保最终统计的准确性。

## 6. 统计指标口径与输出解读

### 6.1. 区间打印 (每秒)

每秒输出的统计行包含了瞬时性能指标：

- `QPS`: 每秒完成的请求数 (包括成功和失败)。
- `最大QPS`: 自压测开始以来的最高秒级 QPS。
- `已建立连接数`: `connected_current` 的瞬时值。
- `在途连接`: `inflight_connect` 的瞬时值。
- `在途查询`: `inflight_query` 的瞬时值。
- `退避事件`: 该秒内发生的 `backoff` 事件总数。
- `成功`/`失败`: 该秒内完成的成功/失败请求数。
- `活跃协程`: `inflight_connect` + `inflight_query`，理论上应恒等于并发数 `--c`。
- `网络时延`: 显示最近一秒探测到的网络时延，带有模式标签（如 `TCP 建连` 或 `ICMP ping`）。为了数据稳定，默认会**跳过前两个周期的结果**。

### 6.2. 最终统计 (Final Stats)

压测结束后输出的汇总统计信息，口径如下：

- **网络时延**: 
    - `avg_lat`: 平均网络时延，排除了预热期数据。
    - `p95`: 网络时延的 95 分位值。
- **尝试级耗时 (Attempt-level)**:
    - `connectAttempt`: **每一次**建连尝试的耗时统计 (sum/min/max/avg)。
    - `queryAttempt`: **每一次**查询尝试的耗时统计 (sum/min/max/avg)。
- **请求级累计连接时间 (Request-level)**:
    - `request_connect_total`: 单个请求生命周期内，所有建连尝试（包括重试）的**耗时总和** (sum/min/max/avg)。
- **建连时延 (成功样本)**: 仅统计**成功建立连接**的那些尝试，并计算 `p95`/min/max/avg。
- **连接生命周期**: 从发起连接到查询完毕关闭的**整个过程耗时**，仅统计最终成功的请求，并计算 `p95`/min/max/avg。
- **查询时间**: 仅统计查询阶段的耗时，计算 sum/min/max/avg/`p95`。
- **退避统计**: 
    - `退避事件总数`: 所有请求累计发生的 `backoff` 事件总数。
    - `含退避请求数与占比`: 有过至少一次退避行为的请求数量及其在总请求中的占比。
    - `每请求平均/最大退避次数`: 分别计算了在所有请求中和仅在含退避的请求中的平均退避次数，以及单个请求的最高退避次数。

## 7. 失败日志

当启用 `--fail-log-enable=1` 时，所有失败的请求都会被记录下来，便于事后分析。

### 7.1. 日志格式

- **text**: `[时间] stage=... driver=... attempt=... target=... errno=... message=...`
- **jsonl**: 每行一个 JSON 对象，包含以下字段：
    - `ts`: 时间戳
    - `stage`: 失败阶段 (`connect` 或 `query`)
    - `driver`: 使用的驱动
    - `host`/`port`/`user`/`db`/`sql`: 连接与查询参数
    - `errno`: 错误码
    - `message`: 归一化后的错误信息
    - `message_raw`: 原始错误信息
    - `connect_time`: 连接耗时
    - `query_time`: 查询耗时
    - `attempt`: 当前是第几次尝试

### 7.2. 健壮性

- **句柄管理**: 日志句柄如果意外失效，会尝试重开一次；若再次失败，则禁用日志功能，保证压测主流程不受影响。
- **自动轮转**: 日志文件达到 100MB 会自动创建新的分片文件（`.partNN`），避免单个日志文件过大。
## 8. 优雅退出与信号处理

为了确保统计数据的完整性和资源的正确释放，工具实现了优雅退出机制。

- **触发条件**: 
    - 用户按下 `Ctrl+C` (`SIGINT`) 或收到 `SIGTERM` 信号。
    - 压测运行达到 `--max_run_time` 指定的时长。
- **退出流程**:
    1. 停止所有正在运行的定时器 (如QPS打印、限时退出)。
    2. 翻转最后一次网络时延的统计缓冲，确保最新数据可被统计。
    3. 向聚合器推送 `cleanup` 事件，将所有在途计数器归零。
    4. 关闭失败日志文件句柄。
    5. 打印最终的汇总统计报告。
- **ICMP 模式的非阻塞执行**: 当使用 `--net_probe_mode=icmp` 时，`ping` 命令会通过协程化的 `System::exec` 执行，避免了外部命令阻塞导致 `Ctrl+C` 响应延迟的问题。

## 9. 使用示例

### 9.1. 基本查询

```bash
php tools/openswoole_bench.php --host=127.0.0.1 --user=root --password=123456 --sql="SELECT 1" --c=100
```

### 9.2. 开启失败日志

```bash
php tools/openswoole_bench.php ... --fail-log-enable=1 --fail-log-format=jsonl --fail-log=./failures.log
```

### 9.3. 设置查询超时

- **客户端超时 (mysqli)**:
```bash
php tools/openswoole_bench.php ... --driver=mysqli --sql_exec_timeout=2.5 --sql_timeout_mode=client
```

- **服务端超时**:
```bash
php tools/openswoole_bench.php ... --sql_exec_timeout=5 --sql_timeout_mode=server
```

### 9.4. 切换网络探测模式

- **使用 ICMP ping**:
```bash
php tools/openswoole_bench.php ... --net_probe_mode=icmp --net_probe_timeout=0.5
```

- **禁用网络探测**:
```bash
php tools/openswoole_bench.php ... --net_probe_mode=none
```

### 9.5. 限时退出

压测运行10分钟后自动退出。

```bash
php tools/openswoole_bench.php ... --max_run_time=10m
```

## 10. 常见问题与注意事项

- **网络时延口径差异**: `--net_probe_mode=tcp` 测量的是到数据库端口的 TCP 建连耗时，受服务端 SYN 队列、WAF 等影响较大；`--net_probe_mode=icmp` 测量的是网络层的 ICMP RTT。两者口径完全不同，数值不具有直接可比性，仅供参考。
- **PDO 客户端查询超时**: `PDO` 驱动本身不提供原生的客户端查询超时设置。当 `--sql_timeout_mode=client` 时，工具会自动回退到 `server` 模式，即通过 `MAX_EXECUTION_TIME` 实现。
- **高并发下的抖动**: 在极高并发下，操作系统的 TCP/IP 栈、文件句柄数限制、CPU 调度、NAT 端口耗尽等都可能成为瓶颈，导致连接超时、QPS 抖动等现象。此时应结合系统监控工具 (`netstat`, `ss`, `dmesg`) 进行综合分析。
- **避免混用 `ping` 值**: 请勿将手动执行 `ping` 命令得到的结果与工具在压测中打印的“网络时延”进行直接比较，因为后者是在并发压力下动态测量的，且 TCP 模式的口径与 ICMP 完全不同。
