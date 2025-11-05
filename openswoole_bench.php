<?php
/**
 * OpenSwoole/Swoole 协程 MySQL 压测工具（方案 C，增强版）
 *
 * 目标落实：
 * - QPS 打点增强：新增已建连（interval_connected）、在途连接（inflight_connect）、在途查询（inflight_query）、退避次数（interval_backoff）
 * - 统计口径完善：新增两类连接耗时指标
 *   1) attempt_connect_time：每次尝试的连接阶段耗时（在 connect_success 与 connect 失败事件上报），sum/min/max/avg
 *   2) request_connect_total：一个请求内所有尝试的连接时间总和（最终成功或最终失败事件上报），sum/min/max/avg
 * - 事件模型一致性：
 *   - 每次尝试进入连接阶段：推送 connect_begin（不计入QPS；用于 inflight_connect++）
 *   - 连接成功：推送 connect_success（不计入QPS；用于 interval_connected++，inflight_connect-- & inflight_query++）
 *   - 连接失败：推送失败事件（stage=connect，含 errno/message 与 attempt_connect_time）；随后推送 backoff（不计入QPS；interval_backoff++），inflight_connect--
 *   - 查询失败：推送失败事件（stage=query，含 request_connect_total 与 query_time），inflight_query--
 *   - 查询成功：推送成功事件（stage=query，含 request_connect_total）；inflight_query--
 *   - 在优雅退出时，push cleanup 事件将 inflight_* 归零（不计入QPS）
 * - 失败日志健壮性：集中 writer 写入（text/JSONL），句柄异常时尝试一次重开并降级禁用；不影响统计。JSONL 保留 message_raw。
 * - 建连超时：--timeout 支持 ms/小数秒，仅用于连接阶段；SQL 执行不限制。
 *   - 在连接创建前启用 Runtime Hook 并设置 socket_connect_timeout（浮点秒）优先生效；PDO/mysqli 继续设置整数秒保底（ATTR_TIMEOUT/MYSQLI_OPT_CONNECT_TIMEOUT）。
 */
// ---------- 基础设置 ----------
ini_set('memory_limit', '-1');
ini_set('display_errors', '1');
error_reporting(E_ALL);
// 默认时区（可被 --tz 参数覆盖）
date_default_timezone_set('Asia/Shanghai');
// [新增] 失败日志轮转阈值常量（100MB，可调试时临时修改）
// 为便于审阅与维护，轮转阈值集中定义
const FAIL_LOG_MAX_BYTES = 100 * 1024 * 1024; // 100MB
// ---------- CLI 解析 ----------
function parse_argv(array $argv): array {
    $args = [];
    foreach ($argv as $i => $arg) {
        if ($i === 0) continue;
        if (strpos($arg, '--') === 0) {
            $pair = substr($arg, 2);
            $kv = explode('=', $pair, 2);
            $key = $kv[0];
            $val = $kv[1] ?? '1';
            $args[$key] = $val;
        }
    }
    return $args;
}
$args = parse_argv($argv);
function arg_str(array $args, string $key, ?string $default = null): ?string { return isset($args[$key]) ? (string)$args[$key] : $default; }
function arg_int(array $args, string $key, int $default): int { return isset($args[$key]) ? (int)$args[$key] : $default; }
function arg_float(array $args, string $key, float $default): float { return isset($args[$key]) ? (float)$args[$key] : $default; }
function arg_bool(array $args, string $key, bool $default): bool {
    if (!isset($args[$key])) return $default;
    $v = strtolower((string)$args[$key]);
    return in_array($v, ['1','true','yes','on'], true);
}
// 超时解析：支持 "300ms"、"1500ms"、"0.3"、"2"、"2.5"、"2s"，非法输入回退到 3.0s（stderr 提示）
function parse_timeout(string $v): float {
    $raw = trim($v);
    if ($raw === '') { @fwrite(STDERR, "[WARN] --timeout 为空，已回退到默认 3.0s\n"); return 3.0; }
    if (preg_match('/^\d+(?:\.\d+)?\s*ms$/i', $raw)) {
        $num = (float)preg_replace('/\s*ms$/i', '', $raw);
        return max(0.0, $num / 1000.0);
    }
    if (preg_match('/^\d+(?:\.\d+)?\s*s$/i', $raw)) {
        $num = (float)preg_replace('/\s*s$/i', '', $raw);
        return max(0.0, $num);
    }
    if (preg_match('/^\d+(?:\.\d+)?$/', $raw)) {
        return (float)$raw; // 纯数字按秒解析
    }
    @fwrite(STDERR, "[WARN] 非法 --timeout 值：{$v}，已回退到默认 3.0s\n");
    return 3.0;
}
// [新增] 时长解析：支持纯数字秒与 s/m/h 后缀（如 300、300s、2.5、5m、1h）；非法输入回退到 0 并提示
function parse_duration(string $v): float {
    $raw = trim($v);
    if ($raw === '' || $raw === '0') return 0.0;
    // 匹配形如 2.5s / 5m / 1h / 300
    if (preg_match('/^\d+(?:\.\d+)?\s*(s|m|h)?$/i', $raw, $m)) {
        $unit = strtolower($m[1] ?? '');
        if ($unit === 's' || $unit === '') {
            return (float)preg_replace('/\s*s$/i', '', $raw);
        } elseif ($unit === 'm') {
            $num = (float)preg_replace('/\s*m$/i', '', $raw);
            return $num * 60.0;
        } elseif ($unit === 'h') {
            $num = (float)preg_replace('/\s*h$/i', '', $raw);
            return $num * 3600.0;
        }
    }
    @fwrite(STDERR, "[WARN] 非法 --max_run_time 值：{$v}，已回退到 0（不限时）\n");
    return 0.0;
}
$driver      = strtolower(arg_str($args, 'driver', 'openmysql'));
$host        = arg_str($args, 'host', null);
$port        = arg_int($args, 'port', 3306);
$user        = arg_str($args, 'user', null);
$password    = arg_str($args, 'password', null);
$dbname      = arg_str($args, 'dbname', null);
$sql         = arg_str($args, 'sql', null);
$concurrency = arg_int($args, 'c', 0);
$timeoutRaw  = (string)arg_str($args, 'timeout', '3'); // 原始字符串
$connTimeoutSec = parse_timeout($timeoutRaw); // 建连超时（浮点秒）
// 归一化：mysqli/PDO 的超时选项为整数秒；避免 0.3s 截断为 0 不触发超时，统一 ceil 并最小值为 1
$connTimeoutSecInt = max(1, (int)ceil($connTimeoutSec));
$retries     = arg_int($args, 'retries', 0);
$backoffMs   = arg_int($args, 'backoff-ms', 200);
$debug       = arg_int($args, 'debug', 0);
$tz          = arg_str($args, 'tz', 'Asia/Shanghai');
if ($tz) { @date_default_timezone_set($tz); }
// SQL 执行超时（秒，0 表示不限）
$sqlExecTimeout = max(0.0, arg_float($args, 'sql_exec_timeout', 0.0));
// SQL 执行超时模式（client=客户端超时主动断开；server=服务端 MAX_EXECUTION_TIME），默认 client
$sqlTimeoutMode = strtolower(arg_str($args, 'sql_timeout_mode', 'client'));
if (!in_array($sqlTimeoutMode, ['client','server'], true)) { $sqlTimeoutMode = 'client'; }
// [新增] 网络探测模式与超时（仅 icmp 使用）
$netProbeMode = strtolower(arg_str($args, 'net_probe_mode', 'tcp'));
if (!in_array($netProbeMode, ['tcp','icmp','none'], true)) { $netProbeMode = 'tcp'; }
$netProbeTimeout = max(0.001, arg_float($args, 'net_probe_timeout', 1.0)); // 仅 icmp 使用
$netProbeLabel = ($netProbeMode === 'tcp' ? 'TCP 建连' : ($netProbeMode === 'icmp' ? 'ICMP ping' : '禁用'));
// 失败日志配置
$failLogEnable = arg_bool($args, 'fail-log-enable', false);
$failLogFormat = strtolower(arg_str($args, 'fail-log-format', 'text'));
$failLogPath   = arg_str($args, 'fail-log', null);
if ($failLogEnable) {
    if ($failLogPath === null || $failLogPath === '') {
        $tsFilename = date('Ymd_His');
        $ext = ($failLogFormat === 'jsonl') ? 'jsonl' : 'log';
        $failLogPath = "bench_fail_{$tsFilename}.{$ext}";
    }
}
// [新增] CLI 参数：max_run_time（默认 0 不限时）
$maxRunTimeRaw = (string)arg_str($args, 'max_run_time', '0');
$maxRunTimeSec = parse_duration($maxRunTimeRaw);
// PDO 客户端查询超时不支持的单次提示控制
$pdoClientTimeoutWarned = false;
function panic(string $msg): void { fwrite(STDERR, $msg . "\n"); exit(1); }
// 必要参数校验（仅启动期校验，业务错误不退出）
if (!$host || !$user || $password === null || !$sql || $concurrency <= 0) {
    panic("用法示例：\n" .
        "php tools/openswoole_bench.php --driver=openmysql --host=... --port=3306 --user=... --password=... " .
        "--c=5000 --timeout=300ms --sql=\"SELECT 1\"\n" .
        "必要参数：--host --port --user --password --sql --c --timeout（支持 300ms/0.3/2s/2.5 等，默认为秒）\n" .
        "说明：--timeout 仅用于连接阶段（建连超时）；--sql_exec_timeout 控制查询阶段超时（单位秒，默认0不限）；--sql_timeout_mode=client|server（默认 client）控制查询超时策略：client=客户端主动断开，server=服务端 MAX_EXECUTION_TIME。sql_exec_timeout=0 时不做执行超时控制（不传 query 超时、不设置 MAX_EXECUTION_TIME、不设 READ_TIMEOUT）。\n" .
        "新增指标：区间‘在途连接’/‘在途查询’/‘退避次数’已启用；尝试级与请求级连接耗时均会在最终统计打印\n" .
        "指标口径说明：‘已建立连接数’为当前状态计数（通常等于‘在途查询’）；‘活跃协程’=在途连接+在途查询；若出现偏差通常源于定时器打点与状态切换的边界时序。\n" .
        "统计口径说明：retries=N 为每个请求的最大重试次数（同一请求最多进行 N 次退避）；退避事件是跨所有请求的总和，因此在区间/最终统计中可远大于 N；新增区间‘含退避请求数’与最终‘每请求退避次数分布摘要’\n" .
        "可选：--dbname --retries --backoff-ms --debug --tz --fail-log-enable --fail-log --fail-log-format --sql_exec_timeout --sql_timeout_mode --max_run_time --net_probe_mode --net_probe_timeout\n");
}
// ---------- 环境检测与封装 ----------
function has_openswoole(): bool { return extension_loaded('openswoole'); }
function has_swoole(): bool { return extension_loaded('swoole'); }
function co_run(callable $fn): void {
    if (function_exists('OpenSwoole\Coroutine\run')) { \OpenSwoole\Coroutine\run($fn);
    } elseif (function_exists('Swoole\Coroutine\run')) { \Swoole\Coroutine\run($fn);
    } else { panic("错误：未检测到 openswoole/swoole 扩展，无法启用协程运行。请安装 openswoole：sudo pecl install openswoole"); }
}
function co_create(callable $fn, ...$args): void {
    if (class_exists('OpenSwoole\Coroutine')) { \OpenSwoole\Coroutine::create($fn, ...$args);
    } elseif (class_exists('Swoole\Coroutine')) { \Swoole\Coroutine::create($fn, ...$args);
    } else { panic("错误：未检测到 openswoole/swoole 扩展，无法创建协程"); }
}
function co_sleep(float $sec): void {
    if (class_exists('OpenSwoole\Coroutine')) { \OpenSwoole\Coroutine::sleep($sec);
    } elseif (class_exists('Swoole\Coroutine')) { \Swoole\Coroutine::sleep($sec);
    } else { usleep((int)($sec * 1e6)); }
}
function timer_tick(int $ms, callable $fn): ?int {
    if (class_exists('OpenSwoole\Timer')) return \OpenSwoole\Timer::tick($ms, $fn);
    if (class_exists('Swoole\Timer')) return \Swoole\Timer::tick($ms, $fn);
    return null;
}
function timer_clear(?int $id): void {
    if ($id === null) return;
    if (class_exists('OpenSwoole\Timer')) { \OpenSwoole\Timer::clear($id); return; }
    if (class_exists('Swoole\Timer')) { \Swoole\Timer::clear($id); return; }
}
function process_signal(int $sig, callable $fn): void {
    if (class_exists('OpenSwoole\Process')) { \OpenSwoole\Process::signal($sig, $fn);
    } elseif (class_exists('Swoole\Process')) { \Swoole\Process::signal($sig, $fn);
    } else { if (function_exists('pcntl_async_signals')) pcntl_async_signals(true); if (function_exists('pcntl_signal')) pcntl_signal($sig, $fn); }
}
function make_channel(int $cap) {
    if (class_exists('OpenSwoole\Coroutine\Channel')) return new \OpenSwoole\Coroutine\Channel($cap);
    if (class_exists('Swoole\Coroutine\Channel')) return new \Swoole\Coroutine\Channel($cap);
    panic("错误：未检测到 openswoole/swoole 扩展，无法创建 Channel");
}
function enable_runtime_hook(): bool {
    // 在回退 PDO/mysqli 时需要协程化钩子；若不可用则宽容继续（仅整数秒建连超时）
    $enabled = false;
    try {
        if (class_exists('OpenSwoole\Runtime')) {
            if (defined('SWOOLE_HOOK_ALL')) { \OpenSwoole\Runtime::enableCoroutine(SWOOLE_HOOK_ALL); }
            else { \OpenSwoole\Runtime::enableCoroutine(true); }
            $enabled = true;
        } elseif (class_exists('Swoole\Runtime')) {
            if (defined('SWOOLE_HOOK_ALL')) { \Swoole\Runtime::enableCoroutine(SWOOLE_HOOK_ALL); }
            else { \Swoole\Runtime::enableCoroutine(true); }
            $enabled = true;
        } else {
            @fwrite(STDERR, "[WARN] 未检测到 OpenSwoole/Swoole Runtime Hook，PDO/mysqli 将使用整数秒建连超时。\n");
        }
    } catch (\Throwable $e) {
        @fwrite(STDERR, "[WARN] 启用 Runtime Hook 异常：" . $e->getMessage() . "\n");
    }
    return $enabled;
}
function env_versions(): string { $php = 'PHP ' . PHP_VERSION; $sw = defined('SWOOLE_VERSION') ? (' / Swoole ' . SWOOLE_VERSION) : ''; return $php . $sw; }
function println(string $s): void { echo $s . "\n"; }
function ts(): string { return date('Y-m-d H:i:s'); }
// 设置协程运行配置中的 socket_connect_timeout（浮点秒）
function set_socket_connect_timeout(float $sec): bool {
    $cfg = ['socket_connect_timeout' => $sec];
    try {
        if (class_exists('OpenSwoole\Coroutine')) { \OpenSwoole\Coroutine::set($cfg); return true; }
    } catch (\Throwable $e) { /* 忽略 */ }
    try {
        if (class_exists('Swoole\Coroutine')) { \Swoole\Coroutine::set($cfg); return true; }
    } catch (\Throwable $e) { /* 忽略 */ }
    return false;
}
// [改造] ICMP ping 探测：优先使用协程 System::exec 非阻塞执行；不可用时回退 shell_exec；解析 time=..ms 或 rtt avg；返回秒或 null
function probe_icmp_latency_sec(string $host, float $timeoutSec): ?float {
    $hostArg = escapeshellarg($host);
    $timeoutInt = max(1, (int)ceil($timeoutSec)); // iputils ping -W 单位秒
    $cmd = "ping -c 1 -W {$timeoutInt} {$hostArg} 2>&1";
    $out = null;
    try {
        if (class_exists('OpenSwoole\\Coroutine\\System')) {
            $res = \OpenSwoole\Coroutine\System::exec($cmd);
            if (is_array($res) && isset($res['output']) && is_string($res['output'])) { $out = $res['output']; }
        } elseif (class_exists('Swoole\\Coroutine\\System')) {
            $res = \Swoole\Coroutine\System::exec($cmd);
            if (is_array($res) && isset($res['output']) && is_string($res['output'])) { $out = $res['output']; }
        } else {
            $out = @shell_exec($cmd);
        }
    } catch (\Throwable $e) {
        $out = null;
    }
    if (!is_string($out) || $out === '') return null;
    // 先解析单次响应：time=... ms 或 time<1 ms
    if (preg_match('/time[=<]\s*([0-9]+(?:\.[0-9]+)?)\s*ms/i', $out, $m)) {
        $isLess = (stripos($out, 'time<') !== false);
        if ($isLess) return 0.001; // time<1 ms 近似按 1ms 处理
        $ms = (float)$m[1];
        return $ms / 1000.0;
    }
    // 兼容 iputils 汇总行：rtt min/avg/max/mdev = a/b/c/d ms
    if (preg_match('/rtt\s+[^=]*=\s*([0-9\.]+)\/([0-9\.]+)\/([0-9\.]+)\/([0-9\.]+)\s*ms/i', $out, $m2)) {
        $avgMs = (float)$m2[2];
        return $avgMs / 1000.0;
    }
    return null;
}
// ---------- 调试输出（轻量、可控） ----------
function debug_attempt(int $debug, int $workerId, int $attempt, string $stage, bool $ok, int $errno, string $message, float $timeSec): void {
    if ($debug < 1) return;
    $result = $ok ? 'success' : 'fail';
    $ms = (int)round($timeSec * 1000);
    $msgNorm = normalize_error_message($message ?? '');
    $errnoStr = is_int($errno) && $errno > 0 ? (string)$errno : '0';
    // 格式示例：[attempt=N stage=connect|query result=success|fail errno=XXXX message=... time=xxxms]
    println("[attempt={$attempt} stage={$stage} result={$result} errno={$errnoStr} message=" . ($msgNorm !== '' ? $msgNorm : '-') . " time={$ms}ms]");
}
// ---------- 错误归类与归一化 ----------
function normalize_error_message(string $msg): string {
    // 轻度归一化：trim、压缩空白、移除明显的动态尾部（不做关键词映射）
    $m = trim($msg);
    if ($m === '') return '';
    // 压缩连续空白为单空格
    $m = preg_replace('/\s+/u', ' ', $m);
    // 移除典型动态尾部片段（尽量保留核心信息）
    $m = preg_replace('/\s+at\s+[^:]+:\s*line\s*\d+\s*$/i', '', $m);                // " at xxx:line yyy"
    $m = preg_replace('/\s+\(connection id:\s*\d+\)\s*$/i', '', $m);                // " (connection id: 12345)"
    // 移除 ip:port（常见动态信息）
    $m = preg_replace('/(\d{1,3}\.){3}\d{1,3}(:\d+)?/', '', $m);
    // 去除仅数字错误码的方括号片段（保留语义）
    $m = preg_replace('/\[[0-9]{3,5}\]/', '', $m);
    // 再次收尾
    $m = trim($m);
    return $m;
}
function extract_errno_from_message(string $msg): int {
    // 兼容 SQLSTATE 提示：SQLSTATE[HY000] [2002] Connection timed out
    if (preg_match('/\[(\d{3,5})\]/', $msg, $m)) return (int)$m[1];
    if (preg_match('/errno\s*(\d{3,5})/i', $msg, $m)) return (int)$m[1];
    return 0;
}
function extract_errno_from_pdo_exception(\Throwable $e): int {
    // PDOException::errorInfo[1] 为 MySQL errno（如 1040、2013、2002）；getCode() 常为 SQLSTATE 字符串
    if ($e instanceof \PDOException) {
        $info = $e->errorInfo ?? null;
        if (is_array($info) && isset($info[1]) && is_numeric($info[1])) return (int)$info[1];
        $code = $e->getCode();
        if (is_numeric($code)) return (int)$code;
        return extract_errno_from_message($e->getMessage() ?? '');
    }
    return 0;
}
function classify_by_message(?int $errno, ?string $message, ?string $stage = null): string {
    // 优先识别服务端限时超时与客户端查询读/协程超时；否则按原始错误信息归组
    $msgRaw = is_string($message) ? $message : '';
    $msgNorm = $msgRaw !== '' ? normalize_error_message($msgRaw) : '';
    // 服务端 MAX_EXECUTION_TIME 超时：错误码 3024 或消息包含 "MAX_EXECUTION_TIME exceeded"
    if (is_int($errno) && $errno === 3024) return 'Execution time exceeded';
    if ($msgRaw !== '' && stripos($msgRaw, 'MAX_EXECUTION_TIME exceeded') !== false) return 'Execution time exceeded';
    // 客户端查询读/协程超时：query 阶段出现 "timeout/timed out"
    if ($stage === 'query' && $msgRaw !== '' && preg_match('/timeout|timed out/i', $msgRaw)) {
        return 'Client query timed out';
    }
    // 回退：按归一化后的原始错误信息分组；若为空则按 errno；仍为空则归类为 Unknown
    if ($msgNorm !== '') return $msgNorm;
    if (is_int($errno) && $errno > 0) return 'errno=' . $errno;
    return 'Unknown';
}
// ---------- 客户端选择 ----------
$selectedClient = null; // openmysql/swoolemysql/pdo/mysqli（实际路径）
$clientLabel    = '';
if ($driver === 'openmysql') {
    if (has_openswoole() && class_exists('OpenSwoole\Coroutine\MySQL')) { $selectedClient = 'openmysql'; $clientLabel = 'OpenSwoole\\Coroutine\\MySQL'; }
    elseif (has_swoole() && class_exists('Swoole\Coroutine\MySQL')) { $selectedClient = 'swoolemysql'; $clientLabel = 'Swoole\\Coroutine\\MySQL'; }
    else {
        println("[" . ts() . "] WARN  未检测到协程 MySQL 客户端，准备回退到 PDO/mysqli。可通过 --driver=pdo|mysqli 指定。");
        $selectedClient = null;
    }
} elseif (in_array($driver, ['pdo', 'mysqli'], true)) {
    $selectedClient = $driver; $clientLabel = strtoupper($driver);
} else { panic("错误：未知 driver={$driver}（可选：openmysql|pdo|mysqli）"); }
if ($selectedClient === null) { $selectedClient = 'mysqli'; $clientLabel = 'MYSQLI'; println("[" . ts() . "] INFO  回退驱动自动选择：MYSQLI（可通过 --driver=pdo 切换）"); }
// 关闭 mysqli 异常上报，避免未捕获 mysqli_sql_exception 终止进程
if ($selectedClient === 'mysqli' && function_exists('mysqli_report')) { @mysqli_report(\MYSQLI_REPORT_OFF); }
// ---------- 启动输出 ----------
println("========================================");
println("MySQL协程压测工具（" . env_versions() . ")");
println("目标数据库：{$host}:{$port}");
println("并发协程数：{$concurrency}");
println("执行SQL：{$sql}");
println("时区：" . date_default_timezone_get());
println("驱动类型：{$clientLabel}");
println("重试策略：retries={$retries}, backoff-ms={$backoffMs}");
// 启动提示：建连超时（支持 ms/小数秒），SQL 执行超时不限制
$__raw = trim($timeoutRaw);
if (preg_match('/^\d+(?:\.\d+)?\s*ms$/i', $__raw)) {
    $ctLabel = "建连超时：{$__raw}（" . sprintf('%.3fs', $connTimeoutSec) . "）";
} else {
    $ctLabelRaw = preg_match('/s$/i', $__raw) ? $__raw : ($__raw . 's');
    $ctLabel = "建连超时：{$ctLabelRaw}";
}
println($ctLabel);
println("SQL 执行超时：" . sprintf('%.3fs', $sqlExecTimeout) . " | 模式：{$sqlTimeoutMode}（client=客户端主动断开，server=MAX_EXECUTION_TIME）");
// 若为 PDO/mysqli，提示 Runtime Hook 对超时精度的影响
$hookClassAvailable = class_exists('OpenSwoole\\Runtime') || class_exists('Swoole\\Runtime');
if (in_array($selectedClient, ['pdo','mysqli'], true)) {
    if ($hookClassAvailable) {
        println("Runtime Hook：可用，将设置 socket_connect_timeout=" . sprintf('%.3fs', $connTimeoutSec));
    } else {
        println("Runtime Hook：不可用，PDO/mysqli 将使用整数秒建连超时（" . $connTimeoutSecInt . "s）");
    }
}
// [新增] 网络时延模式与口径说明
println("网络时延模式：" . $netProbeLabel);
println("口径说明：ICMP ping 与 TCP 建连不同，数值不可直接对比");
println("统计口径：退避事件为跨请求总数；‘含退避请求数’为本区间最终事件中发生过退避的请求计数；最终统计将给出每请求退避次数的平均与最大值。");
println("指标口径：‘已建立连接数’≈‘在途查询’；‘活跃协程’=在途连接+在途查询；边界时序可能产生细微偏差。");
if ($failLogEnable) { println("失败日志：启用 | 路径：{$failLogPath} | 格式=" . ($failLogFormat === 'jsonl' ? 'JSONL' : 'TEXT')); } else { println("失败日志：禁用"); }
// [新增] 输出 max_run_time 提示
if ($maxRunTimeSec > 0.0) { println("运行时长限制：" . sprintf('%.3fs', $maxRunTimeSec) . "（到时将优雅退出）"); } else { println("运行时长限制：不限时"); }
println("========================================");
println("压测中...（按 Ctrl+C 停止）");
println("----------------------------------------");
// ---------- 统计结构（中心聚合协程） ----------
class StatsAgg {
    public int $total = 0; // QPS/最终统计的事件数（成功+失败尝试）
    public int $success = 0;
    public int $fail = 0;
    // 尝试级：连接与查询耗时
    public array $connectAttempt = ['sum' => 0.0, 'min' => PHP_FLOAT_MAX, 'max' => 0.0, 'count' => 0];
    public array $queryAttempt   = ['sum' => 0.0, 'min' => PHP_FLOAT_MAX, 'max' => 0.0, 'count' => 0];
    // 请求级累计连接耗时（最终事件上报）
    public array $requestConnectTotal = ['sum' => 0.0, 'min' => PHP_FLOAT_MAX, 'max' => 0.0, 'count' => 0];
    // 建连时延（成功建连的单次尝试）样本聚合
    public int $connectLatencyCount = 0;
    public float $connectLatencySum = 0.0;
    public float $connectLatencyMin = PHP_FLOAT_MAX;
    public float $connectLatencyMax = 0.0;
    public array $connectLatencySamples = [];
        // [修复网络时延N/A] 网络时延双缓冲与全局累计
        public float $intervalNetLatencyCurrSum = 0.0;
        public int $intervalNetLatencyCurrCount = 0;
        public float $intervalNetLatencyLastSum = 0.0;
        public int $intervalNetLatencyLastCount = 0;
        public float $globalNetLatencySum = 0.0;
        public int $globalNetLatencyCount = 0;
        // 新增：网络探测预热与全局采集开关
        public int $netProbeWarmupRemaining = 2;
        public bool $netProbeCollectGlobal = false;
        // 新增：全局网络时延样本（仅采集全局时追加）
        public array $netLatencySamples = [];
    // 连接时间（生命周期，含建连）：最终成功请求的 connection_duration 聚合
    public array $connectionLifetime = ['sum' => 0.0, 'min' => PHP_FLOAT_MAX, 'max' => 0.0, 'count' => 0];
    // [新增] 查询时间样本（用于 p95 计算）
    public array $querySamples = [];
    // [新增] 连接生命周期样本（用于 p95 计算）
    public array $connectionLifetimeSamples = [];
    // 失败详情：按错误类别分组计数（类别为 classify_by_message 返回值）
    public array $errors  = [];
    // QPS 相关（区间）
    public int $lastCount = 0;
    public int $maxQps    = 0;
    public int $interval_success = 0;
    public int $interval_fail = 0;
    public int $interval_connected = 0; // 本区间建连成功次数（区间事件计数）
    public int $interval_backoff = 0;   // 该区间内执行退避的次数（事件总数口径）
    public int $interval_backoff_requests = 0; // 该区间“含退避的请求数”（基于本区间到达的 final 事件）
    // 在途计数（长生命周期，不重置）
    public int $inflight_connect = 0;   // 当前处于连接过程的尝试数
    public int $inflight_query   = 0;   // 当前处于查询过程的尝试数
    public int $connected_current = 0;  // 当前已建立连接数（口径：与 inflight_query 保持一致）
    // 退避累计统计（最终统计）
    public int $backoff_events_total = 0;    // 累计退避事件总数（跨所有请求）
    public int $backoff_requests_count = 0;  // 累计“含退避的请求”数量（final 事件中 req_backoff_count>0）
    public int $backoff_count_sum = 0;       // 累计“每请求退避次数”的总和（final 事件汇总）
    public int $backoff_count_max = 0;       // 最大每请求退避次数（final 事件）
    public int $final_requests_total = 0;    // 累计最终事件数（用于每请求退避次数平均/占比）
    // 失败日志
    private bool $failLogEnabled = false;
    private ?string $failLogPath = null;
    private string $failLogFormat = 'text'; // text|jsonl
    private $failLogFp = null;
    private bool $failLogReopenTried = false;
    private int $cfgRetries = 0; // 配置：每请求最大重试次数（用于退避统计打印）
    private int $cfgBackoffMs = 0; // 配置：退避毫秒（用于退避统计打印）
    private int $targetConcurrency = 0; // 目标并发
    private int $debug = 0; // 调试级别
    private int $benchStartTs = 0; // 压测开始时间戳（秒）
    // [新增] 失败日志轮转状态
    private int $failLogCurSize = 0;       // 当前日志文件累计字节数
    private int $failLogPartIndex = 0;     // 轮转分片序号（从 0 开始；0 表示首个用户指定文件）
    private ?string $failLogBasePath = null; // 基础路径（首个文件名），用于生成 part 文件
    // [新增] 网络时延标签（打印用）
    public string $netProbeLabel = '';
    public function __construct(bool $failLogEnabled = false, ?string $failLogPath = null, string $failLogFormat = 'text', int $cfgRetries = 0, int $cfgBackoffMs = 0, int $debug = 0, int $targetConcurrency = 0, string $netProbeLabel = '') {
        $this->failLogEnabled = $failLogEnabled;
        $this->failLogPath = $failLogPath;
        $this->failLogFormat = ($failLogFormat === 'jsonl') ? 'jsonl' : 'text';
        $this->cfgRetries = $cfgRetries;
        $this->cfgBackoffMs = $cfgBackoffMs;
        $this->debug = $debug;
        $this->targetConcurrency = $targetConcurrency;
        $this->netProbeLabel = $netProbeLabel;
        if ($this->failLogEnabled && is_string($this->failLogPath) && $this->failLogPath !== '') {
            $this->openFailLog();
        }
        // 初始化建连时延（成功）统计
        $this->connectLatencyCount = 0;
        $this->connectLatencySum = 0.0;
        $this->connectLatencyMin = PHP_FLOAT_MAX;
        $this->connectLatencyMax = 0.0;
        $this->connectLatencySamples = [];
    }
    private function openFailLog(): void {
        try {
            $this->failLogFp = @fopen($this->failLogPath, 'ab');
            if (!$this->failLogFp) {
                $this->failLogEnabled = false; @fwrite(STDERR, "[WARN] 无法打开失败日志文件：{$this->failLogPath}\n");
            } else {
                // [新增] 初始化轮转状态
                $this->failLogBasePath = $this->failLogPath;
                $sz = 0;
                try { if (is_file($this->failLogPath)) { $fs = @filesize($this->failLogPath); if (is_int($fs)) $sz = $fs; } } catch (\Throwable $e) { /* 忽略 */ }
                $this->failLogCurSize = max(0, (int)$sz);
                $this->failLogPartIndex = 0; // 首个文件不带 part 后缀
            }
        } catch (\Throwable $e) {
            $this->failLogEnabled = false; @fwrite(STDERR, "[WARN] 打开失败日志异常：" . $e->getMessage() . "\n");
        }
    }
    // [新增] 构建轮转后的 part 文件路径：首个文件沿用用户指定；后续追加 .partNN 再加扩展
    private function buildPartPath(string $base, int $index): string {
        if ($index <= 0) return $base;
        $pi = pathinfo($base);
        $dir = isset($pi['dirname']) && $pi['dirname'] !== '' ? ($pi['dirname'] . DIRECTORY_SEPARATOR) : '';
        $filename = $pi['filename'] ?? ($pi['basename'] ?? 'bench_fail');
        $ext = isset($pi['extension']) && $pi['extension'] !== '' ? ('.' . $pi['extension']) : '';
        $suffix = '.part' . sprintf('%02d', $index);
        return $dir . $filename . $suffix . $ext;
    }
    // [新增] 执行日志轮转：关闭当前句柄，创建下一个 part 文件
    private function rotateFailLog(): void {
        try {
            if ($this->failLogFp) { @fflush($this->failLogFp); @fclose($this->failLogFp); $this->failLogFp = null; }
        } catch (\Throwable $e) { /* 容错关闭 */ }
        $this->failLogPartIndex++;
        $nextPath = $this->buildPartPath($this->failLogBasePath ?? ($this->failLogPath ?? 'bench_fail'), $this->failLogPartIndex);
        $this->failLogPath = $nextPath;
        try {
            $this->failLogFp = @fopen($this->failLogPath, 'ab');
            if (!$this->failLogFp) {
                // 重开一次
                $this->failLogFp = @fopen($this->failLogPath, 'ab');
                if (!$this->failLogFp) { $this->failLogEnabled = false; @fwrite(STDERR, "[WARN] 失败日志轮转后无法打开文件，已禁用失败日志：{$this->failLogPath}\n"); return; }
            }
            $this->failLogCurSize = 0; // 新文件大小归零
            @fwrite(STDERR, "[INFO] 失败日志已轮转：{$this->failLogPath}\n");
        } catch (\Throwable $e) {
            $this->failLogEnabled = false; @fwrite(STDERR, "[WARN] 失败日志轮转异常，已禁用失败日志：" . $e->getMessage() . "\n");
        }
    }
    public function markStartTs(int $ts): void { $this->benchStartTs = $ts; }
    public function update(array $ev): void {
        // 非计数事件：connect_begin/connect_success/backoff/cleanup
        $stage = (string)($ev['stage'] ?? '');
        $hasOk = array_key_exists('ok', $ev);
        // 在途计数维护
        if ($stage === 'net_probe') { 
            // [修复网络时延N/A] 网络时延事件（不计入QPS）
            $okEv = (bool)($ev['ok'] ?? false);
            if ($okEv && isset($ev['latency_sec'])) {
                $lat = (float)$ev['latency_sec'];
                // 区间双缓冲：始终累加到 Curr
                $this->intervalNetLatencyCurrSum += $lat;
                $this->intervalNetLatencyCurrCount++;
                // 全局累计与样本：仅在预热结束后开启
                if ($this->netProbeCollectGlobal) {
                    $this->globalNetLatencySum += $lat;
                    $this->globalNetLatencyCount++;
                    $this->netLatencySamples[] = $lat;
                }
            } else {
                // 失败样本不累加；调试模式下可轻量提示（此处不做输出）
            }
            return;
        }
        if ($stage === 'connect_begin') { $this->inflight_connect++; return; }
        if ($stage === 'connect_success') { $this->interval_connected++; if ($this->inflight_connect > 0) $this->inflight_connect--; $this->inflight_query++; $this->connected_current = $this->inflight_query; // 进入查询在途，当前已建立连接数与在途查询一致
            // attempt 连接耗时样本（全体尝试口径）
            $ct = (float)($ev['attempt_connect_time'] ?? ($ev['connect_time'] ?? 0.0));
            $this->connectAttempt['sum'] += $ct; $this->connectAttempt['min'] = min($this->connectAttempt['min'], $ct); $this->connectAttempt['max'] = max($this->connectAttempt['max'], $ct); $this->connectAttempt['count']++;
            // 建连时延（成功建连的单次尝试），仅使用 connect_success 携带的 attempt_connect_time
            if (isset($ev['attempt_connect_time']) && is_numeric($ev['attempt_connect_time'])) {
                $t = (float)$ev['attempt_connect_time'];
                $this->connectLatencyCount++;
                $this->connectLatencySum += $t;
                $this->connectLatencyMin = min($this->connectLatencyMin, $t);
                $this->connectLatencyMax = max($this->connectLatencyMax, $t);
                $this->connectLatencySamples[] = $t;
            }
            return; // 不计入 QPS
        }
        if ($stage === 'backoff') { $this->interval_backoff++; $this->backoff_events_total++; return; }
        if ($stage === 'cleanup') {
            $dc = (int)($ev['inflight_connect_delta'] ?? 0); $dq = (int)($ev['inflight_query_delta'] ?? 0);
            $this->inflight_connect = max(0, $this->inflight_connect + $dc);
            $this->inflight_query   = max(0, $this->inflight_query + $dq);
            $this->connected_current = $this->inflight_query;
            return;
        }
        // 计数事件（成功/失败）：connect（失败）或 query（成功/失败）
        $ct = (float)($ev['connect_time'] ?? 0.0);
        $qt = (float)($ev['query_time'] ?? 0.0);
        $ok = (bool)($ev['ok'] ?? false);
        $errno = isset($ev['errno']) ? (int)$ev['errno'] : 0;
        $errMsg = (string)($ev['error'] ?? '');
        // QPS/总计数
        $this->total++;
        if ($ok) { $this->success++; $this->interval_success++; } else { $this->fail++; $this->interval_fail++; }
        // 尝试级耗时统计
        if ($stage === 'connect') {
            // 连接失败样本
            $this->connectAttempt['sum'] += $ct; $this->connectAttempt['min'] = min($this->connectAttempt['min'], $ct); $this->connectAttempt['max'] = max($this->connectAttempt['max'], $ct); $this->connectAttempt['count']++;
            if ($this->inflight_connect > 0) $this->inflight_connect--; // 连接失败，连接在途结束
        } elseif ($stage === 'query') {
            // 查询样本
            $this->queryAttempt['sum'] += $qt; $this->queryAttempt['min'] = min($this->queryAttempt['min'], $qt); $this->queryAttempt['max'] = max($this->queryAttempt['max'], $qt); $this->queryAttempt['count']++;
            // [新增] 记录查询时间样本用于 p95
            $this->querySamples[] = $qt;
            if ($this->inflight_query > 0) $this->inflight_query--; // 查询结束
            $this->connected_current = $this->inflight_query;
        }
        // 请求级累计连接耗时（仅在最终成功或最终失败事件中携带，支持连接或查询的最终事件）
        if (!empty($ev['final']) && isset($ev['request_connect_total'])) {
            $rct = (float)$ev['request_connect_total'];
            $this->requestConnectTotal['sum'] += $rct;
            $this->requestConnectTotal['min'] = min($this->requestConnectTotal['min'], $rct);
            $this->requestConnectTotal['max'] = max($this->requestConnectTotal['max'], $rct);
            $this->requestConnectTotal['count']++;
        }
        // 连接时间（生命周期，含建连）：仅消费最终成功事件携带的 connection_duration
        if (!empty($ev['final']) && $ok && isset($ev['connection_duration'])) {
            $cd = (float)$ev['connection_duration'];
            $this->connectionLifetime['sum'] += $cd;
            $this->connectionLifetime['min'] = min($this->connectionLifetime['min'], $cd);
            $this->connectionLifetime['max'] = max($this->connectionLifetime['max'], $cd);
            $this->connectionLifetime['count']++;
            // [新增] 记录连接生命周期样本用于 p95
            $this->connectionLifetimeSamples[] = $cd;
        }
        // 最终事件：累计 per-request 退避统计与最终请求数
        if (!empty($ev['final'])) {
            $this->final_requests_total++;
            $rb = isset($ev['req_backoff_count']) ? (int)$ev['req_backoff_count'] : 0;
            if ($rb > 0) {
                $this->interval_backoff_requests++;
                $this->backoff_requests_count++;
                $this->backoff_count_sum += $rb;
                $this->backoff_count_max = max($this->backoff_count_max, $rb);
            }
        }
        // 错误详情与失败日志
        if (!$ok) {
            $cat = classify_by_message($errno, $errMsg, $stage);
            $this->errors[$cat] = ($this->errors[$cat] ?? 0) + 1;
            $this->writeFailLog($ev);
        }
    }
    private function writeFailLog(array $ev): void {
        if (!$this->failLogEnabled || !$this->failLogFp) return;
        try {
            // 句柄健壮性检查
            $fp = $this->failLogFp;
            $needReopen = false;
            if (!is_resource($fp)) { $needReopen = true; }
            else {
                $meta = @stream_get_meta_data($fp);
                if (!is_array($meta) || !empty($meta['eof']) || (isset($meta['stream_type']) && $meta['stream_type'] !== 'STDIO')) {
                    // 尝试根据 meta 判断异常（谨慎处理）
                }
            }
            if ($needReopen && !$this->failLogReopenTried) {
                $this->failLogReopenTried = true;
                $this->openFailLog();
                if (!$this->failLogFp || !is_resource($this->failLogFp)) { $this->failLogEnabled = false; @fwrite(STDERR, "[WARN] 失败日志句柄失效，重开失败，已禁用日志\n"); return; }
            }
            $line = '';
            $tsStr = ts();
            $stage = (string)($ev['stage'] ?? '');
            $driver = (string)($ev['driver'] ?? '');
            $host = (string)($ev['host'] ?? '');
            $port = (int)($ev['port'] ?? 0);
            $user = (string)($ev['user'] ?? '');
            $db   = (string)($ev['dbname'] ?? '');
            $sql  = (string)($ev['sql'] ?? '');
            $errno = (int)($ev['errno'] ?? 0);
            $errRaw = (string)($ev['error'] ?? '');
            $errMsg = normalize_error_message($errRaw);
            $ct = (float)($ev['connect_time'] ?? 0.0);
            $qt = (float)($ev['query_time'] ?? 0.0);
            $attempt = (int)($ev['attempt'] ?? 0);
            if ($this->failLogFormat === 'jsonl') {
                $rec = [
                    'ts' => $tsStr,
                    'stage' => $stage,
                    'driver' => $driver,
                    'host' => $host,
                    'port' => $port,
                    'user' => $user,
                    'db' => $db,
                    'sql' => $sql,
                    'errno' => $errno,
                    'message' => $errMsg,
                    'message_raw' => $errRaw,
                    'connect_time' => $ct,
                    'query_time' => $qt,
                    'attempt' => $attempt,
                ];
                $line = json_encode($rec, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            } else {
                $op = ($stage === 'connect') ? ("target={$host}:{$port} user={$user} db={$db}") : ("sql=" . trim($sql));
                $line = "[{$tsStr}] stage={$stage} driver={$driver} attempt={$attempt} {$op} errno={$errno} message=" . ($errMsg !== '' ? $errMsg : '-') .
                    sprintf(" connect=%.6fs", $ct) . ($qt > 0 ? sprintf(" query=%.6fs", $qt) : '');
            }
            // [新增] 写入前检查是否需要轮转（按 entry 字节数）
            $entrySize = strlen($line) + strlen(PHP_EOL);
            if ($this->failLogCurSize + $entrySize > FAIL_LOG_MAX_BYTES) {
                $this->rotateFailLog();
                if (!$this->failLogEnabled || !$this->failLogFp) return; // 轮转失败已禁用
            }
            @fwrite($this->failLogFp, $line . PHP_EOL);
            $this->failLogCurSize += $entrySize;
        } catch (\Throwable $e) {
            // [修改] 异常容错：尝试一次重开；失败则禁用失败日志
            try {
                if ($this->failLogFp) { @fclose($this->failLogFp); }
                $this->openFailLog();
                if ($this->failLogFp) {
                    @fwrite($this->failLogFp, ($line ?? '') . PHP_EOL);
                    $this->failLogCurSize += strlen($line ?? '') + strlen(PHP_EOL);
                    return;
                }
            } catch (\Throwable $e2) { /* 忽略 */ }
            $this->failLogEnabled = false; @fwrite(STDERR, "[WARN] 失败日志写入异常，已禁用：" . $e->getMessage() . "\n");
        }
    }
    public function closeLog(): void {
        try {
            if ($this->failLogFp) { @fflush($this->failLogFp); @fclose($this->failLogFp); $this->failLogFp = null; }
        } catch (\Throwable $e) {
            // 最后尝试一次关闭
            try { if ($this->failLogFp) { @fclose($this->failLogFp); } } catch (\Throwable $e2) { /* 忽略 */ }
        }
    }
    public function qpsTick(): void {
        $cur = $this->total;
        $qps = $cur - $this->lastCount; // 区间总事件数（成功+失败）
        $this->maxQps = max($this->maxQps, $qps);
        $this->lastCount = $cur;
        $active_workers = $this->inflight_connect + $this->inflight_query;
        $netLabel = ($this->intervalNetLatencyLastCount > 0) ? sprintf('%.6fs', ($this->intervalNetLatencyLastSum / max(1, $this->intervalNetLatencyLastCount))) : 'N/A';
        $line = "[" . date('Y-m-d H:i:s') . "] QPS：{$qps} | 最大QPS：{$this->maxQps} | 已建立连接数：{$this->connected_current} | 在途连接：{$this->inflight_connect} | 在途查询：{$this->inflight_query} | 退避事件：{$this->interval_backoff} | 成功：{$this->interval_success} | 失败：{$this->interval_fail} | 活跃协程：{$active_workers}";
        // 预热阶段或无样本：不打印网络时延字段
        if ($this->intervalNetLatencyLastCount > 0 && $this->netProbeWarmupRemaining <= 0) {
            $line .= " | 网络时延（{$this->netProbeLabel}）：{$netLabel}";
        }
        if ($this->debug >= 1 && $active_workers !== $this->targetConcurrency) {
            $drift = $active_workers - $this->targetConcurrency;
            $line .= " | drift=" . $drift;
        }
        println($line);
        // [修复网络时延N/A] 双缓冲翻转：打印使用上一区间后，last<-curr，再清零 curr
        $this->intervalNetLatencyLastSum = $this->intervalNetLatencyCurrSum;
        $this->intervalNetLatencyLastCount = $this->intervalNetLatencyCurrCount;
        $this->intervalNetLatencyCurrSum = 0.0;
        $this->intervalNetLatencyCurrCount = 0;
        // 预热控制：翻转后递减一次；减到 0 时开始采集全局
        if ($this->netProbeWarmupRemaining > 0) {
            $this->netProbeWarmupRemaining--;
            if ($this->netProbeWarmupRemaining === 0) { $this->netProbeCollectGlobal = true; }
        }
        $this->interval_success = 0; $this->interval_fail = 0; $this->interval_connected = 0; $this->interval_backoff = 0; $this->interval_backoff_requests = 0;
    }
    public function printFinal(): void {
        println("----------------------------------------");
        println("最终统计：");
        // [修改] 新增成功率字段（总请求=成功+失败；为0时显示N/A）
        println("总请求：{$this->total} | 成功：{$this->success} | 失败：{$this->fail} | 成功率：" . ($this->total > 0 ? sprintf('%.2f%%', ($this->success * 100.0 / $this->total)) : 'N/A'));
        $elapsedSec = max(1, time() - ($this->benchStartTs > 0 ? $this->benchStartTs : time()));
        $avgQps = $this->total / $elapsedSec;
        // 网络时延最终统计：avg 与 p95（样本为空则均为 N/A）
        $nsCnt = count($this->netLatencySamples);
        if ($nsCnt <= 0) {
            $netStatStr = "avg_lat=N/A p95=N/A";
        } else {
            $avgLat = $this->globalNetLatencySum / max(1, $this->globalNetLatencyCount);
            $samples = $this->netLatencySamples;
            sort($samples);
            $p95Index = (int)ceil(0.95 * $nsCnt) - 1;
            if ($p95Index < 0) { $p95Index = 0; }
            if ($p95Index >= count($samples)) { $p95Index = count($samples) - 1; }
            $p95Lat = $samples[$p95Index];
            $netStatStr = sprintf("avg_lat=%.6fs p95=%.6fs", $avgLat, $p95Lat);
        }
        println("平均QPS：" . sprintf("%.2f", $avgQps) . "（总请求/总时长）" . " | 网络时延：" . $netStatStr);
        if (!empty($this->errors)) {
            arsort($this->errors);
            println("失败详情：");
            foreach ($this->errors as $msg => $cntErr) { println("{$msg}: {$cntErr}"); }
        }
        // attempt_connect_time
        $c_cnt = max(1, $this->connectAttempt['count']);
        $c_avg = $this->connectAttempt['sum'] / $c_cnt;
        println(sprintf("尝试级连接时间：sum=%.6fs min=%.6fs max=%.6fs avg=%.6fs",
            $this->connectAttempt['sum'],
            $this->connectAttempt['min'] === PHP_FLOAT_MAX ? 0.0 : $this->connectAttempt['min'],
            $this->connectAttempt['max'], $c_avg));
        // 请求级累计连接时间
        $r_cnt = max(1, $this->requestConnectTotal['count']);
        $r_avg = $this->requestConnectTotal['sum'] / $r_cnt;
        println(sprintf("请求级累计连接时间：sum=%.6fs min=%.6fs max=%.6fs avg=%.6fs",
            $this->requestConnectTotal['sum'],
            $this->requestConnectTotal['min'] === PHP_FLOAT_MAX ? 0.0 : $this->requestConnectTotal['min'],
            $this->requestConnectTotal['max'], $r_avg));
        // [修改] 查询时间：增加 p95，并在空样本时打印 N/A
        $q_cnt = $this->queryAttempt['count'];
        if ($q_cnt <= 0) {
            println("查询时间：sum=0.000000s min=N/A max=N/A avg=N/A p95=N/A");
        } else {
            $q_avg = $this->queryAttempt['sum'] / $q_cnt;
            $samples = $this->querySamples;
            sort($samples);
            $p95Index = (int)ceil(0.95 * $q_cnt) - 1;
            if ($p95Index < 0) { $p95Index = 0; }
            if ($p95Index >= count($samples)) { $p95Index = count($samples) - 1; }
            $p95 = $samples[$p95Index];
            println(sprintf("查询时间：sum=%.6fs min=%.6fs max=%.6fs avg=%.6fs p95=%.6fs",
                $this->queryAttempt['sum'],
                $this->queryAttempt['min'] === PHP_FLOAT_MAX ? 0.0 : $this->queryAttempt['min'],
                $this->queryAttempt['max'],
                $q_avg,
                $p95
            ));
        }
        // 建连时延（成功样本）：p95/min/max/avg
        if ($this->connectLatencyCount > 0) {
            $avgLat = $this->connectLatencySum / $this->connectLatencyCount;
            $samples = $this->connectLatencySamples;
            sort($samples);
            $p95Index = (int)ceil(0.95 * $this->connectLatencyCount) - 1;
            if ($p95Index < 0) { $p95Index = 0; }
            if ($p95Index >= count($samples)) { $p95Index = count($samples) - 1; }
            $p95 = $samples[$p95Index];
            println(sprintf("建连时延（成功）：p95=%.6fs min=%.6fs max=%.6fs avg=%.6fs",
                $p95,
                $this->connectLatencyMin === PHP_FLOAT_MAX ? 0.0 : $this->connectLatencyMin,
                $this->connectLatencyMax,
                $avgLat
            ));
        } else {
            println("建连时延（成功）：p95=N/A min=N/A max=N/A avg=N/A");
        }
        // [修改] 连接时间（生命周期，含建连）：增加 p95，并在空样本时打印 N/A
        $cl_cnt = $this->connectionLifetime['count'];
        if ($cl_cnt <= 0) {
            println("连接时间（生命周期，含建连）：sum=0.000000s min=N/A max=N/A avg=N/A p95=N/A");
        } else {
            $cl_avg = $this->connectionLifetime['sum'] / $cl_cnt;
            $samples = $this->connectionLifetimeSamples;
            sort($samples);
            $p95Index = (int)ceil(0.95 * $cl_cnt) - 1;
            if ($p95Index < 0) { $p95Index = 0; }
            if ($p95Index >= count($samples)) { $p95Index = count($samples) - 1; }
            $p95 = $samples[$p95Index];
            println(sprintf("连接时间（生命周期，含建连）：sum=%.6fs min=%.6fs max=%.6fs avg=%.6fs p95=%.6fs",
                $this->connectionLifetime['sum'],
                $this->connectionLifetime['min'] === PHP_FLOAT_MAX ? 0.0 : $this->connectionLifetime['min'],
                $this->connectionLifetime['max'],
                $cl_avg,
                $p95
            ));
        }
        // 退避统计（明确区分事件总数 vs 每请求退避次数）
        println("退避统计：");
        println("  退避事件总数：{$this->backoff_events_total}");
        $finalTotal = $this->final_requests_total;
        $ratio = ($finalTotal > 0) ? ($this->backoff_requests_count * 100.0 / $finalTotal) : 0.0;
        println(sprintf("  含退避的请求数：%d / 占比：%.2f%%（基于最终事件）", $this->backoff_requests_count, $ratio));
        $avgAll = ($finalTotal > 0) ? ($this->backoff_count_sum / $finalTotal) : 0.0;
        $avgOnly = ($this->backoff_requests_count > 0) ? ($this->backoff_count_sum / $this->backoff_requests_count) : 0.0;
        println(sprintf("  每请求平均退避次数（全体/仅含退避）：%.4f / %.4f", $avgAll, $avgOnly));
        println("  每请求最大退避次数:" . $this->backoff_count_max);
        println(sprintf("  当前配置：retries=%d, backoff-ms=%d", $this->cfgRetries, $this->cfgBackoffMs));
        // 在途计数收敛检查
        if ($this->inflight_connect !== 0 || $this->inflight_query !== 0) {
            println(sprintf("[WARN] 退出时在途计数未归零：inflight_connect=%d inflight_query=%d", $this->inflight_connect, $this->inflight_query));
        }
    }
}
// ---------- 协程主流程 ----------
$running = true;
// [修复退出与统计] 退出状态标记（用于一次性最终打印与停止QPS定时器/探测协程）
$exiting = false; $finalPrinted = false; $GLOBALS['exiting'] = false; $GLOBALS['finalPrinted'] = false;
co_run(function () use ($selectedClient, $clientLabel, $host, $port, $user, $password, $dbname, $sql, $concurrency, $connTimeoutSec, $connTimeoutSecInt, $retries, $backoffMs, $debug, $failLogEnable, $failLogPath, $failLogFormat, $sqlExecTimeout, $sqlTimeoutMode, $maxRunTimeSec, $netProbeMode, $netProbeTimeout, $netProbeLabel, &$pdoClientTimeoutWarned, &$running) {
    // 在创建连接前统一启用 Runtime Hook 与 socket_connect_timeout（针对 pdo/mysqli 回退路径）
    if (in_array($selectedClient, ['pdo','mysqli'], true)) {
        $hookEnabledGlobal = enable_runtime_hook();
        if ($hookEnabledGlobal) { set_socket_connect_timeout($connTimeoutSec); }
    }
    $statChan = make_channel(max(2048, $concurrency * 2)); // 提高容量避免高并发+重试阻塞
    $doneChan = make_channel($concurrency); // 接收 worker 完成信号
    $agg = new StatsAgg($failLogEnable, $failLogPath, $failLogFormat, $retries, $backoffMs, $debug, $concurrency, $netProbeLabel);
    $agg->markStartTs(time());
// [修复退出与统计] 统一优雅退出管线（信号/限时共用）：清理定时器、停止探测、推送cleanup、flush writer、最终统计一次性打印并退出
$graceful_shutdown = function (string $reason = 'signal') use (&$running, &$exiting, &$finalPrinted, $statChan, $doneChan, $agg, $concurrency, &$timerId) {
    if ($exiting) { return; }
    $exiting = true; $GLOBALS['exiting'] = true; $running = false; $GLOBALS['running'] = false; // [修复退出异常] 统一退出标志，停止新请求与后台协程循环
    $running = false; // 停止新请求调度与后台协程循环
    // 清理QPS定时器，避免继续打印区间行
    timer_clear($timerId);
    // 在退出前翻转一次网络时延双缓冲，以便打印最近区间
    $agg->intervalNetLatencyLastSum = $agg->intervalNetLatencyCurrSum;
    $agg->intervalNetLatencyLastCount = $agg->intervalNetLatencyCurrCount;
    $agg->intervalNetLatencyCurrSum = 0.0;
    $agg->intervalNetLatencyCurrCount = 0;
    // 推送 cleanup 事件将在途计数归零（不计入QPS），确保聚合器收敛
    $statChan->push(['stage' => 'cleanup', 'inflight_connect_delta' => -$agg->inflight_connect, 'inflight_query_delta' => -$agg->inflight_query]);
    // 尝试等待并尽量消费剩余事件
    co_sleep(0.2);
    while ($statChan->length() > 0) { $ev = $statChan->pop(); if (is_array($ev)) { $agg->update($ev); } }
    // 关闭失败日志 writer（容错降级）
    $agg->closeLog();
    // 最终统计一次性打印保护
    if (!$finalPrinted) { $finalPrinted = true; $GLOBALS['finalPrinted'] = true; $agg->printFinal(); }
    return; // [修复退出异常] 避免协程内 exit；最终统计已打印后自然返回
};
    // QPS 定时器
    $timerId = timer_tick(1000, function () use ($agg) { if (!empty($GLOBALS['exiting'])) { return; } $agg->qpsTick(); });
    // [修复网络时延N/A] 启动网络时延探测协程（每秒一次）；支持 tcp/icmp/none
    if ($netProbeMode !== 'none') {
        co_create(function () use (&$running, $statChan, $host, $port, $connTimeoutSec, $debug, $netProbeMode, $netProbeTimeout) {
            $warned = false;
            while ($running && empty($GLOBALS['exiting'])) {
                $latSec = null;
                try {
                    if ($netProbeMode === 'tcp') {
                        if (class_exists('OpenSwoole\\Coroutine\\Socket')) {
                            $sock = new \OpenSwoole\Coroutine\Socket(AF_INET, SOCK_STREAM, 0);
                            $begin = function_exists('hrtime') ? hrtime(true) : (int)(microtime(true) * 1e9);
                            $ok = @$sock->connect($host, $port, $connTimeoutSec);
                            $end = function_exists('hrtime') ? hrtime(true) : (int)(microtime(true) * 1e9);
                            if ($ok) { $latSec = ($end - $begin) / 1e9; @$sock->close(); }
                            else { @$sock->close(); }
                        } elseif (class_exists('Swoole\\Coroutine\\Socket')) {
                            $sock = new \Swoole\Coroutine\Socket(AF_INET, SOCK_STREAM, 0);
                            $begin = function_exists('hrtime') ? hrtime(true) : (int)(microtime(true) * 1e9);
                            $ok = @$sock->connect($host, $port, $connTimeoutSec);
                            $end = function_exists('hrtime') ? hrtime(true) : (int)(microtime(true) * 1e9);
                            if ($ok) { $latSec = ($end - $begin) / 1e9; @$sock->close(); }
                            else { @$sock->close(); }
                        } else {
                            $begin = microtime(true);
                            $errno = 0; $errstr = '';
                            $fp = @stream_socket_client("tcp://{$host}:{$port}", $errno, $errstr, $connTimeoutSec);
                            if (is_resource($fp)) { $latSec = microtime(true) - $begin; @fclose($fp); } else { $latSec = null; }
                        }
                    } else { // icmp
                        $latSec = probe_icmp_latency_sec($host, $netProbeTimeout);
                    }
                } catch (\Throwable $e) {
                    $latSec = null;
                }
                if ($latSec !== null) {
                    $statChan->push(['stage' => 'net_probe', 'ok' => true, 'latency_sec' => $latSec]);
                } else {
                    if ($debug >= 1 && !$warned) { @fwrite(STDERR, "[WARN] 网络时延探测失败或超时，样本未计入；将继续重试。\n"); $warned = true; }
                }
                co_sleep(1.0);
            }
        });
    }
    // [新增] 限时退出定时器：到时设置 running=false，走统一优雅退出路径
    if ($maxRunTimeSec > 0.0) {
        $ms = (int)max(1, round($maxRunTimeSec * 1000));
        if (class_exists('OpenSwoole\Timer')) {
            \OpenSwoole\Timer::after($ms, function () use ($graceful_shutdown) { @fwrite(STDERR, "[" . ts() . "] INFO  已到达 max_run_time，开始优雅退出...\n"); $graceful_shutdown('timeout'); });
        } elseif (class_exists('Swoole\Timer')) {
            \Swoole\Timer::after($ms, function () use ($graceful_shutdown) { @fwrite(STDERR, "[" . ts() . "] INFO  已到达 max_run_time，开始优雅退出...\n"); $graceful_shutdown('timeout'); });
        } else {
            co_create(function () use ($maxRunTimeSec, $graceful_shutdown) { co_sleep($maxRunTimeSec); @fwrite(STDERR, "[" . ts() . "] INFO  已到达 max_run_time，开始优雅退出...\n"); $graceful_shutdown('timeout'); });
        }
    }
    // 信号处理（优雅退出）
    process_signal(SIGINT, function () use ($graceful_shutdown) { $graceful_shutdown('signal'); });
    process_signal(SIGTERM, function () use ($graceful_shutdown) { $graceful_shutdown('signal'); });
    // 聚合协程：消费 statChan，直到所有 worker 完成
    co_create(function () use ($statChan, $doneChan, $agg, $concurrency) {
        $doneCnt = 0;
        while (true) {
            while ($statChan->length() > 0) { $ev = $statChan->pop(); if (is_array($ev)) $agg->update($ev); }
            while ($doneChan->length() > 0) { $ok = $doneChan->pop(); if ($ok === 'done') { $doneCnt++; } }
            if ($doneCnt >= $concurrency) break;
            co_sleep(0.01);
        }
    });
    // 工作协程
    for ($i = 0; $i < $concurrency; $i++) {
        co_create(function (int $workerId) use (&$running, $statChan, $doneChan, $selectedClient, $clientLabel, $host, $port, $user, $password, $dbname, $sql, $connTimeoutSec, $connTimeoutSecInt, $retries, $backoffMs, $debug, $sqlExecTimeout, $sqlTimeoutMode, &$pdoClientTimeoutWarned) {
            while ($running) {
                $requestConnectTotal = 0.0; // 请求级累计连接耗时
                $inQuery = false; // 以便退出时清理在途计数
                $inConnect = false; // 连接在途标记
                $reqBackoffCount = 0; // 每请求维度的退避次数（final 事件上报 req_backoff_count）
                // 连接 + 查询，带重试；按尝试计数：每次失败立即推送事件，成功后推送成功事件
                for ($attempt = 0; $attempt <= max(0, $retries); $attempt++) {
                    // 进入连接阶段（维护在途计数）
                    $statChan->push(['stage' => 'connect_begin']);
                    $inConnect = true;
                    $connStart = microtime(true); // 尝试起点（生命周期起点）
                    if ($selectedClient === 'openmysql') {
                        $cli = new \OpenSwoole\Coroutine\MySQL();
                        $connOk = $cli->connect([
                            'host' => $host, 'port' => $port, 'user' => $user, 'password' => $password,
                            'database' => $dbname, 'timeout' => $connTimeoutSec,
                        ]);
                        $connTime = microtime(true) - $connStart;
                        $requestConnectTotal += $connTime;
                        if (!$connOk) {
                            $errMsg = (string)($cli->connect_error ?? '');
                            $errNo  = (int)($cli->connect_errno ?? 0);
                            debug_attempt($debug, $workerId, $attempt, 'connect', false, $errNo, $errMsg, $connTime);
                            // 立即推送失败事件（连接失败）
                            $statChan->push(['stage' => 'connect', 'ok' => false, 'connect_time' => $connTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                            $inConnect = false;
                            @$cli->close();
                            // 退避事件（仅当 retries>0 且 attempt<retries）
                            if ($retries > 0 && $attempt < $retries) {
                                $reqBackoffCount++;
                                $statChan->push(['stage' => 'backoff']);
                                co_sleep($backoffMs / 1000.0);
                            }
                            continue;
                        }
                        // 连接成功事件（不计入 QPS）
                        debug_attempt($debug, $workerId, $attempt, 'connect', true, 0, '', $connTime);
                        $statChan->push(['stage' => 'connect_success', 'attempt_connect_time' => $connTime, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'attempt' => $attempt]);
                        $inConnect = false;
                        // 查询阶段
                        $inQuery = true;
                        // SQL 执行超时两种模式：client=query($sql,$timeout)；server=SET SESSION MAX_EXECUTION_TIME
                        if ($sqlExecTimeout > 0) {
                            if ($sqlTimeoutMode === 'server') {
                                $ms = (int)max(1, round($sqlExecTimeout * 1000));
                                $setOk = $cli->query("SET SESSION MAX_EXECUTION_TIME={$ms}");
                                if ($setOk === false && $debug >= 1) { println("[WARN] MAX_EXECUTION_TIME 设置失败或不支持（OpenSwoole）"); }
                                $qStart = microtime(true);
                                $res = $cli->query($sql); // 不传客户端查询超时
                            } else { // client 模式
                                $qStart = microtime(true);
                                $res = $cli->query($sql, $sqlExecTimeout);
                            }
                        } else {
                            $qStart = microtime(true);
                            $res = $cli->query($sql); // 不限时
                        }
                        $queryTime = microtime(true) - $qStart;
                        if ($res === false) {
                            $errMsg = (string)($cli->error ?? ''); $errNo = (int)($cli->errno ?? 0);
                            debug_attempt($debug, $workerId, $attempt, 'query', false, $errNo, $errMsg, $queryTime);
                            $statChan->push(['stage' => 'query', 'ok' => false, 'connect_time' => $connTime, 'query_time' => $queryTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                            @$cli->close(); $inQuery = false;
                            // 退避事件（仅当 retries>0 且 attempt<retries）
                            if ($retries > 0 && $attempt < $retries) {
                                $reqBackoffCount++;
                                $statChan->push(['stage' => 'backoff']);
                                co_sleep($backoffMs / 1000.0);
                            }
                            continue;
                        } else { @$cli->close(); $inQuery = false; }
                        debug_attempt($debug, $workerId, $attempt, 'query', true, 0, '', $queryTime);
                        // 成功后推送成功事件（final=true，携带 request_connect_total 与连接生命周期时长）
                        $connLifecycleDuration = microtime(true) - $connStart; // 从 connect_begin 起算，含建连+查询+关闭
                        $statChan->push(['stage' => 'query', 'ok' => true, 'connect_time' => $connTime, 'query_time' => $queryTime, 'connection_duration' => $connLifecycleDuration, 'error' => '', 'errno' => 0, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => true, 'request_connect_total' => $requestConnectTotal, 'req_backoff_count' => $reqBackoffCount]);
                        break;
                    } elseif ($selectedClient === 'swoolemysql') {
                        $cli = new \Swoole\Coroutine\MySQL();
                        $connOk = $cli->connect([
                            'host' => $host, 'port' => $port, 'user' => $user, 'password' => $password,
                            'database' => $dbname, 'timeout' => $connTimeoutSec,
                        ]);
                        $connTime = microtime(true) - $connStart;
                        $requestConnectTotal += $connTime;
                        if (!$connOk) {
                            $errMsg = (string)($cli->connect_error ?? '');
                            $errNo  = (int)($cli->connect_errno ?? 0);
                            debug_attempt($debug, $workerId, $attempt, 'connect', false, $errNo, $errMsg, $connTime);
                            $statChan->push(['stage' => 'connect', 'ok' => false, 'connect_time' => $connTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                            $inConnect = false;
                            @$cli->close();
                            if ($retries > 0 && $attempt < $retries) {
                                $reqBackoffCount++;
                                $statChan->push(['stage' => 'backoff']);
                                co_sleep($backoffMs / 1000.0);
                            }
                            continue;
                        }
                        debug_attempt($debug, $workerId, $attempt, 'connect', true, 0, '', $connTime);
                        $statChan->push(['stage' => 'connect_success', 'attempt_connect_time' => $connTime, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'attempt' => $attempt]);
                        $inConnect = false;
                        $inQuery = true;
                        // SQL 执行超时两种模式：client=query($sql,$timeout)；server=SET SESSION MAX_EXECUTION_TIME
                        if ($sqlExecTimeout > 0) {
                            if ($sqlTimeoutMode === 'server') {
                                $ms = (int)max(1, round($sqlExecTimeout * 1000));
                                $setOk = $cli->query("SET SESSION MAX_EXECUTION_TIME={$ms}");
                                if ($setOk === false && $debug >= 1) { println("[WARN] MAX_EXECUTION_TIME 设置失败或不支持（Swoole）"); }
                                $qStart = microtime(true);
                                $res = $cli->query($sql); // 不传客户端查询超时
                            } else { // client 模式
                                $qStart = microtime(true);
                                $res = $cli->query($sql, $sqlExecTimeout);
                            }
                        } else {
                            $qStart = microtime(true);
                            $res = $cli->query($sql); // 不限时
                        }
                        $queryTime = microtime(true) - $qStart;
                        if ($res === false) {
                            $errMsg = (string)($cli->error ?? ''); $errNo = (int)($cli->errno ?? 0);
                            debug_attempt($debug, $workerId, $attempt, 'query', false, $errNo, $errMsg, $queryTime);
                            $statChan->push(['stage' => 'query', 'ok' => false, 'connect_time' => $connTime, 'query_time' => $queryTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                            @$cli->close(); $inQuery = false;
                            if ($retries > 0 && $attempt < $retries) {
                                $reqBackoffCount++;
                                $statChan->push(['stage' => 'backoff']);
                                co_sleep($backoffMs / 1000.0);
                            }
                            continue;
                        } else { @$cli->close(); $inQuery = false; }
                        debug_attempt($debug, $workerId, $attempt, 'query', true, 0, '', $queryTime);
                        $connLifecycleDuration = microtime(true) - $connStart;
                        $statChan->push(['stage' => 'query', 'ok' => true, 'connect_time' => $connTime, 'query_time' => $queryTime, 'connection_duration' => $connLifecycleDuration, 'error' => '', 'errno' => 0, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => true, 'request_connect_total' => $requestConnectTotal, 'req_backoff_count' => $reqBackoffCount]);
                        break;
                    } elseif ($selectedClient === 'pdo') {
                        $hookEnabled = class_exists('OpenSwoole\\Runtime') || class_exists('Swoole\\Runtime');
                        if ($hookEnabled) { set_socket_connect_timeout($connTimeoutSec); }
                        $pdo = null; $qStart = null;
                        try {
                            $dsn = "mysql:host={$host};port={$port};" . ($dbname ? "dbname={$dbname};" : '') . "charset=utf8mb4";
                            $opts = [ \PDO::ATTR_TIMEOUT => $connTimeoutSecInt, \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION, \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_ASSOC ]; // 整数秒超时（ceil 最小 1）
                            $pdo = new \PDO($dsn, $user, $password, $opts);
                            $connTime = microtime(true) - $connStart;
                            $requestConnectTotal += $connTime;
                            debug_attempt($debug, $workerId, $attempt, 'connect', true, 0, '', $connTime);
                            $statChan->push(['stage' => 'connect_success', 'attempt_connect_time' => $connTime, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'attempt' => $attempt]);
                            $inConnect = false;
                            // 查询阶段：两种模式；PDO 不支持客户端查询级超时，client 模式回退到服务端 MAX_EXECUTION_TIME
                            if ($sqlExecTimeout > 0) {
                                if ($sqlTimeoutMode === 'client') {
                                    if ($debug >= 1 && !$pdoClientTimeoutWarned) { println("[WARN] PDO 客户端超时不支持，回退到服务端 MAX_EXECUTION_TIME"); $pdoClientTimeoutWarned = true; }
                                }
                                // server 模式或回退均设置 MAX_EXECUTION_TIME
                                $ms = (int)max(1, round($sqlExecTimeout * 1000));
                                try { $okSet = $pdo->exec("SET SESSION MAX_EXECUTION_TIME={$ms}"); } catch (\Throwable $e2) { $okSet = false; }
                                if ($okSet === false && $debug >= 1) { println("[WARN] MAX_EXECUTION_TIME 设置失败或不支持（PDO）"); }
                            }
                            // 查询阶段（不设置客户端查询超时）
                            $inQuery = true; $qStart = microtime(true);
                            $stmt = $pdo->query($sql);
                            $queryTime = microtime(true) - $qStart;
                            $qOk = ($stmt !== false);
                            if (!$qOk) {
                                $info = $pdo->errorInfo();
                                $errMsg = is_array($info) && isset($info[2]) ? (string)$info[2] : '';
                                $errNo  = is_array($info) && isset($info[1]) && is_numeric($info[1]) ? (int)$info[1] : 0;
                                debug_attempt($debug, $workerId, $attempt, 'query', false, $errNo, $errMsg, $queryTime);
                                $statChan->push(['stage' => 'query', 'ok' => false, 'connect_time' => $connTime, 'query_time' => $queryTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                                $pdo = null; $inQuery = false;
                                if ($retries > 0 && $attempt < $retries) {
                                    $reqBackoffCount++;
                                    $statChan->push(['stage' => 'backoff']);
                                    co_sleep($backoffMs / 1000.0);
                                }
                                continue;
                            } else { $pdo = null; $inQuery = false; }
                            debug_attempt($debug, $workerId, $attempt, 'query', true, 0, '', $queryTime);
                            $connLifecycleDuration = microtime(true) - $connStart;
                            $statChan->push(['stage' => 'query', 'ok' => true, 'connect_time' => $connTime, 'query_time' => $queryTime, 'connection_duration' => $connLifecycleDuration, 'error' => '', 'errno' => 0, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => true, 'request_connect_total' => $requestConnectTotal, 'req_backoff_count' => $reqBackoffCount]);
                            break;
                        } catch (\Throwable $e) {
                            $errMsg = (string)$e->getMessage(); $errNo = extract_errno_from_pdo_exception($e);
                            $now = microtime(true); $connTime = $now - $connStart; $requestConnectTotal += $connTime;
                            debug_attempt($debug, $workerId, $attempt, 'connect', false, $errNo, $errMsg, $connTime);
                            $statChan->push(['stage' => 'connect', 'ok' => false, 'connect_time' => $connTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                            $inConnect = false;
                            if ($retries > 0 && $attempt < $retries) {
                                $reqBackoffCount++;
                                $statChan->push(['stage' => 'backoff']);
                                co_sleep($backoffMs / 1000.0);
                            }
                            continue;
                        }
                    } elseif ($selectedClient === 'mysqli') {
                        $hookEnabled = class_exists('OpenSwoole\\Runtime') || class_exists('Swoole\\Runtime');
                        if ($hookEnabled) { set_socket_connect_timeout($connTimeoutSec); }
                        $mysqli = null; $res = null;
                        try {
                            $mysqli = mysqli_init();
                            // 注：mysqli/PDO 的超时选项为整数秒；避免 0.3s 等被截断为 0，这里统一 ceil 并设最小值为 1
                            @mysqli_options($mysqli, \MYSQLI_OPT_CONNECT_TIMEOUT, $connTimeoutSecInt);
                            if ($sqlExecTimeout > 0 && $sqlTimeoutMode === 'client') { @mysqli_options($mysqli, \MYSQLI_OPT_READ_TIMEOUT, (int)ceil($sqlExecTimeout)); }
                            $ok = @mysqli_real_connect($mysqli, $host, $user, $password, $dbname, $port);
                            $connTime = microtime(true) - $connStart;
                            $requestConnectTotal += $connTime;
                            $inConnect = false; // 无论成败，连接阶段结束（失败路径下也会在下方设为 false）
                            if (!$ok) {
                                $errMsg = (string)(mysqli_connect_error() ?: ''); $errNo = (int)(mysqli_connect_errno() ?: 0);
                                debug_attempt($debug, $workerId, $attempt, 'connect', false, $errNo, $errMsg, $connTime);
                                $statChan->push(['stage' => 'connect', 'ok' => false, 'connect_time' => $connTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                                if ($mysqli instanceof \mysqli) { @mysqli_close($mysqli); }
                                if ($retries > 0 && $attempt < $retries) {
                                    $reqBackoffCount++;
                                    $statChan->push(['stage' => 'backoff']);
                                    co_sleep($backoffMs / 1000.0);
                                }
                                continue;
                            }
                        } catch (\Throwable $e) {
                            $connTime = microtime(true) - $connStart; $requestConnectTotal += $connTime;
                            $errMsg = (string)$e->getMessage(); $errNo = extract_errno_from_message($e->getMessage());
                            debug_attempt($debug, $workerId, $attempt, 'connect', false, $errNo, $errMsg, $connTime);
                            $statChan->push(['stage' => 'connect', 'ok' => false, 'connect_time' => $connTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                            if ($mysqli instanceof \mysqli) { @mysqli_close($mysqli); }
                            if ($retries > 0 && $attempt < $retries) {
                                $reqBackoffCount++;
                                $statChan->push(['stage' => 'backoff']);
                                co_sleep($backoffMs / 1000.0);
                            }
                            continue;
                        }
                        // 连接成功事件
                        debug_attempt($debug, $workerId, $attempt, 'connect', true, 0, '', $connTime);
                        $statChan->push(['stage' => 'connect_success', 'attempt_connect_time' => $connTime, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'attempt' => $attempt]);
                        // 查询阶段
                        // 会话级执行时限（仅 server 模式设置 MAX_EXECUTION_TIME）
                        if ($sqlExecTimeout > 0 && $sqlTimeoutMode === 'server') {
                            $ms = (int)max(1, round($sqlExecTimeout * 1000));
                            $setOk = @mysqli_query($mysqli, "SET SESSION MAX_EXECUTION_TIME={$ms}");
                            if ($setOk === false && $debug >= 1) { println("[WARN] MAX_EXECUTION_TIME 设置失败或不支持（mysqli）"); }
                        }
                        try {
                            $inQuery = true; $qStart = microtime(true);
                            $res = @mysqli_query($mysqli, $sql);
                            $queryTime = microtime(true) - $qStart;
                            if ($res === false) {
                                $errMsg = (string)($mysqli->error ?? ''); $errNo = (int)($mysqli->errno ?? 0);
                                debug_attempt($debug, $workerId, $attempt, 'query', false, $errNo, $errMsg, $queryTime);
                                $statChan->push(['stage' => 'query', 'ok' => false, 'connect_time' => $connTime, 'query_time' => $queryTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                                if ($mysqli instanceof \mysqli) { @mysqli_close($mysqli); }
                                $inQuery = false;
                                if ($retries > 0 && $attempt < $retries) {
                                    $reqBackoffCount++;
                                    $statChan->push(['stage' => 'backoff']);
                                    co_sleep($backoffMs / 1000.0);
                                }
                                continue;
                            } else {
                                if ($res instanceof \mysqli_result) { @mysqli_free_result($res); }
                                if ($mysqli instanceof \mysqli) { @mysqli_close($mysqli); }
                                $inQuery = false;
                            }
                            debug_attempt($debug, $workerId, $attempt, 'query', true, 0, '', $queryTime);
                            $connLifecycleDuration = microtime(true) - $connStart;
                            $statChan->push(['stage' => 'query', 'ok' => true, 'connect_time' => $connTime, 'query_time' => $queryTime, 'connection_duration' => $connLifecycleDuration, 'error' => '', 'errno' => 0, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => true, 'request_connect_total' => $requestConnectTotal, 'req_backoff_count' => $reqBackoffCount]);
                            break;
                        } catch (\Throwable $e) {
                            $queryTime = microtime(true) - $qStart; $inQuery = false;
                            $errMsg = (string)$e->getMessage(); $errNo = extract_errno_from_message($e->getMessage());
                            debug_attempt($debug, $workerId, $attempt, 'query', false, $errNo, $errMsg, $queryTime);
                            $statChan->push(['stage' => 'query', 'ok' => false, 'connect_time' => $connTime, 'query_time' => $queryTime, 'error' => $errMsg, 'errno' => $errNo, 'driver' => $clientLabel, 'host' => $host, 'port' => $port, 'user' => $user, 'dbname' => $dbname, 'sql' => $sql, 'attempt' => $attempt, 'final' => ($attempt >= $retries), 'request_connect_total' => ($attempt >= $retries ? $requestConnectTotal : null), 'req_backoff_count' => ($attempt >= $retries ? $reqBackoffCount : null)]);
                            if (isset($res) && $res instanceof \mysqli_result) { @mysqli_free_result($res); }
                            if ($mysqli instanceof \mysqli) { @mysqli_close($mysqli); }
                            if ($retries > 0 && $attempt < $retries) {
                                $reqBackoffCount++;
                                $statChan->push(['stage' => 'backoff']);
                                co_sleep($backoffMs / 1000.0);
                            }
                            continue;
                        }
                    } else { $errMsg = '未知客户端类型'; break; }
                }
                if (!$running) {
                    // 优雅退出：若存在在途状态，清理归零（不计入 QPS）
                    $cleanupConnectDelta = $inConnect ? -1 : 0; $cleanupQueryDelta = $inQuery ? -1 : 0;
                    $statChan->push(['stage' => 'cleanup', 'inflight_connect_delta' => $cleanupConnectDelta, 'inflight_query_delta' => $cleanupQueryDelta]);
                    break;
                }
            }
            // worker 完成信号
            $doneChan->push('done');
        }, $i);
    }
    // 主协程空转等待退出，聚合协程通过 doneChan 计数完成
    while ($running) { co_sleep(0.2); if (!$running) break; }
    // 清理与最终统计（非信号/限时触发时的兜底；统一一次性保护）
    if (!$exiting) {
        timer_clear($timerId);
        // 等待并尽量消费剩余事件
        co_sleep(0.5);
        while ($statChan->length() > 0) {
            $ev = $statChan->pop();
            if (is_array($ev)) { $agg->update($ev); }
        }
        // 若仍有在途计数，强制归零以避免最终统计 WARN
        if ($agg->inflight_connect !== 0 || $agg->inflight_query !== 0) {
            $agg->update([
                'stage' => 'cleanup',
                'inflight_connect_delta' => -$agg->inflight_connect,
                'inflight_query_delta' => -$agg->inflight_query,
            ]);
        }
        $agg->closeLog();
        if (!$finalPrinted) { $finalPrinted = true; $agg->printFinal(); }
    }
});
exit(0);
