-- ============================================================================
-- Case 01: 分区列（STRING）与整数常量比较导致隐式转换
-- ============================================================================
-- 【问题描述】
--   四张表的分区字段 dt 均为 STRING 类型（格式如 '20260308'）。
--   当研发人员在 WHERE 条件中直接使用整数常量（如 20260308 而非 '20260308'）
--   进行比较时，引擎会对分区列做隐式 CAST(STRING -> BIGINT/DOUBLE)，导致：
--     1. 分区裁剪（Partition Pruning）完全失效，退化为全分区扫描
--     2. 无法转换的分区值（如含非数字字符）会产生 NULL 或报错
--     3. 大范围比较时数值排序与字典序不一致，可能导致结果错误
--
-- 【易犯场景】
--   1. 日期分区值形如 '20260308'，看起来像整数，研发人员习惯省略引号
--   2. 从 Python/Java 代码拼接 SQL 时，变量是 int 类型未做 toString
--   3. 从 BI 工具生成的 SQL 中，日期参数自动转为数值类型
--   4. BETWEEN 范围查询时两侧用整数，看似没问题实则触发全列转换
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - STRING 类型分区列 dt 与 INT/BIGINT 常量比较，发生隐式转换
--   - 分区裁剪可能失效，建议使用字符串常量
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 1a: 等值比较 —— 分区列与整数常量
-- 研发人员查最近一天的失败 app，顺手写了 dt = 20260308
-- 没有加引号，STRING 列会被 CAST 为 DOUBLE/BIGINT 做比较
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.start_time,
    a.end_time,
    a.executor_num,
    a.executor_memory,
    a.executor_cores,
    a.driver_memory,
    a.platform,
    -- 计算任务运行时长（毫秒转秒）
    ROUND((a.end_time - a.start_time) / 1000, 2) AS duration_sec
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = 20260308          -- ❌ 整数常量，应为 '20260308'
  AND a.result != 0                          -- 只看失败任务
  AND a.platform IN ('platform_a', 'platform_b')
ORDER BY a.end_time DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- Case 1b: BETWEEN 范围比较 —— 两侧均为整数
-- 查最近一周的 job 执行情况，BETWEEN 两侧用整数，STRING 分区列被隐式转换
-- 这种写法在开发中极为常见，因为日期看起来就是数字
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.action,
    j.status,
    j.submit_time,
    j.start_time,
    j.end_time,
    j.failed_reason,
    -- 计算 job 排队等待时间
    (j.start_time - j.submit_time)           AS queue_wait_ms,
    -- 计算 job 实际执行时间
    (j.end_time - j.start_time)              AS run_duration_ms
FROM spark_analytics.spark_job_metrics j
WHERE j.dt BETWEEN 20260301 AND 20260308  -- ❌ 整数范围，应为字符串
  AND j.status = 'FAILED'
ORDER BY j.submit_time DESC;


-- ---------------------------------------------------------------------------
-- Case 1c: 大于/小于比较 —— 分区列做数值范围过滤
-- 查历史数据时，用大于号加整数做开放范围过滤
-- 当分区值为纯数字字符串时结果可能偶然正确，但引擎仍做了全列隐式转换
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.stage_attempt_id,
    s.num_tasks,
    s.status,
    s.submit_time,
    s.start_time,
    s.end_time,
    -- 计算 stage 耗时
    (s.end_time - s.start_time)              AS stage_duration_ms,
    -- 计算 stage 中每个 task 的平均耗时
    CASE
        WHEN s.num_tasks > 0
        THEN ROUND((s.end_time - s.start_time) / s.num_tasks, 2)
        ELSE 0
    END                                       AS avg_task_duration_ms
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt > 20260300           -- ❌ 整数，应为 '20260300'
  AND s.dt < 20260309           -- ❌ 整数，应为 '20260309'
  AND s.num_tasks > 100                       -- 只看大 stage
ORDER BY s.num_tasks DESC
LIMIT 500;


-- ---------------------------------------------------------------------------
-- Case 1d: 多表 JOIN 中分区列均与整数比较
-- 四表关联分析场景，每张表的分区条件都用了整数，全部触发隐式转换
-- 在复杂查询中更容易忽视这类问题
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    j.job_id,
    j.status                                  AS job_status,
    s.stage_id,
    s.num_tasks,
    t.task_id,
    t.status                                  AS task_status,
    t.task_run_time,
    t.gc_time,
    -- 计算 GC 时间占比
    CASE
        WHEN t.task_run_time > 0
        THEN ROUND(t.gc_time * 100.0 / t.task_run_time, 2)
        ELSE 0
    END                                       AS gc_ratio_pct
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE a.dt = 20260308           -- ❌ 四处分区条件全部用整数
  AND j.dt = 20260308           -- ❌
  AND s.dt = 20260308           -- ❌
  AND t.dt = 20260308           -- ❌
  AND a.result != 0
ORDER BY t.task_run_time DESC
LIMIT 100;
