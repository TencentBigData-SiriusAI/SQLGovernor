-- ============================================================================
-- Case 05: CASE WHEN 分支返回类型不一致导致隐式转换
-- ============================================================================
-- 【问题描述】
--   CASE WHEN 表达式要求所有 THEN/ELSE 分支返回统一类型。当不同分支返回
--   不同类型时，引擎会做隐式转换来统一，可能导致：
--     1. 数值转为字符串后排序语义变化
--     2. NULL 处理逻辑异常（缺省 ELSE NULL 的类型推导）
--     3. 嵌套 CASE 中多层类型推导链，结果难以预测
--     4. 下游使用该列做 GROUP BY/JOIN 时产生意外行为
--
-- 【易犯场景】
--   1. THEN 返回数值，ELSE 返回文本描述（如 THEN duration ELSE '未知'）
--   2. 不同分支返回 BIGINT/DOUBLE/STRING 混合值
--   3. 缺少 ELSE 分支时默认 NULL 的类型与 THEN 不一致
--   4. CASE 表达式作为 JOIN key 或 GROUP BY key 时类型混乱
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - CASE WHEN 各分支返回类型不一致，存在隐式类型转换
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 5a: THEN 返回 BIGINT，ELSE 返回 STRING
-- 对 app 的 result 做可读性转换，但混用了数值和字符串
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.spark_version,
    -- ❌ CASE 分支类型混合：BIGINT 和 STRING
    CASE
        WHEN a.result = 0 THEN '成功'                    -- STRING
        WHEN a.result = 1 THEN a.result                  -- ❌ BIGINT
        WHEN a.result > 1 THEN a.result                  -- ❌ BIGINT
        ELSE '未知状态'                                    -- STRING
    END AS result_desc,
    -- ❌ THEN 返回计算值 (BIGINT)，ELSE 返回字符串
    CASE
        WHEN a.end_time > a.start_time
            THEN (a.end_time - a.start_time)             -- BIGINT
        ELSE '无法计算'                                    -- ❌ STRING
    END AS duration_ms,
    -- ❌ 嵌套 CASE 中类型不一致
    CASE a.platform
        WHEN 'platform_a' THEN a.executor_num                 -- BIGINT
        WHEN 'platform_b'    THEN a.executor_cores               -- BIGINT
        ELSE CONCAT('platform_', a.platform)             -- ❌ STRING
    END AS resource_indicator,
    a.start_time,
    a.end_time
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
ORDER BY a.start_time DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- Case 5b: CASE WHEN 作为 GROUP BY key，分支类型混乱
-- 将 stage 按 num_tasks 分桶，但分桶标签混用数值和字符串
-- 后续 GROUP BY 时字典序和数值序不一致导致分桶结果异常
-- ---------------------------------------------------------------------------
SELECT
    -- ❌ 分桶标签混用 INT 和 STRING
    CASE
        WHEN s.num_tasks <= 10   THEN 1                  -- INT
        WHEN s.num_tasks <= 100  THEN 2                  -- INT
        WHEN s.num_tasks <= 1000 THEN 3                  -- INT
        ELSE '大型Stage'                                  -- ❌ STRING
    END AS task_bucket,
    COUNT(*)                                  AS stage_count,
    AVG(s.num_tasks)                          AS avg_tasks,
    MAX(s.num_tasks)                          AS max_tasks,
    SUM(s.num_tasks)                          AS total_tasks,
    -- stage 平均耗时
    AVG(s.end_time - s.start_time)            AS avg_duration_ms,
    -- ❌ CASE 中 THEN 是 DOUBLE，ELSE 是 STRING
    AVG(
        CASE
            WHEN s.end_time > s.start_time
                THEN (s.end_time - s.start_time) * 1.0 / s.num_tasks   -- DOUBLE
            ELSE '0'                                                     -- ❌ STRING
        END
    ) AS avg_task_duration
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
GROUP BY
    CASE
        WHEN s.num_tasks <= 10   THEN 1
        WHEN s.num_tasks <= 100  THEN 2
        WHEN s.num_tasks <= 1000 THEN 3
        ELSE '大型Stage'
    END
ORDER BY task_bucket;


-- ---------------------------------------------------------------------------
-- Case 5c: 多层嵌套 CASE 导致复杂隐式转换链
-- task 级别分析中，多个 CASE 表达式嵌套且各层返回类型不同
-- 这是最难排查的隐式转换场景
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.status,
    t.task_run_time,
    t.gc_time,
    -- ❌ 外层 CASE 混合了内层 CASE 的不同返回类型
    CASE
        WHEN t.task_run_time > 60000 THEN
            CASE
                WHEN t.gc_time > t.task_run_time * 0.5
                    THEN CONCAT('GC严重: ', t.gc_time)   -- STRING
                ELSE t.gc_time                           -- ❌ BIGINT
            END
        WHEN t.task_run_time > 10000 THEN t.task_run_time  -- ❌ BIGINT
        ELSE '正常'                                         -- STRING
    END AS performance_tag,
    -- ❌ CASE 与聚合函数结合，类型更难追踪
    CASE t.status
        WHEN 'SUCCESS' THEN t.executor_cpu_time          -- BIGINT
        WHEN 'FAILED'  THEN t.failed_reason              -- STRING
        ELSE NULL                                         -- NULL 类型推导不确定
    END AS status_detail
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.task_run_time > 5000
ORDER BY t.task_run_time DESC
LIMIT 300;
