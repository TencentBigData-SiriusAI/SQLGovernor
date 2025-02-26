-- ============================================================================
-- Case 02: JOIN 关联键类型不匹配导致隐式转换
-- ============================================================================
-- 【问题描述】
--   多表 JOIN 时，关联字段在不同表中类型不一致（如一侧为 STRING，另一侧为
--   BIGINT），引擎会对其中一侧做隐式类型转换。这会导致：
--     1. 无法利用 Sort-Merge Join 的有序性优化，退化为更慢的 Join 策略
--     2. Shuffle 时 hash 值计算方式不同，导致数据倾斜或结果错误
--     3. STRING '123' 和 BIGINT 123 的 hash 不同，可能丢失匹配行
--     4. 大数据量下性能严重退化
--
-- 【易犯场景】
--   1. 不同表中同名字段类型定义不同（如上游 STRING 下游 BIGINT）
--   2. 子查询中 CAST 了一侧字段但忘了另一侧
--   3. 临时表/视图中字段类型与原表不一致
--   4. 用 CONCAT 拼接的 key 与纯 STRING/BIGINT key 做 JOIN
--
-- 【预期诊断结果】
--   应触发"隐式转换"告警：
--   - JOIN 关联键两侧类型不一致，存在隐式转换风险
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 2a: stage_id (STRING) 与显式 CAST 为 BIGINT 的 stage_id 做 JOIN
-- 研发人员为了"优化"将 stage_id 转为数值类型，但另一侧仍为 STRING
-- 导致 JOIN 双侧 hash 不一致，引擎做隐式转换
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.num_tasks,
    s.status                                     AS stage_status,
    t.task_id,
    t.status                                     AS task_status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time,
    t.result_size,
    -- 计算 task 级别 GC 占比
    ROUND(t.gc_time * 100.0 / GREATEST(t.task_run_time, 1), 2)
                                                  AS gc_pct,
    -- 计算 CPU 利用率
    ROUND(t.executor_cpu_time * 100.0 / GREATEST(t.task_run_time * 1000000, 1), 2)
                                                  AS cpu_util_pct
FROM spark_analytics.spark_stage_metrics s
INNER JOIN spark_analytics.spark_task_metrics t
    ON CAST(s.stage_id AS BIGINT) = t.stage_id   -- ❌ 左侧 CAST 为 BIGINT，右侧为 STRING
    AND s.app_id = t.app_id
WHERE s.dt = '20260308'
  AND t.dt = '20260308'
  AND s.num_tasks > 50
ORDER BY t.task_run_time DESC
LIMIT 200;


-- ---------------------------------------------------------------------------
-- Case 2b: 子查询返回 BIGINT 类型与外层 STRING 类型 JOIN
-- 子查询中对 job_id 做了 COUNT 聚合，返回值是 BIGINT
-- 外层用这个 BIGINT 值与原始 STRING 类型的 job_id 做关联，类型不匹配
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.action,
    j.status,
    j.start_time,
    j.end_time,
    agg.total_stages,
    agg.max_tasks,
    agg.total_task_count,
    -- job 平均每 stage 的 task 数
    ROUND(agg.total_task_count / GREATEST(agg.total_stages, 1), 2)
                                                  AS avg_tasks_per_stage
FROM spark_analytics.spark_job_metrics j
INNER JOIN (
    -- 子查询：聚合 stage 信息，注意 stage_id 做了 COUNT 变为 BIGINT
    SELECT
        app_id,
        COUNT(DISTINCT stage_id)                  AS total_stages,
        MAX(num_tasks)                            AS max_tasks,
        SUM(num_tasks)                            AS total_task_count,
        -- ❌ 将 stage_id 取 MAX 后仍为 STRING，但拼接 app_id + stage_count 变 STRING
        CONCAT(app_id, '_', COUNT(DISTINCT stage_id)) AS job_key
    FROM spark_analytics.spark_stage_metrics
    WHERE dt = '20260308'
    GROUP BY app_id
) agg
    ON j.app_id = agg.app_id
    -- ❌ CONCAT 结果为 STRING，但若外层做数值比较则隐式转换
    AND CAST(j.job_id AS BIGINT) = agg.total_stages  -- ❌ STRING job_id 转 BIGINT 与聚合值比较
WHERE j.dt = '20260308'
ORDER BY agg.total_task_count DESC
LIMIT 100;


-- ---------------------------------------------------------------------------
-- Case 2c: 多表 JOIN 链中中间环节类型不一致
-- app -> job -> stage -> task 四表 JOIN，其中人为对某些 key 做了类型转换
-- 模拟实际开发中从不同来源表拼接时字段类型不统一的情况
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    j.job_id,
    j.action,
    s.stage_id,
    s.num_tasks,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    t.shuffle_read_bytes,
    t.shuffle_write_bytes,
    -- 计算 shuffle 读写比
    ROUND(t.shuffle_write_bytes * 1.0 / GREATEST(t.shuffle_read_bytes, 1), 4)
                                                  AS shuffle_rw_ratio
FROM spark_analytics.spark_app_metrics a
-- ❌ app_id 两侧一致（STRING = STRING），但加了多余的 CAST 使得类型变化
INNER JOIN spark_analytics.spark_job_metrics j
    ON CAST(a.app_id AS VARCHAR(200)) = j.app_id  -- ❌ VARCHAR 与 STRING 可能不同处理
    AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON j.app_id = s.app_id
    -- ❌ 将 stage_ids (STRING逗号分隔) 当做单个 stage_id 来匹配
    AND j.stage_ids = CAST(s.stage_id AS BIGINT)  -- ❌ STRING vs BIGINT
    AND j.dt = s.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = CAST(t.stage_id AS INT)      -- ❌ STRING vs INT
    AND s.dt = t.dt
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY t.task_run_time DESC
LIMIT 50;
