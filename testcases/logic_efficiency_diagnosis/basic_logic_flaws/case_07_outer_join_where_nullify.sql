-- ============================================================================
-- Case 07: 外连接被 WHERE 条件意外破坏
-- ============================================================================
-- 【问题描述】
--   LEFT JOIN 的目的是保留左表所有行，右表不匹配时补 NULL。
--   但如果在 WHERE 中对右表的列做非 NULL 比较，未匹配的行（NULL）
--   会被过滤掉，LEFT JOIN 实质退化为 INNER JOIN。这是数仓 SQL 中
--   最隐蔽的逻辑疏漏之一：
--     1. SQL 不会报错，执行正常
--     2. 结果集悄悄缩小，难以察觉
--     3. 与 INNER JOIN 的区别只在未匹配行，数据量大时更难验证
--
-- 【易犯场景】
--   1. LEFT JOIN 后在 WHERE 中过滤右表的 status、type 等列
--   2. WHERE 中对右表列做计算或比较（NULL 参与比较结果为 NULL，被过滤）
--   3. 多表 LEFT JOIN 链中，某个右表的过滤条件写在 WHERE 而非 ON
--   4. 重构时把 ON 条件移到 WHERE，忘记 LEFT JOIN 的语义
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - LEFT JOIN 后 WHERE 中引用了右表列，导致外连接退化
--   - 建议将右表过滤条件放到 ON 子句中
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 7a: LEFT JOIN 后 WHERE 过滤右表 status，退化为 INNER JOIN
-- 业务需求：列出所有 app，标记其失败 job（没有失败 job 的 app 也要展示）
-- ❌ 错误：WHERE j.status = 'FAILED' 过滤掉了右表 NULL 行
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    (a.end_time - a.start_time)                             AS app_duration,
    j.job_id,
    j.status                                                AS job_status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
  AND j.status = 'FAILED'                                   -- ❌ 右表列条件放 WHERE，LEFT JOIN 退化
ORDER BY app_duration DESC;

-- ✅ 正确写法：将右表过滤条件放到 ON 中
-- LEFT JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id
--     AND a.dt = j.dt
--     AND j.status = 'FAILED'


-- ---------------------------------------------------------------------------
-- Case 7b: LEFT JOIN 后 WHERE 对右表列做计算比较
-- 业务需求：所有 stage 及其 task 耗时（无 task 的 stage 也要保留）
-- ❌ 错误：WHERE 中对 t.task_run_time 做比较，NULL 行被过滤
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status                                                AS stage_status,
    s.num_tasks,
    (s.end_time - s.start_time)                             AS stage_duration,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_stage_metrics s
LEFT JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE s.dt = '20260308'
  AND t.task_run_time > 10000                               -- ❌ 右表列条件在 WHERE 中，LEFT JOIN 退化
ORDER BY t.task_run_time DESC;

-- ✅ 正确写法：将右表过滤条件放到 ON 中
-- ON s.app_id = t.app_id AND s.stage_id = t.stage_id
--     AND s.dt = t.dt
--     AND t.task_run_time > 10000


-- ---------------------------------------------------------------------------
-- Case 7c: 多表 LEFT JOIN 链中，中间某表的条件写在 WHERE
-- 业务需求：app → job → stage 全链路，保留所有 app
-- ❌ 错误：stage 的 status 条件写在 WHERE，连带破坏了 job 的 LEFT JOIN
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    j.job_id,
    j.status                                                AS job_status,
    s.stage_id,
    s.status                                                AS stage_status,
    s.num_tasks
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
LEFT JOIN spark_analytics.spark_stage_metrics s
    ON j.app_id = s.app_id
    AND j.dt = s.dt
WHERE a.dt = '20260308'
  AND s.status = 'COMPLETE'                                 -- ❌ 右表 stage 的条件在 WHERE
                                                            -- 没有 stage 的 job 被过滤
                                                            -- 连带没有 job 的 app 也被过滤
ORDER BY a.app_id, j.job_id, s.stage_id;

-- ✅ 正确写法：条件放 ON
-- LEFT JOIN spark_analytics.spark_stage_metrics s
--     ON j.app_id = s.app_id
--     AND j.dt = s.dt
--     AND s.status = 'COMPLETE'


-- ---------------------------------------------------------------------------
-- Case 7d: LEFT JOIN + WHERE IS NOT NULL 的反模式
-- 业务需求：找出没有失败 task 的 stage
-- ❌ 错误：先 LEFT JOIN 再 WHERE t.task_id IS NULL 过滤，
--   但同时又加了 t.status = 'FAILED'，两个条件矛盾
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status                                                AS stage_status,
    s.num_tasks,
    (s.end_time - s.start_time)                             AS stage_duration
FROM spark_analytics.spark_stage_metrics s
LEFT JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE s.dt = '20260308'
  AND t.status = 'FAILED'                                   -- ❌ 先过滤了右表，LEFT JOIN 退化
  AND t.task_id IS NULL                                     -- ❌ 与上一行矛盾：status='FAILED' 时 task_id 不可能为 NULL
ORDER BY stage_duration DESC;

-- ✅ 正确写法：将失败条件放在 ON 中，然后用 IS NULL 判断
-- LEFT JOIN spark_analytics.spark_task_metrics t
--     ON s.app_id = t.app_id
--     AND s.stage_id = t.stage_id
--     AND s.dt = t.dt
--     AND t.status = 'FAILED'
-- WHERE s.dt = '20260308'
--   AND t.task_id IS NULL    -- 此时找出的是没有失败 task 的 stage


-- ---------------------------------------------------------------------------
-- Case 7e: LEFT JOIN 后 WHERE 用 COALESCE 掩盖退化问题
-- 开发者用 COALESCE 试图"修复" NULL，但实际修改了语义
-- ❌ 错误：COALESCE(j.status, 'UNKNOWN') != 'FAILED' 把 NULL 变成 UNKNOWN
--   虽然没退化，但语义变了：无 job 的 app 也被标记为"非失败"
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    COALESCE(j.status, 'UNKNOWN')                           AS job_status,     -- 语义变化
    COALESCE(j.failed_reason, 'N/A')                        AS fail_reason,
    COUNT(*)                                                AS row_count
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
  AND COALESCE(j.status, 'UNKNOWN') != 'FAILED'            -- ❌ 将"无 job"和"非失败 job"混为一谈
GROUP BY a.app_id, a.app_name, a.`user`,
         COALESCE(j.status, 'UNKNOWN'),
         COALESCE(j.failed_reason, 'N/A')
ORDER BY row_count DESC;

-- ✅ 正确写法：区分"无 job"和"非失败 job"
-- WHERE a.dt = '20260308'
--   AND (j.status IS NULL OR j.status != 'FAILED')
-- 或：将条件放到 ON 中
