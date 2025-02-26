-- ============================================================================
-- Case 07: JOIN 关联语义偏差
-- ============================================================================
-- 【问题描述】
--   JOIN 类型的选择直接决定了结果集的完整性和语义。常见错误包括：
--     1. 业务需要"有 job 的 app"但用 LEFT JOIN，多算了无 job 的 app
--     2. 业务需要"所有 app"但用 INNER JOIN，丢掉了无 job 的 app
--     3. LEFT JOIN 后 WHERE 过滤右表，等价于变成了 INNER JOIN
--     4. 多表 JOIN 顺序不当导致数据意外丢失
--     5. FULL OUTER JOIN 与业务"交集"需求不匹配
--
-- 【易犯场景】
--   1. 查"有 job 记录的 app 的统计信息"，用 LEFT JOIN 多了无 job 的 app
--   2. 想保留所有 app（含没有 job 的），但 INNER JOIN 把它们丢掉了
--   3. LEFT JOIN job 表后，WHERE 中过滤了 job.status，把无 job 的也过滤了
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - JOIN 类型可能与业务需求不匹配
--   - LEFT JOIN 后 WHERE 过滤右表字段，语义等价于 INNER JOIN
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 7a: 业务要"有 job 的 app"但用 LEFT JOIN 多算了无 job 的
-- 业务需求：统计每个"有 Job 运行记录"的 App 的 Job 数量和成功率
-- ❌ 错误：使用 LEFT JOIN，没有 job 的 app 也被包含进来，
--   其 job_count=0, success_rate=NULL，干扰统计
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    COUNT(j.job_id)                                          AS job_count,
    COUNT(CASE WHEN j.`status` = 0 THEN 1 END) * 100.0
        / NULLIF(COUNT(j.job_id), 0)                         AS success_rate
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name;
-- ❌ LEFT JOIN 导致没有 job 的 app 也出现在结果中（job_count=0）
-- ❌ 业务只关心"有 job 的 app"，无 job 的 app 干扰汇总统计
-- ✅ 正确写法：使用 INNER JOIN
-- INNER JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 7b: 业务要"所有 app"但用 INNER JOIN 丢掉了无 job 的
-- 业务需求：查看所有 App 的 Job 执行概况（含没有 Job 的 App）
-- ❌ 错误：使用 INNER JOIN，没有 job 的 app 被丢弃了
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    COUNT(j.job_id)                                          AS job_count
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name, a.`user`, a.`result`;
-- ❌ INNER JOIN 丢失了没有 job 记录的 app
-- ❌ 业务需要看"所有 app"（含没提交 job 的），但这些 app 被过滤了
-- ✅ 正确写法：使用 LEFT JOIN
-- LEFT JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 7c: LEFT JOIN 后 WHERE 条件过滤右表等价于 INNER JOIN
-- 业务需求：查看所有 App 及其失败的 Job 信息（含没有失败 Job 的 App）
-- ❌ 错误：LEFT JOIN 后在 WHERE 中过滤 j.status = -1，
--   没有 job 的 app 的 j.status 为 NULL，被 WHERE 过滤掉了
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.`status`,
    j.failed_reason
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
  AND j.`status` = -1;
-- ❌ LEFT JOIN 保留了所有 app，但 WHERE j.status = -1 把无 job 的 app 过滤了
-- ❌ 等价于 INNER JOIN，违背了"保留所有 app"的初衷
-- ✅ 正确写法：将右表过滤条件放在 ON 子句中
-- LEFT JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id
--     AND a.dt = j.dt
--     AND j.`status` = -1
-- WHERE a.dt = '20260308'


-- ---------------------------------------------------------------------------
-- Case 7d: 多表 JOIN 顺序导致数据丢失
-- 业务需求：查看所有 App 的 Stage 和 Task 信息（含无 Stage 的 App）
-- ❌ 错误：App LEFT JOIN Stage 没问题，但 Stage INNER JOIN Task
--   导致没有 Task 的 Stage 被丢弃，连带无 Stage 的 App 也丢失了
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    s.stage_id,
    COUNT(t.task_id)                                         AS task_count
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name, s.stage_id;
-- ❌ App LEFT JOIN Stage 保留了无 stage 的 app
-- ❌ 但 Stage INNER JOIN Task 要求 stage 必须有 task
-- ❌ 无 stage 的 app 的 s.* 全为 NULL，INNER JOIN task 失败，这些 app 被丢弃
-- ✅ 正确写法：全部用 LEFT JOIN
-- LEFT JOIN spark_analytics.spark_task_metrics t
--     ON s.app_id = t.app_id AND s.stage_id = t.stage_id
--     AND s.dt = t.dt


-- ---------------------------------------------------------------------------
-- Case 7e: FULL OUTER JOIN 与业务"交集"需求不匹配
-- 业务需求：找出"同时在 App 表和 Job 表中都存在"的 app_id
-- ❌ 错误：使用 FULL OUTER JOIN，结果包含了只在一侧存在的 app_id，
--   不是交集而是并集
-- ---------------------------------------------------------------------------
SELECT
    COALESCE(a.app_id, j.app_id)                             AS app_id,
    a.app_name,
    COUNT(j.job_id)                                          AS job_count
FROM spark_analytics.spark_app_metrics a
FULL OUTER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
   OR j.dt = '20260308'
GROUP BY COALESCE(a.app_id, j.app_id), a.app_name;
-- ❌ FULL OUTER JOIN 产生并集，包含"只在 app 表"或"只在 job 表"的记录
-- ❌ 业务要的是交集（两表都有的 app_id），应该用 INNER JOIN
-- ✅ 正确写法：使用 INNER JOIN
-- INNER JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id AND a.dt = j.dt
-- WHERE a.dt = '20260308'
