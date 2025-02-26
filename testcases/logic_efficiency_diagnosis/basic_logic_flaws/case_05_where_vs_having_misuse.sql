-- ============================================================================
-- Case 05: WHERE 与 HAVING 使用时机混淆
-- ============================================================================
-- 【问题描述】
--   WHERE 和 HAVING 都用于过滤，但执行时机不同：
--     1. WHERE 在 GROUP BY 之前执行，过滤原始行
--     2. HAVING 在 GROUP BY 之后执行，过滤分组结果
--   混淆二者会导致：
--     - 本应先过滤再聚合的条件放到 HAVING，浪费计算资源且语义错误
--     - 本应聚合后过滤的条件放到 WHERE，直接报语法错误或逻辑不对
--     - 过滤时机不同导致聚合基数不同，指标值不同
--
-- 【易犯场景】
--   1. 把行级过滤条件写到 HAVING，虽然部分引擎支持但语义不同
--   2. 先写 GROUP BY 再补过滤条件，习惯性写到 HAVING
--   3. 想过滤"分组后"的结果但把条件放在 WHERE（会报错或逻辑不对）
--   4. 在 HAVING 中用非聚合列过滤，不同引擎行为不一致
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - WHERE / HAVING 使用时机不当，可能影响聚合结果
--   - 建议行级条件放 WHERE，组级条件放 HAVING
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 5a: 行级过滤条件误放到 HAVING，聚合基数包含了不该有的数据
-- 业务需求：统计失败 app 的用户排行
-- ❌ 错误：result != 0 应在 WHERE 中过滤，放 HAVING 则先聚合所有 app 再过滤
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.platform,
    COUNT(*)                                                AS app_count,
    SUM(CASE WHEN a.result = 0 THEN 1 ELSE 0 END)          AS success_count,
    SUM(CASE WHEN a.result != 0 THEN 1 ELSE 0 END)         AS fail_count,
    AVG(a.end_time - a.start_time)                          AS avg_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.`user`, a.platform
HAVING a.result != 0                                        -- ❌ 行级条件放在了 HAVING
ORDER BY fail_count DESC;

-- ✅ 正确写法：行级条件放到 WHERE
-- WHERE a.dt = '20260308' AND a.result != 0


-- ---------------------------------------------------------------------------
-- Case 5b: 把聚合过滤条件写在 WHERE 中，逻辑错误
-- 业务需求：找出平均耗时超过 5 分钟的 user
-- ❌ 错误：AVG 是聚合函数，不能在 WHERE 中使用
-- ---------------------------------------------------------------------------
SELECT
    a.`user`,
    a.platform,
    COUNT(*)                                                AS app_count,
    AVG(a.end_time - a.start_time)                          AS avg_duration,
    MAX(a.end_time - a.start_time)                          AS max_duration
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND AVG(a.end_time - a.start_time) > 300000               -- ❌ WHERE 中不能用聚合函数
GROUP BY a.`user`, a.platform
ORDER BY avg_duration DESC;

-- ✅ 正确写法：聚合条件放到 HAVING
-- HAVING AVG(a.end_time - a.start_time) > 300000


-- ---------------------------------------------------------------------------
-- Case 5c: HAVING 中过滤非聚合列，不同引擎行为不一致
-- 业务需求：统计各 platform 的失败 job 数量，只看 FAILED 状态
-- ❌ 错误：j.status 不是聚合列，放在 HAVING 中行为不确定
-- ---------------------------------------------------------------------------
SELECT
    a.platform,
    j.status,
    COUNT(*)                                                AS job_count,
    AVG(j.end_time - j.start_time)                          AS avg_job_duration,
    SUM(CASE WHEN j.failed_reason IS NOT NULL THEN 1 ELSE 0 END) AS has_reason_count
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.platform, j.status
HAVING j.status = 'FAILED'                                  -- ❌ 行级过滤条件，应放 WHERE
   AND COUNT(*) > 10
ORDER BY job_count DESC;

-- ✅ 正确写法：行级条件放 WHERE，组级条件放 HAVING
-- WHERE ... AND j.status = 'FAILED'
-- HAVING COUNT(*) > 10


-- ---------------------------------------------------------------------------
-- Case 5d: WHERE 和 HAVING 混淆导致过滤顺序逻辑错误
-- 业务需求：统计 GC 时间异常的 stage（单 task GC > 30s 且 stage 平均 GC > 10s）
-- ❌ 错误：两个条件的过滤层级搞反了
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    COUNT(*)                                                AS task_count,
    AVG(t.gc_time)                                          AS avg_gc_time,
    MAX(t.gc_time)                                          AS max_gc_time,
    SUM(t.gc_time)                                          AS total_gc_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND AVG(t.gc_time) > 10000                                -- ❌ WHERE 中不能用聚合函数 AVG
GROUP BY t.app_id, t.stage_id
HAVING t.gc_time > 30000                                    -- ❌ 行级条件放在 HAVING
ORDER BY avg_gc_time DESC
LIMIT 100;

-- ✅ 正确写法：
-- WHERE t.dt = '20260308' AND t.gc_time > 30000   -- 行级先过滤
-- HAVING AVG(t.gc_time) > 10000                                -- 组级后过滤


-- ---------------------------------------------------------------------------
-- Case 5e: HAVING 过滤导致本应出现的分组被意外排除
-- 业务需求：展示每个 app 的 job 统计，包括"0 个失败 job"的 app
-- ❌ 错误：HAVING 过滤了 failed_count，0 失败的 app 被排除
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    COUNT(j.job_id)                                         AS total_jobs,
    SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END)   AS failed_count,
    SUM(CASE WHEN j.status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS success_count,
    ROUND(SUM(CASE WHEN j.status = 'FAILED' THEN 1 ELSE 0 END) * 100.0
        / COUNT(j.job_id), 2)                               AS fail_rate
FROM spark_analytics.spark_app_metrics a
LEFT JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
WHERE a.dt = '20260308'
GROUP BY a.app_id, a.app_name, a.`user`
HAVING failed_count >= 0                                    -- ❌ 看似无害，但如果改为 > 0 就丢失了全成功的 app
   AND total_jobs > 0                                       -- ❌ LEFT JOIN 的目的就是保留无 job 的 app，这里又过滤掉了
ORDER BY fail_rate DESC;

-- ✅ 正确写法：不需要 HAVING，或只在需要组级过滤时使用
-- 去掉 HAVING total_jobs > 0（如果需要全量 app 报表）
-- 如果只要有 job 的 app，应改用 INNER JOIN 而非 LEFT JOIN + HAVING
