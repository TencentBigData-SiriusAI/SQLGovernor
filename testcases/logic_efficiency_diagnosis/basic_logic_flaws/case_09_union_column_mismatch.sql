-- ============================================================================
-- Case 09: UNION 列顺序/类型/语义不一致
-- ============================================================================
-- 【问题描述】
--   UNION / UNION ALL 要求各分支的列数量相同，但不检查列语义：
--     1. 列顺序搞反：两个分支的列位置对调，数据混乱但不报错
--     2. 类型隐式转换：一个分支是 STRING，另一个是 BIGINT，隐式转换
--     3. 语义不一致：同一列位置放了不同含义的字段
--     4. NULL 填充列对位错误：补的 NULL/常量列与其他分支对不上
--   UNION 的列匹配完全靠位置，不看列名，因此极易出错。
--
-- 【易犯场景】
--   1. 多表合并时 SELECT 列的顺序不一致
--   2. 补齐缺失列时 NULL 占位符放错位置
--   3. 两个分支的同位列一个是 ID 一个是 name，语义完全不同
--   4. 复制第一个分支修改时漏调某列
--
-- 【预期诊断结果】
--   应触发"基础逻辑疏漏"告警：
--   - UNION 各分支列的语义或类型可能不一致
--   - 建议检查列顺序和数据类型
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 9a: 两个分支列顺序对调，数据混乱
-- 分支1 是 (app_id, app_name, user, duration)
-- 分支2 是 (app_id, user, app_name, duration) — user 和 app_name 对调了
-- ❌ 错误：列顺序不同，user 和 app_name 数据交叉
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result = 0

UNION ALL

SELECT
    a.app_id,
    a.`user`,                                               -- ❌ 第二列应该是 app_name，写成了 user
    a.app_name,                                             -- ❌ 第三列应该是 user，写成了 app_name
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260307'
  AND a.result = 0;

-- ✅ 正确写法：保持列顺序一致
-- SELECT app_id, app_name, `user`, duration_ms ...


-- ---------------------------------------------------------------------------
-- Case 9b: NULL 填充列对位错误
-- app 表和 job 表合并，用 NULL 填充缺失列，但位置放错了
-- ❌ 错误：第二个分支的 NULL 填充位置与第一个分支的列不对应
-- ---------------------------------------------------------------------------
SELECT
    'APP'                                                   AS source,
    a.app_id                                                AS entity_id,
    a.app_name                                              AS entity_name,
    a.`user`,
    a.platform,
    (a.end_time - a.start_time)                             AS duration_ms,
    a.result                                                AS status_code,
    CAST(NULL AS STRING)                                    AS extra_info
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'

UNION ALL

SELECT
    'JOB'                                                   AS source,
    j.job_id                                                AS entity_id,
    j.`action`                                              AS entity_name,
    CAST(NULL AS STRING)                                    AS `user`,         -- ❌ user 放在第4列正确
    j.status,                                               -- ❌ 第5列应该是 platform，这里放了 status
    (j.end_time - j.start_time)                             AS duration_ms,
    CAST(NULL AS BIGINT)                                    AS status_code,    -- ❌ 第7列对不上，app 的 result 是 BIGINT
    j.failed_reason                                         AS extra_info
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308';

-- ✅ 正确写法：严格对齐列的语义和类型
-- SELECT 'JOB', j.job_id, j.action, NULL AS user, NULL AS platform,
--        (j.end_time-j.start_time), CAST(0 AS BIGINT), j.failed_reason


-- ---------------------------------------------------------------------------
-- Case 9c: UNION ALL 各分支聚合粒度不一致
-- 分支1 是 app 级聚合，分支2 是 job 级明细，粒度完全不同
-- ❌ 错误：合并不同粒度的数据，统计结果无法解释
-- ---------------------------------------------------------------------------
SELECT
    a.`user`                                                AS dimension,
    'user_app_count'                                        AS metric_name,
    CAST(COUNT(*) AS STRING)                                AS metric_value
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
GROUP BY a.`user`

UNION ALL

SELECT
    j.app_id                                                AS dimension,     -- ❌ 上面是 user 维度，这里是 app_id 维度
    'job_duration'                                          AS metric_name,
    CAST((j.end_time - j.start_time) AS STRING)             AS metric_value   -- ❌ 上面是聚合值，这里是明细值
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308';

-- ✅ 正确写法：确保各分支粒度一致
-- 要么都是聚合结果，要么都是明细数据
-- 分支2 也应该 GROUP BY app_id 后再 UNION


-- ---------------------------------------------------------------------------
-- Case 9d: 类型隐式转换导致数据精度丢失
-- 分支1 的 duration 是 BIGINT，分支2 是 STRING 拼接结果
-- ❌ 错误：同一列一个是数值一个是字符串，隐式转换不确定
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    (s.end_time - s.start_time)                             AS duration_val   -- BIGINT
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'

UNION ALL

SELECT
    t.app_id,
    t.stage_id,
    CONCAT(CAST(t.task_run_time AS STRING), 'ms')           AS duration_val   -- ❌ STRING，类型不匹配
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308';

-- ✅ 正确写法：统一类型
-- 第二个分支也返回 BIGINT：t.task_run_time AS duration_val
