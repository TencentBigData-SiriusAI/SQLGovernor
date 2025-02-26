-- ============================================================================
-- Case 01: OR 与 AND 优先级混淆
-- ============================================================================
-- 【问题描述】
--   SQL 中 AND 的优先级高于 OR。当 WHERE 中 AND 和 OR 混用而缺少括号时，
--   实际执行的逻辑与开发者的预期往往不一致：
--     1. A OR B AND C 实际等价于 A OR (B AND C)，而非 (A OR B) AND C
--     2. 缺少括号时 OR 会"扩大"匹配范围，导致多余的数据被选中
--     3. 多个 OR 与 AND 交叉时，逻辑变得极难人工推理
--     4. 在多表 JOIN 的 WHERE 条件中更容易出错
--
-- 【易犯场景】
--   1. 同时过滤"失败或超时"的 app，AND 和 OR 顺序搞混
--   2. 对多个平台做 OR 条件，同时 AND 其他过滤条件
--   3. 复制粘贴条件时忘记给 OR 子条件加括号
--   4. 代码审查时因缩进对齐误判了逻辑优先级
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - OR 与 AND 混用但缺少括号，逻辑可能与预期不符
--   - 建议对 OR 条件添加明确的括号
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 1a: 基础 OR/AND 优先级错误
-- 业务需求：查找"失败 或 超时"的 app（result != 0 或 duration > 10分钟）
--   且限定平台为 spark
-- ❌ 错误：OR 打破了 AND 的约束，第一个条件不受 platform 限制
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.result,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0                                         -- ❌ 这个条件因 OR 独立生效
  OR (a.end_time - a.start_time) > 600000                   -- ❌ OR 优先级低于 AND
     AND a.platform = 'spark'                               -- 只约束了 OR 右边的条件
ORDER BY duration_ms DESC
LIMIT 100;
-- 实际逻辑：(imp_date='20260308' AND result!=0) OR (duration>600000 AND platform='spark')
-- 预期逻辑：imp_date='20260308' AND (result!=0 OR duration>600000) AND platform='spark'

-- ✅ 正确写法：给 OR 条件加括号
-- WHERE a.dt = '20260308'
--   AND (a.result != 0 OR (a.end_time - a.start_time) > 600000)
--   AND a.platform = 'spark'


-- ---------------------------------------------------------------------------
-- Case 1b: 多平台 OR 过滤与 AND 条件组合错误
-- 业务需求：查找 spark 或 flink 平台上失败的 job
-- ❌ 错误：第二个 OR 条件脱离了前面的 AND 约束
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.failed_reason,
    (j.end_time - j.start_time)                             AS job_duration
FROM spark_analytics.spark_job_metrics j
INNER JOIN spark_analytics.spark_app_metrics a
    ON j.app_id = a.app_id
    AND j.dt = a.dt
WHERE j.dt = '20260308'
  AND j.status = 'FAILED'
  AND a.platform = 'spark'
  OR a.platform = 'platform_c'                                   -- ❌ OR 脱离了上面所有 AND 的约束
ORDER BY job_duration DESC;
-- 实际逻辑：(imp_date AND status='FAILED' AND platform='spark') OR (platform='platform_c')
-- platform='platform_c' 的所有行都会返回，不管日期和状态

-- ✅ 正确写法：
-- AND (a.platform = 'spark' OR a.platform = 'platform_c')
-- 或：AND a.platform IN ('spark', 'platform_c')


-- ---------------------------------------------------------------------------
-- Case 1c: 三元 OR/AND 组合，逻辑完全混乱
-- 业务需求：查找"大内存 且 失败"或"多executor 且 超时"的 app
-- ❌ 错误：三个 AND 一个 OR 混合，缺少括号使逻辑不可预测
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.executor_num,
    a.executor_memory,
    a.result,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.executor_memory > 8192
  AND a.result != 0
  OR a.executor_num > 50                                    -- ❌ OR 只与紧邻的 AND 结合
  AND (a.end_time - a.start_time) > 1800000
ORDER BY duration_ms DESC
LIMIT 200;
-- 实际：(imp_date AND mem>8192 AND result!=0) OR (executor>50 AND duration>1800000)
-- 第二个分支完全没有日期过滤！

-- ✅ 正确写法：明确分组
-- WHERE a.dt = '20260308'
--   AND (
--       (a.executor_memory > 8192 AND a.result != 0)
--       OR
--       (a.executor_num > 50 AND (a.end_time - a.start_time) > 1800000)
--   )


--这个Case废除，因为OR在JOIN ON条件中的优先级问题
-- ---------------------------------------------------------------------------
-- Case 1d: OR 在 JOIN ON 条件中的优先级问题
-- 业务需求：关联 app 和 job，条件为 app_id 相等且（同日期 或 前一天）
-- ❌ 错误：OR 打破了 ON 中的 AND 链
-- ---------------------------------------------------------------------------
-- SELECT
--     a.app_id,
--     a.app_name,
--     j.job_id,
--     j.status,
--     a.dt                                       AS app_date,
--     j.dt                                       AS job_date
-- FROM spark_analytics.spark_app_metrics a
-- INNER JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id
--     AND a.dt = j.dt
--     OR j.dt = '20260307'                       -- ❌ OR 脱离了 app_id 关联
-- WHERE a.dt = '20260308'
-- ORDER BY a.app_id;
-- 实际：(app_id相等 AND 同日期) OR (job日期=0307)
-- 第二个条件会导致所有 0307 的 job 与所有 app 笛卡尔积

-- ✅ 正确写法：
-- ON a.app_id = j.app_id
--     AND (a.dt = j.dt OR j.dt = '20260307')


-- ---------------------------------------------------------------------------
-- Case 1e: NOT 与 OR/AND 组合导致逻辑反转错误
-- 业务需求：排除"失败且超时"的 app，保留其他所有 app
-- ❌ 错误：NOT 只作用于第一个条件，OR 部分未被取反
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    (a.end_time - a.start_time)                             AS duration_ms
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND NOT a.result != 0                                     -- ❌ NOT 只取反了 result 条件
  OR (a.end_time - a.start_time) > 600000                   -- ❌ OR 不受 NOT 影响
LIMIT 100;
-- 实际：(imp_date AND result=0) OR (duration>600000)
-- 预期：imp_date AND NOT (result!=0 AND duration>600000)

-- ✅ 正确写法：用括号和德摩根定律
-- WHERE a.dt = '20260308'
--   AND NOT (a.result != 0 AND (a.end_time - a.start_time) > 600000)
-- 等价于：AND (a.result = 0 OR (a.end_time - a.start_time) <= 600000)
