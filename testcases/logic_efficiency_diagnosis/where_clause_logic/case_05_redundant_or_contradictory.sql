-- ============================================================================
-- Case 05: 冗余条件与矛盾条件
-- ============================================================================
-- 【问题描述】
--   WHERE 条件中存在冗余或相互矛盾的条件，表现为：
--     1. 恒真条件（1=1, OR TRUE）：不起过滤作用，浪费可读性
--     2. 恒假条件（1=0, AND FALSE）：永远返回空集
--     3. 相互矛盾的条件：如 status = 'A' AND status = 'B'，空集
--     4. 冗余条件：如 x > 5 AND x > 3，第二个多余
--     5. 包含关系混淆：如 x IN ('A','B') AND x = 'A'，IN 多余
--   这些问题通常是增量修改 SQL 时引入的"代码腐化"。
--
-- 【易犯场景】
--   1. 动态拼接 SQL 时默认加了 1=1，但后续条件也有逻辑问题
--   2. 多次修改 WHERE 条件，新旧条件矛盾但未清理
--   3. 复制粘贴其他人的查询，未根据需求调整条件
--   4. 调试时临时加条件后忘记删除
--
-- 【预期诊断结果】
--   应触发"Where条件逻辑问题"告警：
--   - 存在矛盾条件，查询将返回空集
--   - 存在冗余条件，建议简化
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 5a: 矛盾条件导致永远返回空集
-- 业务需求：查找失败的 app
-- ❌ 错误：result = 0（成功）AND result != 0（失败）矛盾
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
  AND a.result = 0                                          -- ❌ 成功
  AND a.result != 0                                         -- ❌ 失败，与上一行矛盾
ORDER BY duration_ms DESC;

-- ✅ 正确写法：根据需求选一个
-- AND a.result != 0   -- 如果要查失败的 app


-- ---------------------------------------------------------------------------
-- Case 5b: OR 1=1 使整个 WHERE 条件失效
-- 业务需求：查找特定平台的大内存 app
-- ❌ 错误：OR 1=1 使条件恒真，所有行都返回
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    a.executor_memory,
    a.executor_num
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND (a.platform = 'spark' AND a.executor_memory > 8192
       OR 1 = 1)                                            -- ❌ OR 1=1 导致整个括号恒真
ORDER BY a.executor_memory DESC
LIMIT 100;

-- ✅ 正确写法：移除 OR 1=1
-- AND a.platform = 'spark' AND a.executor_memory > 8192


-- ---------------------------------------------------------------------------
-- Case 5c: 多次修改后残留矛盾的范围条件
-- 业务需求：查找排队时间在 1-5 分钟的 job
-- ❌ 错误：前后两组条件指定了不重叠的范围
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.submit_time,
    j.start_time,
    (j.start_time - j.submit_time)                          AS queue_time
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
  AND (j.start_time - j.submit_time) > 300000               -- > 5 分钟
  AND (j.start_time - j.submit_time) < 60000                -- < 1 分钟 ❌ 与上一行矛盾
ORDER BY queue_time DESC;

-- ✅ 正确写法：确认范围
-- AND (j.start_time - j.submit_time) >= 60000    -- >= 1 分钟
-- AND (j.start_time - j.submit_time) <= 300000   -- <= 5 分钟


-- ---------------------------------------------------------------------------
-- Case 5d: 冗余的包含条件
-- 业务需求：查找 task_run_time > 30s 的 task
-- ❌ 错误：第二个条件 > 10000 是冗余的（被 > 30000 包含）
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.task_run_time > 30000                               -- > 30s
  AND t.task_run_time > 10000                               -- ❌ 冗余，已被 > 30000 包含
  AND t.status = 'SUCCESS'
ORDER BY t.task_run_time DESC
LIMIT 100;

-- ✅ 正确写法：移除冗余条件
-- AND t.task_run_time > 30000


-- ---------------------------------------------------------------------------
-- Case 5e: IN 列表与等值条件冗余叠加
-- 业务需求：查找 COMPLETE 状态的 stage
-- ❌ 错误：IN 和 = 同时指定了同一列的条件
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.num_tasks,
    (s.end_time - s.start_time)                             AS stage_duration
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.status IN ('COMPLETE', 'RUNNING', 'PENDING')        -- IN 包含三个值
  AND s.status = 'COMPLETE'                                 -- ❌ 冗余：= 已限定为 COMPLETE
                                                            -- IN 列表多余，且后续如果 IN 被修改
                                                            -- 两个条件可能变矛盾
ORDER BY stage_duration DESC;

-- ✅ 正确写法：保留一个即可
-- AND s.status = 'COMPLETE'
-- 或如果要多值：AND s.status IN ('COMPLETE', 'RUNNING')
