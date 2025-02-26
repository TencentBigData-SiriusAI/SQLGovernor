-- ============================================================================
-- Case 09: 多表链式 JOIN 中间环节断裂导致笛卡尔积
-- ============================================================================
-- 【问题描述】
--   在数仓中，表之间往往有层级关系：app → job → stage → task。
--   链式 JOIN 时如果中间环节的关联断裂（如 job-stage 之间没有通过
--   app_id 衔接），会导致后续所有表产生笛卡尔积。
--   具体表现为：
--     1. A JOIN B ON ... JOIN C ON（C 只关联了 A，没关联 B）
--     2. 链条中间断开：A-B 关联 OK，B-C 无关联，C-D 关联 OK
--     3. 多表 JOIN 顺序错误导致关联关系错位
--     4. 后面的表应该关联前面最近的表但误关联了更前面的表
--
-- 【易犯场景】
--   1. 四表关联时复制粘贴 ON 条件后修改不完整
--   2. 重构 SQL 时调整了 JOIN 顺序但没有更新 ON 条件
--   3. 中间表 job 被误删或注释掉，stage 直接关联 app 但缺少层级关系
--   4. 新增表到已有的多表 JOIN 中，ON 条件写到了错误的表上
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - 多表 JOIN 链式关联断裂，中间环节缺失关联条件
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 9a: B-C 断裂 —— job 和 stage 之间无关联
-- app→job 正确关联，stage 跳过 job 直接关联 app，但实际上
-- 需要通过 job 来精确定位 stage（通过 stage_ids）
-- ❌ 错误：stage 直接关联 app 而非通过 job 的 stage_ids
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.platform,
    j.job_id,
    j.status                          AS job_status,
    j.stage_ids,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    (s.end_time - s.start_time)       AS stage_duration_ms,
    (j.end_time - j.start_time)       AS job_duration_ms
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt          -- ✅ app-job 正确
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt
    -- ❌ stage 只关联了 app，没有关联 job
    -- 一个 app 有多个 job，每个 job 关联不同的 stage
    -- 这里 stage 与 job 产生了多对多关联
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY stage_duration_ms DESC
LIMIT 200;

-- ✅ 正确写法：stage 应通过 job 的 stage_ids 精确关联
-- 或至少保证 stage 与 job 有某种关联逻辑


-- ---------------------------------------------------------------------------
-- Case 9b: 跳级关联 —— task 跳过 stage 直接关联 job
-- app→job→stage 链正确，但 task 跳过 stage 直接关联 job 的日期
-- ❌ 错误：task 应关联 stage，不应跳级关联 job
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.status                          AS job_status,
    s.stage_id,
    s.num_tasks,
    t.task_id,
    t.status                          AS task_status,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt          -- ✅ 正确
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.app_id = s.app_id
    AND a.dt = s.dt          -- ✅ 正确
INNER JOIN spark_analytics.spark_task_metrics t
    ON j.app_id = t.app_id                             -- ❌ 跳级关联 job
    AND j.dt = t.dt
    -- ❌ 缺少 s.stage_id = t.stage_id
    -- task 应通过 stage 关联，这里跳过了 stage 直接关联 job
    -- 导致每个 stage 下的 task 与所有 stage 交叉匹配
WHERE a.dt = '20260308'
ORDER BY t.task_run_time DESC
LIMIT 500;

-- ✅ 正确写法：
-- INNER JOIN task t
--     ON s.app_id = t.app_id
--     AND s.stage_id = t.stage_id
--     AND s.dt = t.dt


-- ---------------------------------------------------------------------------
-- Case 9c: 注释掉中间表后链断裂
-- 原来是四表链式 JOIN，调试时注释掉了 job 表，
-- stage 原本关联 job，现在变成与 app 产生笛卡尔积
-- ❌ 错误：注释掉中间环节后关联链断裂
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    -- j.job_id,                       -- 注释掉了 job 表相关字段
    -- j.status AS job_status,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    t.task_id,
    t.task_run_time,
    t.gc_time
FROM spark_analytics.spark_app_metrics a
-- ❌ 下面的 JOIN job 表被注释掉了
-- INNER JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id
--     AND a.dt = j.dt
INNER JOIN spark_analytics.spark_stage_metrics s
    ON a.dt = s.dt
    -- ❌ 原来是 ON j.app_id = s.app_id，现在 j 被注释掉了
    -- 改成用日期关联，等同于笛卡尔积
INNER JOIN spark_analytics.spark_task_metrics t
    ON s.app_id = t.app_id
    AND s.stage_id = t.stage_id
    AND s.dt = t.dt          -- ✅ 正确
WHERE a.dt = '20260308'
ORDER BY t.task_run_time DESC
LIMIT 200;

-- ✅ 正确写法：恢复 job 表 JOIN，或者 stage 直接关联 app
-- ON a.app_id = s.app_id
--    AND a.dt = s.dt
