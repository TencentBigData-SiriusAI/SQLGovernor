-- ============================================================================
-- Case 08: CASE WHEN 语法错误
-- ============================================================================
-- 【问题描述】
--   CASE WHEN 是数仓 SQL 中最常用的条件表达式，但其语法细节容易出错：
--     1. 缺少 END 关键字
--     2. WHEN 后缺少 THEN
--     3. 两种 CASE 语法混用（简单 CASE vs 搜索 CASE）
--     4. ELSE 分支遗漏导致 NULL（虽非语法错误但是逻辑隐患）
--     5. CASE WHEN 嵌套时括号/END 不匹配
--     6. THEN 后面直接跟 WHEN（遗漏了返回值）
--
-- 【易犯场景】
--   1. 复杂 CASE WHEN 有多个分支时，END 容易漏写或位置错误
--   2. 嵌套 CASE WHEN 时，内层和外层的 END 混乱
--   3. 从简单 CASE (CASE col WHEN val) 改为搜索 CASE (CASE WHEN cond) 时语法混用
--   4. 复制分支时 THEN 和返回值衔接出错
--   5. CASE WHEN 作为 JOIN 条件或子查询的一部分时更容易出错
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - CASE WHEN 语法不完整或格式错误
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 8a: 缺少 END 关键字
-- CASE WHEN 表达式没有以 END 结尾
-- ❌ 错误：CASE 表达式必须以 END 结束
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.executor_num,
    a.executor_memory,
    CASE
        WHEN a.result = 0 THEN '成功'
        WHEN a.result = 1 THEN '失败'
        WHEN a.result = 2 THEN '被终止'
        ELSE '未知'
                                      AS result_desc,  -- ❌ 缺少 END
    a.platform,
    a.start_time,
    a.end_time
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
ORDER BY a.end_time DESC
LIMIT 100;

-- ✅ 正确写法：
-- ELSE '未知'
-- END AS result_desc,


-- ---------------------------------------------------------------------------
-- Case 8b: WHEN 后缺少 THEN —— 条件和返回值之间没有 THEN
-- ❌ 错误：每个 WHEN 分支必须跟 THEN + 返回值
-- ---------------------------------------------------------------------------
SELECT
    j.app_id,
    j.job_id,
    j.status,
    j.submit_time,
    j.start_time,
    j.end_time,
    (j.end_time - j.start_time)       AS job_duration,
    CASE
        WHEN j.status = 'SUCCEEDED' '成功'        -- ❌ 缺少 THEN
        WHEN j.status = 'FAILED' THEN '失败'
        WHEN j.status = 'RUNNING' THEN '运行中'
        ELSE '其他'
    END                                AS status_cn,
    j.failed_reason
FROM spark_analytics.spark_job_metrics j
WHERE j.dt = '20260308'
ORDER BY j.submit_time DESC
LIMIT 100;

-- ✅ 正确写法：
-- WHEN j.status = 'SUCCEEDED' THEN '成功'


-- ---------------------------------------------------------------------------
-- Case 8c: 简单 CASE 和搜索 CASE 语法混用
-- 简单 CASE (CASE expr WHEN val) 和搜索 CASE (CASE WHEN cond) 混用
-- ❌ 错误：两种语法不能混用
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.num_tasks,
    (s.end_time - s.start_time)       AS stage_duration,
    -- ❌ 开头用了简单 CASE 语法，中间混入了搜索 CASE 语法
    CASE s.status
        WHEN 'COMPLETE' THEN '已完成'
        WHEN 'FAILED' THEN '失败'
        WHEN s.num_tasks > 1000 THEN '大Stage'    -- ❌ 简单CASE中不能写条件表达式
        ELSE '其他'
    END                                AS stage_desc,
    s.submit_time,
    s.start_time,
    s.end_time
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
ORDER BY stage_duration DESC
LIMIT 200;

-- ✅ 正确写法：统一使用搜索 CASE
-- CASE
--     WHEN s.status = 'COMPLETE' THEN '已完成'
--     WHEN s.status = 'FAILED' THEN '失败'
--     WHEN s.num_tasks > 1000 THEN '大Stage'
--     ELSE '其他'
-- END


-- ---------------------------------------------------------------------------
-- Case 8d: 嵌套 CASE WHEN 中 END 不匹配
-- 外层和内层 CASE 嵌套，END 个数不对
-- ❌ 错误：嵌套 CASE 必须有对应数量的 END
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.status,
    t.task_run_time,
    t.gc_time,
    CASE
        WHEN t.status = 'SUCCESS' THEN
            CASE
                WHEN t.task_run_time > 60000 THEN '慢任务成功'
                WHEN t.task_run_time > 10000 THEN '正常成功'
                ELSE '快速成功'
            -- ❌ 内层 CASE 缺少 END
        WHEN t.status = 'FAILED' THEN
            CASE
                WHEN t.gc_time > t.task_run_time * 0.5 THEN 'GC导致失败'
                ELSE '其他失败'
            END
        ELSE '未知状态'
    END                                AS task_category,
    t.executor_cpu_time
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
ORDER BY t.task_run_time DESC
LIMIT 100;

-- ✅ 正确写法：每个 CASE 都有对应的 END
-- CASE
--     WHEN t.status = 'SUCCESS' THEN
--         CASE ... END              -- 内层 END
--     WHEN t.status = 'FAILED' THEN
--         CASE ... END              -- 内层 END
--     ELSE '未知状态'
-- END                               -- 外层 END


-- ---------------------------------------------------------------------------
-- Case 8e: THEN 后直接跟 WHEN（遗漏返回值）
-- THEN 关键字后面应该是返回值表达式，但直接跟了下一个 WHEN
-- ❌ 错误：THEN 后缺少返回值
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.executor_num,
    a.executor_memory,
    CASE
        WHEN a.executor_num >= 100 AND a.executor_memory >= 8192 THEN
                                      -- ❌ THEN 后面没有返回值，直接跟了 WHEN
        WHEN a.executor_num >= 50 THEN '中大型'
        WHEN a.executor_num >= 10 THEN '中型'
        ELSE '小型'
    END                                AS app_scale,
    a.platform,
    a.result
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
ORDER BY a.executor_num DESC
LIMIT 100;

-- ✅ 正确写法：
-- WHEN a.executor_num >= 100 AND a.executor_memory >= 8192 THEN '超大型'
