-- ============================================================================
-- Case 05: 逗号（,）与分号（;）位置错误
-- ============================================================================
-- 【问题描述】
--   逗号和分号的位置错误是 SQL 编写中最常见但最难排查的语法问题：
--     1. SELECT 列表末尾多余逗号（trailing comma）
--     2. SELECT 列表中间缺少逗号，两列"粘连"成别名
--     3. FROM 子句后多余逗号（隐式 CROSS JOIN）
--     4. ORDER BY 列之间缺少逗号
--     5. 多语句之间分号使用不当
--   这类错误的报错信息往往指向错误位置之后的行，定位困难。
--
-- 【易犯场景】
--   1. 复制粘贴列名后忘记删除末尾逗号
--   2. 注释掉最后一列后，倒数第二列的逗号变成多余
--   3. 格式化工具处理后逗号位置异常
--   4. 多列 SELECT 中间插入新列时上下行逗号衔接出错
--   5. 删除某列后相邻列的逗号未处理
--
-- 【预期诊断结果】
--   应触发"语法错误"告警：
--   - 多余逗号/缺少逗号/分号位置异常
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 5a: SELECT 末尾多余逗号（trailing comma）
-- 最后一列 platform 后面有多余逗号，FROM 前面多了一个逗号
-- 这通常是因为注释掉了原本的最后一列
-- ❌ 错误：platform 后多余逗号
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.executor_num,
    a.executor_memory,
    a.executor_cores,
    a.start_time,
    a.end_time,
    a.driver_memory,
    a.platform,                       -- ❌ 多余逗号（原本下面还有一列被删掉了）
    -- a.rss_enabled                  -- 被注释掉了，但上面的逗号没删
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.result != 0
ORDER BY a.end_time DESC
LIMIT 100;

-- ✅ 正确写法：删掉 a.platform 后的逗号
-- a.platform


-- ---------------------------------------------------------------------------
-- Case 5b: SELECT 列之间缺少逗号 —— 列名"粘连"成别名
-- app_name 和 `user` 之间缺少逗号，导致 `user` 被解析为 app_name 的别名
-- SQL 可能不报错（被当作别名），但结果完全错误
-- ❌ 错误：a.app_name 后缺少逗号
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name                        -- ❌ 缺少逗号，下一行被当作 app_name 的别名
    a.`user`,
    a.result,
    a.platform,
    a.executor_num,
    a.executor_memory,
    ROUND((a.end_time - a.start_time) / 1000, 2) AS duration_sec
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
ORDER BY duration_sec DESC
LIMIT 50;

-- ✅ 正确写法：
-- a.app_name,
-- a.`user`,


-- ---------------------------------------------------------------------------
-- Case 5c: FROM 子句多个表之间意外加了逗号 —— 变成隐式 CROSS JOIN
-- 本意是 INNER JOIN，但误用逗号分隔表名，变成笛卡尔积
-- ❌ 错误：逗号分隔变成 CROSS JOIN，结果集爆炸性膨胀
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    j.job_id,
    j.status                          AS job_status,
    j.submit_time,
    j.start_time                      AS job_start,
    j.end_time                        AS job_end,
    j.failed_reason
FROM spark_analytics.spark_app_metrics a,         -- ❌ 逗号导致隐式 CROSS JOIN
     spark_analytics.spark_job_metrics j           -- ❌ 应用 INNER JOIN ... ON
WHERE a.dt = '20260308'
  AND j.dt = '20260308'
  AND a.app_id = j.app_id                           -- WHERE 中的 JOIN 条件
  AND j.status = 'FAILED'
ORDER BY j.submit_time DESC
LIMIT 100;

-- ✅ 正确写法：使用显式 JOIN
-- FROM spark_analytics.spark_app_metrics a
-- INNER JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id
--     AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 5d: ORDER BY 列之间缺少逗号
-- 多列排序时，两个排序条件之间漏了逗号
-- ❌ 错误：排序列之间缺少逗号
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    s.status,
    s.num_tasks,
    s.submit_time,
    s.start_time,
    s.end_time,
    (s.end_time - s.start_time)       AS stage_duration_ms,
    s.stage_attempt_id
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.num_tasks > 10
ORDER BY s.app_id                     -- ❌ 缺少逗号
         s.num_tasks DESC             -- ❌ 被当作 app_id 的别名或直接报错
LIMIT 200;

-- ✅ 正确写法：
-- ORDER BY s.app_id, s.num_tasks DESC


-- ---------------------------------------------------------------------------
-- Case 5e: INSERT 语句列清单中逗号缺失 + VALUES 多余逗号
-- 在建表/INSERT场景中的逗号错误
-- ❌ 错误：列清单逗号和 SELECT 列逗号双重错误
-- ---------------------------------------------------------------------------
INSERT OVERWRITE TABLE result_table PARTITION (dt = '20260308')
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.status,
    t.task_run_time
    t.gc_time,                        -- ❌ 上一行 task_run_time 后缺逗号
    t.executor_cpu_time,              -- ❌ 最后一列后面不应有逗号（如果下面没有更多列）
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.status = 'SUCCESS';

-- ✅ 正确写法：
-- t.task_run_time,
-- t.gc_time,
-- t.executor_cpu_time
-- FROM ...
