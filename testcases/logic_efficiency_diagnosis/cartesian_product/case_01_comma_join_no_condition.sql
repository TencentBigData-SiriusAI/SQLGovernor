-- ============================================================================
-- Case 01: FROM 中逗号分隔多表但 WHERE 缺少关联条件
-- ============================================================================
-- 【问题描述】
--   在 FROM 子句中使用逗号分隔多张表（隐式 JOIN）时，如果 WHERE 中缺少
--   表间的关联条件，就会产生笛卡尔积。这是最经典的笛卡尔积成因：
--     1. 逗号分隔语法本身就是 CROSS JOIN 的语法糖
--     2. WHERE 只写了过滤条件，漏写了关联条件
--     3. 结果集 = 表A行数 × 表B行数，可能从万行膨胀到亿行
--     4. SQL 不会报错，执行成功但产出数据严重膨胀
--
-- 【易犯场景】
--   1. 从单表查询改为多表查询，习惯在 FROM 后逗号追加表名
--   2. WHERE 条件写了分区过滤但忘了写表间关联
--   3. 复制粘贴时丢失了 JOIN 条件部分
--   4. 三表以上逗号分隔时，其中两张表之间漏了关联
--   5. 调试时临时注释掉 JOIN 条件后忘记恢复
--
-- 【预期诊断结果】
--   应触发"笛卡尔积"告警：
--   - FROM 中多表之间缺少关联条件，将产生笛卡尔积
--   - 建议添加 JOIN ON 关联条件或改为显式 JOIN 语法
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 1a: 双表逗号分隔，WHERE 只有分区过滤，无关联条件
-- app 表和 job 表通过逗号分隔，但 WHERE 中只有分区过滤
-- ❌ 错误：缺少 a.app_id = j.app_id 的关联条件
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform,
    a.executor_num,
    a.executor_memory,
    j.job_id,
    j.`action`,
    j.status                          AS job_status,
    j.submit_time,
    j.start_time                      AS job_start,
    j.end_time                        AS job_end,
    j.failed_reason,
    (j.end_time - j.start_time)       AS job_duration_ms
FROM spark_analytics.spark_app_metrics a,
     spark_analytics.spark_job_metrics j             -- ❌ 逗号分隔
WHERE a.dt = '20260308'
  AND j.dt = '20260308'
  -- ❌ 缺少 AND a.app_id = j.app_id 关联条件
  AND a.result != 0
  AND j.status = 'FAILED'
ORDER BY j.submit_time DESC
LIMIT 100;

-- ✅ 正确写法：添加关联条件或改为显式 JOIN
-- FROM spark_analytics.spark_app_metrics a
-- INNER JOIN spark_analytics.spark_job_metrics j
--     ON a.app_id = j.app_id
--     AND a.dt = j.dt


-- ---------------------------------------------------------------------------
-- Case 1b: 三表逗号分隔，其中两表之间缺少关联
-- app、job、stage 三表逗号分隔，app-job 有关联，但 stage 与其他表无关联
-- ❌ 错误：stage 表与 app/job 之间没有关联条件
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    j.job_id,
    j.status                          AS job_status,
    s.stage_id,
    s.num_tasks,
    s.status                          AS stage_status,
    (s.end_time - s.start_time)       AS stage_duration_ms
FROM spark_analytics.spark_app_metrics a,
     spark_analytics.spark_job_metrics j,
     spark_analytics.spark_stage_metrics s            -- ❌ 三表逗号分隔
WHERE a.dt = '20260308'
  AND j.dt = '20260308'
  AND s.dt = '20260308'
  AND a.app_id = j.app_id                              -- ✅ app-job 有关联
  -- ❌ 缺少 s.app_id = a.app_id 的关联，stage 与其他表产生笛卡尔积
  AND a.result != 0
ORDER BY s.num_tasks DESC
LIMIT 200;

-- ✅ 正确写法：
-- AND a.app_id = j.app_id
-- AND s.app_id = a.app_id
-- AND s.dt = a.dt


-- ---------------------------------------------------------------------------
-- Case 1c: 四表逗号分隔，完全无关联条件
-- 所有表之间都没有关联条件，产生四重笛卡尔积
-- 假设每表仅1万行，结果集 = 10000^4 = 10^16 行，直接 OOM
-- ❌ 错误：四表之间完全无关联
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.status,
    s.stage_id,
    s.num_tasks,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    t.executor_cpu_time
FROM spark_analytics.spark_app_metrics a,
     spark_analytics.spark_job_metrics j,
     spark_analytics.spark_stage_metrics s,
     spark_analytics.spark_task_metrics t             -- ❌ 四表逗号分隔
WHERE a.dt = '20260308'
  AND j.dt = '20260308'
  AND s.dt = '20260308'
  AND t.dt = '20260308'
  -- ❌ 四张表之间完全没有关联条件，产生超级笛卡尔积
  AND a.result != 0
  AND j.status = 'FAILED'
LIMIT 1000;

-- ✅ 正确写法：改为显式 JOIN 并添加关联条件
-- FROM a JOIN j ON a.app_id = j.app_id AND ...
-- JOIN s ON a.app_id = s.app_id AND ...
-- JOIN t ON s.app_id = t.app_id AND s.stage_id = t.stage_id AND ...
