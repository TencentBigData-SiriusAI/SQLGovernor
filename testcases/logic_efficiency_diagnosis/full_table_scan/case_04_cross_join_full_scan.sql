-- ============================================================================
-- Case 04: 笛卡尔积 / 缺失 JOIN 条件导致的暴力扫描
-- ============================================================================
-- 【问题描述】
--   多表 JOIN 时缺少关联条件或使用了 CROSS JOIN，产生笛卡尔积。
--   Spark 运行数据的四张表通过 app_id 关联，若漏掉关联条件，
--   N × M 的数据膨胀会导致内存溢出或超长运行时间。
--
-- 【易犯场景】
--   1. 多表 JOIN 时 ON 子句写漏了某个关联字段
--   2. 把 JOIN 条件误写到 WHERE 中，部分条件遗漏变成了隐式笛卡尔积
--   3. 想做"全组合"分析，用了 CROSS JOIN 但没意识到数据量级
--   4. 子查询中两个数据集没有正确关联
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：
--   - 检测到笛卡尔积（CROSS JOIN 或缺少 JOIN 条件）
--   - 缺少分区过滤条件
-- ============================================================================

-- Case 4a: 隐式笛卡尔积 —— FROM 多表但缺少 JOIN/WHERE 关联条件
-- 研发人员想关联 app 和 job 表，但忘了写 ON 条件
-- app 表日均 ~10万行 × job 表日均 ~50万行 = 500亿行笛卡尔积
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.status AS job_status
FROM spark_analytics.spark_app_metrics a,
     spark_analytics.spark_job_metrics j
WHERE a.dt = '20260308'
  AND j.dt = '20260308';


-- Case 4b: 显式 CROSS JOIN + 无分区过滤
-- 想对比不同平台的 stage 表现，错误地使用了 CROSS JOIN
SELECT
    s1.app_id AS app1,
    s1.stage_name AS stage1,
    s2.app_id AS app2,
    s2.stage_name AS stage2
FROM spark_analytics.spark_stage_metrics s1
CROSS JOIN spark_analytics.spark_stage_metrics s2
WHERE s1.status = 0
  AND s2.status = -1;
