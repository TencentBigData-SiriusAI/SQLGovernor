-- ============================================================================
-- Case 08: 大表 JOIN 时一侧缺失分区过滤
-- ============================================================================
-- 【问题描述】
--   多表 JOIN 是数仓中最常见的操作。当 JOIN 的某一侧大表没有分区条件时，
--   该表会全分区扫描，即使另一侧已正确限定分区。
--   尤其在 app → job → stage → task 的四表关联场景中，
--   task 表单日数据可达千万级，漏加分区条件后果严重。
--
-- 【易犯场景】
--   1. 主表加了分区条件，JOIN 的维表/事实表忘加
--   2. 四表 JOIN 时部分表加了分区条件、部分漏了
--   3. 用临时表/CTE 做中间结果时，源表忘加分区
--   4. 研发人员认为"JOIN 条件的 app_id 能自动限定范围"（实际不能）
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：
--   - JOIN 中的表 xxx 缺少分区过滤条件
-- ============================================================================

-- Case 8a: 双表 JOIN，右表缺失分区条件
-- app 表有分区过滤，但 stage 表只有 JOIN 条件没有分区过滤
-- 引擎需要全量扫描 stage 表来做 JOIN 匹配
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    s.stage_id,
    s.stage_name,
    s.input_size,
    s.output_size,
    s.task_num
FROM spark_analytics.spark_app_metrics a
JOIN spark_analytics.spark_stage_metrics s
  ON a.app_id = s.app_id
WHERE a.dt = '20260308'
  AND a.result != 0;


-- Case 8b: 三表 JOIN，中间表和末端表都缺分区条件
-- 研发人员想关联 app → job → stage 分析失败链路
-- 只给 app 表加了分区，job 和 stage 表全部裸奔
SELECT
    a.app_id,
    a.app_name,
    j.job_id,
    j.action,
    j.failed_reason,
    s.stage_id,
    s.stage_name,
    s.faliure_reason AS stage_failure_reason,
    s.task_num
FROM spark_analytics.spark_app_metrics a
JOIN spark_analytics.spark_job_metrics j
  ON a.app_id = j.app_id
JOIN spark_analytics.spark_stage_metrics s
  ON a.app_id = s.app_id
WHERE a.dt = '20260308'
  AND j.status = -1;


-- Case 8c: 四表全链路 JOIN，只有第一张表有分区条件
-- 这是最典型的"逐层 JOIN 遗忘分区"场景
-- app 有分区 → job/stage/task 全无分区 = 三张大表全扫描
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    j.job_id,
    s.stage_id,
    s.stage_name,
    t.task_id,
    t.task_run_time,
    t.gc_time,
    t.shuffle_output_size
FROM spark_analytics.spark_app_metrics a
JOIN spark_analytics.spark_job_metrics j
  ON a.app_id = j.app_id
JOIN spark_analytics.spark_stage_metrics s
  ON a.app_id = s.app_id
JOIN spark_analytics.spark_task_metrics t
  ON s.app_id = t.app_id
  AND s.stage_id = t.stage_id
WHERE a.dt = '20260308';
