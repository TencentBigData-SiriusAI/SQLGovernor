-- ============================================================================
-- Case 07: 子查询 / IN 子句内层未限定分区
-- ============================================================================
-- 【问题描述】
--   外层查询有分区条件，但 IN / EXISTS 的子查询内层缺失分区过滤。
--   引擎执行子查询时仍然会全表扫描内层表，尤其是 task 表数据量巨大，
--   内层全扫描会导致整体查询极其缓慢。
--
-- 【易犯场景】
--   1. "查今天的 app 中，所有曾经失败过的 job" —— job 的子查询没带日期
--   2. 先写好外层 SQL，后续补子查询时忘记加分区条件
--   3. 子查询涉及不同表，研发人员只给主表加了分区条件
--   4. 用 IN (SELECT ...) 做关联过滤，内层 SELECT 遗漏分区
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：
--   - 子查询中的表缺少分区过滤条件
-- ============================================================================

-- Case 7a: IN 子查询内层无分区过滤
-- 外层限定了 app 表的 dt，但内层 job 表没有分区条件
-- job 表子查询会扫描全部历史分区来找 status = -1 的 app_id
SELECT
    app_id,
    app_name,
    `user`,
    result
FROM spark_analytics.spark_app_metrics
WHERE dt = '20260308'
  AND app_id IN (
    SELECT app_id
    FROM spark_analytics.spark_job_metrics
    WHERE status = -1
  );


-- Case 7b: EXISTS 子查询内层无分区过滤
-- 查找"有过大 stage（input_size > 100GB）"的 app
-- 外层有分区条件，但 stage 表子查询没有限定日期
SELECT
    a.app_id,
    a.app_name,
    a.executor_num,
    a.executor_memory
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND EXISTS (
    SELECT 1
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.app_id = a.app_id
      AND s.input_size > 107374182400  -- 100GB
  );


-- Case 7c: 嵌套子查询两层都缺失分区过滤
-- 最内层 task 表无分区，中间层 stage 表也无分区，层层全扫描
SELECT
    a.app_id,
    a.app_name
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id IN (
    SELECT s.app_id
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.status = -1
      AND s.stage_id IN (
        SELECT t.stage_id
        FROM spark_analytics.spark_task_metrics t
        WHERE t.gc_time > 120000  -- GC 超过 2 分钟
      )
  );
