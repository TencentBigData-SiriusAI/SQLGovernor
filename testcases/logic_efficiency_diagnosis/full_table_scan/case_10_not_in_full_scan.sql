-- ============================================================================
-- Case 10: NOT IN / NOT EXISTS 排除型查询导致全表扫描
-- ============================================================================
-- 【问题描述】
--   NOT IN / NOT EXISTS 是排除型查询，引擎通常无法进行有效优化，
--   需要将外表每一行与内表全量数据做比对，尤其当内表（子查询）
--   缺失分区条件时，会触发内表全分区扫描。
--   同时 NOT IN 在子查询结果含 NULL 时还会产生错误的空结果集。
--
-- 【易犯场景】
--   1. "查今天提交但没有成功完成的 app" —— 用 NOT IN 排除成功的
--   2. "查有 app 但没有产生 job 的异常任务" —— NOT EXISTS 子查询
--   3. 数据质量校验：查"stage 中有但 task 中没有对应记录的异常数据"
--   4. 增量同步：用 NOT IN 查"新表有但旧表没有"的增量数据
--
-- 【预期诊断结果】
--   应触发"暴力扫描"告警：
--   - NOT IN / NOT EXISTS 子查询缺少分区过滤，导致全表扫描
--   - NOT IN 可能因 NULL 值产生非预期结果
-- ============================================================================

-- Case 10a: NOT IN 子查询无分区条件
-- 查找"今天提交但历史上从未成功过的 app"
-- 内层子查询没有分区条件，扫描 app 表全量历史数据
SELECT
    app_id,
    app_name,
    `user`,
    result
FROM spark_analytics.spark_app_metrics
WHERE dt = '20260308'
  AND app_id NOT IN (
    SELECT app_id
    FROM spark_analytics.spark_app_metrics
    WHERE result = 0
  );


-- Case 10b: NOT EXISTS 子查询无分区条件
-- 查找"有 stage 记录但没有 task 记录"的异常 stage
-- 外层有分区条件，但 task 表子查询没有，触发 task 全表扫描
SELECT
    s.app_id,
    s.stage_id,
    s.stage_name,
    s.task_num,
    s.status
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.task_num > 0
  AND NOT EXISTS (
    SELECT 1
    FROM spark_analytics.spark_task_metrics t
    WHERE t.app_id = s.app_id
      AND t.stage_id = s.stage_id
  );


-- Case 10c: 双重 NOT IN 嵌套，两层子查询都无分区条件
-- 查找"今天运行的 app 中，既不在成功列表也不在已知失败列表中的异常 app"
-- 两个 NOT IN 子查询均无分区过滤，两次全表扫描
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.result,
    a.platform
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308'
  AND a.app_id NOT IN (
    -- 子查询1：全量历史中成功过的 app（无分区条件）
    SELECT app_id
    FROM spark_analytics.spark_job_metrics
    WHERE status = 0
  )
  AND a.app_id NOT IN (
    -- 子查询2：全量历史中产生过 stage 失败的 app（无分区条件）
    SELECT app_id
    FROM spark_analytics.spark_stage_metrics
    WHERE status = -1
  );
