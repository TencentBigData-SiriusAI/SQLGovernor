-- ============================================================================
-- Case 02: INSERT 从空表/空分区 SELECT 写入目标表
-- ============================================================================
-- 【问题描述】
--   INSERT INTO ... SELECT 是数仓 ETL 最核心的操作。当 SELECT 来源的
--   表或分区为空时，INSERT 会成功执行但写入 0 行数据。这会导致：
--     1. 目标表该分区变为空分区，下游链路继续"空跑"
--     2. 如果是 INSERT OVERWRITE，会把目标分区已有数据清空
--     3. 数据质量监控如果只检查"任务是否成功"，无法发现问题
--     4. 指标报表显示为0，业务方无法区分是"真的为0"还是"数据缺失"
--
-- 【易犯场景】
--   1. 上游数据延迟但 ETL 调度已触发，源表当天分区为空
--   2. INSERT OVERWRITE 覆盖写入时，源数据为空导致目标分区被清空
--   3. 复杂 ETL 逻辑中间步骤产出空表，后续步骤继续基于空表计算
--   4. 多分支 ETL 中某个分支条件永远不满足，持续产出空结果
--
-- 【预期诊断结果】
--   应触发"空表问题"告警：
--   - SELECT 结果可能为空，INSERT 写入 0 行数据
--   - 建议在 INSERT 前添加源数据非空校验
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 2a: INSERT OVERWRITE 从空分区写入
-- 假设目标表是 app 的汇总表，从 app 表当天分区取数据
-- 如果当天分区为空，OVERWRITE 会把目标分区已有数据清空！
-- 这是数仓中最危险的空表操作
-- ---------------------------------------------------------------------------
-- （以 SELECT 模拟 INSERT OVERWRITE 的数据来源）
SELECT
    a.`user`                                      AS user_name,
    a.platform,
    COUNT(*)                                      AS app_count,
    SUM(CASE WHEN a.`result` = 0 THEN 1 ELSE 0 END)
                                                  AS success_count,
    SUM(CASE WHEN a.`result` != 0 THEN 1 ELSE 0 END)
                                                  AS fail_count,
    ROUND(SUM(CASE WHEN a.`result` = 0 THEN 1 ELSE 0 END) * 100.0
        / COUNT(*), 2)                            AS success_rate,
    AVG(a.executor_num)                           AS avg_executor_num,
    AVG(a.executor_memory)                        AS avg_executor_mem,
    AVG(a.end_time - a.start_time)                AS avg_duration_ms,
    MAX(a.end_time - a.start_time)                AS max_duration_ms,
    -- ❌ 如果分区为空，以下所有统计值都不会产出，目标表分区被清空
    '20270310'                                    AS dt
FROM spark_analytics.spark_app_metrics a
-- ❌ 查询未来日期分区，数据尚未写入，SELECT 返回0行
-- INSERT OVERWRITE 会把目标表该分区的已有数据清除！
WHERE a.dt = '20270310'
GROUP BY a.`user`, a.platform;


-- ---------------------------------------------------------------------------
-- Case 2b: 多表 JOIN 后 INSERT，某张源表为空导致结果为空
-- 典型的 ETL 场景：从 app + job + stage 关联取数写入宽表
-- stage 表数据延迟导致 JOIN 结果为空，宽表该分区被写空
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    a.platform,
    a.executor_num,
    a.executor_memory,
    a.start_time                                  AS app_start,
    a.end_time                                    AS app_end,
    j.job_id,
    j.action,
    j.status                                      AS job_status,
    j.failed_reason,
    s.stage_id,
    s.num_tasks,
    s.status                                      AS stage_status,
    (s.end_time - s.start_time)                   AS stage_duration_ms,
    -- 宽表计算字段
    (a.end_time - a.start_time)                   AS app_duration_ms,
    (j.end_time - j.start_time)                   AS job_duration_ms,
    a.dt                             AS dt
FROM spark_analytics.spark_app_metrics a
INNER JOIN spark_analytics.spark_job_metrics j
    ON a.app_id = j.app_id
    AND a.dt = j.dt
-- ❌ stage 表当天分区可能为空（数据采集延迟），整个 JOIN 结果为空
INNER JOIN spark_analytics.spark_stage_metrics s
    ON j.app_id = s.app_id
    AND j.dt = s.dt
WHERE a.dt = '20270308';


-- ---------------------------------------------------------------------------
-- Case 2c: 条件过滤后结果为空的 INSERT
-- 过滤条件过于严格或业务条件不满足，SELECT 返回0行
-- 这种情况 SQL 逻辑本身没问题，但应有空结果检查
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`user`,
    a.`result`,
    a.platform,
    a.spark_version,
    a.executor_num,
    a.executor_memory,
    a.executor_cores,
    a.start_time,
    a.end_time,
    (a.end_time - a.start_time)               AS duration_ms,
    a.dt                         AS dt
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20270308'
  -- ❌ 多重过滤条件组合后可能无数据满足
  AND a.`result` = 137                          -- 特定错误码
  AND a.platform = 'platform_b'                       -- 特定平台
  AND a.spark_version = '3.5.0'               -- 特定版本
  AND a.executor_num > 100                    -- 大规模任务
  AND a.rss_enabled = 1                       -- RSS 开启
  -- 以上条件组合极为严苛，很可能无数据满足，INSERT 写入0行
ORDER BY duration_ms DESC;
