-- ============================================================================
-- Case 08: 排名/TopN 逻辑错误
-- ============================================================================
-- 【问题描述】
--   排名和 TopN 查询中的语义错误，常见包括：
--     1. ROW_NUMBER 不处理并列，应用 RANK 的场景用了 ROW_NUMBER
--     2. 排序方向 ASC/DESC 搞反，取到了相反方向的数据
--     3. PARTITION BY 范围错误，分组内排名变成了全局排名或反之
--     4. TopN 过滤时用 > N 而非 <= N，数量不对
--     5. 多字段排序优先级与业务需求不一致
--
-- 【易犯场景】
--   1. 想取"耗时最长的 App"但排序方向搞反取了最短的
--   2. 想要并列排名但用 ROW_NUMBER 导致同耗时的 App 排名不同
--   3. 想取每组 Top 3 但过滤条件写成 rn > 3
--
-- 【预期诊断结果】
--   应触发"业务意图不匹配"告警：
--   - 排名函数的选择可能与业务需求不匹配
--   - 排序方向可能与"最大/最小"的业务语义相反
-- ============================================================================


-- ---------------------------------------------------------------------------
-- Case 8a: ROW_NUMBER 应为 RANK 导致并列排名丢失
-- 业务需求：按耗时给 Stage 排名，相同耗时应并列排名
-- ❌ 错误：ROW_NUMBER 给相同值分配不同排名，
--   耗时相同的 stage 排名不同，不符合"并列排名"的业务语义
-- ---------------------------------------------------------------------------
SELECT
    s.app_id,
    s.stage_id,
    (s.end_time - s.start_time)                              AS duration_ms,
    ROW_NUMBER() OVER (
        PARTITION BY s.app_id
        ORDER BY (s.end_time - s.start_time) DESC
    )                                                        AS duration_rank
FROM spark_analytics.spark_stage_metrics s
WHERE s.dt = '20260308'
  AND s.`status` = 0;
-- ❌ ROW_NUMBER 对相同耗时的 stage 分配不同排名（如 1, 2, 3）
-- ❌ 业务需要并列排名（如 1, 1, 3），应使用 RANK
-- ✅ 正确写法：使用 RANK
-- RANK() OVER (PARTITION BY s.app_id ORDER BY (s.end_time - s.start_time) DESC)


-- ---------------------------------------------------------------------------
-- Case 8b: 排序方向 ASC/DESC 搞反
-- 业务需求：找出每个 App 中耗时最长的 Top 3 个 Stage
-- ❌ 错误：ORDER BY 使用 ASC（从小到大），取的是耗时最短的 stage
-- ---------------------------------------------------------------------------
SELECT *
FROM (
    SELECT
        s.app_id,
        s.stage_id,
        s.stage_name,
        (s.end_time - s.start_time)                          AS duration_ms,
        ROW_NUMBER() OVER (
            PARTITION BY s.app_id
            ORDER BY (s.end_time - s.start_time) ASC
        )                                                    AS rn
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.dt = '20260308'
) ranked
WHERE rn <= 3;
-- ❌ ORDER BY ASC 排序后 rn=1 是耗时最短的，不是最长的
-- ❌ 业务要"耗时最长的 Top 3"，但实际取到了耗时最短的 Bottom 3
-- ✅ 正确写法：ORDER BY DESC
-- ORDER BY (s.end_time - s.start_time) DESC


-- ---------------------------------------------------------------------------
-- Case 8c: PARTITION BY 范围错误导致分组内排名变全局排名
-- 业务需求：在每个 App 内部，按 Task 的 input_size 排名
-- ❌ 错误：PARTITION BY 遗漏了 stage_id，导致一个 app 内
--   所有 stage 的 task 混在一起排名，而非按 stage 分组排名
-- ---------------------------------------------------------------------------
SELECT
    t.app_id,
    t.stage_id,
    t.task_id,
    t.input_size,
    ROW_NUMBER() OVER (
        PARTITION BY t.app_id
        ORDER BY t.input_size DESC
    )                                                        AS rank_in_stage
FROM spark_analytics.spark_task_metrics t
WHERE t.dt = '20260308'
  AND t.`status` = 0;
-- ❌ 别名写 rank_in_stage，但 PARTITION BY 只有 app_id 没有 stage_id
-- ❌ 排名是在整个 app 范围内而非 stage 内部
-- ✅ 正确写法：PARTITION BY 加上 stage_id
-- PARTITION BY t.app_id, t.stage_id


-- ---------------------------------------------------------------------------
-- Case 8d: TopN 过滤时用 > N 而非 <= N
-- 业务需求：取每个 App 中耗时最长的 Top 5 个 Stage
-- ❌ 错误：WHERE rn > 5 取的是排名 6 及以后的，丢弃了 Top 5
-- ---------------------------------------------------------------------------
SELECT *
FROM (
    SELECT
        s.app_id,
        s.stage_id,
        (s.end_time - s.start_time)                          AS duration_ms,
        ROW_NUMBER() OVER (
            PARTITION BY s.app_id
            ORDER BY (s.end_time - s.start_time) DESC
        )                                                    AS rn
    FROM spark_analytics.spark_stage_metrics s
    WHERE s.dt = '20260308'
) ranked
WHERE rn > 5;
-- ❌ WHERE rn > 5 取的是排名 6+，不是 Top 5
-- ❌ 恰好过滤掉了我们需要的 Top 5，留下了不需要的部分
-- ✅ 正确写法：WHERE rn <= 5


-- ---------------------------------------------------------------------------
-- Case 8e: 多字段排序优先级与业务需求不一致
-- 业务需求：按"失败优先、耗时最长优先"排序 App
-- ❌ 错误：排序字段先写了 duration DESC，再写 result，
--   导致耗时优先级高于失败/成功，不符合"失败优先"的业务语义
-- ---------------------------------------------------------------------------
SELECT
    a.app_id,
    a.app_name,
    a.`result`,
    (a.end_time - a.start_time)                              AS duration_ms,
    ROW_NUMBER() OVER (
        ORDER BY (a.end_time - a.start_time) DESC,
                 a.`result` DESC
    )                                                        AS priority_rank
FROM spark_analytics.spark_app_metrics a
WHERE a.dt = '20260308';
-- ❌ 先按 duration DESC 排序，result 只在 duration 相同时才生效
-- ❌ 业务要"失败优先"，但一个成功但耗时很长的 app 排名可能高于失败的
-- ✅ 正确写法：失败优先（result DESC），耗时其次
-- ORDER BY a.`result` DESC, (a.end_time - a.start_time) DESC
