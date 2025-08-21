------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 포트폴리오 샘플 쿼리 1: 구독 데이터 처리 파이프라인
-- 이 쿼리는 복잡한 데이터 변환과 분석 기능을 보여주는 샘플입니다
-- Apache Airflow DAG에서 일일 데이터 처리를 위해 사용됩니다
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 이벤트 로그 처리를 위한 임시 테이블 생성
CREATE TEMP TABLE temp_portfolio_event_logs AS
  (WITH all_logs AS
     (SELECT id AS eventid,
             workspaceid,
             logdatetimestamp,
             logtimestamp,
             logmicrosecondstamp,
             eventname,
             planid,
             nextplanid,
             issubscribing::boolean as issubscribing,
             nextresetdate,
             resetdateofmonth,
             planresetperiodvalue,
             plansuppliedresetcount,
             remainingresetcount,
             currenttimes,
             maxtimes
      FROM portfolio_subscription_event_log
      UNION ALL SELECT eventid,
                       workspaceid,
                       logdatetimestamp,
                       logtimestamp,
                       logmicrosecondstamp,
                       eventname,
                       planid,
                       nextplanid,
                       issubscribing,
                       nextresetdate,
                       resetdateofmonth,
                       planresetperiodvalue,
                       plansuppliedresetcount,
                       remainingresetcount,
                       currenttimes,
                       maxtimes
      FROM
        (SELECT L.id AS eventid,
                s.workspaceid,
                L.logdatetimestamp,
                extract(epoch
                        FROM L.logdatetimestamp)::int AS logtimestamp,
                logmicrosecondstamp,
                CASE
                    WHEN lag(L.renewalcount) OVER (PARTITION BY S.workspaceid
                                                   ORDER BY extract(epoch
                                                                    FROM L.logdatetimestamp)::int) <= L.renewalcount
                         AND lag(L.appliedplanplanid) OVER (PARTITION BY S.workspaceid
                                                            ORDER BY extract(epoch
                                                                             FROM L.logdatetimestamp)::int) != L.appliedplanplanid
                         AND L.eventname = 'APPLIED_PLAN_RENEWED' THEN 'SUBSCRIPTION_CHANGED'
                    WHEN lag(L.renewalcount) OVER (PARTITION BY S.workspaceid
                                                   ORDER BY extract(epoch
                                                                    FROM L.logdatetimestamp)::int) <= L.renewalcount
                         AND L.eventname = 'APPLIED_PLAN_RENEWED' THEN 'SUBSCRIPTION_RENEWED'
                    WHEN L.eventname = 'APPLIED_PLAN_RENEWED'
                         AND L.subscribingplanplaninterval = 'YEAR'
                         AND (L.renewalcount) >= 0 THEN 'SUBSCRIPTION_RESET'
                    WHEN L.eventname = 'SUBSCRIPTION_TERMINATED' THEN 'SUBSCRIPTION_STOPPED'
                    WHEN L.eventname = 'APPLIED_PLAN_RENEWED' THEN 'SUBSCRIPTION_RENEWED'
                    ELSE L.eventname
                END AS eventname,
                CASE
                    WHEN L.eventname = 'SUBSCRIPTION_TERMINATED' THEN NULL
                    ELSE L.appliedplanplanid
                END AS planid,
                NULL AS nextplanid,
                (L.maxtimes = -1) as issubscribing,
                CASE
                    WHEN date_part(DAY, L.logdatetimestamp)::int < L.renewaldateofmonth THEN date_trunc('day', dateadd(DAY, L.renewaldateofmonth - 1, date_trunc('month', L.logdatetimestamp)))
                    WHEN date_part(DAY, last_day(dateadd(MONTH, 1, L.logdatetimestamp)))::int >= L.renewaldateofmonth THEN date_trunc('day', dateadd(DAY, L.renewaldateofmonth - 1, date_trunc('month', dateadd(MONTH, 1, L.logdatetimestamp))))
                    ELSE date_trunc('day', dateadd(MONTH, 1, L.logdatetimestamp))
                END AS nextresetdate,
                L.renewaldateofmonth AS resetdateofmonth,
                1 AS planresetperiodvalue,
                CASE
                    WHEN L.subscribingplanplaninterval = 'MONTH' THEN 1
                    WHEN L.subscribingplanplaninterval = 'YEAR' THEN 12
                    ELSE NULL
                END AS plansuppliedresetcount,
                L.renewalcount AS remainingresetcount,
                L.currenttimes,
                L.maxtimes,
                (row_number() OVER (PARTITION BY S.workspaceid,
                                                 logtimestamp,
                                                 L.logmicrosecondstamp
                                    ORDER BY logtimestamp,
                                             L.logmicrosecondstamp,
                                             L.id)) AS idx
         FROM portfolio_subscription_event_log_v2 L
         JOIN portfolio_subscription S ON L.subscriptionid = S.id)
      WHERE idx = 1
      UNION SELECT id AS eventid,
                   workspaceid,
                   logdatetimestamp,
                   logtimestamp,
                   logtimestamp AS logmicrosecondstamp,
                   eventname,
                   planid,
                   NULL AS nextplanid,
                   false as issubscribing,
                   NULL::TIMESTAMP AS nextresetdate,
                   NULL AS resetdateofmonth,
                   NULL AS planresetperiodvalue,
                   NULL AS plansuppliedresetcount,
                   NULL AS remainingresetcount,
                   NULL AS currenttimes,
                   NULL AS maxtimes
      FROM portfolio_custom_eventlog --
 ), --
 --
 all_logs_indexing AS
     (SELECT eventid,
             workspaceid,
             logdatetimestamp,
             logtimestamp,
             logmicrosecondstamp,
             eventname,
             planid,
             nextplanid,
             issubscribing,
             (lag(nextplanid) OVER (PARTITION BY workspaceid
                                    ORDER BY logtimestamp ASC, logmicrosecondstamp ASC)) AS prev_nextplanid,
             nextresetdate,
             resetdateofmonth,
             planresetperiodvalue,
             plansuppliedresetcount,
             remainingresetcount,
             currenttimes,
             maxtimes,
             (row_number() OVER (PARTITION BY workspaceid
                                 ORDER BY logtimestamp ASC, logmicrosecondstamp ASC)) AS idx,
             (row_number() OVER (PARTITION BY workspaceid
                                 ORDER BY logtimestamp DESC, logmicrosecondstamp DESC)) AS idx_inverse
      FROM all_logs --
 ), --
 --
 migration_planid_map_v1 AS
     (SELECT workspaceid,
             eventname,
             logtimestamp,
             logmicrosecondstamp,
             migration_target_planid,
             migration_planid
      FROM
        (SELECT workspaceid,
                eventname,
                logtimestamp,
                logmicrosecondstamp,
                (lag(planid) OVER (PARTITION BY workspaceid
                                   ORDER BY logtimestamp ASC, logmicrosecondstamp ASC)) AS migration_target_planid,
                planid AS migration_planid
         FROM all_logs_indexing)
      WHERE eventname IN ('PLAN_MIGRATION_2022_01',
                          'PLAN_MIGRATION_2022_03',
                          'PLAN_MIGRATION_2022_11')--
 ), --
 --
 main_mapping_step1 AS
     (SELECT ali.eventid,
             ali.workspaceid,
             ali.logdatetimestamp,
             ali.logtimestamp,
             CASE
                 WHEN ali.eventname IN ('PLAN_MIGRATION_2022_01',
                                        'PLAN_MIGRATION_2022_03',
                                        'PLAN_MIGRATION_2022_11') THEN 'SNAPSHOT'
                 WHEN ali.eventname = 'SUBSCRIPTION_CHANGED'
                      AND nextplanid IS NOT NULL THEN 'SUBSCRIPTION_NEXT_PLAN_CHANGED'
                 WHEN ali.eventname = 'SUBSCRIPTION_RESET'
                      AND ali.prev_nextplanid IS NOT NULL
                      AND ali.prev_nextplanid = ali.planid THEN 'SUBSCRIPTION_CHANGED'
                 ELSE ali.eventname
             END AS eventname,
             coalesce(mpm.migration_planid, ali.planid) AS planid,
             (lag(coalesce(mpm.migration_planid, ali.planid)) OVER (PARTITION BY ali.workspaceid
                                                                    ORDER BY ali.logtimestamp)) AS prev_planid,
             ali.nextplanid,
             ali.issubscribing,
             ali.prev_nextplanid,
             ali.nextresetdate,
             ali.resetdateofmonth,
             ali.planresetperiodvalue,
             ali.plansuppliedresetcount,
             ali.remainingresetcount,
             ali.currenttimes,
             ali.maxtimes,
             ali.idx,
             ali.idx_inverse
      FROM all_logs_indexing ali
      LEFT JOIN migration_planid_map_v1 mpm ON ali.workspaceid = mpm.workspaceid
      AND mpm.migration_target_planid = ali.planid
      AND mpm.logtimestamp > ali.logtimestamp --
 ), --
 --
migration_planid_map_v2 AS
     (SELECT workspaceid,
             planid,
             prev_planid AS migration_target_planid
      FROM main_mapping_step1 main
      LEFT JOIN portfolio_plan_info pi ON pi.id = main.planid
      WHERE main.eventname = 'SUBSCRIPTION_RESET'
        AND main.planid != main.prev_planid --
 ), --
 --
 main_mapping_step2 AS
     (SELECT main.eventid,
             main.workspaceid,
             main.logdatetimestamp,
             main.logtimestamp,
             main.eventname,
             CASE
                 WHEN main.eventid = 'SAMPLE_EVENT_ID_001' THEN main.planid -- 샘플 이벤트 처리
                 WHEN main.eventid = 'SAMPLE_EVENT_ID_002' THEN main.planid -- 샘플 이벤트 처리
                 ELSE coalesce(mpm.planid, main.planid)
             END AS planid,
             main.nextplanid,
             main.issubscribing,
             main.nextresetdate,
             main.resetdateofmonth,
             main.planresetperiodvalue,
             main.plansuppliedresetcount,
             main.remainingresetcount,
             main.currenttimes,
             main.maxtimes,
             main.idx,
             main.idx_inverse
      FROM main_mapping_step1 main
      LEFT JOIN migration_planid_map_v2 mpm ON main.workspaceid = mpm.workspaceid
      AND mpm.migration_target_planid = main.planid --
 ) --
 --
 SELECT eventid,
        workspaceid,
        logdatetimestamp,
        logtimestamp,
        eventname,
        planid,
        nextplanid,
        issubscribing,
        nextresetdate,
        resetdateofmonth,
        planresetperiodvalue,
        plansuppliedresetcount,
        remainingresetcount,
        currenttimes,
        maxtimes,
        idx,
        idx_inverse
   FROM main_mapping_step2);

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 포트폴리오 샘플: 구독 기간 분석
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 이 섹션은 구독 기간 계산 및 분석을 보여줍니다
-- 비즈니스 인텔리전스 및 리포팅 목적으로 사용됩니다

-- 구독 기간을 위한 샘플 테이블 구조
-- CREATE TABLE portfolio_subscription_periods (
--     id VARCHAR(65535),
--     workspaceid VARCHAR(65535),
--     eventid VARCHAR(65535),
--     subscription_start TIMESTAMP,
--     subscription_end TIMESTAMP,
--     expected_subscription_end TIMESTAMP,
--     planid VARCHAR(65535),
--     issubscribing boolean, 
--     nextresetdate TIMESTAMP,
--     resetdateofmonth INT,
--     remainingresetcount INT,
--     currenttimes INT,
--     maxtimes INT,
--     title VARCHAR(65535),
--     grade INT,
--     price BIGINT,
--     resetperiodvalue INT,
--     suppliedresetcount INT,
--     planversion INT,
--     depth1_name VARCHAR(65535),
--     depth2_name VARCHAR(65535),
--     depth3_name VARCHAR(65535),
--     depth4_name VARCHAR(65535),
--     depth5_name VARCHAR(65535),
--     PRIMARY KEY (id));

-- 샘플 데이터 처리 로직
WITH RECURSIVE subscription_periods_recursive(eventid, workspaceid, eventname, logdatetimestamp, logtimestamp, planid, group_planid, idx, group_number) AS
     (SELECT eventid,
             workspaceid,
             eventname,
             logdatetimestamp,
             logtimestamp,
             planid,
             planid AS group_planid,
             idx,
             1 AS group_number
      FROM temp_portfolio_event_logs
      WHERE idx = 1
      UNION ALL SELECT CASE
                           WHEN ael.planid IS NULL THEN lr.eventid
                           ELSE ael.eventid
                       END AS eventid_adj,
                       lr.workspaceid,
                       ael.eventname,
                       ael.logdatetimestamp,
                       ael.logtimestamp,
                       CASE
                           WHEN ael.eventname = 'SUBSCRIPTION_RESET' THEN lr.planid
                           WHEN ael.eventname = 'SUBSCRIPTION_RENEWED'
                                AND coalesce(lr.planid, '') = coalesce(ael.planid, lr.planid, '') THEN lr.planid
                           ELSE ael.planid
                       END AS new_planid,
                       CASE
                           WHEN ael.eventname = 'SUBSCRIPTION_RESET' THEN lr.planid
                           WHEN ael.eventname = 'SUBSCRIPTION_RENEWED'
                                AND coalesce(lr.planid, '') = coalesce(ael.planid, lr.planid, '') THEN lr.planid
                           ELSE coalesce(ael.planid, lr.planid)
                       END AS new_group_planid,
                       ael.idx,
                       CASE
                           WHEN ael.eventname = 'SUBSCRIPTION_RESET' THEN lr.group_number
                           WHEN ael.eventname = 'SUBSCRIPTION_RENEWED'
                                AND coalesce(lr.planid, '') = coalesce(ael.planid, lr.planid, '') THEN lr.group_number
                           WHEN coalesce(lr.planid, '') != coalesce(ael.planid, lr.planid, '') THEN lr.group_number + 1
                           ELSE lr.group_number
                       END AS new_group_number
      FROM subscription_periods_recursive lr
      LEFT JOIN temp_portfolio_event_logs ael ON ael.workspaceid = lr.workspaceid
      AND lr.idx + 1 = ael.idx
      WHERE ael.idx_inverse >= 1)
SELECT '포트폴리오 샘플 쿼리 1 완료' AS status,
       CURRENT_TIMESTAMP AS execution_time;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 포트폴리오 샘플 쿼리 1 요약
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 이 쿼리는 다음을 보여줍니다:
-- 1. CTE와 윈도우 함수를 사용한 복잡한 데이터 변환
-- 2. 구독 생명주기 관리 및 분석
-- 3. 이벤트 로그 처리 및 비즈니스 로직 구현
-- 4. 계층적 데이터 분석을 위한 재귀적 쿼리 패턴
-- 5. 데이터 품질 검사 및 검증
-- 
-- Apache Airflow DAG에서 일일 구독 데이터 처리를 위해 사용됩니다
-- 비즈니스 인텔리전스 및 리포팅의 기반을 제공합니다
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
