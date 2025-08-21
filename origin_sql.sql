------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- general
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

CREATE TEMP TABLE temp_all_event_logs AS
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
      FROM subscription_service_subscription_event_log
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
         FROM subscription_service_v20210925_subscription_event_log L
         JOIN subscription_service_subscription S ON L.subscriptionid = S.id)
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
      FROM custom_eventlog_adjust --
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
      WHERE eventname IN ('20220105-ZERO-PRICE-PLAN-MIGRATION',
                          '20220314-OLD-PROMOTION-PLAN-MIGRATION',
                          '20221121-PREMIUM-PLAN-MIGRATION')--
 ), --
 --
 main_mapping_step1 AS
     (SELECT ali.eventid,
             ali.workspaceid,
             ali.logdatetimestamp,
             ali.logtimestamp,
             CASE
                 WHEN ali.eventname IN ('20220105-ZERO-PRICE-PLAN-MIGRATION',
                                        '20220314-OLD-PROMOTION-PLAN-MIGRATION',
                                        '20221121-PREMIUM-PLAN-MIGRATION') THEN 'SNAPSHOT'
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
      LEFT JOIN custom_plan_info pi ON pi.id = main.planid
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
                 WHEN main.eventid = '01FXQK6EW8XRADMYQ4W1SWB4WC' THEN main.planid -- 이상한 이력 하드코딩

                 WHEN main.eventid = '01FXAQ51Z5JC3QFNNZYZA1TNX9' THEN main.planid -- 이상한 이력 하드코딩

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
-- custom_plan_subscription_periods
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- DROP TABLE custom_plan_subscription_periods;
-- CREATE TABLE custom_plan_subscription_periods (
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
--     PRIMARY KEY (id)); --

DELETE
FROM custom_plan_subscription_periods;


INSERT INTO custom_plan_subscription_periods (id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, planid, issubscribing, nextresetdate, resetdateofmonth, remainingresetcount, currenttimes, maxtimes, title, grade, price, resetperiodvalue, suppliedresetcount, planversion, depth1_name, depth2_name, depth3_name, depth4_name, depth5_name)
  (WITH RECURSIVE preprocessed_event_log_recursive(eventid, workspaceid, eventname, logdatetimestamp, logtimestamp, planid, group_planid, idx, group_number) AS
     (SELECT eventid,
             workspaceid,
             eventname,
             logdatetimestamp,
             logtimestamp,
             planid,
             planid AS group_planid,
             idx,
             1 AS group_number
      FROM temp_all_event_logs
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
      FROM preprocessed_event_log_recursive lr
      LEFT JOIN temp_all_event_logs ael ON ael.workspaceid = lr.workspaceid
      AND lr.idx + 1 = ael.idx
      WHERE ael.idx_inverse >= 1 --
 ), --
 --
 _result AS
     (SELECT workspaceid,
             eventid,
             logtimestamp_start,
             logdatetimestamp_start,
             planid
      FROM
        (SELECT workspaceid,
                (last_value(eventid) OVER (PARTITION BY workspaceid,
                                                        group_number
                                           ORDER BY logtimestamp ASC ROWS BETWEEN CURRENT ROW AND unbounded following)) AS eventid,
                (first_value(logtimestamp) OVER (PARTITION BY workspaceid,
                                                              group_number
                                                 ORDER BY logtimestamp ASC ROWS unbounded preceding)) AS logtimestamp_start,
                (first_value(logdatetimestamp) OVER (PARTITION BY workspaceid,
                                                                  group_number
                                                     ORDER BY logtimestamp ASC ROWS unbounded preceding)) AS logdatetimestamp_start,
                group_planid AS planid
         FROM preprocessed_event_log_recursive)
      WHERE planid IS NOT NULL
      GROUP BY 1,
               2,
               3,
               4,
               5--
), --
--
item_type AS
     (SELECT p.id,
             p.title,
             p.grade,
             p.price,
             p.resetperiodvalue,
             p.suppliedresetcount,
             p.planversion,
             pc.depth1_name,
             pc.depth2_name,
             pc.depth3_name,
             pc.depth4_name,
             pc.depth5_name
      FROM subscription_service_plan p
      LEFT JOIN
        (SELECT planid,
                depth1_name,
                depth2_name,
                depth3_name,
                depth4_name,
                depth5_name
         FROM
           (SELECT pc.planid,
                   pc.type,
                   pcv.name
            FROM subscription_service_plan_category pc
            LEFT JOIN subscription_service_plan_category_value pcv ON pc.plancategoryvalueid = pcv.id) PIVOT (max(name) AS name
                                                                                                              FOR TYPE IN ('planCategoryDepth1' AS depth1, 'planCategoryDepth2' AS depth2, 'planCategoryDepth3' AS depth3, 'planCategoryDepth4' AS depth4, 'planCategoryDepth5' AS depth5))) pc ON pc.planid = p.id --
 ), --
 --
 subscription_end AS
     (SELECT e1.eventid,
             MIN(e2.logdatetimestamp) AS end_timestamp
      FROM _result e1
      JOIN preprocessed_event_log_recursive e2 ON e1.workspaceid = e2.workspaceid
      AND e2.logtimestamp > e1.logtimestamp_start
      AND COALESCE(e2.planid, '') != COALESCE(e1.planid, '')
      GROUP BY e1.eventid --
 ) --
 --
 SELECT SUBSTRING(MD5(RANDOM()::text), 1, 32) AS id,
        r.workspaceid,
        r.eventid,
        r.logdatetimestamp_start AS subscription_start,
        CASE
            WHEN s.status != 'ACTIVE'
                 AND se.end_timestamp IS NULL THEN dateadd(MONTH, ael.remainingresetcount*ael.planresetperiodvalue, date_trunc('day', ael.nextresetdate))
            ELSE se.end_timestamp
        END AS subscription_end,
        CASE
            WHEN se.end_timestamp IS NULL THEN dateadd(MONTH, ael.remainingresetcount*ael.planresetperiodvalue, date_trunc('day', ael.nextresetdate))
            ELSE NULL
        END AS expected_subscription_end,
        r.planid,
        ael.issubscribing,
        ael.nextresetdate,
        ael.resetdateofmonth,
        ael.remainingresetcount,
        ael.currenttimes,
        ael.maxtimes,
        it.title,
        it.grade,
        it.price,
        it.resetperiodvalue,
        it.suppliedresetcount,
        it.planversion,
        it.depth1_name,
        it.depth2_name,
        it.depth3_name,
        it.depth4_name,
        it.depth5_name
   FROM _result r
   LEFT JOIN subscription_end se ON se.eventid = r.eventid
   LEFT JOIN temp_all_event_logs ael ON ael.eventid = r.eventid
   LEFT JOIN item_type it ON r.planid = it.id
   LEFT JOIN subscription_service_subscription s ON s.workspaceid = r.workspaceid);

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- custom_plan_subscription_usage_reset_periods
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
-- DROP TABLE custom_plan_subscription_usage_reset_periods;
-- CREATE TABLE custom_plan_subscription_usage_reset_periods (
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
--     reset_period_start TIMESTAMP,
--     reset_period_end TIMESTAMP,
--     PRIMARY KEY (id));
 -- DELETE
-- FROM custom_plan_subscription_usage_reset_periods;

CREATE TEMP TABLE temp_custom_plan_subscription_usage_reset_periods AS
  (WITH RECURSIVE preprocessed_event_log_recursive(eventid, workspaceid, eventname, logdatetimestamp, logtimestamp, planid, group_planid, idx, group_number) AS
     (SELECT eventid,
             workspaceid,
             eventname,
             logdatetimestamp,
             logtimestamp,
             planid,
             planid AS group_planid,
             idx,
             1 AS group_number
      FROM temp_all_event_logs
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
                           WHEN ael.eventname = 'SUBSCRIPTION_RESET' THEN lr.group_number + 1
                           WHEN ael.eventname = 'SUBSCRIPTION_RENEWED'
                                AND coalesce(lr.planid, '') = coalesce(ael.planid, lr.planid, '') THEN lr.group_number + 1
                           WHEN coalesce(lr.planid, '') != coalesce(ael.planid, lr.planid, '') THEN lr.group_number + 1
                           ELSE lr.group_number
                       END AS new_group_number
      FROM preprocessed_event_log_recursive lr
      LEFT JOIN temp_all_event_logs ael ON ael.workspaceid = lr.workspaceid
      AND lr.idx + 1 = ael.idx
      WHERE ael.idx_inverse >= 1 --
 ), --
 --
 _result AS
     (SELECT workspaceid,
             eventid,
             logtimestamp_start,
             logdatetimestamp_start,
             planid
      FROM
        (SELECT workspaceid,
                (last_value(eventid) OVER (PARTITION BY workspaceid,
                                                        group_number
                                           ORDER BY logtimestamp ASC, eventid ASC ROWS BETWEEN CURRENT ROW AND unbounded following)) AS eventid,
                (first_value(logtimestamp) OVER (PARTITION BY workspaceid,
                                                              group_number
                                                 ORDER BY logtimestamp ASC, eventid ASC ROWS unbounded preceding)) AS logtimestamp_start,
                (first_value(logdatetimestamp) OVER (PARTITION BY workspaceid,
                                                                  group_number
                                                     ORDER BY logtimestamp ASC, eventid ASC ROWS unbounded preceding)) AS logdatetimestamp_start,
                group_planid AS planid
         FROM preprocessed_event_log_recursive)
      WHERE planid IS NOT NULL
      GROUP BY 1,
               2,
               3,
               4,
               5--
), --
--
 item_type AS
     (SELECT p.id,
             p.title,
             p.grade,
             p.price,
             p.resetperiodvalue,
             p.suppliedresetcount,
             p.planversion,
             pc.depth1_name,
             pc.depth2_name,
             pc.depth3_name,
             pc.depth4_name,
             pc.depth5_name
      FROM subscription_service_plan p
      LEFT JOIN
        (SELECT planid,
                depth1_name,
                depth2_name,
                depth3_name,
                depth4_name,
                depth5_name
         FROM
           (SELECT pc.planid,
                   pc.type,
                   pcv.name
            FROM subscription_service_plan_category pc
            LEFT JOIN subscription_service_plan_category_value pcv ON pc.plancategoryvalueid = pcv.id) PIVOT (max(name) AS name
                                                                                                              FOR TYPE IN ('planCategoryDepth1' AS depth1, 'planCategoryDepth2' AS depth2, 'planCategoryDepth3' AS depth3, 'planCategoryDepth4' AS depth4, 'planCategoryDepth5' AS depth5))) pc ON pc.planid = p.id --
 ), --
 --
 subscription_end AS
     (SELECT e1.eventid,
             MIN(e2.logdatetimestamp) AS end_timestamp
      FROM _result e1
      JOIN preprocessed_event_log_recursive e2 ON e1.workspaceid = e2.workspaceid
      AND e2.logtimestamp > e1.logtimestamp_start
      AND CASE
              WHEN COALESCE(e2.planid, '') != COALESCE(e1.planid, '') THEN TRUE
              WHEN e2.eventname = 'SUBSCRIPTION_RESET' THEN TRUE
              WHEN e2.eventname = 'SUBSCRIPTION_RENEWED' THEN TRUE
              ELSE FALSE
          END
      GROUP BY e1.eventid --
 ) --
 --
 SELECT SUBSTRING(MD5(RANDOM()::text), 1, 32) AS id,
        r.workspaceid,
        r.eventid,
        r.logdatetimestamp_start AS subscription_start,
        CASE
            WHEN s.status != 'ACTIVE'
                 AND se.end_timestamp IS NULL THEN dateadd(MONTH, ael.remainingresetcount*ael.planresetperiodvalue, date_trunc('day', ael.nextresetdate))
            ELSE se.end_timestamp
        END AS subscription_end,
        CASE
            WHEN se.end_timestamp IS NULL THEN dateadd(MONTH, ael.remainingresetcount*ael.planresetperiodvalue, date_trunc('day', ael.nextresetdate))
            ELSE NULL
        END AS expected_subscription_end,
        r.planid,
        ael.issubscribing,
        ael.nextresetdate,
        ael.resetdateofmonth,
        ael.remainingresetcount,
        ael.currenttimes,
        ael.maxtimes,
        it.title,
        it.grade,
        it.price,
        it.resetperiodvalue,
        it.suppliedresetcount,
        it.planversion,
        it.depth1_name,
        it.depth2_name,
        it.depth3_name,
        it.depth4_name,
        it.depth5_name,
        r.logdatetimestamp_start AS reset_period_start,
        se.end_timestamp AS reset_period_end
   FROM _result r
   LEFT JOIN subscription_end se ON se.eventid = r.eventid
   LEFT JOIN temp_all_event_logs ael ON ael.eventid = r.eventid
   LEFT JOIN item_type it ON r.planid = it.id
   LEFT JOIN subscription_service_subscription s ON s.workspaceid = r.workspaceid);


DELETE
FROM custom_plan_subscription_usage_reset_periods;


INSERT INTO custom_plan_subscription_usage_reset_periods(id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, planid, issubscribing, nextresetdate, resetdateofmonth, remainingresetcount, currenttimes, maxtimes, title, grade, price, resetperiodvalue, suppliedresetcount, planversion, depth1_name, depth2_name, depth3_name, depth4_name, depth5_name, reset_period_start, reset_period_end)
  (WITH RECURSIVE plan_subscription_usage_reset_periods(id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, planid, issubscribing, nextresetdate, resetdateofmonth, remainingresetcount, currenttimes, maxtimes, title, grade, price, resetperiodvalue, suppliedresetcount, planversion, depth1_name, depth2_name, depth3_name, depth4_name, depth5_name, reset_period_start, reset_period_end, month_diff) AS
     (WITH periods AS
        (SELECT id,
                workspaceid,
                eventid,
                subscription_start,
                subscription_end,
                expected_subscription_end,
                planid,
                issubscribing,
                nextresetdate,
                resetdateofmonth,
                remainingresetcount,
                currenttimes,
                maxtimes,
                title,
                grade,
                price,
                resetperiodvalue,
                suppliedresetcount,
                planversion,
                depth1_name,
                depth2_name,
                depth3_name,
                depth4_name,
                depth5_name,
                MONTHS_BETWEEN(coalesce(sp.subscription_end, sysdate + interval '9 hour')::TIMESTAMP, sp.subscription_start::TIMESTAMP)::float AS month_diff
         FROM temp_custom_plan_subscription_usage_reset_periods sp --
 ) --
 --
 SELECT id,
        workspaceid,
        eventid,
        subscription_start,
        subscription_end,
        expected_subscription_end,
        planid,
        issubscribing,
        nextresetdate,
        resetdateofmonth,
        remainingresetcount,
        currenttimes,
        maxtimes,
        title,
        grade,
        price,
        resetperiodvalue,
        suppliedresetcount,
        planversion,
        depth1_name,
        depth2_name,
        depth3_name,
        depth4_name,
        depth5_name,
        subscription_start AS reset_period_start,
        CASE
            WHEN dateadd(MONTH, resetperiodvalue, subscription_start) >= coalesce(subscription_end, sysdate + interval '9 hour') THEN subscription_end
            WHEN date_part(DAY, last_day(dateadd(MONTH, resetperiodvalue, subscription_start)))::int >= resetdateofmonth THEN date_trunc('day', dateadd(DAY, resetdateofmonth - 1, date_trunc('month', dateadd(MONTH, resetperiodvalue, subscription_start))))
            ELSE date_trunc('day', dateadd(MONTH, resetperiodvalue, subscription_start))
        END AS reset_period_end,
        month_diff
      FROM periods
      UNION ALL SELECT rp.id,
                       rp.workspaceid,
                       rp.eventid,
                       rp.subscription_start,
                       rp.subscription_end,
                       rp.expected_subscription_end,
                       rp.planid,
                       rp.issubscribing,
                       rp.nextresetdate,
                       rp.resetdateofmonth,
                       rp.remainingresetcount,
                       rp.currenttimes,
                       rp.maxtimes,
                       rp.title,
                       rp.grade,
                       rp.price,
                       rp.resetperiodvalue,
                       rp.suppliedresetcount,
                       rp.planversion,
                       rp.depth1_name,
                       rp.depth2_name,
                       rp.depth3_name,
                       rp.depth4_name,
                       rp.depth5_name,
                       rp.reset_period_end AS reset_period_start_recursive,
                       CASE
                           WHEN dateadd(MONTH, rp.resetperiodvalue, rp.reset_period_end) >= coalesce(rp.subscription_end, sysdate + interval '9 hour') THEN rp.subscription_end
                           WHEN date_part(DAY, last_day(dateadd(MONTH, rp.resetperiodvalue, rp.reset_period_end)))::int >= rp.resetdateofmonth THEN date_trunc('day', dateadd(DAY, rp.resetdateofmonth - 1, date_trunc('month', dateadd(MONTH, rp.resetperiodvalue, rp.reset_period_end))))
                           ELSE date_trunc('day', dateadd(MONTH, rp.resetperiodvalue, rp.reset_period_end))
                       END AS reset_period_end_recursive,
                       rp.month_diff - rp.resetperiodvalue AS next_month_diff
      FROM plan_subscription_usage_reset_periods rp
      WHERE next_month_diff > 0
        AND CASE
                WHEN dateadd(MONTH, rp.resetperiodvalue, rp.reset_period_end) >= coalesce(rp.subscription_end, sysdate + interval '9 hour') THEN rp.subscription_end
                WHEN date_part(DAY, last_day(dateadd(MONTH, rp.resetperiodvalue, rp.reset_period_end)))::int >= rp.resetdateofmonth THEN date_trunc('day', dateadd(DAY, rp.resetdateofmonth - 1, date_trunc('month', dateadd(MONTH, rp.resetperiodvalue, rp.reset_period_end))))
                ELSE date_trunc('day', dateadd(MONTH, rp.resetperiodvalue, rp.reset_period_end))
            END < coalesce(rp.subscription_end, rp.expected_subscription_end) --
 ) --
 --
 SELECT id,
        workspaceid,
        eventid,
        subscription_start,
        subscription_end,
        expected_subscription_end,
        planid,
        issubscribing,
        nextresetdate,
        resetdateofmonth,
        remainingresetcount,
        currenttimes,
        maxtimes,
        title,
        grade,
        price,
        resetperiodvalue,
        suppliedresetcount,
        planversion,
        depth1_name,
        depth2_name,
        depth3_name,
        depth4_name,
        depth5_name,
        reset_period_start,
        reset_period_end
   FROM plan_subscription_usage_reset_periods
   WHERE reset_period_start IS NOT NULL);

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- custom_plan_subscription_payment_renew_periods
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
-- DROP TABLE custom_plan_subscription_payment_renew_periods;
-- CREATE TABLE custom_plan_subscription_payment_renew_periods (
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
--     reset_period_start TIMESTAMP,
--     reset_period_end TIMESTAMP,
--     PRIMARY KEY (id));
 -- DELETE
-- FROM custom_plan_subscription_payment_renew_periods;

CREATE TEMP TABLE temp_custom_plan_subscription_payment_renew_periods AS
  (WITH RECURSIVE preprocessed_event_log_recursive(eventid, workspaceid, eventname, logdatetimestamp, logtimestamp, planid, group_planid, idx, group_number) AS
     (SELECT eventid,
             workspaceid,
             eventname,
             logdatetimestamp,
             logtimestamp,
             planid,
             planid AS group_planid,
             idx,
             1 AS group_number
      FROM temp_all_event_logs
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
                                AND coalesce(lr.planid, '') = coalesce(ael.planid, lr.planid, '') THEN lr.group_number + 1
                           WHEN coalesce(lr.planid, '') != coalesce(ael.planid, lr.planid, '') THEN lr.group_number + 1
                           ELSE lr.group_number
                       END AS new_group_number
      FROM preprocessed_event_log_recursive lr
      LEFT JOIN temp_all_event_logs ael ON ael.workspaceid = lr.workspaceid
      AND lr.idx + 1 = ael.idx
      WHERE ael.idx_inverse >= 1 --
 ), --
 --
 _result AS
     (SELECT workspaceid,
             eventid,
             logtimestamp_start,
             logdatetimestamp_start,
             planid
      FROM
        (SELECT workspaceid,
                (last_value(eventid) OVER (PARTITION BY workspaceid,
                                                        group_number
                                           ORDER BY logtimestamp ASC, eventid ASC ROWS BETWEEN CURRENT ROW AND unbounded following)) AS eventid,
                (first_value(logtimestamp) OVER (PARTITION BY workspaceid,
                                                              group_number
                                                 ORDER BY logtimestamp ASC, eventid ASC ROWS unbounded preceding)) AS logtimestamp_start,
                (first_value(logdatetimestamp) OVER (PARTITION BY workspaceid,
                                                                  group_number
                                                     ORDER BY logtimestamp ASC, eventid ASC ROWS unbounded preceding)) AS logdatetimestamp_start,
                group_planid AS planid
         FROM preprocessed_event_log_recursive)
      WHERE planid IS NOT NULL
      GROUP BY 1,
               2,
               3,
               4,
               5--
), --
--
 item_type AS
     (SELECT p.id,
             p.title,
             p.grade,
             p.price,
             p.resetperiodvalue,
             p.suppliedresetcount,
             p.planversion,
             pc.depth1_name,
             pc.depth2_name,
             pc.depth3_name,
             pc.depth4_name,
             pc.depth5_name
      FROM subscription_service_plan p
      LEFT JOIN
        (SELECT planid,
                depth1_name,
                depth2_name,
                depth3_name,
                depth4_name,
                depth5_name
         FROM
           (SELECT pc.planid,
                   pc.type,
                   pcv.name
            FROM subscription_service_plan_category pc
            LEFT JOIN subscription_service_plan_category_value pcv ON pc.plancategoryvalueid = pcv.id) PIVOT (max(name) AS name
                                                                                                              FOR TYPE IN ('planCategoryDepth1' AS depth1, 'planCategoryDepth2' AS depth2, 'planCategoryDepth3' AS depth3, 'planCategoryDepth4' AS depth4, 'planCategoryDepth5' AS depth5))) pc ON pc.planid = p.id --
 ), --
 --
 subscription_end AS
     (SELECT e1.eventid,
             MIN(e2.logdatetimestamp) AS end_timestamp
      FROM _result e1
      JOIN preprocessed_event_log_recursive e2 ON e1.workspaceid = e2.workspaceid
      AND e2.logtimestamp > e1.logtimestamp_start
      AND CASE
              WHEN COALESCE(e2.planid, '') != COALESCE(e1.planid, '') THEN TRUE
              WHEN e2.eventname = 'SUBSCRIPTION_RENEWED' THEN TRUE
              ELSE FALSE
          END
      GROUP BY e1.eventid --
 ) --
 --
 SELECT SUBSTRING(MD5(RANDOM()::text), 1, 32) AS id,
        r.workspaceid,
        r.eventid,
        r.logdatetimestamp_start AS subscription_start,
        CASE
            WHEN s.status != 'ACTIVE'
                 AND se.end_timestamp IS NULL THEN dateadd(MONTH, ael.remainingresetcount*ael.planresetperiodvalue, date_trunc('day', ael.nextresetdate))
            ELSE se.end_timestamp
        END AS subscription_end,
        CASE
            WHEN se.end_timestamp IS NULL THEN dateadd(MONTH, ael.remainingresetcount*ael.planresetperiodvalue, date_trunc('day', ael.nextresetdate))
            ELSE NULL
        END AS expected_subscription_end,
        r.planid,
        ael.issubscribing,
        ael.nextresetdate,
        ael.resetdateofmonth,
        ael.remainingresetcount,
        ael.currenttimes,
        ael.maxtimes,
        it.title,
        it.grade,
        it.price,
        it.resetperiodvalue,
        it.suppliedresetcount,
        it.planversion,
        it.depth1_name,
        it.depth2_name,
        it.depth3_name,
        it.depth4_name,
        it.depth5_name,
        r.logdatetimestamp_start AS reset_period_start,
        se.end_timestamp AS reset_period_end
   FROM _result r
   LEFT JOIN subscription_end se ON se.eventid = r.eventid
   LEFT JOIN temp_all_event_logs ael ON ael.eventid = r.eventid
   LEFT JOIN item_type it ON r.planid = it.id
   LEFT JOIN subscription_service_subscription s ON s.workspaceid = r.workspaceid);


DELETE
FROM custom_plan_subscription_payment_renew_periods;


INSERT INTO custom_plan_subscription_payment_renew_periods(id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, planid, issubscribing, nextresetdate, resetdateofmonth, remainingresetcount, currenttimes, maxtimes, title, grade, price, resetperiodvalue, suppliedresetcount, planversion, depth1_name, depth2_name, depth3_name, depth4_name, depth5_name, reset_period_start, reset_period_end)
  (WITH RECURSIVE plan_subscription_payment_renew_periods(id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, planid, issubscribing, nextresetdate, resetdateofmonth, remainingresetcount, currenttimes, maxtimes, title, grade, price, resetperiodvalue, suppliedresetcount, planversion, depth1_name, depth2_name, depth3_name, depth4_name, depth5_name, reset_period_start, reset_period_end, month_diff) AS
     (WITH periods AS
        (SELECT id,
                workspaceid,
                eventid,
                subscription_start,
                subscription_end,
                expected_subscription_end,
                planid,
                issubscribing,
                nextresetdate,
                resetdateofmonth,
                remainingresetcount,
                currenttimes,
                maxtimes,
                title,
                grade,
                price,
                resetperiodvalue,
                suppliedresetcount,
                planversion,
                depth1_name,
                depth2_name,
                depth3_name,
                depth4_name,
                depth5_name,
                MONTHS_BETWEEN(coalesce(sp.subscription_end, sysdate + interval '9 hour')::TIMESTAMP, sp.subscription_start::TIMESTAMP)::float AS month_diff
         FROM temp_custom_plan_subscription_payment_renew_periods sp --
 ) --
 --
 SELECT id,
        workspaceid,
        eventid,
        subscription_start,
        subscription_end,
        expected_subscription_end,
        planid,
        issubscribing,
        nextresetdate,
        resetdateofmonth,
        remainingresetcount,
        currenttimes,
        maxtimes,
        title,
        grade,
        price,
        resetperiodvalue,
        suppliedresetcount,
        planversion,
        depth1_name,
        depth2_name,
        depth3_name,
        depth4_name,
        depth5_name,
        subscription_start AS reset_period_start,
        CASE
            WHEN dateadd(MONTH, resetperiodvalue * suppliedresetcount, subscription_start) >= coalesce(subscription_end, sysdate + interval '9 hour') THEN subscription_end
            WHEN date_part(DAY, last_day(dateadd(MONTH, resetperiodvalue * suppliedresetcount, subscription_start)))::int >= resetdateofmonth THEN date_trunc('day', dateadd(DAY, resetdateofmonth - 1, date_trunc('month', dateadd(MONTH, resetperiodvalue * suppliedresetcount, subscription_start))))
            ELSE date_trunc('day', dateadd(MONTH, resetperiodvalue * suppliedresetcount, subscription_start))
        END AS reset_period_end,
        month_diff
      FROM periods
      UNION ALL SELECT rp.id,
                       rp.workspaceid,
                       rp.eventid,
                       rp.subscription_start,
                       rp.subscription_end,
                       rp.expected_subscription_end,
                       rp.planid,
                       rp.issubscribing,
                       rp.nextresetdate,
                       rp.resetdateofmonth,
                       rp.remainingresetcount,
                       rp.currenttimes,
                       rp.maxtimes,
                       rp.title,
                       rp.grade,
                       rp.price,
                       rp.resetperiodvalue,
                       rp.suppliedresetcount,
                       rp.planversion,
                       rp.depth1_name,
                       rp.depth2_name,
                       rp.depth3_name,
                       rp.depth4_name,
                       rp.depth5_name,
                       rp.reset_period_end AS reset_period_start_recursive,
                       CASE
                           WHEN dateadd(MONTH, rp.resetperiodvalue*rp.suppliedresetcount, rp.reset_period_end) >= coalesce(rp.subscription_end, sysdate + interval '9 hour') THEN rp.subscription_end
                           WHEN date_part(DAY, last_day(dateadd(MONTH, rp.resetperiodvalue*rp.suppliedresetcount, rp.reset_period_end)))::int >= rp.resetdateofmonth THEN date_trunc('day', dateadd(DAY, rp.resetdateofmonth - 1, date_trunc('month', dateadd(MONTH, rp.resetperiodvalue*rp.suppliedresetcount, rp.reset_period_end))))
                           ELSE date_trunc('day', dateadd(MONTH, rp.resetperiodvalue*rp.suppliedresetcount, rp.reset_period_end))
                       END AS reset_period_end_recursive,
                       rp.month_diff - (rp.resetperiodvalue*rp.suppliedresetcount) AS next_month_diff
      FROM plan_subscription_payment_renew_periods rp
      WHERE next_month_diff > 0
        AND CASE
                WHEN dateadd(MONTH, rp.resetperiodvalue*rp.suppliedresetcount, rp.reset_period_end) >= coalesce(rp.subscription_end, sysdate + interval '9 hour') THEN rp.subscription_end
                WHEN date_part(DAY, last_day(dateadd(MONTH, rp.resetperiodvalue*rp.suppliedresetcount, rp.reset_period_end)))::int >= rp.resetdateofmonth THEN date_trunc('day', dateadd(DAY, rp.resetdateofmonth - 1, date_trunc('month', dateadd(MONTH, rp.resetperiodvalue*rp.suppliedresetcount, rp.reset_period_end))))
                ELSE date_trunc('day', dateadd(MONTH, rp.resetperiodvalue*rp.suppliedresetcount, rp.reset_period_end))
            END < coalesce(rp.subscription_end, rp.expected_subscription_end) --
 ) --
 --
 SELECT id,
        workspaceid,
        eventid,
        subscription_start,
        subscription_end,
        expected_subscription_end,
        planid,
        issubscribing,
        nextresetdate,
        resetdateofmonth,
        remainingresetcount,
        currenttimes,
        maxtimes,
        title,
        grade,
        price,
        resetperiodvalue,
        suppliedresetcount,
        planversion,
        depth1_name,
        depth2_name,
        depth3_name,
        depth4_name,
        depth5_name,
        reset_period_start,
        reset_period_end
   FROM plan_subscription_payment_renew_periods
   WHERE reset_period_start IS NOT NULL);

--
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- custom_plan_subscription_monthly_periods
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--   DROP TABLE custom_plan_subscription_monthly_periods;
--   CREATE TABLE custom_plan_subscription_monthly_periods (
--     id VARCHAR(65535),
--     workspaceid VARCHAR(65535),
--     eventid VARCHAR(65535),
--     subscription_start TIMESTAMP,
--     subscription_end TIMESTAMP,
--     expected_subscription_end TIMESTAMP,
--     subscription_status VARCHAR(65535),
--     planid VARCHAR(65535),
--     sel_resetdateofmonth int,
--     resetperiodvalue int,
--     suppliedresetcount int,
--     reset_period_start TIMESTAMP,
--     reset_period_end TIMESTAMP,
--     PRIMARY KEY (id));

DELETE
FROM custom_plan_subscription_monthly_periods;


INSERT INTO custom_plan_subscription_monthly_periods(id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, subscription_status, planid, sel_resetdateofmonth, resetperiodvalue, suppliedresetcount, reset_period_start, reset_period_end)
  (WITH RECURSIVE plan_subscription_monthly_periods(id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, subscription_status, planid, sel_resetdateofmonth, resetperiodvalue, suppliedresetcount, reset_period_start, reset_period_end, month_diff) AS
     (WITH periods AS
        (SELECT sp.id,
                sp.workspaceid,
                sp.eventid,
                sp.subscription_start,
                sp.subscription_end,
                sp.expected_subscription_end,
                CASE
                    WHEN sp.subscription_end IS NULL THEN 'ACTIVE'
                    ELSE 'STOP'
                END AS subscription_status,
                sp.planid,
                sp.resetdateofmonth::int AS sel_resetdateofmonth,
                sp.resetperiodvalue,
                sp.suppliedresetcount,
                MONTHS_BETWEEN(coalesce(sp.subscription_end, sysdate + interval '9 hour')::TIMESTAMP, sp.subscription_start::TIMESTAMP)::float AS month_diff
         FROM custom_plan_subscription_periods sp --
 ) --
 --
 SELECT id,
        workspaceid,
        eventid,
        subscription_start,
        subscription_end,
        expected_subscription_end,
        subscription_status,
        planid,
        sel_resetdateofmonth,
        resetperiodvalue,
        suppliedresetcount,
        subscription_start AS reset_period_start,
        CASE
            WHEN dateadd(MONTH, 1, subscription_start) >= coalesce(subscription_end, sysdate + interval '9 hour') THEN subscription_end
            WHEN date_part(DAY, last_day(dateadd(MONTH, 1, subscription_start)))::int >= sel_resetdateofmonth THEN date_trunc('day', dateadd(DAY, sel_resetdateofmonth - 1, date_trunc('month', dateadd(MONTH, 1, subscription_start))))
            ELSE date_trunc('day', dateadd(MONTH, 1, subscription_start))
        END AS reset_period_end,
        month_diff
      FROM periods
      UNION ALL SELECT rp.id,
                       rp.workspaceid,
                       rp.eventid,
                       rp.subscription_start,
                       rp.subscription_end,
                       rp.expected_subscription_end,
                       rp.subscription_status,
                       rp.planid,
                       rp.sel_resetdateofmonth,
                       rp.resetperiodvalue,
                       rp.suppliedresetcount,
                       rp.reset_period_end AS reset_period_start_recursive,
                       CASE
                           WHEN dateadd(MONTH, 1, rp.reset_period_end) >= coalesce(rp.subscription_end, sysdate + interval '9 hour') THEN rp.subscription_end
                           WHEN date_part(DAY, last_day(dateadd(MONTH, 1, rp.reset_period_end)))::int >= rp.sel_resetdateofmonth THEN date_trunc('day', dateadd(DAY, rp.sel_resetdateofmonth - 1, date_trunc('month', dateadd(MONTH, 1, rp.reset_period_end))))
                           ELSE date_trunc('day', dateadd(MONTH, 1, rp.reset_period_end))
                       END AS reset_period_end_recursive,
                       rp.month_diff - 1 AS next_month_diff
      FROM plan_subscription_monthly_periods rp
      WHERE next_month_diff > 0 ) --
 --
 SELECT id,
        workspaceid,
        eventid,
        subscription_start,
        subscription_end,
        expected_subscription_end,
        subscription_status,
        planid,
        sel_resetdateofmonth,
        resetperiodvalue,
        suppliedresetcount,
        reset_period_start,
        reset_period_end
   FROM plan_subscription_monthly_periods
   WHERE reset_period_start IS NOT NULL);

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- custom_plan_subscription_calendar_month_periods
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--
--   DROP TABLE custom_plan_subscription_calendar_month_periods;
--   CREATE TABLE custom_plan_subscription_calendar_month_periods (
--     id VARCHAR(65535),
--     workspaceid VARCHAR(65535),
--     eventid VARCHAR(65535),
--     subscription_start TIMESTAMP,
--     subscription_end TIMESTAMP,
--     expected_subscription_end TIMESTAMP,
--     subscription_status VARCHAR(65535),
--     planid VARCHAR(65535),
--     sel_resetdateofmonth int,
--     resetperiodvalue int,
--     suppliedresetcount int,
--     reset_period_start TIMESTAMP,
--     reset_period_end TIMESTAMP,
--     PRIMARY KEY (id));

DELETE
FROM custom_plan_subscription_calendar_month_periods;


INSERT INTO custom_plan_subscription_calendar_month_periods(id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, subscription_status, planid, sel_resetdateofmonth, resetperiodvalue, suppliedresetcount, reset_period_start, reset_period_end)
  (WITH RECURSIVE plan_subscription_calendar_month_periods(id, workspaceid, eventid, subscription_start, subscription_end, expected_subscription_end, subscription_status, planid, sel_resetdateofmonth, resetperiodvalue, suppliedresetcount, reset_period_start, reset_period_end, month_diff) AS
     (WITH periods AS
        (SELECT sp.id,
                sp.workspaceid,
                sp.eventid,
                sp.subscription_start,
                sp.subscription_end,
                sp.expected_subscription_end,
                CASE
                    WHEN sp.subscription_end IS NULL THEN 'ACTIVE'
                    ELSE 'STOP'
                END AS subscription_status,
                sp.planid,
                sp.resetdateofmonth::int AS sel_resetdateofmonth,
                sp.resetperiodvalue,
                sp.suppliedresetcount,
                MONTHS_BETWEEN(coalesce(sp.subscription_end, sysdate + interval '9 hour')::TIMESTAMP, date_trunc('month', sp.subscription_start)::TIMESTAMP)::float AS month_diff
         FROM custom_plan_subscription_periods sp --
 ) --
 --
 SELECT id,
        workspaceid,
        eventid,
        subscription_start,
        subscription_end,
        expected_subscription_end,
        subscription_status,
        planid,
        sel_resetdateofmonth,
        resetperiodvalue,
        suppliedresetcount,
        subscription_start AS reset_period_start,
        CASE
            WHEN date_trunc('month', dateadd(MONTH, 1, subscription_start)) >= coalesce(subscription_end, sysdate + interval '9 hour') THEN subscription_end
            ELSE date_trunc('month', dateadd(MONTH, 1, subscription_start))
        END AS reset_period_end,
        month_diff
      FROM periods
      UNION ALL SELECT rp.id,
                       rp.workspaceid,
                       rp.eventid,
                       rp.subscription_start,
                       rp.subscription_end,
                       rp.expected_subscription_end,
                       rp.subscription_status,
                       rp.planid,
                       rp.sel_resetdateofmonth,
                       rp.resetperiodvalue,
                       rp.suppliedresetcount,
                       rp.reset_period_end AS reset_period_start_recursive,
                       CASE
                           WHEN date_trunc('month', dateadd(MONTH, 1, rp.reset_period_end)) >= coalesce(rp.subscription_end, sysdate + interval '9 hour') THEN rp.subscription_end
                           ELSE date_trunc('month', dateadd(MONTH, 1, rp.reset_period_end))
                       END AS reset_period_end_recursive,
                       rp.month_diff - 1 AS next_month_diff
      FROM plan_subscription_calendar_month_periods rp
      WHERE next_month_diff > 0 ) --
 --
 SELECT id,
        workspaceid,
        eventid,
        subscription_start,
        subscription_end,
        expected_subscription_end,
        subscription_status,
        planid,
        sel_resetdateofmonth,
        resetperiodvalue,
        suppliedresetcount,
        reset_period_start,
        reset_period_end
   FROM plan_subscription_calendar_month_periods
   WHERE reset_period_start IS NOT NULL);


SELECT 'complete' AS TRIGGER,
       SYSDATE||' UTC' AS TIMESTAMP;

