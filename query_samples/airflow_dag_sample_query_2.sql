------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 포트폴리오 샘플 쿼리 2: 결제 빌링 데이터 처리 파이프라인
-- 이 쿼리는 복잡한 빌링 데이터 변환 및 분석 기능을 보여주는 샘플입니다
-- Apache Airflow DAG에서 일일 빌링 데이터 처리를 위해 사용됩니다
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- 결제 빌링 데이터를 위한 샘플 테이블 구조
-- CREATE TABLE portfolio_payment_billing_rawdata (
--     purchaseitemcategory varchar(65535),
--     workspaceid varchar(65535),
--     invoiceid varchar(65535),
--     paymentid varchar(65535),
--     paymenthistoryid varchar(65535),
--     purchaseitemid varchar(65535),
--     status varchar(65535),
--     amount decimal(19,8),
--     finalamount decimal(19,8),
--     refundedamount decimal(19,8),
--     finalamount_diff decimal(19,8),
--     subscription_initial_start_point boolean,
--     type varchar(65535),
--     invoicetype varchar(65535),
--     datevalue timestamp,
--     planid varchar(65535),
--     extracharge_planid varchar(65535),
--     featureusageaction varchar(65535),
--     pluginaction varchar(65535),
--     invoice_subscription_start timestamp,
--     invoice_subscription_end timestamp,
--     invoicecreatedat timestamp,
--     createdat timestamp,
--     refundedat timestamp,
--     subscription_start timestamp,
--     subscription_end timestamp,
--     prev_datevalue timestamp,
--     new_yn_year boolean,
--     new_yn_quarter boolean,
--     new_yn_month boolean,
--     workspace_all_count_year decimal(19,10),
--     workspace_all_count_quarter decimal(19,10),
--     workspace_all_count_month decimal(19,10)
-- );

-- 기존 데이터 삭제
DELETE FROM portfolio_payment_billing_rawdata;

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
             issubscribing::boolean AS issubscribing,
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
                (L.maxtimes = -1) AS issubscribing,
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
                   FALSE AS issubscribing,
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

-- 주요 빌링 데이터 처리 로직
INSERT INTO portfolio_payment_billing_rawdata(purchaseitemcategory, workspaceid, invoiceid, paymentid, paymenthistoryid, purchaseitemid, status, amount, finalamount, refundedamount, finalamount_diff, subscription_initial_start_point, TYPE, invoicetype, datevalue, planid, extracharge_planid, featureusageaction, pluginaction, invoice_subscription_start, invoice_subscription_end, invoicecreatedat, createdat, refundedat, subscription_start, subscription_end, prev_datevalue, new_yn_year, new_yn_quarter, new_yn_month, workspace_all_count_year, workspace_all_count_quarter, workspace_all_count_month)
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
      FROM preprocessed_event_log_recursive lr
      LEFT JOIN temp_portfolio_event_logs ael ON ael.workspaceid = lr.workspaceid
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
      FROM portfolio_subscription_plan p
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
            FROM portfolio_subscription_plan_category pc
            LEFT JOIN portfolio_subscription_plan_category_value pcv ON pc.plancategoryvalueid = pcv.id) PIVOT (max(name) AS name
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
 ), --
 --
 PLAN_SUBSCRIPTION_PERIODS AS
     (SELECT SUBSTRING(MD5(RANDOM()::text), 1, 32) AS id,
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
      LEFT JOIN temp_portfolio_event_logs ael ON ael.eventid = r.eventid
      LEFT JOIN item_type it ON r.planid = it.id
      LEFT JOIN portfolio_subscription s ON s.workspaceid = r.workspaceid), --
 --
 --
 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 -- 구독 기간 처리 완료
 -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
 --
 PLAN_TYPE AS
     (SELECT pc.planid,
             p.title,
             p.grade,
             p.price,
             p.resetperiodvalue,
             p.suppliedresetcount,
             pc.depth1_name,
             pc.depth2_name,
             pc.depth3_name,
             pc.depth4_name,
             pc.depth5_name
      FROM portfolio_subscription_plan p
      JOIN
        (SELECT pc.planid,
                pc.type,
                pcv.name
         FROM portfolio_subscription_plan_category pc
         LEFT JOIN portfolio_subscription_plan_category_value pcv ON pc.plancategoryvalueid = pcv.id) PIVOT (max(name) AS name
                                                                                                           FOR TYPE IN ('planCategoryDepth1' AS depth1, 'planCategoryDepth2' AS depth2, 'planCategoryDepth3' AS depth3, 'planCategoryDepth4' AS depth4, 'planCategoryDepth5' AS depth5)) pc ON pc.planid = p.id --
 ), --
--
INVOICE_TYPE AS
     (SELECT invoiceid,
             depth1_name,
             depth2_name,
             depth3_name
      FROM
        (SELECT ic.invoiceid,
                icv.level,
                icv.labelcode
         FROM portfolio_invoice_category ic
         JOIN portfolio_invoice_category_value icv ON ic.invoicecategoryvalueid = icv.value) PIVOT (max(labelcode) AS name
                                                                                                              FOR LEVEL IN ('1' AS depth1, '2' AS depth2, '3' AS depth3)) --
 ), --
--
 TEST_PLAN_LIST AS
     (SELECT DISTINCT planid
      FROM PLAN_TYPE
      WHERE CASE
                WHEN depth2_name = 'Trial Plan' THEN TRUE
                WHEN depth5_name = 'Free' THEN TRUE
                WHEN depth1_name = 'Test' THEN TRUE
                WHEN depth2_name = 'Demo Plan' THEN TRUE
                ELSE FALSE
            END--
 ), --
--
TEST_WORKSPACE AS
     (SELECT DISTINCT m.workspaceid
      FROM portfolio_test_user tu
      JOIN portfolio_member m ON m.userid = tu.userid --
 ), --
--
INVOICE AS
     (SELECT i.id,
             p.id AS paymentid,
             CASE
                 WHEN ii.planid IS NOT NULL THEN 'PLAN'
                 ELSE 'EXTRACHARGE'
             END AS purchaseitemcategory,
             CASE
                 WHEN ri.paymentid IS NULL THEN 'PAID'
                 WHEN ri.paymentid IS NOT NULL THEN 'REFUNDED'
             END AS status,
             i.type AS invoicetype,
             i.workspaceid,
             i.amount,
             i.finalamount,
             p.type,
             ii.periodstart,
             ii.periodend,
             i.createdat AS invoicecreatedat,
             p.createdat,
             ii.id AS purchaseitemid,
             ii.title,
             ii.planid,
             ii.featureusageaction,
             ii.pluginaction,
             ri.refundedat,
             ri.refundedamount
      FROM portfolio_payment_invoice i
      JOIN portfolio_payment_service p ON p.invoiceid = i.id
      AND p.status != 'FAILED'
      JOIN
        (SELECT ii.*,
                (row_number() OVER (PARTITION BY ii.invoiceid
                                    ORDER BY (CASE
                                                  WHEN ii.type = 'PLAN' THEN 1
                                                  ELSE 2
                                              END) ASC)) AS r_num
         FROM portfolio_payment_invoice_item ii
         WHERE ii.transactiontype = 'PURCHASE') ii ON ii.invoiceid = i.id
      AND ii.r_num = 1
      LEFT JOIN
        (SELECT paymentid,
                min(refundedat) AS refundedat,
                sum(amount) AS refundedamount
         FROM portfolio_payment_refund_info
         GROUP BY 1) ri ON ri.paymentid = p.id
      WHERE CASE
                WHEN i.workspaceid IN
                       (SELECT workspaceid
                        FROM TEST_WORKSPACE) THEN FALSE
                WHEN ii.planid IN
                       (SELECT planid
                        FROM TEST_PLAN_LIST) THEN FALSE
                ELSE TRUE
            END
        AND i.id != 'SAMPLE_INVOICE_ID_001' -- 샘플 인보이스 제외
 ), --
--
 ZERO_VOUCHER_PLAN_RAW AS
     (SELECT i.purchaseitemid,
             i.workspaceid,
             (row_number()over(PARTITION BY i.workspaceid, i.title
                               ORDER BY i.createdat ASC)) AS r_num,
             (row_number()over(PARTITION BY i.workspaceid, i.title
                               ORDER BY i.createdat DESC)) AS r_num_inverse,
             (CASE
                  WHEN i.title = 'Starter Plan'
                       AND pt.suppliedresetcount = 12 THEN 50000/2
                  WHEN i.title = 'Starter Plan'
                       AND pt.suppliedresetcount = 1 THEN 50000/24
                  WHEN i.title = 'Business Plan'
                       AND pt.suppliedresetcount = 12 THEN 75000/2
                  WHEN i.title = 'Business Plan'
                       AND pt.suppliedresetcount = 6 THEN 75000/4
                  WHEN i.title = 'Business Plan'
                       AND pt.suppliedresetcount = 1 THEN 75000/24
                  WHEN i.title = 'Premium Plan'
                       AND pt.suppliedresetcount = 12 THEN 100000/2
                  WHEN i.title = 'Premium Plan'
                       AND pt.suppliedresetcount = 1 THEN 100000/24
                  WHEN i.title = 'Voucher10'
                       AND pt.suppliedresetcount = 12 THEN 100000/2
                  WHEN i.title = 'Voucher10'
                       AND pt.suppliedresetcount = 1 THEN 100000/24
                  WHEN i.title = 'Voucher20'
                       AND pt.suppliedresetcount = 12 THEN 200000/2
                  WHEN i.title = 'Voucher20'
                       AND pt.suppliedresetcount = 1 THEN 200000/24
                  WHEN i.title = 'Voucher40'
                       AND pt.suppliedresetcount = 12 THEN 400000/2
                  WHEN i.title = 'Voucher40'
                       AND pt.suppliedresetcount = 1 THEN 400000/24
                  WHEN i.title = 'Premium'
                       AND pt.suppliedresetcount = 1 THEN 33000
              END) AS price,
             (CASE
                  WHEN i.title = 'Starter Plan' THEN 50000
                  WHEN i.title = 'Business Plan' THEN 75000
                  WHEN i.title = 'Premium Plan' THEN 100000
                  WHEN i.title = 'Voucher10' THEN 100000
                  WHEN i.title = 'Voucher20' THEN 200000
                  WHEN i.title = 'Voucher40' THEN 400000
                  WHEN i.title = 'Premium'
                       AND pt.suppliedresetcount = 1 THEN 33000
              END) AS origin_price
      FROM INVOICE i
      JOIN PLAN_TYPE pt ON i.planid = pt.planid
      AND pt.depth1_name = 'Government Support'
      AND pt.depth5_name != 'Free'
      AND i.finalamount = 0 --
 ), --
 --
 ZERO_VOUCHER_PLAN_LIST AS
     (SELECT main.purchaseitemid,
             main.workspaceid,
             main.price,
             main.origin_price,
             CASE
                 WHEN main.r_num = 1
                      AND main.r_num_inverse = 1 THEN main.origin_price
                 WHEN main.r_num = 1
                      AND main.r_num_inverse != 1 THEN main.price * main.r_num_inverse
                 ELSE NULL
             END AS alrter_price
      FROM ZERO_VOUCHER_PLAN_RAW main --
 ), --
 --
 INVOICE_ADJ_VOUCHER AS
     (SELECT i.purchaseitemcategory,
             i.workspaceid,
             i.id AS invoiceid,
             i.paymentid,
             i.purchaseitemid,
             i.status,
             i.amount,
             coalesce(vpl.alrter_price, i.finalamount) AS adj_finalamount,
             (-coalesce(vpl.alrter_price, i.refundedamount)) AS adj_refundedamount,
             (CASE
                  WHEN i.refundedamount IS NULL THEN greatest(adj_finalamount, 1)
                  ELSE adj_finalamount + adj_refundedamount
              END) AS finalamount_diff,
             (sum(finalamount_diff) OVER (PARTITION BY i.workspaceid
                                          ORDER BY i.createdat ASC,i.id DESC ROWS unbounded preceding) - finalamount_diff) = 0 AS subscription_initial_start_point,
             i.type,
             i.invoicetype,
             i.createdat,
             i.invoicecreatedat,
             ((i.periodstart)::TIMESTAMP) AS invoice_subscription_start,
             ((i.periodend)::TIMESTAMP) AS invoice_subscription_end,
             i.planid, --
 -- X-1 버전 특수 컬럼
 sp.planid AS extracharge_planid, --
 -- X-1 버전 특수 컬럼
 i.featureusageaction,
 i.pluginaction,
 i.refundedat,
 sp.subscription_start,
 sp.subscription_end
      FROM INVOICE i
      LEFT JOIN ZERO_VOUCHER_PLAN_LIST vpl ON i.purchaseitemid = vpl.purchaseitemid
      AND i.workspaceid = vpl.workspaceid --
--   X-1 버전 특수 컬럼

      LEFT JOIN PLAN_SUBSCRIPTION_PERIODS sp ON i.workspaceid = sp.workspaceid
      AND sp.subscription_start <= i.createdat
      AND i.createdat < coalesce(sp.subscription_end, sysdate + interval '9 hour')
      AND i.purchaseitemcategory = 'EXTRACHARGE' --
--   X-1 버전 특수 컬럼

      WHERE CASE
                WHEN vpl.purchaseitemid IS NULL THEN TRUE
                WHEN vpl.purchaseitemid IS NOT NULL
                     AND vpl.alrter_price IS NOT NULL THEN TRUE
                ELSE FALSE
            END --
), --
--
 BILLING_RAWDATA AS
     (SELECT b.purchaseitemcategory,
             b.workspaceid,
             b.invoiceid,
             b.paymentid,
             b.purchaseitemid,
             b.status,
             b.amount,
             b.finalamount,
             b.refundedamount,
             b.finalamount_diff,
             b.subscription_initial_start_point,
             b.type,
             b.invoicetype,
             b.datevalue,
             b.planid,
             b.extracharge_planid,
             b.featureusageaction,
             b.pluginaction,
             b.invoice_subscription_start,
             b.invoice_subscription_end,
             b.invoicecreatedat,
             b.createdat,
             b.refundedat,
             b.subscription_start,
             b.subscription_end,
             (lag(b.datevalue) OVER (PARTITION BY coalesce(b.workspaceid, 'sample')
                                     ORDER BY b.datevalue ASC,b.invoiceid ASC)) AS prev_datevalue,
             (max(b.subscription_initial_start_point::int) OVER (PARTITION BY coalesce(b.workspaceid, 'sample'),
                                                                              date_trunc('year', b.datevalue))) = 1 AS new_yn_year,
             (max(b.subscription_initial_start_point::int) OVER (PARTITION BY coalesce(b.workspaceid, 'sample'),
                                                                              date_trunc('quarter', b.datevalue))) = 1 AS new_yn_quarter,
             (max(b.subscription_initial_start_point::int) OVER (PARTITION BY coalesce(b.workspaceid, 'sample'),
                                                                              date_trunc('month', b.datevalue))) = 1 AS new_yn_month,
             (1::float/(count(coalesce(b.workspaceid, 'sample')) OVER (PARTITION BY coalesce(b.workspaceid, 'sample'),
                                                                                date_trunc('year', b.datevalue))))::float AS workspace_all_count_year,
             (1::float/(count(coalesce(b.workspaceid, 'sample')) OVER (PARTITION BY coalesce(b.workspaceid, 'sample'),
                                                                                date_trunc('quarter', b.datevalue))))::float AS workspace_all_count_quarter,
             (1::float/(count(coalesce(b.workspaceid, 'sample')) OVER (PARTITION BY coalesce(b.workspaceid, 'sample'),
                                                                                date_trunc('month', b.datevalue))))::float AS workspace_all_count_month
      FROM
        (SELECT iav.purchaseitemcategory,
                iav.workspaceid,
                iav.invoiceid,
                iav.paymentid,
                iav.purchaseitemid,
                'PAID' AS status,
                ceil(abs(iav.amount)::float/1.1)*sign(iav.amount) AS amount,
                ceil(abs(iav.adj_finalamount)::float/1.1)*sign(iav.adj_finalamount) AS finalamount,
                ceil(abs(iav.adj_refundedamount)::float/1.1)*sign(iav.adj_refundedamount) AS refundedamount,
                ceil(abs(iav.finalamount_diff)::float/1.1)*sign(iav.finalamount_diff) AS finalamount_diff,
                iav.subscription_initial_start_point,
                iav.type,
                iav.invoicetype,
                iav.createdat AS datevalue,
                iav.planid,
                iav.extracharge_planid,
                iav.featureusageaction,
                iav.pluginaction,
                iav.invoice_subscription_start,
                iav.invoice_subscription_start,
                iav.invoicecreatedat,
                iav.createdat,
                iav.refundedat,
                iav.subscription_start,
                iav.subscription_end
         FROM INVOICE_ADJ_VOUCHER iav
         UNION ALL SELECT iav.purchaseitemcategory,
                          iav.workspaceid,
                          iav.invoiceid,
                          iav.paymentid,
                          iav.purchaseitemid,
                          'REFUNDED' AS status,
                          ceil(abs(iav.amount)::float/1.1)*sign(iav.amount) AS amount,
                          (-ceil(ri.amount::float/1.1)) AS finalamount,
                          (-ceil(ri.amount::float/1.1)) AS refundedamount,
                          ceil(abs(iav.finalamount_diff)::float/1.1) * sign(iav.finalamount_diff) AS finalamount_diff,
                          iav.subscription_initial_start_point,
                          iav.type,
                          iav.invoicetype,
                          ri.refundedat AS datevalue,
                          iav.planid,
                          iav.extracharge_planid,
                          iav.featureusageaction,
                          iav.pluginaction,
                          iav.invoice_subscription_start,
                          iav.invoice_subscription_end,
                          iav.invoicecreatedat,
                          iav.createdat,
                          iav.refundedat,
                          iav.subscription_start,
                          iav.subscription_end
         FROM INVOICE_ADJ_VOUCHER iav
         JOIN portfolio_payment_refund_info ri ON ri.paymentid = iav.paymentid) b --
 ), --
 --
 billing_result AS
     (SELECT br.purchaseitemcategory,
             br.workspaceid,
             br.invoiceid,
             br.paymentid,
             br.purchaseitemid,
             br.status,
             br.amount,
             br.finalamount,
             br.refundedamount,
             br.finalamount_diff,
             br.subscription_initial_start_point,
             br.type,
             br.invoicetype,
             br.datevalue,
             br.planid,
             br.extracharge_planid,
             br.featureusageaction,
             br.pluginaction,
             br.invoice_subscription_start,
             br.invoice_subscription_end,
             br.invoicecreatedat,
             br.createdat,
             br.refundedat,
             br.subscription_start,
             br.subscription_end,
             br.prev_datevalue,
             br.new_yn_year,
             br.new_yn_quarter,
             br.new_yn_month,
             br.workspace_all_count_year,
             br.workspace_all_count_quarter,
             br.workspace_all_count_month,
             ph.id AS paymenthistoryid,
             count(1) OVER (PARTITION BY ph.id) AS validation_idx
      FROM BILLING_RAWDATA br
      LEFT JOIN
        (SELECT *,
                count(1) OVER (PARTITION BY paymentid) AS idx
         FROM portfolio_payment_history) ph ON ph.paymentid = br.paymentid
      AND br.status = ph.paymentstatus
      AND CASE
              WHEN date_trunc('second', br.datevalue) = date_trunc('second', ph.timestamp) THEN TRUE
              WHEN abs(datediff(SECOND, date_trunc('second', br.datevalue), date_trunc('second', ph.timestamp))) <= 5 THEN TRUE
              WHEN ph.idx = 1
                   AND abs(datediff(MINUTE, date_trunc('microseconds', br.datevalue), date_trunc('microseconds', ph.timestamp))) <= 1 THEN TRUE
              ELSE FALSE
          END --
 )--
 --
 SELECT purchaseitemcategory,
        workspaceid,
        invoiceid,
        paymentid,
        paymenthistoryid,
        purchaseitemid,
        status,
        amount,
        finalamount,
        refundedamount,
        finalamount_diff,
        subscription_initial_start_point,
        "type",
        invoicetype,
        datevalue,
        planid,
        extracharge_planid,
        featureusageaction,
        pluginaction,
        invoice_subscription_start,
        invoice_subscription_end,
        invoicecreatedat,
        createdat,
        refundedat,
        subscription_start,
        subscription_end,
        prev_datevalue,
        new_yn_year,
        new_yn_quarter,
        new_yn_month,
        workspace_all_count_year,
        workspace_all_count_quarter,
        workspace_all_count_month
   FROM billing_result
   WHERE validation_idx = 1);

-- 최종 요약
SELECT '포트폴리오 샘플 쿼리 2 완료' AS status,
       '결제 빌링 데이터 처리 파이프라인' AS query_type,
       CURRENT_TIMESTAMP AS execution_time;

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 포트폴리오 샘플 쿼리 2 요약
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- 이 쿼리는 다음을 보여줍니다:
-- 1. 여러 CTE와 윈도우 함수를 사용한 복잡한 빌링 데이터 변환
-- 2. 결제 처리 및 구독 기간 관리
-- 3. 인보이스 분류 및 바우처 플랜 처리
-- 4. 환불 처리 및 금액 계산
-- 5. 다차원 빌링 분석 및 리포팅
-- 
-- Apache Airflow DAG에서 일일 빌링 데이터 처리를 위해 사용됩니다
-- 재무 리포팅 및 비즈니스 인텔리전스의 기반을 제공합니다
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

