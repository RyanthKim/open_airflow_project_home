-- ===================================================================================
-- 바이럴 효과 측정을 위한 서명자 가입 분석 쿼리
-- 요청자 -- 서명요청 --> 서명자
-- 서명자가 서명 경험을 통해 가입 후 결제를 했는지 여부를 판단하는 바이럴 효과 측정
-- ===================================================================================

-- ===================================================================================
-- 1단계: 참여자 데이터 테이블 생성 및 관리
-- ===================================================================================

-- drop table example_participant_data
-- create table example_participant_data (
--     documentid varchar(65535),
--     participantid varchar(65535),
--     contact varchar(65535),
--     "type" varchar(65535),
--     "order" bigint,
--     contactinfo varchar(65535),
--     signingmethod varchar(65535),
--     locale varchar(65535),
--     "to" TIMESTAMP,
--     "from" TIMESTAMP,
--     signingduration bigint,
--     createdat TIMESTAMP
-- )

-- 참여자 데이터 임시 테이블 생성
CREATE TEMP TABLE temp_participant_data AS
  (SELECT doc.documentid,
          split_part(doc.entity, 'Participant#', 2) AS participantid,
          doc.contact,
          doc."type",
          CASE
              WHEN doc.contact LIKE '%.%'
                   OR doc.contact LIKE '%@%' THEN 'email'
              ELSE 'kakao'
          END AS contactinfo,
          doc.signingmethod,
          doc.locale,
          doc."to"::TIMESTAMP,
          doc."from"::TIMESTAMP,
          doc.signingduration::bigint,
          doc.createdat + interval '9 hour' AS createdat
   FROM spectrum.example_document_service doc
   WHERE doc.__en = 'Participant'
     AND doc."type" IN ('SIGNER',
                        'VIEWER')
     AND doc.documentid IS NOT NULL
     AND doc.createddate >= to_char(sysdate + interval '9 hour' - interval '1 month', 'YYYY-MM-DD'));

-- 기존 데이터와 중복되는 참여자 정보 삭제
DELETE
FROM example_participant_data USING temp_participant_data
WHERE temp_participant_data.documentid = example_participant_data.documentid
  AND temp_participant_data.participantid != example_participant_data.participantid;

-- 새로운 참여자 데이터 삽입 (개인정보 보호를 위한 해시 처리)
INSERT INTO example_participant_data(documentid, participantid, contact, "type", "order", contactinfo, signingmethod, locale, "to", "from", signingduration, createdat)
  (SELECT temp.doc.documentid,
          temp.doc.participantid,
          CASE
              WHEN temp.doc.contactinfo = 'email' THEN (hash(split_part(temp.doc.contact, '@', 1))||'@'||split_part(temp.doc.contact, '@', 2))::varchar(65535)
              WHEN temp.doc.contactinfo = 'kakao' THEN (hash('+82'||SUBSTRING(temp.doc.contact, 2)))::varchar(65535)
          END AS contact,
          temp.doc."type",
          CASE
              WHEN dense_rank() over(PARTITION BY temp.doc.documentid
                                     ORDER BY temp.doc."to" ASC) = 1 THEN 1
              ELSE row_number() over(PARTITION BY temp.doc.documentid
                                     ORDER BY temp.doc.participantid ASC)
          END AS "order",
          temp.doc.contactinfo,
          temp.doc.signingmethod,
          temp.doc.locale,
          temp.doc."to",
          temp.doc."from",
          temp.doc.signingduration,
          temp.doc.createdat
   FROM temp_participant_data temp.doc
   LEFT JOIN example_participant_data pd ON temp.doc.participantid = pd.participantid
   WHERE pd.contact IS NULL );

-- ===================================================================================
-- 2단계: 바이럴 효과 측정을 위한 분석 테이블 생성
-- ===================================================================================

-- drop table example_viral_effect_analysis
-- create table example_viral_effect_analysis (
--   participantid varchar(65535),
--   requester_userid varchar(65535),
--   requester_workspaceid varchar(65535),
--   requester_estimated_workspaceid varchar(65535),
--   documentid varchar(65535),
--   contact varchar(65535),
--   contactinfo varchar(65535),
--   signingmethod varchar(65535),
--   signingat timestamp,
--   documentcreatedat timestamp,
--   id varchar(65535),
--   idprovidertype varchar(65535),
--   profileemail varchar(65535),
--   profilename varchar(65535),
--   profilesurveybusinessregistrationnumber varchar(65535),
--   profilesurveycity varchar(65535),
--   profilesurveycompanycategory varchar(65535),
--   profilesurveycompanyid varchar(65535),
--   profilesurveycompanyname varchar(65535),
--   profilesurveycompanyscale varchar(65535),
--   profilesurveydepartment varchar(65535),
--   profilesurveyexpectedstarttime varchar(65535),
--   profilesurveyexpectedusage varchar(65535),
--   profilesurveyfunnel varchar(65535),
--   profilesurveyphonenumber varchar(65535),
--   profilesurveysearchword varchar(65535),
--   profilesurveyusertype varchar(65535),
--   activatedat timestamp,
--   createdat timestamp,
--   updatedat timestamp
-- )

-- 기존 분석 데이터 정리
DELETE
FROM example_viral_effect_analysis
WHERE example_viral_effect_analysis.id IS NULL;

------------------------------------------------------------------------------------------------------------
/*
바이럴 효과 측정을 위한 데이터 준비 단계

1. 참여자 정보 + 문서 정보와 함께 JOIN
2. 열람 이력이 있는 서명자만 필터링
3. 열람 이력 시간순으로 contact 별로 row_number()로 순서를 매김
4. row_number() = 1로 첫번째 열람 이력의 정보만 필터링
*/
CREATE TEMP TABLE temp_viral_effect_data AS
  (WITH main AS
     (SELECT pd.participantid,
             cdc.userid AS requester_userid,
             cdc.workspaceid AS requester_workspaceid,
             cdc.estimated_workspaceid AS requester_estimated_workspaceid,
             pd.documentid,
             pd.contact,
             pd.contactinfo,
             pd.signingmethod,
             pd."from" AS signingat,
             cdc.createdat AS documentcreatedat,
             (row_number() over(PARTITION BY contact
                                ORDER BY pd."from" ASC)) AS idx
      FROM example_participant_data pd
      JOIN example_document_creation cdc ON pd.documentid = cdc.id
      AND cdc.currentsigningorder >= pd."order"
     )
   SELECT *
   FROM main
   WHERE main.idx = 1);

------------------------------------------------------------------------------------------------------------
/*
바이럴 효과 측정을 위한 핵심 분석 단계

1. 임시 테이블과 유저 정보의 profileemail을 붙인 테이블
2. profilesurveyphonenumber를 붙인 테이블로 UNION
3. contact 별로 첫번째 열람 이력을 추출했으므로 email, phone number 별로 2개의 row가 나올 수 있음
4. user.id 별로 signingat 순, contactinfo 순으로 row_number() = 1
5. 가입일자가 열람 일자보다 나중인 경우만 서명 경험으로 인한 가입으로 판단
*/
INSERT INTO example_viral_effect_analysis(participantid, requester_userid, requester_workspaceid, requester_estimated_workspaceid, documentid, contact, contactinfo, signingmethod, signingat, documentcreatedat, id, idprovidertype, profileemail, profilename, profilesurveybusinessregistrationnumber, profilesurveycity, profilesurveycompanycategory, profilesurveycompanyid, profilesurveycompanyname, profilesurveycompanyscale, profilesurveydepartment, profilesurveyexpectedstarttime, profilesurveyexpectedusage, profilesurveyfunnel, profilesurveyphonenumber, profilesurveysearchword, profilesurveyusertype, activatedat, createdat)
  (WITH participant_join_info AS
     (SELECT a.*,
             (row_number() over(PARTITION BY a.id
                                ORDER BY a.signingat ASC, CASE
                                                              WHEN a.contactinfo = 'email' THEN 1
                                                              ELSE 2
                                                          END)) AS r_num
      FROM
        (SELECT cpc.participantid,
                cpc.requester_userid,
                cpc.requester_workspaceid,
                cpc.requester_estimated_workspaceid,
                cpc.documentid,
                cpc.contact,
                cpc.contactinfo,
                cpc.signingmethod,
                cpc.signingat,
                cpc.documentcreatedat,
                u.id,
                u.idprovidertype,
                u.profileemail,
                u.profilename,
                u.profilesurveybusinessregistrationnumber,
                u.profilesurveycity,
                u.profilesurveycompanycategory,
                u.profilesurveycompanyid,
                u.profilesurveycompanyname,
                u.profilesurveycompanyscale,
                u.profilesurveydepartment,
                u.profilesurveyexpectedstarttime,
                u.profilesurveyexpectedusage,
                u.profilesurveyfunnel,
                u.profilesurveyphonenumber,
                u.profilesurveysearchword,
                u.profilesurveyusertype,
                u.activatedat,
                u.createdat
         FROM temp_viral_effect_data cpc
         JOIN example_user_service u ON u.profilesurveyphonenumber = cpc.contact
         LEFT JOIN example_test_user tu ON tu.userid = u.id
         WHERE tu.id IS NULL
         UNION ALL SELECT cpc.participantid,
                          cpc.requester_userid,
                          cpc.requester_workspaceid,
                          cpc.requester_estimated_workspaceid,
                          cpc.documentid,
                          cpc.contact,
                          cpc.contactinfo,
                          cpc.signingmethod,
                          cpc.signingat,
                          cpc.documentcreatedat,
                          u.id,
                          u.idprovidertype,
                          u.profileemail,
                          u.profilename,
                          u.profilesurveybusinessregistrationnumber,
                          u.profilesurveycity,
                          u.profilesurveycompanycategory,
                          u.profilesurveycompanyid,
                          u.profilesurveycompanyname,
                          u.profilesurveycompanyscale,
                          u.profilesurveydepartment,
                          u.profilesurveyexpectedstarttime,
                          u.profilesurveyexpectedusage,
                          u.profilesurveyfunnel,
                          u.profilesurveyphonenumber,
                          u.profilesurveysearchword,
                          u.profilesurveyusertype,
                          u.activatedat,
                          u.createdat
         FROM temp_viral_effect_data cpc
         JOIN example_user_service u ON u.profileemail = cpc.contact
         LEFT JOIN example_test_user tu ON tu.userid = u.id
         WHERE tu.id IS NULL) a
     )
   SELECT participantid,
          requester_userid,
          requester_workspaceid,
          requester_estimated_workspaceid,
          documentid,
          contact,
          contactinfo,
          signingmethod,
          signingat,
          documentcreatedat,
          id,
          idprovidertype,
          profileemail,
          profilename,
          profilesurveybusinessregistrationnumber,
          profilesurveycity,
          profilesurveycompanycategory,
          profilesurveycompanyid,
                          profilesurveycompanyname,
                          profilesurveycompanyscale,
                          profilesurveydepartment,
                          profilesurveyexpectedstarttime,
                          profilesurveyexpectedusage,
                          profilesurveyfunnel,
                          profilesurveyphonenumber,
                          profilesurveysearchword,
                          profilesurveyusertype,
                          activatedat,
                          createdat
   FROM participant_join_info
   WHERE r_num = 1
     AND activatedat > signingat
     AND id NOT IN
       (SELECT DISTINCT id
        FROM example_viral_effect_analysis));

------------------------------------------------------------------------------------------------------------

SELECT 'viral_effect_analysis_complete' AS TRIGGER,
       SYSDATE||' UTC' AS TIMESTAMP;

