"""
daily_table_update DAG 전용 Task 함수들과 워크플로우 설정을 모아둔 모듈
이 DAG에서만 사용되는 특화된 Task 함수들과 설정들
"""

import os
import time
import requests
from libs.config_utils import initialize_components

# ============================================================================
# 워크플로우 설정
# ============================================================================

# sheet_id : 시트ID , 
# sheet_name : 시트탭 이름, 
# table_name : 업데이트할 redshift 테이블명
sheet_update_list = [ 
    {"sheet_id":"example_sheet_id_123456789", "sheet_name":"categoryRaw", "table_name":"example_portfolio_category"},
    {"sheet_id":"example_sheet_id_123456789", "sheet_name":"categoryInfo", "table_name":"example_portfolio_category_value"}
]

# 문서 카테고리 형태소 분석 메서드 필수값
document_title_mining_value = {
    "sql":'''
        select
            d.id as documentid,
            d.userid,
            d.workspaceid,
            d_origin.title,
            d.estimated_workspaceid,
            d.createdat
        from example_portfolio_document d
        left join example_portfolio_service d_origin on d_origin.id = d.id
        left join example_portfolio_user tu on tu.userid = d.userid
        where tu.userid is null
        and d.id not in (select documentid from example_portfolio_document_category)
    ''',
    "table_name":'example_portfolio_document_category',
    "column_names":['documentid', 'userid', 'workspaceid', 'estimated_workspaceid', 'createdat', 'nouns', 'category1', 'category2'],
    "process_type":'document',
    "delete_table":False,
    "json_file_path_category1":'libs/documentCategoryJson/dicCategory1.json',
    "json_file_path_category2":'libs/documentCategoryJson/dicCategory2.json',
    "dic_file_path":'libs/documentCategoryJson/dic_user.txt'
}

# 템플릿 카테고리 형태소 분석 메서드 필수값
template_title_mining_value = {
    "sql":'''
        WITH TEST_WORKSPACE AS
        (SELECT DISTINCT m.workspaceid
        FROM example_portfolio_user tu
        JOIN example_portfolio_member m ON m.userid = tu.userid)

        SELECT t.id as templateid,
            t.workspaceid,
            t.memberid,
            t.title,
            t.createdat
        FROM example_portfolio_template t
        WHERE t.workspaceid NOT IN
            (SELECT workspaceid
            FROM TEST_WORKSPACE)
        and t.id not in (select templateid from example_portfolio_template_category)
    ''',
    "table_name":'example_portfolio_template_category',
    "column_names":['templateid', 'workspaceid', 'memberid', 'title', 'createdat', 'nouns', 'category1', 'category2'],
    "process_type":'template',
    "delete_table":True,
    "json_file_path_category1":'libs/documentCategoryJson/dicCategory1.json',
    "json_file_path_category2":'libs/documentCategoryJson/dicCategory2.json',
    "dic_file_path":'libs/documentCategoryJson/dic_user.txt'
}

# 문서 서식 카테고리 형태소 분석 메서드 필수값
document_title_to_form_mining_value = {
    "sql":'''
        select
            d.id as documentid,
            d.userid,
            d.workspaceid,
            d_origin.title,
            d.estimated_workspaceid,
            d.createdat
        from example_portfolio_document d
        left join example_portfolio_service d_origin on d_origin.id = d.id
        left join example_portfolio_user tu on tu.userid = d.userid
        where tu.userid is null
        and d.id not in (select documentid from example_portfolio_document_form_category)
    ''',
    "table_name":'example_portfolio_document_form_category',
    "column_names":['documentid', 'userid', 'workspaceid', 'estimated_workspaceid', 'createdat', 'nouns', 'category1'],
    "process_type":'document',
    "delete_table":False,
    "json_file_path":'libs/formCategoryJson/formCategory1.json',
    "dic_file_path":'libs/formCategoryJson/dic_user.txt'
}

# 템플릿 서식 카테고리 형태소 분석 메서드 필수값
template_title_to_form_mining_value = {
    "sql":'''
        WITH TEST_WORKSPACE AS
        (SELECT DISTINCT m.workspaceid
        FROM example_portfolio_user tu
        JOIN example_portfolio_member m ON m.userid = tu.userid)

        SELECT t.id as templateid,
            t.workspaceid,
            t.memberid,
            t.title,
            t.createdat
        FROM example_portfolio_template t
        WHERE t.workspaceid NOT IN
            (SELECT workspaceid
            FROM TEST_WORKSPACE)
        and t.id not in (select templateid from example_portfolio_template_form_category)
    ''',
    "table_name":'example_portfolio_template_form_category',
    "column_names":['templateid', 'workspaceid', 'memberid', 'title', 'createdat', 'nouns', 'category1'],
    "process_type":'template',
    "delete_table":True,
    "json_file_path":'libs/formCategoryJson/formCategory1.json',
    "dic_file_path":'libs/formCategoryJson/dic_user.txt'
}

# ============================================================================
# Task 함수들
# ============================================================================

# 컴포넌트 초기화
components = initialize_components()

def sheets_update_task(**context):
    """Task 1: DAG 시작 알림 + 시트 업데이트"""
    # DAG 시작 알림 전송
    from libs.notification_utils import send_workflow_start_notification
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    send_workflow_start_notification(dag_id, run_id)
    
    print(":::::::::시트 업데이트 시작:::::::::")
    
    sheets_manager = components['sheets_manager']
    db_manager = components['db_manager']
    
    for i in sheet_update_list:
        sheet_id = i['sheet_id']
        sheet_name = i['sheet_name']
        table_name = i['table_name']
        print(f'- {sheet_name} 실행')
        try:
            # 직접 호출: 시트 조회 → 데이터 import
            data = sheets_manager.get_sheet(sheet_id, sheet_name)
            db_manager.append_data(data, table_name, delete_existing=True)
            print(f'  ✓ {sheet_name} 성공')
        except Exception as e:
            print(f"  ✗ {sheet_name} 실패: {e}")
            raise Exception(f"시트 업데이트 실패 ({sheet_name}): {e}")
    
    print(":::::::::시트 업데이트 완료:::::::::")
    return "시트 업데이트 완료"

def redash_query_task(query_number, **context):
    """개별 Redash 쿼리 실행 Task"""
    print(f":::::::::Redash 쿼리 {query_number} 실행 시작:::::::::")
    
    redash_manager = components['redash_manager']
    
    start_time = time.time()
    try:
        result = redash_manager.get_refresh_query_result(query_number)
        print(f'  ✓ 쿼리 {query_number} 결과: {result}')
        end_time = time.time()
        print(f'  - 소요 시간: [{end_time - start_time:.6f} seconds]')
        return f"쿼리 {query_number} 실행 완료"
    except Exception as e:
        print(f'  ✗ 쿼리 {query_number} 에러 발생: {e}')
        raise Exception(f"쿼리 {query_number} 실패: {e}")

def text_mining_task(**context):
    """Task 3: 텍스트 마이닝 실행 (모든 분석 순차 실행)"""
    print(":::::::::텍스트 마이닝 시작:::::::::")
    
    contract_analyzer = components['contract_analyzer']
    form_analyzer = components['form_analyzer']
    
    # Step 1: 문서 카테고리 분석
    print("--- 문서 카테고리 분석 ---")
    try:
        contract_analyzer.analyze_document_contract_categories(
            sql=document_title_mining_value["sql"],
            table_name=document_title_mining_value["table_name"],
            column_names=document_title_mining_value["column_names"],
            category1_file_path=document_title_mining_value["json_file_path_category1"],
            category2_file_path=document_title_mining_value["json_file_path_category2"],
            user_dic_path=document_title_mining_value["dic_file_path"],
            delete_table=document_title_mining_value["delete_table"]
        )
        print("  ✓ 문서 카테고리 분석 완료")
    except Exception as e:
        print(f"  ✗ 문서 카테고리 분석 실패: {e}")
        raise Exception(f"문서 카테고리 분석 실패: {e}")
    
    # Step 2: 템플릿 카테고리 분석
    print("--- 템플릿 카테고리 분석 ---")
    try:
        contract_analyzer.analyze_template_contract_categories(
            sql=template_title_mining_value["sql"],
            table_name=template_title_mining_value["table_name"],
            column_names=template_title_mining_value["column_names"],
            category1_file_path=template_title_mining_value["json_file_path_category1"],
            category2_file_path=template_title_mining_value["json_file_path_category2"],
            user_dic_path=template_title_mining_value["dic_file_path"],
            delete_table=template_title_mining_value["delete_table"]
        )
        print("  ✓ 템플릿 카테고리 분석 완료")
    except Exception as e:
        print(f"  ✗ 템플릿 카테고리 분석 실패: {e}")
        raise Exception(f"템플릿 카테고리 분석 실패: {e}")
    
    # Step 3: 문서 서식 카테고리 분석
    print("--- 문서 서식 카테고리 분석 ---")
    try:
        form_analyzer.analyze_document_form_categories(
            sql=document_title_to_form_mining_value["sql"],
            table_name=document_title_to_form_mining_value["table_name"],
            column_names=document_title_to_form_mining_value["column_names"],
            category_file_path=document_title_to_form_mining_value["json_file_path"],
            user_dic_path=document_title_to_form_mining_value["dic_file_path"],
            delete_table=document_title_to_form_mining_value["delete_table"]
        )
        print("  ✓ 문서 서식 카테고리 분석 완료")
    except Exception as e:
        print(f"  ✗ 문서 서식 카테고리 분석 실패: {e}")
        raise Exception(f"문서 서식 카테고리 분석 실패: {e}")
    
    # Step 4: 템플릿 서식 카테고리 분석
    print("--- 템플릿 서식 카테고리 분석 ---")
    try:
        form_analyzer.analyze_template_form_categories(
            sql=template_title_to_form_mining_value["sql"],
            table_name=template_title_to_form_mining_value["table_name"],
            column_names=template_title_to_form_mining_value["column_names"],
            category_file_path=template_title_to_form_mining_value["json_file_path"],
            user_dic_path=template_title_to_form_mining_value["dic_file_path"],
            delete_table=template_title_to_form_mining_value["delete_table"]
        )
        print("  ✓ 템플릿 서식 카테고리 분석 완료")
    except Exception as e:
        print(f"  ✗ 템플릿 서식 카테고리 분석 실패: {e}")
        raise Exception(f"템플릿 서식 카테고리 분석 실패: {e}")
    
    print(":::::::::텍스트 마이닝 완료:::::::::")
    return "텍스트 마이닝 완료"
