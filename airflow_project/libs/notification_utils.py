"""
슬랙 알림 관련 공통 함수들을 모아둔 모듈
"""

import os
import requests

def send_slack_notification(message, run_id=None, webhook_url=None):
    """
    슬랙 알림 전송 함수
    
    Args:
        message (str): 전송할 메시지
        run_id (str, optional): Airflow run ID
        webhook_url (str, optional): 슬랙 웹훅 URL (기본값: 환경변수에서 가져옴)
    
    Returns:
        bool: 전송 성공 여부
    """
    # 웹훅 URL 가져오기
    if webhook_url is None:
        webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    
    if not webhook_url:
        print("경고: SLACK_WEBHOOK_URL이 설정되지 않았습니다.")
        return False
    
    headers = {'Content-type': 'application/json'}
    
    try:
        # run_id가 있으면 메시지에 추가
        if run_id:
            run_id_short = run_id[:8] if run_id else 'unknown'
            full_message = f"{message} (Run: {run_id_short})"
        else:
            full_message = message
        
        # 슬랙으로 전송
        response = requests.post(
            url=webhook_url, 
            headers=headers, 
            json={"text": f"<!subteam^S021SV3VCHY> - {full_message}"}
        )
        
        if response.status_code == 200:
            print(f"Slack 알림 전송 완료: {full_message}")
            return True
        else:
            print(f"Slack 알림 전송 실패: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Slack 알림 전송 실패: {e}")
        return False

def send_workflow_start_notification(dag_name, run_id=None):
    """워크플로우 시작 알림 전송"""
    message = f"🚀 {dag_name} 시작"
    return send_slack_notification(message, run_id)

def send_workflow_complete_notification(dag_name, run_id=None):
    """워크플로우 완료 알림 전송"""
    message = f"✅ {dag_name} 완료"
    return send_slack_notification(message, run_id)

def send_workflow_error_notification(dag_name, error_message, run_id=None):
    """워크플로우 에러 알림 전송"""
    message = f"❌ {dag_name} 에러 발생: {error_message}"
    return send_slack_notification(message, run_id)

# ============================================================================
# DAG Callback 함수들
# ============================================================================

def slack_start_callback(context):
    """DAG 시작 시 호출되는 Callback 함수"""
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    print(f"DAG 시작 Callback 실행: {dag_id} (Run: {run_id})")
    send_workflow_start_notification(dag_id, run_id)

def slack_success_callback(context):
    """DAG 성공 시 호출되는 Callback 함수"""
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    print(f"DAG 성공 Callback 실행: {dag_id} (Run: {run_id})")
    send_workflow_complete_notification(dag_id, run_id)

def slack_failure_callback(context):
    """DAG 실패 시 호출되는 Callback 함수"""
    dag_id = context['dag'].dag_id
    run_id = context['run_id']
    task_instance = context['task_instance']
    task_id = task_instance.task_id
    exception = context.get('exception', 'Unknown error')
    error_message = f"Task '{task_id}' 실패: {exception}"
    print(f"DAG 실패 Callback 실행: {dag_id} - {error_message} (Run: {run_id})")
    send_workflow_error_notification(dag_id, error_message, run_id)
