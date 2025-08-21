from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Task 함수들을 tasks에서 import
from daily_table_update_tasks import (
    sheets_update_task,
    redash_query_task,
    text_mining_task,
)

# DAG Callback 함수들을 import
from libs.notification_utils import (
    slack_success_callback,    # DAG 성공 시
    slack_failure_callback     # DAG 실패 시
)

# DAG 설정
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(0),  # 오늘부터 시작 (내일 자정부터 실행)
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'daily_table_update',
    default_args=default_args,
    description='A DAG to update tables daily with 3 core tasks and DAG-level notifications',
    schedule_interval='0 0 * * *',  # 매일 오전 9시에 실행
    catchup=False,
    is_paused_upon_creation=True,  # 생성 시 자동 Paused (Docker 재시작 시 자동 실행 방지)
    on_success_callback=slack_success_callback,  # DAG 성공 시 한 번만 알림
    on_failure_callback=slack_failure_callback,  # DAG 실패 시 한 번만 알림
)

# Task 1: 시트 업데이트
sheets_update = PythonOperator(
    task_id='sheets_update_task',
    python_callable=sheets_update_task,
    dag=dag,
)

# Task 2-1: 독립적 쿼리들 (의존성 없음) - independent_queries 순서와 동일
redash_query_1001 = PythonOperator(
    task_id='redash_query_1001',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1001},
    dag=dag,
)

redash_query_1002 = PythonOperator(
    task_id='redash_query_1002',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1002},
    dag=dag,
)

redash_query_1003 = PythonOperator(
    task_id='redash_query_1003',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1003},
    dag=dag,
)

redash_query_1004 = PythonOperator(
    task_id='redash_query_1004',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1004},
    dag=dag,
)

redash_query_1005 = PythonOperator(
    task_id='redash_query_1005',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1005},
    dag=dag,
)

redash_query_1006 = PythonOperator(
    task_id='redash_query_1006',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1006},
    dag=dag,
)

redash_query_1007 = PythonOperator(
    task_id='redash_query_1007',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1007},
    dag=dag,
)

redash_query_1008 = PythonOperator(
    task_id='redash_query_1008',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1008},
    dag=dag,
)

redash_query_1009 = PythonOperator(
    task_id='redash_query_1009',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1009},
    dag=dag,
)

redash_query_1010 = PythonOperator(
    task_id='redash_query_1010',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1010},
    dag=dag,
)

redash_query_1011 = PythonOperator(
    task_id='redash_query_1011',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1011},
    dag=dag,
)

redash_query_1012 = PythonOperator(
    task_id='redash_query_1012',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1012},
    dag=dag,
)

redash_query_1013 = PythonOperator(
    task_id='redash_query_1013',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1013},
    dag=dag,
)

redash_query_1014 = PythonOperator(
    task_id='redash_query_1014',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1014},
    dag=dag,
)

redash_query_1015 = PythonOperator(
    task_id='redash_query_1015',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1015},
    dag=dag,
)

redash_query_1016 = PythonOperator(
    task_id='redash_query_1016',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1016},
    dag=dag,
)

redash_query_1017 = PythonOperator(
    task_id='redash_query_1017',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1017},
    dag=dag,
)

redash_query_1018 = PythonOperator(
    task_id='redash_query_1018',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1018},
    dag=dag,
)

redash_query_1019 = PythonOperator(
    task_id='redash_query_1019',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1019},
    dag=dag,
)

redash_query_1020 = PythonOperator(
    task_id='redash_query_1020',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1020},
    dag=dag,
)

redash_query_1021 = PythonOperator(
    task_id='redash_query_1021',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 1021},
    dag=dag,
)

# Task 2-2: 의존적 쿼리들
redash_query_2001 = PythonOperator(
    task_id='redash_query_2001',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 2001},
    dag=dag,
)

redash_query_2002 = PythonOperator(
    task_id='redash_query_2002',
    python_callable=redash_query_task,
    op_kwargs={'query_number': 2002},
    dag=dag,
)

# Task 3: 텍스트 마이닝 실행
text_mining = PythonOperator(
    task_id='text_mining_task',
    python_callable=text_mining_task,
    dag=dag,
)

# 개별 쿼리 의존성 설정 - 완전 직렬 실행 (Redash 워커 부하 최소화)
sheets_update >> redash_query_1001 >> redash_query_1002 >> redash_query_1003 >> redash_query_1004 >> redash_query_1005 >> redash_query_1006 >> redash_query_1007 >> redash_query_1008 >> redash_query_1009 >> redash_query_1010 >> redash_query_1011 >> redash_query_1012 >> redash_query_1013 >> redash_query_1014 >> redash_query_1015 >> redash_query_1016 >> redash_query_1017 >> redash_query_1018 >> redash_query_1019 >> redash_query_1020 >> redash_query_1021

# 의존적 쿼리 의존성 설정
redash_query_1004 >> redash_query_2001  # 1004 완료 후 2001 실행
redash_query_1013 >> redash_query_2001  # 1013 완료 후 2001 실행
sheets_update >> redash_query_2002      # 시트 업데이트 완료 후 2002 실행

# 텍스트 마이닝 의존성
[redash_query_2001, redash_query_2002] >> text_mining  # 의존적 쿼리들 완료 후 텍스트 마이닝

