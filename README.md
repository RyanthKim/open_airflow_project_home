# 🚀 Apache Airflow Data Pipeline Portfolio Project

## 📋 프로젝트 개요

이 프로젝트는 **Apache Airflow를 활용한 데이터 파이프라인 구축**을 학습하여 **실제 실무에 적용하고 사용 중인 포트폴리오**입니다.

> 🚀 **실무 적용 현황**: 현재 실제 업무 환경에서 데이터 파이프라인으로 활용되어 업무 효율성 증대 및 프로세스 자동화에 기여하고 있습니다.

### 🎯 **학습 목표**
- **Apache Airflow 기초 학습**: DAG 설계 및 구현 방법 습득
- **데이터 파이프라인 이해**: 기본적인 ETL 프로세스 구조 학습
- **Docker 환경 경험**: 컨테이너 기반 개발 환경 구축 연습
- **API 연동 워크플로우화**: 기존에 알고 있던 API 연동을 Airflow DAG로 구성하는 방법 학습
- **자연어 처리 워크플로우화**: 기존에 알고 있던 텍스트 마이닝을 Airflow Task로 자동화하는 방법 학습

### 🔍 **학습 과정에서 경험한 내용**
- **실무 환경 체험**: 실제 업무에서 사용하는 도구들을 활용한 개발 경험
- **모듈화 연습**: 코드를 기능별로 분리하여 관리하는 방법 학습
- **의존성 관리 학습**: Task 간 실행 순서와 의존성 처리 방법 이해
- **모니터링 시스템 구축**: 기본적인 알림 시스템 구현 경험
- **확장 가능한 구조 고려**: 향후 기능 추가를 고려한 설계 연습

## 🏗️ 프로젝트 아키텍처

```
airflow_project_home/
├── airflow_project/                    # Airflow 프로젝트 핵심 디렉토리
│   ├── dags/                          # Airflow DAG 정의
│   │   └── daily_table_update_dag.py  # 메인 데이터 업데이트 DAG
│   ├── tasks/                         # DAG별 Task 함수들
│   │   └── daily_table_update_tasks.py
│   └── libs/                          # 공통 라이브러리
│       ├── config_utils.py            # 환경 설정 및 컴포넌트 초기화
│       ├── database_utils.py          # 데이터베이스 연결 및 관리
│       ├── sheets_utils.py            # Google Sheets API 연동
│       ├── redash_utils.py            # Redash API 연동
│       ├── contract_form_category_analyzer.py  # 텍스트 마이닝 분석
│       ├── notification_utils.py      # Slack 알림 시스템
│       ├── documentCategoryJson/      # 카테고리 분석용 사전 데이터
│       └── formCategoryJson/          # 서식 분석용 사전 데이터
├── requirements.txt                    # Python 의존성 관리 (프로젝트 루트)
├── Dockerfile                         # Airflow 컨테이너 빌드 (프로젝트 루트)
├── docker-compose.yaml                # Docker 서비스 오케스트레이션
├── google-credentials.json            # Google API 인증 정보 (데모용)
├── .env example                       # 환경 변수 설정 예시
├── .gitignore                         # Git 무시 파일 설정
├── query_samples/                     # DAG에서 사용되는 샘플 쿼리들
│   ├── airflow_dag_sample_query_1.sql # 구독 데이터 처리 파이프라인
│   ├── airflow_dag_sample_query_2.sql # 결제 빌링 데이터 처리 파이프라인
│   └── airflow_dag_sample_query_3.sql # 바이럴 효과 측정 분석 파이프라인
└── README.md                          # 프로젝트 문서
```

## 📊 샘플 쿼리 소개

이 프로젝트는 실제 실무에서 사용되는 복잡한 데이터 처리 쿼리들을 포트폴리오용으로 안전하게 변환한 샘플들을 포함하고 있습니다.

### **1. 구독 데이터 처리 파이프라인** (`airflow_dag_sample_query_1.sql`)
- **목적**: 구독 서비스의 이벤트 로그를 처리하여 구독 기간 및 상태를 분석
- **주요 기능**: 
  - 복잡한 데이터 변환 및 CTE 활용
  - 이벤트 로그 처리 및 비즈니스 로직 구현
  - 재귀적 쿼리 패턴과 윈도우 함수 활용
  - 구독 생명주기 관리 및 분석
- **사용 사례**: 일일 구독 데이터 처리, 비즈니스 인텔리전스 및 리포팅

### **2. 결제 빌링 데이터 처리 파이프라인** (`airflow_dag_sample_query_2.sql`)
- **목적**: 결제 및 빌링 데이터를 처리하여 재무 분석 및 리포팅 지원
- **주요 기능**:
  - 복잡한 빌링 데이터 변환 및 분석
  - 결제 처리 및 구독 기간 관리
  - 인보이스 분류 및 바우처 플랜 처리
  - 환불 처리 및 금액 계산
  - 다차원 빌링 분석 및 리포팅
- **사용 사례**: 일일 빌링 데이터 처리, 재무 리포팅 및 비즈니스 인텔리전스

### **3. 바이럴 효과 측정 분석 파이프라인** (`airflow_dag_sample_query_3.sql`)
- **목적**: 서명 경험을 통한 서명자의 가입 및 결제 여부를 분석하여 바이럴 효과 측정
- **주요 기능**:
  - **1단계**: 참여자 데이터 테이블 생성 및 관리 (개인정보 보호를 위한 해시 처리)
  - **2단계**: 참여자 정보와 문서 정보의 JOIN 처리
  - **3단계**: 열람 이력 기반 서명자 필터링 및 순서 관리
  - **4단계**: 이메일과 전화번호별 중복 제거 및 우선순위 처리
  - **5단계**: 서명 경험을 통한 가입 여부 판단 로직
  - **6단계**: 바이럴 효과 측정을 위한 데이터 분석
- **사용 사례**: 마케팅 효과 분석, 바이럴 성과 측정, 사용자 행동 분석
- **특징**: 복잡한 데이터 파이프라인을 단계별로 구분하여 체계적인 분석 수행

### **🔒 보안 조치**
- 모든 테이블명을 `example_` 접두사로 변경
- 실제 회사 정보 및 민감한 비즈니스 로직 제거
- 샘플 데이터 및 가격 정보로 대체

---

## 🚀 학습하며 구현한 주요 기능

### 1. **데이터 파이프라인 워크플로우**
```
Google Sheets → Redshift → Redash → Text Mining → Slack 알림
```

- **데이터 수집**: 기존에 학습한 Google Sheets API를 활용하여 데이터 추출
- **데이터 저장**: 기존에 학습한 데이터베이스 저장 방법 활용
- **데이터 분석**: 쿼리 실행 순서와 의존성 관리 방법을 새로 학습
- **텍스트 분석**: 기존에 학습한 한국어 자연어 처리 라이브러리 활용
- **모니터링**: 기존에 학습한 Slack 알림 시스템 활용

### 2. **의존성 관리 학습**
```python
# 독립적 쿼리들 (병렬 실행 가능)
independent_queries = [1001, 1002, 1003, ...]

# 의존적 쿼리들 (순차 실행 필요)
dependent_queries = [2001, 2002]  # 의존성: 1009, 1012
```

- **의존성 이해**: 쿼리 간 의존성을 정의하는 방법 학습
- **실행 순서 관리**: 독립적인 작업과 순차적인 작업을 구분하는 방법 연습
- **안전한 처리**: 의존성이 있는 작업들을 순서대로 처리하는 방법 익히기

### 3. **코드 모듈화 연습**
```python
# 단일 책임 원칙을 따른 컴포넌트 분리
components = {
    'db_manager': DatabaseManager(),
    'sheets_manager': GoogleSheetsManager(),
    'redash_manager': RedashManager(),
    'contract_analyzer': DocumentTemplateContractAnalyzer(),
    'form_analyzer': DocumentTemplateFormAnalyzer()
}
```

- **기능별 분리**: 업무에서 이미 활용하고 있는 모듈화 방법 적용
- **코드 재사용**: 업무에서 이미 활용하고 있는 재사용 구조 적용
- **유지보수 고려**: 업무에서 이미 활용하고 있는 코드 품질 관리 방법 적용

## 🛠️ 사용한 기술 스택 및 학습 내용

### **Core Technologies**
- **Apache Airflow 2.9.3**: 워크플로우 오케스트레이션 및 스케줄링
- **Python 3.11+**: 백엔드 로직 및 데이터 처리
- **Docker & Docker Compose**: 컨테이너 기반 개발 환경
- **PostgreSQL**: 로컬 개발용 데이터베이스

### **Data Processing & APIs**
- **Google Sheets API (gspread)**: 기존에 학습한 데이터 소스 연동 및 자동화 기술 활용
- **Redash API (redash-toolbelt)**: 기존에 학습한 데이터 쿼리 실행 및 결과 관리 기술 활용
- **KoNLPy (Komoran)**: 기존에 학습한 한국어 자연어 처리 및 형태소 분석 기술 활용
- **Pandas 2.0.0**: 기존에 학습한 데이터 조작 및 분석 기술 활용
- **Redshift Connector**: 기존에 학습한 AWS Redshift 데이터베이스 연동 기술 활용

### **Infrastructure & DevOps**
- **환경 변수 관리**: 민감 정보 보안 및 환경별 설정 분리
- **로깅 시스템**: 체계적인 로그 관리 및 디버깅
- **에러 처리**: 예외 상황에 대한 견고한 에러 핸들링
- **모니터링**: Slack 웹훅을 통한 실시간 알림
- **컨테이너 최적화**: Java 17, GCC 컴파일러 등 개발 도구 포함

## 📦 설치 및 실행 가이드

### **1. 환경 설정**
```bash
# 프로젝트 클론
git clone <repository-url>
cd airflow_project_home

# 환경 변수 파일 생성 (데모용)
cp .env.example .env
# .env 파일에 데모 환경 변수 설정
```

### **2. Docker 환경 실행**
```bash
# Airflow 서비스 시작
docker-compose up -d

# 서비스 상태 확인
docker-compose ps

# 로그 모니터링
docker-compose logs -f airflow-scheduler
```

### **3. Airflow 접속 및 확인**
- **Web UI**: http://localhost:8080
- **기본 계정**: `airflow` / `airflow`
- **DAG 활성화**: `daily_table_update` DAG 활성화 후 실행

**참고**: Docker를 처음 실행하는 경우, admin 계정을 추가해야 할 수 있습니다:
```bash
# admin 계정 추가
docker-compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin123
```

### **4. Dockerfile 상세 정보**
```dockerfile
FROM apache/airflow:2.9.3
# Java 17, GCC 컴파일러, Python 개발 도구 포함
# KoNLPy 및 기타 한국어 처리 라이브러리 지원
```

## 🔧 환경 변수 설정 (운영 환경 준비)

### **필수 파일 및 디렉토리**
- **`.env`**: 환경 변수 설정 파일 (`.env example` 참고)
- **`google-credentials.json`**: Google Sheets API 인증 파일 (프로젝트 루트에 위치)

### **환경 변수 설정**
`.env` 파일에 다음 환경 변수들을 설정합니다:

```bash
# Airflow 기본 설정
AIRFLOW_UID=50000
AIRFLOW_GID=0
TZ=Asia/Seoul
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__EXPOSE_CONFIG=True

# 데이터베이스 연결 (운영 환경에 맞게 설정)
DB_ID=your_db_user
DB_PW=your_db_password
DB_HOST=your_db_host
DB_PORT=5432
DB_NAME=your_database_name

# Redash 설정 (운영 환경)
REDASH_API_KEY=your_redash_api_key
REDASH_URL=https://your-redash-instance.com

# Slack 설정 (운영 환경)
SLACK_WEBHOOK_URL=your_slack_webhook_url
SLACK_TEAM_ID=your_team_id

# Google Sheets API (운영 환경)
GOOGLE_CREDENTIALS_FILE=path/to/your/google-credentials.json
```

**참고**: `.env example` 파일을 참고하여 필요한 환경 변수를 설정하세요.

## 📊 DAG 구조 및 실행 흐름

### **daily_table_update DAG 구조**
```
start → sheets_update_task → [21개 독립적 Redash 쿼리] → [2개 의존적 Redash 쿼리] → text_mining_task → end
```

### **Task별 상세 기능**

#### **1. sheets_update_task**
- Google Sheets에서 카테고리 데이터 추출
- Redshift 데이터베이스에 데이터 적재
- 데이터 동기화 상태 모니터링

#### **2. redash_queries_task**
- **21개 독립적 쿼리**: 완전 직렬 실행으로 Redash 워커 부하 최소화
- **2개 의존적 쿼리**: 선행 조건 만족 후 실행
- **쿼리 세분화**: 각 쿼리를 개별 Task로 분리하여 정밀한 실행 제어 및 재시작 가능
- 쿼리 실행 시간 및 결과 모니터링

#### **3. text_mining_task**
- 문서 제목 텍스트 마이닝 분석
- 템플릿 제목 카테고리 분류
- 자연어 처리 결과 데이터베이스 저장

## 🔍 모니터링 및 디버깅

### **로그 시스템**
- **DAG 레벨 로그**: 전체 워크플로우 실행 흐름 추적
- **Task 레벨 로그**: 개별 태스크 실행 상태 및 결과 확인
- **에러 로그**: 실패 원인 분석 및 문제 해결 가이드

### **Slack 알림 시스템**
- 🚀 **워크플로우 시작**: DAG 실행 시작 알림
- ✅ **워크플로우 완료**: 성공적인 실행 완료 알림
- ❌ **워크플로우 실패**: 에러 상세 정보와 함께 실패 알림

## 🎯 이 프로젝트를 통해 학습한 내용

### **Apache Airflow 기초 학습**
- **DAG 이해**: 기본적인 워크플로우 설계 방법 학습
- **Task 의존성**: 태스크 간 실행 순서 관리 방법 익히기
- **스케줄링**: 정기적인 작업 실행 설정 방법 연습
- **모니터링**: 작업 상태를 확인하고 알림 받는 방법 학습
- **환경 설정**: Docker를 활용한 개발 환경 구축 경험

### **데이터 처리 기초 학습**
- **데이터 파이프라인**: 기본적인 ETL 프로세스 이해 및 구현 연습
- **API 연동**: 업무에서 이미 활용하고 있는 외부 서비스 연동 기술 적용
- **데이터 처리**: 업무에서 이미 활용하고 있는 데이터 변환 및 저장 기술 적용
- **텍스트 마이닝**: 업무에서 이미 활용하고 있는 한국어 자연어 처리 기술 적용
- **데이터 품질**: 업무에서 이미 활용하고 있는 데이터 검증 방법 적용

### **DevOps 기초 경험**
- **컨테이너 활용**: Docker를 사용한 개발 환경 구축 연습
- **환경 변수 관리**: 설정 정보를 분리하여 관리하는 방법 학습
- **로그 확인**: 기본적인 로그 관리 및 디버깅 방법 익히기
- **에러 처리**: 예외 상황에 대한 기본적인 처리 방법 학습
- **확장 고려**: 향후 기능 추가를 고려한 기본적인 구조 설계

## 🔄 확장 및 커스터마이징

### **새로운 DAG 추가**
1. `tasks/` 폴더에 `{dag_name}_tasks.py` 파일 생성
2. `dags/` 폴더에 `{dag_name}_dag.py` 파일 생성
3. 단일 책임 원칙을 따라 기능별로 분리

### **새로운 기능 추가**
1. `libs/` 폴더에 새로운 유틸리티 모듈 생성
2. 재사용 가능한 컴포넌트로 설계
3. 기존 코드와의 호환성 유지

## 📚 학습 자료 및 참고 문서

### **공식 문서**
- [Apache Airflow 공식 문서](https://airflow.apache.org/docs/)
- [Docker Compose 공식 문서](https://docs.docker.com/compose/)
- [KoNLPy 한국어 자연어 처리](https://konlpy.org/)



## 🤝 피드백 및 개선점

이 프로젝트는 학습 목적으로 제작된 포트폴리오입니다. 개선점이나 더 나은 구현 방법에 대한 피드백을 환영합니다. 

### **기여 방법**
1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request



## 📄 라이선스

이 프로젝트는 **MIT 라이선스** 하에 배포됩니다.

---

## 🎉 **프로젝트 완성도**

### **✅ 이 프로젝트를 통해 새로 학습한 기능**
- [x] Apache Airflow 2.9.3 기본 DAG 작성 및 실행
- [x] Airflow DAG에서 Task 간 의존성 관리 방법 학습
- [x] Airflow 환경에서의 에러 처리 및 모니터링 방법 학습
- [x] Airflow와 Docker 환경 연동 방법 학습
- [x] 환경 변수 기반 설정 관리

### **✅ 이미 학습 완료된 기능 (이 프로젝트에서 활용)**
- [x] Google Sheets & Redash API 연동
- [x] 한국어 텍스트 마이닝 (KoNLPy)
- [x] Slack 알림 시스템 구현
- [x] 데이터베이스 연동 및 데이터 처리
- [x] 코드 모듈화 및 구조화

### **🚀 앞으로 학습하고 싶은 방향**
- [ ] 실시간 데이터 스트리밍 파이프라인 구축 방법 학습
- [ ] 머신러닝 모델을 파이프라인에 통합하는 방법 익히기
- [ ] 클라우드 환경에서의 데이터 파이프라인 구축 경험
- [ ] 성능 모니터링 및 최적화 기법 학습
- [ ] CI/CD 파이프라인 구축 방법 익히기
- [ ] 쿠버네티스 기반 오케스트레이션 학습
- [ ] 데이터 품질 모니터링 시스템 구축 방법 학습
- [ ] 대규모 시스템 아키텍처 설계 방법 익히기

---

**마지막 업데이트**: 2025년 1월  
**버전**: 1.0.0  
**프로젝트 유형**: 학습 목적의 포트폴리오  
**기술 스택**: Apache Airflow 2.9.3, Python 3.11+, Docker, PostgreSQL, KoNLPy, Redshift
