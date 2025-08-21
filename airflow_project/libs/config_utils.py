"""
환경 변수 관리 및 컴포넌트 초기화를 담당하는 모듈
"""

import os

def get_env_var(var_name, default=None, required=True):
    """환경 변수를 안전하게 가져오는 함수"""
    value = os.environ.get(var_name)
    if value is None:
        if required:
            raise ValueError(f"Required environment variable {var_name} is not set")
        return default
    return value

def initialize_components():
    """모든 매니저 컴포넌트를 초기화하고 반환"""
    try:
        # 환경 변수 검증
        id = get_env_var('DB_ID', required=True)
        pw = get_env_var('DB_PW', required=True)
        host = get_env_var('DB_HOST', required=True)
        port = get_env_var('DB_PORT', required=True)
        dbname = get_env_var('DB_NAME', required=True)
        redash_api_key = get_env_var('REDASH_API_KEY', required=True)
        
        # 컴포넌트 초기화
        from .database_utils import DatabaseManager
        from .sheets_utils import GoogleSheetsManager
        from .redash_utils import RedashManager
        from .contract_form_category_analyzer import DocumentTemplateContractAnalyzer, DocumentTemplateFormAnalyzer
        
        # Database 관련 매니저들
        db_manager = DatabaseManager(id, pw, host, port, dbname)
        sheets_manager = GoogleSheetsManager()
        
        # Redash 매니저
        redash_manager = RedashManager(redash_api_key)
        
        # 카테고리 분석 매니저들
        contract_analyzer = DocumentTemplateContractAnalyzer(db_manager)
        form_analyzer = DocumentTemplateFormAnalyzer(db_manager)
        
        return {
            'db_manager': db_manager,
            'sheets_manager': sheets_manager,
            'redash_manager': redash_manager,
            'contract_analyzer': contract_analyzer,
            'form_analyzer': form_analyzer
        }
        
    except Exception as e:
        print(f"Error initializing components: {e}")
        # 기본값으로 초기화 (에러 발생 시)
        return {
            'db_manager': None,
            'sheets_manager': None,
            'redash_manager': None,
            'contract_analyzer': None,
            'form_analyzer': None
        }
