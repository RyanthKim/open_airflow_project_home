"""
Database 관련 유틸리티 클래스들
데이터베이스 연결 및 기본 작업을 담당
"""

from sqlalchemy import create_engine
import pandas as pd
import datetime

class DatabaseManager:
    """데이터베이스 연결 및 기본 작업을 담당하는 클래스"""
    
    def __init__(self, id, pw, host, port, dbname):
        self.db_url = f'postgresql+psycopg2://{id}:{pw}@{host}:{port}/{dbname}'
        self.engine = create_engine(self.db_url, connect_args={"options": ""})

    @staticmethod
    def timestamp():
        """현재 타임스탬프를 반환"""
        return str(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

    def execute_sql(self, sql, sql_file=False):
        """SQL 실행 및 결과 반환"""
        if sql_file:
            print(f' - DB 조회 시작 : {self.timestamp()} - {sql}')
        else:
            print(f' - DB 조회 시작 : {self.timestamp()}')
        
        result = pd.DataFrame()
        if sql_file:
            try:
                with open(f'update_sql_list/{sql}', 'r', encoding='utf-8') as file:
                    sql = file.read().strip()
            except FileNotFoundError:
                print(f"Error: SQL file 'update_sql_list/{sql}' not found")
                return result
            except Exception as e:
                print(f"Error reading SQL file: {e}")
                return result
        
        try:
            result = pd.read_sql(sql=sql, con=self.engine)
        except Exception as error:
            print(f"Database Error: {error}")
            return pd.DataFrame()
        
        print(f'\t DB 조회 완료 : {self.timestamp()}')
        return result

    def append_data(self, rawdata, table_name, delete_existing=False):
        """데이터를 테이블에 추가 (delete_existing 옵션 포함)"""
        if delete_existing:
            self.execute_sql(f'delete from {table_name}; select 1;')
        
        df = pd.DataFrame(rawdata)
        df.columns = df.columns.str.lower()
        print(f'\r - DB dump 시작 : {self.timestamp()} - {table_name} - [{len(df)} rows]')
        
        with self.engine.connect() as conn:
            exists = conn.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name=%s);",
                [table_name]
            ).scalar()
        
        message = f"\t table : {table_name} 신규테이블 입니다." if not exists else f"\t table : {table_name} 이미 존재 하여 데이터를 추가합니다."
        print(message)
        
        try:
            df.to_sql(name=table_name, con=self.engine, if_exists='append', chunksize=10000, index=False, method='multi')
        except Exception as error:
            print("Error: %s" % error)
        
        print(f'\t DB dump 완료 : {self.timestamp()}')
