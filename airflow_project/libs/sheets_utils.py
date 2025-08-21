"""
Google Sheets 관련 유틸리티 클래스
Google Sheets API를 통한 데이터 조회를 담당
"""

import gspread
import pandas as pd
import os

class GoogleSheetsManager:
    """Google Sheets 관련 작업을 담당하는 클래스"""
    
    def __init__(self):
        """Google Sheets API 인증 파일 경로 설정"""
        self.auth_file = '/opt/airflow/google-credentials.json'
        if not os.path.exists(self.auth_file):
            raise FileNotFoundError(f"Google Sheets API 인증 파일을 찾을 수 없습니다: {self.auth_file}")

    def authenticate_google_sheets(self):
        """Google Sheets API 인증"""
        try:
            gc = gspread.service_account(filename=self.auth_file)
            return gc
        except Exception as e:
            raise Exception(f"Google Sheets API 인증 실패: {e}")

    def get_sheet(self, id, sheetName):
        """Google Sheets에서 데이터 조회"""
        print(f' - 시트 조회 시작 : {sheetName}')
        
        try:
            gc = self.authenticate_google_sheets()
            gsheet = gc.open_by_url(f"https://docs.google.com/spreadsheets/d/{id}")
            wsheet = gsheet.worksheet(sheetName)
            temp = wsheet.get()
            result = pd.DataFrame(temp[1:], columns=temp[0])
            result.columns = result.columns.str.lower()
            print('\t 시트 조회 완료')
            return result
        except Exception as e:
            print(f"Google Sheets API 오류: {e}")
            raise Exception(f"시트 조회 중 오류 발생: {e}")
