"""
문서/템플릿 계약 및 서식 카테고리 분석을 담당하는 유틸리티 클래스들
계약 카테고리 분석과 서식 카테고리 분석만을 위한 특화된 모듈
"""

from konlpy.tag import Komoran
import json
import pandas as pd

class BaseCategoryAnalyzer:
    """카테고리 분석의 공통 기능을 담당하는 기본 클래스"""
    
    @staticmethod
    def open_json(file_path):
        """JSON 파일을 읽어서 반환"""
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        return json_data


class DocumentTemplateContractAnalyzer:
    """문서/템플릿 계약 카테고리 분석을 담당하는 클래스"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def process_contract_data(self, data, category1_file_path, category2_file_path, user_dic_path):
        """계약 카테고리 데이터 처리"""
        try:
            h = 1
            komoran = Komoran(userdic=user_dic_path)

            dic_category_1 = self.open_json(category1_file_path)
            dic_category_2 = self.open_json(category2_file_path)

            # 형태소 매칭하는 부분
            processed_data = []
            for _, row_data in data.iterrows():
                try:
                    para = str(row_data['title']).strip().encode('utf-8', 'ignore').decode('utf-8')
                    ex_nouns = komoran.nouns(para)
                    j = '기타'
                    k = '기타'
                    for noun in ex_nouns:
                        if noun in dic_category_1:
                            j = dic_category_1[noun]
                            k = dic_category_2[noun]
                            break
                    category1 = j
                    category2 = k
                except Exception as e:
                    ex_nouns = ''
                    category1 = '기타'
                    category2 = '기타'
                    
                if row_data.get('documentid'):  # document인 경우
                    processed_data.append([
                        row_data['documentid'], row_data['userid'], row_data['workspaceid'], 
                        row_data['estimated_workspaceid'], row_data['createdat'],
                        str(ex_nouns), category1, category2
                    ])
                elif row_data.get('templateid'):  # template인 경우
                    processed_data.append([
                        row_data['templateid'], row_data['workspaceid'], row_data['memberid'], 
                        row_data['title'], row_data['createdat'], str(ex_nouns), category1, category2
                    ])
                
                h += 1

            return processed_data
        except Exception as e:
            raise Exception(f"Error processing contract data: {e}")

    def analyze_document_contract_categories(self, sql, table_name, column_names, category1_file_path, category2_file_path, user_dic_path, delete_table=False):
        """문서 계약 카테고리 분석"""
        return self._analyze_categories(sql, table_name, column_names, category1_file_path, category2_file_path, user_dic_path, delete_table)

    def analyze_template_contract_categories(self, sql, table_name, column_names, category1_file_path, category2_file_path, user_dic_path, delete_table=False):
        """템플릿 계약 카테고리 분석"""
        return self._analyze_categories(sql, table_name, column_names, category1_file_path, category2_file_path, user_dic_path, delete_table)

    def _analyze_categories(self, sql, table_name, column_names, category1_file_path, category2_file_path, user_dic_path, delete_table=False):
        """카테고리 분석 공통 로직"""
        try:
            try:
                result = self.db_manager.execute_sql(sql)
            except Exception as e:
                raise Exception(f"Error executing SQL: {e}")
            
            batch_size = 100000
            total_rows = len(result)
            num_batches = (total_rows + batch_size - 1) // batch_size
            
            for i in range(num_batches):
                print(f'- {i + 1} / {num_batches} 번째')
                start_index = i * batch_size
                end_index = min((i + 1) * batch_size, total_rows)
                batch_data = result[start_index:end_index]
                
                try:
                    processed_data = self.process_contract_data(batch_data, category1_file_path, category2_file_path, user_dic_path)
                except Exception as e:
                    raise Exception(f"Error in processing data batch {i + 1}: {e}")
                
                df = pd.DataFrame(processed_data, columns=column_names)
                # delete_table 옵션을 delete_existing으로 전달
                self.db_manager.append_data(rawdata=df, table_name=table_name, delete_existing=(delete_table and i == 0))
            
            print('끝')
        
        except Exception as e:
            raise Exception(f"Error in category analysis: {e}")

    @staticmethod
    def open_json(file_path):
        """JSON 파일을 읽어서 반환"""
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        return json_data


class DocumentTemplateFormAnalyzer:
    """문서/템플릿 서식 카테고리 분석을 담당하는 클래스"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager

    def process_form_data(self, data, category_file_path, user_dic_path):
        """서식 카테고리 데이터 처리"""
        try:
            h = 1
            komoran = Komoran(userdic=user_dic_path)
            
            dic_category_1 = self.open_json(category_file_path)

            # 형태소 매칭하는 부분
            processed_data = []
            for _, row_data in data.iterrows():
                try:
                    para = str(row_data['title']).strip().encode('utf-8', 'ignore').decode('utf-8')
                    ex_nouns = komoran.nouns(para)
                    j = '기타'
                    for noun in ex_nouns:
                        if noun in dic_category_1:
                            j = dic_category_1[noun]
                            break
                    category1 = j
                except Exception as e:
                    ex_nouns = ''
                    category1 = '기타'
                    
                if row_data.get('documentid'):  # document인 경우
                    processed_data.append([
                        row_data['documentid'], row_data['userid'], row_data['workspaceid'], 
                        row_data['estimated_workspaceid'], row_data['createdat'],
                        str(ex_nouns), category1
                    ])
                elif row_data.get('templateid'):  # template인 경우
                    processed_data.append([
                        row_data['templateid'], row_data['workspaceid'], row_data['memberid'], 
                        row_data['title'], row_data['createdat'], str(ex_nouns), category1
                    ])
                
                h += 1

            return processed_data
        except Exception as e:
            raise Exception(f"Error processing form data: {e}")

    def analyze_document_form_categories(self, sql, table_name, column_names, category_file_path, user_dic_path, delete_table=False):
        """문서 서식 카테고리 분석"""
        return self._analyze_form_categories(sql, table_name, column_names, category_file_path, user_dic_path, delete_table)

    def analyze_template_form_categories(self, sql, table_name, column_names, category_file_path, user_dic_path, delete_table=False):
        """템플릿 서식 카테고리 분석"""
        return self._analyze_form_categories(sql, table_name, column_names, category_file_path, user_dic_path, delete_table)

    def _analyze_form_categories(self, sql, table_name, column_names, category_file_path, user_dic_path, delete_table=False):
        """서식 카테고리 분석 공통 로직"""
        try:
            try:
                result = self.db_manager.execute_sql(sql)
            except Exception as e:
                raise Exception(f"Error executing SQL: {e}")
            
            batch_size = 100000
            total_rows = len(result)
            num_batches = (total_rows + batch_size - 1) // batch_size
            
            for i in range(num_batches):
                print(f'- {i + 1} / {num_batches} 번째')
                start_index = i * batch_size
                end_index = min((i + 1) * batch_size, total_rows)
                batch_data = result[start_index:end_index]
                
                try:
                    processed_data = self.process_form_data(batch_data, category_file_path, user_dic_path)
                except Exception as e:
                    raise Exception(f"Error in processing data batch {i + 1}: {e}")
                
                df = pd.DataFrame(processed_data, columns=column_names)
                # delete_table 옵션을 delete_existing으로 전달 (첫 번째 배치에서만)
                self.db_manager.append_data(rawdata=df, table_name=table_name, delete_existing=(delete_table and i == 0))
            
            print('끝')
        
        except Exception as e:
            raise Exception(f"Error in form category analysis: {e}")

    @staticmethod
    def open_json(file_path):
        """JSON 파일을 읽어서 반환"""
        with open(file_path, 'r', encoding='utf-8') as f:
            json_data = json.load(f)
        return json_data
