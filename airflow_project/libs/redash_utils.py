"""
Redash 관련 유틸리티 클래스
Redash API를 통한 쿼리 실행 및 결과 조회를 담당
"""

import requests
import time

class RedashManager:
    """Redash API 관련 작업을 담당하는 클래스"""
    
    def __init__(self, api_key):
        self.url = 'https://redash.example-company.com'
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f'{api_key}',
            "Content-Type": 'application/json'
        })
        
    def poll_job(self, s, job):
        """쿼리 작업 상태를 폴링하여 완료 대기"""
        # TODO: add timeout
        while job['status'] not in (3, 4):
            job_id = job['id']
            response = s.get(f'{self.url}/api/jobs/{job_id}')
            job = response.json()['job']
            time.sleep(1)

        if job['status'] == 3:
            return job['query_result_id']
        
        return None

    def get_refresh_query_result(self, query_id):
        """쿼리를 새로 실행하고 결과 반환"""
        s = self.session
        response = s.post(f'{self.url}/api/queries/{query_id}/refresh')

        if response.status_code != 200:
            raise Exception('Refresh failed.')
        
        result_id = self.poll_job(s, response.json()['job'])
        
        if result_id:
            response = s.get(f'{self.url}/api/queries/{query_id}/results/{result_id}.json')
            if response.status_code != 200:
                raise Exception('Failed getting results.')
        else:
            raise Exception('Query execution failed.')

        return response.json()['query_result']['data']['rows']
    
    def get_cached_query_result(self, query_id):
        """캐시된 쿼리 결과 반환"""
        s = self.session
        response = s.get(f'{self.url}/api/queries/{query_id}/results')
        return response.json()['query_result']['data']['rows']
    
    # Custom Operator에서 필요한 메서드들
    def refresh_query(self, query_id):
        """쿼리를 새로 실행하고 job_id 반환"""
        response = self.session.post(f'{self.url}/api/queries/{query_id}/refresh')
        
        if response.status_code != 200:
            raise Exception(f'쿼리 {query_id} 실행 실패: {response.status_code}')
        
        job_data = response.json()
        return job_data['job']['id']
    
    def get_job_status(self, job_id):
        """job의 상태를 확인하고 반환"""
        response = self.session.get(f'{self.url}/api/jobs/{job_id}')
        
        if response.status_code != 200:
            raise Exception(f'Job {job_id} 상태 확인 실패: {response.status_code}')
        
        job = response.json()['job']
        
        # Redash job status 매핑
        # 1: pending, 2: running, 3: finished, 4: failed
        status_mapping = {
            1: 'pending',
            2: 'running', 
            3: 'finished',
            4: 'failed'
        }
        
        return {
            'status': status_mapping.get(job['status'], 'unknown'),
            'error': job.get('error'),
            'query_result_id': job.get('query_result_id')
        }
    
    def get_query_result(self, query_id):
        """쿼리 결과를 반환"""
        response = self.session.get(f'{self.url}/api/queries/{query_id}/results')
        
        if response.status_code != 200:
            raise Exception(f'쿼리 {query_id} 결과 조회 실패: {response.status_code}')
        
        return response.json()['query_result']['data']['rows']
