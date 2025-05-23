from airflow.models.baseoperator import BaseOperator
from airflow.hooks.base import BaseHook
import pandas as pd

class SeoulApiToCsvOperator(BaseOperator):
    template_fields = ('endpoint', 'path', 'file_name', 'base_dt')

    def __init__(self, dataset_nm, path, file_name, base_dt=None, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = 'openapi.seoul.go.kr'
        self.path = path
        self.file_name = file_name
        self.endpoint = '{{var.value.MyApikey}}/json/' + dataset_nm
        self.base_dt = base_dt

    def execute(self, context):
        import os

        connection = BaseHook.get_connection(self.http_conn_id)
        self.base_url = f'http://{connection.host}:{connection.port}/{self.endpoint}'

        total_row_df = pd.DataFrame()
        start_row = 1
        end_row = 1000
        while True:
            self.log.info(f'시작:{start_row}')
            self.log.info(f'끝{end_row}')
            row_df = self._call_api(self.base_url, start_row, end_row)
            #아래 부분 total_row_df,row_df부분을 {}중괄호로 덮었더니, 해시 문제가 발생했다고 하는데, 정확히 이해는 못 함
            total_row_df = pd.concat([total_row_df,row_df])
            if len(row_df) < 1000:
                break
            else:
                start_row = end_row + 1
                end_row += 1000

        if not os.path.exists(self.path):
            #실수 했었던 부분self.path 부분을 ()로 덮었더니, 텍스트로 취급한다고 함
            os.system(f'mkdir -p {self.path}')
        total_row_df.to_csv(self.path + '/' + self.file_name, encoding='utf-8', index=False)

    def _call_api(self,base_url,start_row,end_row):
        import requests
        import json

        headers = {'Content-Type': 'application/json',
                   'charset': 'utf-8',
                   'Accept': '*/*'
                   }
        request_url = f'{base_url}/{start_row}/{end_row}/'
        if self.base_dt is not None:
            request_url = f'{base_url}/{start_row}/{end_row}/{self.base_dt}'

        #아래 코드를 개행을 잘 못해서 if 문 안으로 넣어 문제가 발생하였었음
        response = requests.get(request_url, headers)
        contents = json.loads(response.text)

        key_nm = list(contents.keys())[0]
        row_data = contents.get(key_nm).get('row')
        row_df = pd.DataFrame(row_data)

        return row_df
