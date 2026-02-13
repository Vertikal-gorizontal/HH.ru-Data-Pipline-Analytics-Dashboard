import boto3
from botocore.exceptions import ClientError
import requests
import json
from datetime import datetime, date
from io import BytesIO
import logging
from dotenv import load_dotenv
import os
from pathlib import Path

logs = Path('logs')
logs.mkdir(exist_ok=True)

log_path = logs / 'upload_log.txt'


load_dotenv()

# Подключаем логгирование
logging.basicConfig(filename=log_path, filemode='a', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Функция для подключения к s3
def get_s3_client():
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('S3_ENDPOINT'),
        aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
    )
    return s3

s3 = get_s3_client()

# Функция для создания бакета
def create_bucket(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Бакет с именем {bucket_name} уже создан")
    except ClientError as error:
        error_code = error.response['Error']['Code']

        if error_code == '404':
            s3.create_bucket(Bucket=bucket_name)
            print(f'mission completed. Bucket {bucket_name} was created')
        else:
            raise error

# Функция для проверки наличия объекта в бакете. Используется ниже для предотвращения загрузки копий
def object_exists(bucket_name, obj_name):
    try:
        s3.head_object(Bucket=bucket_name, Key=obj_name)
        return True
    except ClientError as error:
        if error.response['Error']['Code'] == '404':
            return False
        else:
            raise error
        


# Функция для парсинга вакансий с hh.ru. После парсинга данные сразу загружаются в созданный бакет в облаке
def parser_for_vacancies(bucket_name, search_text='data', area=1, per_page=100, max_pages=5):
    today = date.today().isoformat()
    base_url = "https://api.hh.ru/vacancies"

    for page in range(max_pages):

        params = {
            'text': search_text,
            'area': area,
            'per_page': per_page,
            'page': page
        }
        response = requests.get(base_url, params=params)
        if response.status_code != 200:
            print(f'Error god damn on page {page+1}. status_code: {response.status_code}')
            logging.error(f'Error god damn on page {page+1}. status_code: {response.status_code}')
            continue

        data = response.json()
        items = data.get('items', [])
        if not items:
            print(f'No items on page {page+1}')
            logging.warning(f'No items on page {page+1}')
        
        
        json_bytes = json.dumps(data, ensure_ascii=False).encode('utf-8')
        file_obj = BytesIO(json_bytes)
        object_name = f'{today}/vacancies{page+1}.json'

        if object_exists(bucket_name, object_name):
            print(f'File with name {object_name} alredy exists in {bucket_name}')
            logging.info(f'File with name {object_name} alredy exists in {bucket_name}')
            continue

        try:
            s3.upload_fileobj(file_obj, bucket_name, object_name)
            print(f'File was upload in {bucket_name} and named {object_name}')
            logging.info(f'File was upload in {bucket_name} and named {object_name}')
        except ClientError as error:
            print(f'Failed to upload page {page+1}: {error}')
            logging.error(f'Failed to upload page {page+1}: {error}')


if __name__ == '__main__':
    parser_for_vacancies(os.getenv('S3_BUCKET'))










