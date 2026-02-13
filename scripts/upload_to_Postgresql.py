import json
from datetime import datetime, date
import boto3 
import psycopg2
from psycopg2.extras import execute_values
from pg_config_connect import pg_config
import os
from dotenv import load_dotenv
from pathlib import Path
import logging

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

# Функция для загрузки данных из S3 (raw-хранилище) в PostgreSQL - staging table
def from_s3_to_postgresql():
    conn = pg_config.connect()

    cur=conn.cursor()

    today = date.today().isoformat()
    response = s3.list_objects_v2(Bucket=os.getenv('S3_BUCKET'), Prefix=f'{today}/') # получили словарь, под ключем 'Contents' в которых лежат файлы и инф о них
    files = [obj['Key'] for obj in response.get('Contents', [])] # получили список названий файлов, лежащих в S3
    
    for file_key in files: # для каждого файла в s3 открываем json, находим id вакансии и переносим в Postgre
        obj = s3.get_object(Bucket=os.getenv('S3_BUCKET'), Key=file_key)
        file_data = json.loads(obj['Body'].read())
        rows = []
        for vacancy_info in file_data.get('items', []):
            job_id = vacancy_info['id']
            data = json.dumps(vacancy_info, ensure_ascii=False)

            rows.append((job_id, data))


        if rows:
            execute_values(cur, """
                            INSERT INTO staging_vacancies (id, data)
                            VALUES %s ON CONFLICT (id) DO NOTHING 
                            """, rows)
            conn.commit()
            print(f'{len(rows)} rows from {file_key} was uploaded to PostgreSQL')
            logging.info(f'{len(rows)} rows from {file_key} was uploaded to PostgreSQL')
        else:
            print(f'Файл {file_key} пуст')
            logging.warning(f'Файл {file_key} пуст')
    cur.close()
    conn.close()

if __name__ == "__main__":
    from_s3_to_postgresql()

        