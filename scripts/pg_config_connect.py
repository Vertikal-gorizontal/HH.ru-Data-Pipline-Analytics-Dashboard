import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

class PostgresConfigHH_RU:
    def __init__(self):
        self.host = os.getenv("PG_HOST")
        self.port = int(os.getenv('PG_PORT'))
        self.dbname = os.getenv('PG_DBNAME')
        self.user = os.getenv('PG_USER')
        self.password = os.getenv('PG_PASSWORD')

    def connect(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            dbname=self.dbname,
            user=self.user,
            password=self.password
        )
        return conn
    
    def engine_alchemy(self):
        return f'postgresql+psycopg2://{self.user}:{self.password}@{self.host}:{self.port}/{self.dbname}'

pg_config = PostgresConfigHH_RU()