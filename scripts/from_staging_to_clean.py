import psycopg2 
from pg_config_connect import pg_config


# функция, преобразующая таблицу в PostgreSQL в clean формат, доставая всю нужную, полную информацию из джэйсон файлов
def from_staging_to_clean():
    conn = pg_config.connect()

    cur = conn.cursor()

    sql = """
        INSERT INTO clean_vacancies (
        id, vacancy_name, employer, area, experience, salary_from, salary_to, published_at, schedule, requirements, responsibilities, vacancy_url)
        SELECT 
            (data->>'id')::BIGINT AS id,
            data->>'name' AS vacancy_name,
            data->'employer'->>'name' AS employer,
            data->'area'->>'name' AS area,
            data->'experience'->>'name' AS experience,
            (data->'salary'->>'from')::NUMERIC AS salary_from,
            (data->'salary'->>'to')::NUMERIC AS salary_to,
            (data->>'published_at')::TIMESTAMP AS published_at,
            data->'schedule'->>'name' AS schedule,
            data#>>'{snippet,requirement}' AS requirement,
            data#>>'{snippet,responsibility}' AS responsibility,
            data->>'alternate_url' AS vacancy_url
        FROM staging_vacancies
        ON CONFLICT (id) DO NOTHING;
        """
    
    cur.execute(sql)
    conn.commit()
    print('Transform was completed')

    cur.close()
    conn.close()

if __name__ == "__main__":
    from_staging_to_clean()
