from sklearn.feature_extraction.text import CountVectorizer
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
import re
from collections import Counter
from pg_config_connect import pg_config
from wordcloud import WordCloud
import matplotlib.pyplot as plt


# коннектимся к Postgresql используя sqlalchemy
string_for_engine = pg_config.engine_alchemy()
engine = create_engine( # create_engine - функция в pg_config_connect
    string_for_engine
)

df = pd.read_sql('SELECT requirements, responsibilities FROM clean_vacancies', engine)
print(f'Выбрано {len(df)} строк') # выбираем строки текстового формата, для дальнейшего анализа

# функция для очистки текста от лишних знаков. оставляем только буквы, цифры и пробелы с помощью регулярок
def clean_text(text):
    if not text:
        return ''
    text = text.lower()
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'[^a-zа-я0-9\s]', '', text)
    return text 

df['req_clean'] = df['requirements'].apply(clean_text)
df['resp_clean'] = df['responsibilities'].apply(clean_text)
# делаю русские стоп-слова, которые не будут участвовать в итоговой выборке
russian_stop_words = ['и', 'в', "на", "с", "по", "для", "от", "или", "до", "как", "а", "но", "что", "к"]

# Вычисляем топ-50 слов с помощью каунтера
req_words_list =' '.join(df['req_clean']).split()
counter = Counter(req_words_list)
print('top 50 words в требованиях:')
count = 0
for word, _ in counter.most_common(50):
    if word in russian_stop_words:
        count += 1
for word, freq in counter.most_common(50+count):
    if word not in russian_stop_words:
        print(word, freq)

# Выбираем 50 наиболее часто встречающихся фраз, работая с матрицами
vectorizer = CountVectorizer(ngram_range=(2,3), stop_words=russian_stop_words) 
X = vectorizer.fit_transform(df['req_clean']) # создаем разряженную матрицу из компаний и уникальных фраз (длина 2-3 слова).
sum_words = X.sum(axis=0) # суммируем результаты, сплющивая матрицу, чтобы в итоге получилась матрица 1 на n со значением количества встречаемых слов
words_freq = [(word, sum_words[0, idx]) for word, idx in vectorizer.vocabulary_.items()] # трансформируем результат в формат листа с кортежами (фраза, количество)
words_freq = sorted(words_freq, key=lambda x: x[1], reverse=True) # сортируем и выводим самые часто встречающиеся

print('top 50 фраз в требованиях:')
for phrase, freq in words_freq[:50]:
    print(phrase, freq)

# строим облако фраз, где размер демонстрирует частоту встречаемости фразы

text = ' '.join(df['req_clean'])
wordcloud = WordCloud(width=800, height=400, font_path="C:/Windows/Fonts/arial.ttf").generate(text)

plt.figure(figsize=(15,7))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()



