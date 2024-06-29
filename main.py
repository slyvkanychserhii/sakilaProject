
from db import DB
import re
from datetime import datetime


SEARCH_SETTINGS = [
    ['title', 'названию', 1],
    ['description', 'описанию', 1],
    ['category', 'жанру', 1],
    ['release_year', 'году выпуска', 1],
    ['actors', 'актерам', 1]]


class DB1(DB):
    _config = {'host': 'host',
               'user': 'user',
               'password': 'password',
               'database': 'sakila'}


class DB2(DB):
    _config = {'host': 'host',
               'user': 'user',
               'password': 'password',
               'database': 'sakila_stat'}


def yes(text):
    yes_no = input(f'{text} [Y/n]?')
    return False if yes_no.lower() in ('n', 'no') else True


def set_settings():
    print(f'\nНастройка параметров поиска:')
    print(f'\t- искать по:')
    for i in range(len(SEARCH_SETTINGS)):
        if yes(f'\t\t- {SEARCH_SETTINGS[i][1]}'):
            SEARCH_SETTINGS[i][2] = 1
        else:
            SEARCH_SETTINGS[i][2] = 0
    print(f'Настройка завершена!')


def mark_words(s, words, color='\x1b[7m'):
    if not s:
        return ''
    colored = ''
    new_s = []
    for i in s.split():
        for j in words:
            if i.lower().startswith(j.lower()):
                colored = f'{color}{i[:len(j)]}\x1b[0m{i[len(j):]}'
        new_s.append(colored if colored else i)
        colored = ''
    return ' '.join(new_s)


def expand_release_year(words):
    years = []
    for word in words:
        pattern = r"^(\d{4})-(\d{4})$"
        match = re.match(pattern, word)
        if match:
            year_a, year_b = int(match.group(1)), int(match.group(2))
            if year_a < year_b:
                for i in range(year_b - year_a + 1):
                    years.append(str(year_a + i))
    return years


@DB1
def get_films(cnx, cursor, keywords):
    search_fields = f'CONCAT_WS(\' \', {", ".join([i[0] for i in SEARCH_SETTINGS if i[2] == 1])})'
    regexp_query = f'SELECT FID FROM ss_film_list WHERE {search_fields} REGEXP %s'
    matches = 'UNION ALL\n'.join([f'{regexp_query}\n' for _ in range(len(keywords))])
    query = f'''
        SELECT t2.mrank, t1.*
        FROM ss_film_list t1
            INNER JOIN (SELECT FID, COUNT(FID) AS mrank FROM (
                {matches}) matches
                GROUP BY FID) t2 ON t1.FID = t2.FID
        ORDER BY t2.mrank DESC
        '''
    cursor.execute(query, (*[f'\\b{word}\\w*\\b' for word in keywords],))
    return cursor.fetchall()


def process_search(keywords):
    rows = get_films(keywords)
    if rows:
        pag_step = 3
        last_page = len(rows) // pag_step if len(rows) % pag_step == 0 else len(rows) // pag_step + 1
        for i, row in enumerate(rows):
            print(f'\x1b[38;5;245m\n{i + 1}.\t{"id:":^12}\x1b[0m {row[1]}')
            print(f'\t\x1b[38;5;245m{"Название:":^12}\x1b[0m {mark_words(row[2], keywords)}')
            print(f'\t\x1b[38;5;245m{"Описание:":^12}\x1b[0m {mark_words(row[3], keywords)}')
            print(f'\t\x1b[38;5;245m{"Жанр:":^12}\x1b[0m {mark_words(row[4], keywords)}')
            print(f'\t\x1b[38;5;245m{"Год выпуска:":^12}\x1b[0m {mark_words(str(row[8]), keywords)}')
            print(f'\t\x1b[38;5;245m{"Актеры:":^12}\x1b[0m {mark_words(row[9], keywords)}')
            if (i + 1) % pag_step == 0 and (i + 1) < len(rows):
                print('---------------------------------------------------------------')
                print(f'Стр. {(i + 1) // pag_step} / {last_page}', end='')
                if not yes('\t\tЕщё'):
                    break
        else:
            print('---------------------------------------------------------------')
            print(f'Стр. {last_page} / {last_page}')
    else:
        print('\nФильмы, удовлетворяющие условиям поиска, не найдены. Попробуйте изменить запрос.')


@DB2
def get_phrase_id(cnx, cursor, keywords):
    phrase = ' '.join(keywords)
    cursor.execute('SELECT id FROM Phrases WHERE phrase = %s', (phrase,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute('INSERT INTO Phrases (phrase) VALUES (%s)', (phrase,))
        cnx.commit()
        return cursor.lastrowid


@DB2
def register_query(cnx, cursor, phrase_id):
    query_date = datetime.now()
    cursor.execute('INSERT INTO Queries (phrase_id, query_date) VALUES (%s, %s)', (phrase_id, query_date))
    cnx.commit()


@DB2
def get_word_id(cnx, cursor, word):
    cursor.execute('SELECT id FROM Words WHERE word = %s', (word,))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        cursor.execute('INSERT INTO Words (word) VALUES (%s)', (word,))
        cnx.commit()
        return cursor.lastrowid


@DB2
def link_phrase_word(cnx, cursor, phrase_id, word_id):
    cursor.execute('INSERT IGNORE INTO PhrasesWords (phrase_id, word_id) VALUES (%s, %s)', (phrase_id, word_id))
    cnx.commit()


def save_request(keywords):
    phrase_id = get_phrase_id(keywords)
    register_query(phrase_id)
    for word in keywords:
        word_id = get_word_id(word)
        link_phrase_word(phrase_id, word_id)


@DB2
def get_popular_words(cnx, cursor):
    query = '''
        SELECT t1.word, COUNT(t3.id) AS query_count
        FROM Words t1
            INNER JOIN PhrasesWords t2 ON t1.id = t2.word_id
            INNER JOIN Queries t3 ON t2.phrase_id = t3.phrase_id
        GROUP BY t1.word
        ORDER BY query_count DESC
        LIMIT 10;
        '''
    cursor.execute(query)
    return cursor.fetchall()


@DB2
def get_common_word_pairs(cnx, cursor):
    query = """
    SELECT t3.word AS word1, t4.word AS word2, COUNT(*) AS pair_count
    FROM PhrasesWords t1
        INNER JOIN PhrasesWords t2 ON t1.phrase_id = t2.phrase_id AND t1.word_id < t2.word_id
        INNER JOIN Words t3 ON t1.word_id = t3.id
        INNER JOIN Words t4 ON t2.word_id = t4.id
    GROUP BY t3.word, t4.word
    ORDER BY pair_count DESC
    LIMIT 10;
    """
    cursor.execute(query)
    return cursor.fetchall()


def get_statistics():
    print('\nТоп 10 самых популярных слов (диаграмма):\n')
    popular_words = get_popular_words()
    max_length = max(len(word) for word, _ in popular_words)
    max_count = max(count for _, count in popular_words)
    scale_factor = 30 / max_count
    for i, word in enumerate(popular_words):
        bar = '⣿' * int(word[1] * scale_factor)
        print(f'\x1b[38;5;{250 - i}m{word[0]:^{max_length}} | {bar} ({word[1]})\x1b[0m')
    if not yes(f'\nЕщё'):
        return
    print('\nТоп 10 самых популярных пар слов, которые чаще всего встречаются вместе в одной фразе:\n')
    common_word_pairs = get_common_word_pairs()
    for i, word_pair in enumerate(common_word_pairs):
        print(f'\x1b[38;5;{250 - i}m{word_pair[0]:^10} ♥ {word_pair[1]:^10} ({word_pair[2]})\x1b[0m')


while True:
    commands = f'1-Настройки; 2-Статистика; 3-Выход'
    print(f'\nВведите команду ({commands}) или ключевые слова для поиска:')
    s = input(f'> ').strip()

    if s.isdigit() and len(s) == 1:
        if s == '1':
            set_settings()
        elif s == '2':
            get_statistics()
        elif s == '3':
            break
        else:
            print('\nКоманда не распознана. Попробуйте ещё раз.')
    else:
        words = [word.strip() for word in s.split()]
        words = [word.lower() for word in words if len(word) > 2]
        years = expand_release_year(words)
        if words:
            process_search(words + years)
            save_request(words)
        else:
            print('\nВ запросе нет значимых слов. Попробуйте изменить запрос.')

DB1.close()
DB2.close()
