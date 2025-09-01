import psycopg2
import json
import os
from dotenv import load_dotenv
import uuid
import bcrypt

load_dotenv()

# --- Конфигурация и подключение к БД ---
DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")

COLUMN_MAPPING = {
    'dep': 'deps', 'pos': 'pos', 'tag': 'tags',
    'token': 'tokens', 'lemma': 'lemmas', 'morph': 'morph'
}

def get_db_connection():
    """Создает и возвращает новое подключение к базе данных."""
    try:
        port_int = int(DB_PORT) if DB_PORT else 5432 # Добавим дефолтное значение на всякий случай
        conn = psycopg2.connect(host=DB_HOST, database=DB_NAME, user=DB_USER, password=DB_PASSWORD, port=port_int)
        return conn
    except psycopg2.OperationalError as e:
        print(f"Ошибка подключения к БД: {e}")
        return None

# --- Функции для работы с пользователями ---
def add_user(conn, login, nickname, password, role, status):
    if not conn: return False
    try:
        hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO users (login, nickname, password_hash, role, status) VALUES (%s, %s, %s, %s, %s);",
                (login, nickname, hashed_password, role, status)
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при добавлении пользователя: {e}")
        conn.rollback()
        return False

def get_user_by_login(conn, login):
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, login, nickname, password_hash, role, status FROM users WHERE login = %s;", (login,))
            user_data = cur.fetchone()
            if user_data:
                return {
                    "id": user_data[0],
                    "login": user_data[1],
                    "nickname": user_data[2],
                    "password_hash": user_data[3],
                    "role": user_data[4],
                    "status": user_data[5]
                }
            return None
    except Exception as e:
        print(f"Ошибка при получении пользователя по логину: {e}")
        return None

def get_user_by_id(conn, user_id):
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, login, nickname, password_hash, role, status FROM users WHERE id = %s;", (user_id,))
            user_data = cur.fetchone()
            if user_data:
                return {
                    "id": user_data[0],
                    "login": user_data[1],
                    "nickname": user_data[2],
                    "password_hash": user_data[3],
                    "role": user_data[4],
                    "status": user_data[5]
                }
            return None
    except Exception as e:
        print(f"Ошибка при получении пользователя по ID: {e}")
        return None

def authenticate_user(conn, login, password):
    user = get_user_by_login(conn, login)
    if user:
        if bcrypt.checkpw(password.encode('utf-8'), user["password_hash"].encode('utf-8')):
            return user
    return None

def get_all_moderators(conn):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, login, nickname, role, status FROM users WHERE role = 'moderator' ORDER BY nickname;")
            return [{"id": row[0], "login": row[1], "nickname": row[2], "role": row[3], "status": row[4]} for row in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении списка модераторов: {e}")
        return []

def update_user_status(conn, user_id, status):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE users SET status = %s WHERE id = %s;", (status, user_id))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при обновлении статуса пользователя: {e}")
        conn.rollback()
        return False

def update_user_details(conn, user_id, nickname, password=None, role=None):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            query_parts = []
            params = []
            if nickname is not None:
                query_parts.append("nickname = %s")
                params.append(nickname)
            if password is not None:
                hashed_password = bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
                query_parts.append("password_hash = %s")
                params.append(hashed_password)
            if role is not None:
                query_parts.append("role = %s")
                params.append(role)
            
            if not query_parts:
                return False # No fields to update

            query = f"UPDATE users SET {', '.join(query_parts)} WHERE id = %s;"
            params.append(user_id)
            
            cur.execute(query, tuple(params))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при обновлении данных пользователя: {e}")
        conn.rollback()
        return False

# --- Функции для работы с данными ---
def get_relaxed_signature(full_signature, length):
    """
    Извлекает части POS (часть речи) и TAG (тег) из полной сигнатуры паттерна.
    """
    if not full_signature or not isinstance(full_signature, str):
        return ""
    parts = full_signature.split('_')
    if len(parts) < 3 * length:
        parts.extend([''] * (3 * length - len(parts)))
    pos = parts[length:2*length]
    tags = parts[2*length:3*length]
    return '_'.join(pos + tags)

def get_pattern_by_id(pattern_id):
    """
    Получает полную информацию о паттерне по его ID.
    Создает собственное подключение, чтобы быть кэшируемой функцией.
    """
    conn = get_db_connection()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, pattern_text, phrase_length, total_frequency, total_quantity FROM unique_patterns WHERE id = %s", (pattern_id,))
            p = cur.fetchone()
            if p:
                relaxed_sig = get_relaxed_signature(p[1], p[2])
                return {"id": p[0], "text": p[1], "len": p[2], "freq": p[3], "qty": p[4], "relaxed_sig": relaxed_sig}
            return None
    except Exception as e:
        print(f"Ошибка при получении паттерна по ID: {e}")
        return None
    finally:
        if conn: conn.close()

# --- Функции для модерации ---
def get_next_unmoderated_pattern(conn, user_id, phrase_length, min_total_frequency=0, min_total_quantity=0, pattern_id_to_exclude=None):
    """
    Получает следующий немодерированный паттерн для указанного пользователя,
    соответствующий заданным критериям.
    """
    if not conn: return None
    try:
        with conn.cursor() as cur:
            query = """
                SELECT up.id, up.pattern_text, up.phrase_length, up.total_frequency, up.total_quantity
                FROM unique_patterns up
                LEFT JOIN moderation_patterns mp ON up.id = mp.pattern_id AND mp.user_id = %(user_id)s
                WHERE mp.id IS NULL
                  AND up.phrase_length = %(phrase_length)s
                  AND up.total_frequency >= %(min_freq)s
                  AND up.total_quantity >= %(min_qty)s
            """
            params = {
                'user_id': user_id,
                'phrase_length': phrase_length,
                'min_freq': min_total_frequency,
                'min_qty': min_total_quantity
            }
            
            if pattern_id_to_exclude:
                query += " AND up.id != %(exclude_id)s"
                params['exclude_id'] = pattern_id_to_exclude

            query += " ORDER BY up.total_frequency DESC, up.id LIMIT 1;"
            
            cur.execute(query, params)
            pattern = cur.fetchone()
            if pattern:
                return {
                    "id": pattern[0], "pattern_text": pattern[1], "phrase_length": pattern[2],
                    "total_frequency": pattern[3], "total_quantity": pattern[4]
                }
            return None
    except Exception as e:
        print(f"Ошибка при получении следующего паттерна для модерации: {e}")
        return None

def count_unmoderated_patterns(conn, user_id, phrase_length, min_total_frequency=0, min_total_quantity=0):
    """
    Считает количество немодерированных паттернов, соответствующих критериям.
    """
    if not conn: return 0
    try:
        with conn.cursor() as cur:
            query = """
                SELECT COUNT(up.id)
                FROM unique_patterns up
                LEFT JOIN moderation_patterns mp ON up.id = mp.pattern_id AND mp.user_id = %(user_id)s
                WHERE mp.id IS NULL
                  AND up.phrase_length = %(phrase_length)s
                  AND up.total_frequency >= %(min_freq)s
                  AND up.total_quantity >= %(min_qty)s
            """
            params = {
                'user_id': user_id,
                'phrase_length': phrase_length,
                'min_freq': min_total_frequency,
                'min_qty': min_total_quantity
            }
            cur.execute(query, params)
            count = cur.fetchone()
            return count[0] if count else 0
    except Exception as e:
        print(f"Ошибка при подсчете немодерированных паттернов: {e}")
        return 0

def get_examples_by_pattern_id(conn, pattern_id):
    """
    Получает примеры фраз для заданного ID паттерна из таблицы pattern_examples.
    """
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT example_text, example_frequency FROM pattern_examples WHERE pattern_id = %s ORDER BY example_frequency DESC LIMIT 50;", (pattern_id,))
            return cur.fetchall()
    except Exception as e:
        print(f"Ошибка при получении примеров для паттерна {pattern_id}: {e}")
        return []

def save_moderation_record(conn, pattern_id, user_id, rating, comment, tag):
    """
    Сохраняет запись о модерации в базу данных.
    """
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute(
                "INSERT INTO moderation_patterns (pattern_id, user_id, rating, comment, tag) VALUES (%s, %s, %s, %s, %s);",
                (pattern_id, user_id, rating, comment, tag)
            )
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при сохранении записи модерации: {e}")
        conn.rollback()
        return False

def process_moderation_submission(conn, pattern_id):
    """
    Пересчитывает и обновляет агрегированные данные модерации для паттерна
    в таблице unique_patterns.
    """
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(id), AVG(rating), STDDEV_SAMP(rating)
                FROM moderation_patterns WHERE pattern_id = %s;
            """, (pattern_id,))
            stats = cur.fetchone()
            moderation_count, avg_rating, stddev_rating = stats[0], stats[1], stats[2]
            cur.execute("""
                UPDATE unique_patterns SET moderation_count = %s, avg_rating = %s, stddev_rating = %s
                WHERE id = %s;
            """, (moderation_count, avg_rating, stddev_rating, pattern_id))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при обработке результатов модерации для паттерна {pattern_id}: {e}")
        conn.rollback()
        return False

def get_moderation_history(conn, user_id):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT mp.id, mp.pattern_id, up.pattern_text, mp.rating, mp.comment, mp.tag, mp.submitted_at
                FROM moderation_patterns mp JOIN unique_patterns up ON mp.pattern_id = up.id
                WHERE mp.user_id = %s ORDER BY mp.submitted_at DESC;
            """, (user_id,))
            return [{"id": r[0], "pattern_id": r[1], "pattern_text": r[2], "rating": r[3], "comment": r[4], "tag": r[5], "submitted_at": r[6]} for r in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении истории модераций: {e}")
        return []

def get_moderated_patterns_ordered_by_rating(conn, min_rating=1, max_rating=5, limit=100):
    """
    Получает отмодерированные паттерны, отсортированные по убыванию среднего рейтинга.
    """
    if not conn: return []
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    up.id, 
                    up.pattern_text, 
                    up.phrase_length, 
                    up.total_frequency, 
                    up.total_quantity,
                    up.avg_rating,
                    up.moderation_count
                FROM unique_patterns up
                WHERE up.avg_rating IS NOT NULL 
                  AND up.avg_rating >= %s 
                  AND up.avg_rating <= %s
                ORDER BY up.avg_rating DESC, up.total_frequency DESC
                LIMIT %s;
            """
            cur.execute(query, (min_rating, max_rating, limit))
            patterns = []
            for row in cur.fetchall():
                patterns.append({
                    "id": row[0],
                    "pattern_text": row[1],
                    "phrase_length": row[2],
                    "total_frequency": row[3],
                    "total_quantity": row[4],
                    "avg_rating": row[5],
                    "moderation_count": row[6]
                })
            return patterns
    except Exception as e:
        print(f"Ошибка при получении отмодерированных паттернов: {e}")
        return []

def update_moderation_entry(conn, entry_id, new_rating, new_comment, new_tag):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE moderation_patterns SET rating = %s, comment = %s, tag = %s, submitted_at = NOW() WHERE id = %s;", (new_rating, new_comment, new_tag, entry_id))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при обновлении записи модерации: {e}")
        conn.rollback()
        return False

def delete_moderation_record(conn, entry_id):
    if not conn: return False, None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pattern_id FROM moderation_patterns WHERE id = %s;", (entry_id,))
            res = cur.fetchone()
            if not res: return False, None
            pattern_id = res[0]
            cur.execute("DELETE FROM moderation_patterns WHERE id = %s;", (entry_id,))
            conn.commit()
            return True, pattern_id
    except Exception as e:
        print(f"Ошибка при удалении записи модерации: {e}")
        conn.rollback()
        return False, None

def create_temp_table_for_session(conn, selected_lengths):
    """
    Создает временную таблицу для сессии, содержащую n-граммы только выбранных длин.
    Это значительно ускоряет последующие запросы на фильтрацию и получение подсказок.
    """
    if not conn or not selected_lengths:
        return None
    
    # Генерируем уникальное имя для временной таблицы
    table_name = f"temp_ngrams_{str(uuid.uuid4()).replace('-', '_')}"

    try:
        with conn.cursor() as cur:
            # ON COMMIT PRESERVE ROWS важно, так как Streamlit может выполнять коммиты между rerun'ами
            cur.execute(f"""
                CREATE TEMP TABLE IF NOT EXISTS {table_name} (
                    LIKE ngrams INCLUDING ALL
                ) ON COMMIT PRESERVE ROWS;
            """)

            lengths_tuple = tuple(selected_lengths)
            # Вставляем данные во временную таблицу
            cur.execute(f"INSERT INTO {table_name} SELECT * FROM ngrams WHERE len IN %s;", (lengths_tuple,))

            # Создаем индексы для ускорения
            # B-Tree index for frequency is useful for sorting and range queries.
            cur.execute(f"CREATE INDEX ON {table_name} (freq_mln);")
            # GIN indexes are crucial for accelerating containment queries (@>) on JSONB arrays.
            cur.execute(f"CREATE INDEX ON {table_name} USING gin (deps);")
            cur.execute(f"CREATE INDEX ON {table_name} USING gin (pos);")
            cur.execute(f"CREATE INDEX ON {table_name} USING gin (tags);")
            cur.execute(f"CREATE INDEX ON {table_name} USING gin (tokens);")
            cur.execute(f"CREATE INDEX ON {table_name} USING gin (lemmas);")
            cur.execute(f"CREATE INDEX ON {table_name} USING gin (morph);")
            conn.commit()
            return table_name
    except Exception as e:
        print(f"Ошибка при создании временной таблицы: {e}")
        conn.rollback()
        return None

def get_all_unique_lengths(conn):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT len FROM ngrams ORDER BY len;")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении длин: {e}")
        return []

def get_unique_values_for_rule(conn, position, rule_type, selected_lengths, all_blocks, block_id_to_exclude, rule_id_to_exclude, min_frequency, min_quantity, table_name="ngrams"):
    if not conn: return []
    db_column_name = COLUMN_MAPPING.get(rule_type, rule_type)
    preceding_where_clauses = build_where_clauses(all_blocks, block_id_to_exclude, rule_id_to_exclude, table_name=table_name)
    
    if selected_lengths:
        preceding_where_clauses.append(f"{table_name}.len IN ({', '.join(map(str, selected_lengths))})")
    
    if min_frequency > 0:
        preceding_where_clauses.append(f"{table_name}.freq_mln >= {float(min_frequency)}")

    preceding_where_str = " AND " + " AND ".join(preceding_where_clauses) if preceding_where_clauses else ""
    base_where = f"jsonb_array_length({table_name}.{db_column_name}) > {position}"

    # Применяем min_quantity через HAVING для корректной фильтрации
    having_clause = f"HAVING COUNT(id) >= {int(min_quantity)}" if min_quantity > 0 else ""

    query_template = "SELECT {field}, SUM(freq_mln), COUNT(id) FROM {table_name} WHERE {base_where} {preceding_where_str} GROUP BY 1 {having_clause} ORDER BY 2 DESC;"
    if db_column_name == 'morph':
        field = f"jsonb_array_elements_text({table_name}.morph->{position})"
    else:
        field = f"{table_name}.{db_column_name}->>{position}"
    
    query = query_template.format(field=field, table_name=table_name, base_where=base_where, preceding_where_str=preceding_where_str, having_clause=having_clause)
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return [(r[0], r[1], r[2]) for r in cur.fetchall() if r[0] is not None]
    except Exception as e:
        print(f"Ошибка при получении уникальных значений: {e}")
        return []

def get_frequent_sequences(conn, sequence_type, phrase_length, filter_blocks, selected_lengths, table_name="ngrams", limit=100):
    if not conn: return []
    db_column_name = COLUMN_MAPPING.get(sequence_type, sequence_type)
    
    if not (1 <= phrase_length <= 10): # Ограничение на длину фразы для безопасности и производительности
        return []

    select_parts = []
    group_by_parts = []
    for i in range(phrase_length):
        select_parts.append(f"{table_name}.{db_column_name}->>{i}")
        group_by_parts.append(f"{table_name}.{db_column_name}->>{i}")
    
    select_clause = ", ".join(select_parts)
    group_by_clause = ", ".join(group_by_parts)

    where_clauses = build_where_clauses(filter_blocks, table_name=table_name)
    if selected_lengths:
        where_clauses.append(f"{table_name}.len IN ({', '.join(map(str, selected_lengths))})")

    # Добавляем условие на длину фразы для текущего запроса
    where_clauses.append(f"len = {phrase_length}")
    where_clauses.append(f"jsonb_array_length({table_name}.{db_column_name}) = {phrase_length}")

    full_where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
        SELECT
            {select_clause},
            SUM(freq_mln) as total_frequency,
            COUNT(id) as total_quantity
        FROM
            {table_name}
        WHERE
            {full_where_clause}
        GROUP BY
            {group_by_clause}
        ORDER BY
            total_frequency DESC
        LIMIT {limit};
    """
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            # Возвращаем список кортежей: (val1, val2, ..., valN, total_frequency)
            return cur.fetchall()
    except Exception as e:
        print(f"Ошибка при получении частых последовательностей {sequence_type} для длины {phrase_length}: {e}")
        return []

def get_suggestion_data(conn, selected_lengths, filter_blocks, min_frequency, min_quantity, table_name="ngrams"):
    """
    Получает данные для панели подсказок.
    Эта версия оптимизирована и использует один сложный SQL-запрос вместо множества UNION ALL,
    что значительно повышает производительность.
    """
    if not conn or not selected_lengths:
        return {}

    max_len = max(selected_lengths)

    where_clauses = build_where_clauses(filter_blocks, table_name=table_name)
    if table_name == "ngrams":
        where_clauses.append(f"len IN ({', '.join(map(str, selected_lengths))})")
    if min_frequency > 0:
        where_clauses.append(f"freq_mln >= {float(min_frequency)}")

    base_where_str = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
    WITH filtered_ngrams AS (
        SELECT * FROM {table_name} WHERE {base_where_str}
    ),
    unpacked_values AS (
        SELECT i.pos AS position, 'dep' AS type, fn.deps->>i.pos AS value, fn.freq_mln
        FROM filtered_ngrams fn, LATERAL generate_series(0, jsonb_array_length(fn.deps) - 1) AS i(pos)
        UNION ALL
        SELECT i.pos AS position, 'pos' AS type, fn.pos->>i.pos AS value, fn.freq_mln
        FROM filtered_ngrams fn, LATERAL generate_series(0, jsonb_array_length(fn.pos) - 1) AS i(pos)
        UNION ALL
        SELECT i.pos AS position, 'tag' AS type, fn.tags->>i.pos AS value, fn.freq_mln
        FROM filtered_ngrams fn, LATERAL generate_series(0, jsonb_array_length(fn.tags) - 1) AS i(pos)
        UNION ALL
        SELECT i.pos AS position, 'morph' AS type, m.value, fn.freq_mln
        FROM filtered_ngrams fn,
                LATERAL generate_series(0, jsonb_array_length(fn.morph) - 1) AS i(pos),
                LATERAL jsonb_array_elements_text(fn.morph->i.pos) AS m(value)
    )
    SELECT
        uv.position, uv.type, uv.value, SUM(uv.freq_mln) AS total_freq, COUNT(*) AS total_qty
    FROM unpacked_values uv
    WHERE uv.value IS NOT NULL AND uv.value != '' AND uv.position < {max_len}
    GROUP BY uv.position, uv.type, uv.value
    HAVING COUNT(*) >= {int(min_quantity)}
    ORDER BY uv.position, total_freq DESC;
    """

    try:
        with conn.cursor() as cur:
            cur.execute(query)
            results = cur.fetchall()
            
            suggestion_data = {}
            active_filters = {(b['position'], r['type']) for b in filter_blocks for r in b['rules'] if r.get('values')}

            for pos, r_type, r_val, r_freq, r_qty in results:
                if (pos, r_type) in active_filters:
                    continue
                if pos not in suggestion_data:
                    suggestion_data[pos] = []
                suggestion_data[pos].append({"type": r_type, "value": r_val, "freq": r_freq, "qty": r_qty})

            return suggestion_data
    except Exception as e:
        print(f"Ошибка при получении данных для подсказок: {e}")
        conn.rollback()
        return {}

# --- Функции для сохранения/загрузки НАБОРОВ ---
def save_filter_set(conn, name, data):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO saved_filters (name, filters_json) VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET filters_json = EXCLUDED.filters_json;", (name, json.dumps(data)))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка сохранения набора: {e}")
        conn.rollback()
        return False

def load_filter_set_names(conn):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM saved_filters ORDER BY name")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка загрузки имен наборов: {e}")
        return []

def load_filter_set_by_name(conn, name):
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT filters_json FROM saved_filters WHERE name = %s", (name,))
            res = cur.fetchone()
            return res[0] if res else None
    except Exception as e:
        print(f"Ошибка загрузки набора: {e}")
        return None

def delete_filter_set_by_name(conn, name):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM saved_filters WHERE name = %s", (name,))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка удаления набора: {e}")
        conn.rollback()
        return False

# --- Функции для сохранения/загрузки БЛОКОВ ---
def save_block(conn, name, data):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO saved_blocks (name, block_json) VALUES (%s, %s) ON CONFLICT (name) DO UPDATE SET block_json = EXCLUDED.block_json;", (name, json.dumps(data)))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка сохранения блока: {e}")
        conn.rollback()
        return False

def load_block_names(conn):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM saved_blocks ORDER BY name")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка загрузки имен блоков: {e}")
        return []

def load_block_by_name(conn, name):
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT block_json FROM saved_blocks WHERE name = %s", (name,))
            res = cur.fetchone()
            return res[0] if res else None
    except Exception as e:
        print(f"Ошибка загрузки блока: {e}")
        return None

def delete_block_by_name(conn, name):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM saved_blocks WHERE name = %s", (name,))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка удаления блока: {e}")
        conn.rollback()
        return False

# --- Функции для слияния паттернов ---

def find_next_merge_candidate_group(conn, already_seen_ids=None):
    """
    Находит ОДНУ группу кандидатов для слияния, используя оптимизированный SQL.
    Пропускает паттерны, которые уже были объединены или пропущены в этой сессии.
    """
    if not conn: return None
    if already_seen_ids is None: already_seen_ids = []

    try:
        with conn.cursor() as cur:
            cur.execute("SELECT max(phrase_length) FROM unique_patterns;")
            max_len = cur.fetchone()[0]
            if not max_len: return None

            # Иерархия поиска: tag -> pos -> dep
            for diff_name in ['tag', 'pos', 'dep']:
                for length in range(1, max_len + 1):
                    for i in range(length):
                        if diff_name == 'dep': part_index = i + 1
                        elif diff_name == 'pos': part_index = length + i + 1
                        else: part_index = length * 2 + i + 1

                        all_parts = list(range(1, length * 3 + 1))
                        if part_index in all_parts: all_parts.remove(part_index)
                        if not all_parts: continue

                        signature_parts = [f"split_part(pattern_text, '_', {j})" for j in all_parts]
                        signature_sql = " || '_' || ".join(signature_parts)
                        
                        # ID паттернов, которые нужно исключить из поиска
                        exclude_ids_sql = "AND id NOT IN %s" if already_seen_ids else ""
                        params = (tuple(already_seen_ids),) if already_seen_ids else ()

                        query = f"""
                            WITH candidate_groups AS (
                                SELECT 
                                    {signature_sql} as signature,
                                    array_agg(id) as pattern_ids,
                                    SUM(total_frequency) as group_frequency
                                FROM public.unique_patterns
                                WHERE phrase_length = {length} {exclude_ids_sql}
                                GROUP BY {signature_sql}
                                HAVING count(id) > 1
                            )
                            SELECT pattern_ids
                            FROM candidate_groups
                            ORDER BY group_frequency DESC
                            LIMIT 1;
                        """
                        
                        cur.execute(query, params)
                        result = cur.fetchone()

                        if result and result[0]:
                            return {
                                "pattern_ids": result[0],
                                "difference_type": diff_name,
                                "difference_position": i + 1
                            }
            return None # Ничего не найдено
    except Exception as e:
        print(f"Ошибка при поиске кандидатов на слияние: {e}")
        conn.rollback()
        return None

def get_patterns_data_by_ids(conn, pattern_ids):
    """Получает подробные данные для списка ID паттернов."""
    if not conn or not pattern_ids: return []
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    up.id, up.pattern_text, up.phrase_length, up.total_frequency, up.total_quantity,
                    (
                        SELECT jsonb_agg(jsonb_build_object('text', pe.example_text, 'freq', pe.example_frequency) ORDER BY pe.example_frequency DESC)
                        FROM (
                            SELECT example_text, example_frequency
                            FROM pattern_examples
                            WHERE pattern_id = up.id
                            ORDER BY example_frequency DESC
                            LIMIT 50
                        ) pe
                    ) as examples
                FROM unique_patterns up
                WHERE up.id = ANY(%s)
                ORDER BY up.total_frequency DESC;
            """
            cur.execute(query, (pattern_ids,))
            results = []
            for row in cur.fetchall():
                results.append({
                    "id": row[0],
                    "text": row[1],
                    "len": row[2],
                    "freq": row[3],
                    "qty": row[4],
                    "examples": row[5] or []
                })
            return results
    except Exception as e:
        print(f"Ошибка при получении данных паттернов: {e}")
        return []

def execute_pattern_merge(conn, source_pattern_ids, target_pattern_id):
    """
    Выполняет деструктивное слияние паттернов.
    Эта функция предназначена для выполнения ОДНОЙ операции слияния.
    Предполагается, что она будет вызвана внутри другой функции, управляющей транзакцией.
    """
    if not conn or not source_pattern_ids or not target_pattern_id:
        return False, "Неверные входные данные."

    with conn.cursor() as cur:
        # Шаг 1: Получить данные целевого паттерна
        cur.execute("SELECT deps, pos, tags, lemmas, tokens, morph FROM ngrams WHERE pattern_id = %s LIMIT 1;", (target_pattern_id,))
        target_data = cur.fetchone()
        if not target_data:
            raise Exception(f"Не удалось найти данные для целевого паттерна ID {target_pattern_id}.")
        
        target_deps, target_pos, target_tags, target_lemmas, target_tokens, target_morph = target_data

        # Шаг 2: Обновить n-граммы
        update_query = """
            UPDATE ngrams SET
                pattern_id = %s, deps = %s, pos = %s, tags = %s,
                lemmas = %s, tokens = %s, morph = %s
            WHERE pattern_id = ANY(%s);
        """
        cur.execute(update_query, (
            target_pattern_id, 
            json.dumps(target_deps), json.dumps(target_pos), json.dumps(target_tags),
            json.dumps(target_lemmas), json.dumps(target_tokens), json.dumps(target_morph), 
            source_pattern_ids
        ))
        
        # Шаг 3: Удалить исходные паттерны (каскадное удаление позаботится об остальном)
        cur.execute("DELETE FROM unique_patterns WHERE id = ANY(%s);", (source_pattern_ids,))

    return True, f"Слиты {source_pattern_ids} в {target_pattern_id}."

# --- Функции для слияния паттернов ---

def mark_patterns_as_merged(conn, pattern_ids):
    """Устанавливает флаг merged = TRUE для списка ID паттернов."""
    if not conn or not pattern_ids:
        return False
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE unique_patterns SET merged = TRUE WHERE id = ANY(%s);", (pattern_ids,))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при установке флага merged: {e}")
        conn.rollback()
        return False

def mark_patterns_as_skipped(conn, pattern_ids, diff_level, diff_types):
    """
    Помечает группу паттернов как пропущенную (отмодерированную).
    Устанавливает флаги для соответствующих типов различий (dep, pos, tag)
    и уровень различий, на котором было принято решение.
    """
    if not conn or not pattern_ids or not diff_types:
        return False
    try:
        with conn.cursor() as cur:
            update_parts = [f"moderated_{t} = TRUE" for t in diff_types]
            update_parts.append(f"moderation_diff_level = {diff_level}")
            
            query = f"""
                UPDATE unique_patterns 
                SET {', '.join(update_parts)} 
                WHERE id = ANY(%s);
            """
            cur.execute(query, (pattern_ids,))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при пометке паттернов как пропущенных: {e}")
        conn.rollback()
        return False

def find_next_merge_candidate_group(conn, length):
    """
    Находит следующую группу кандидатов для слияния для ЗАДАННОЙ ДЛИНЫ.
    Иерархия поиска:
    1. Сначала ищет группы с 1 отличием (tag -> pos -> dep).
    2. Если не найдено, ищет группы с 2 отличиями.
    3. Если не найдено, ищет группы с 3 отличиями.
    Учитывает новые флаги модерации (moderated_dep, moderated_pos, moderated_tag).
    """
    if not conn: return None

    # --- Вспомогательная функция для выполнения поиска ---
    def find_group_with_n_diffs(n_diffs):
        import itertools
        
        # Генерируем все возможные комбинации позиций для заданного числа различий
        all_possible_indices = range(1, length * 3 + 1)
        index_combinations = itertools.combinations(all_possible_indices, n_diffs)

        for combo in index_combinations:
            diff_indices = list(combo)
            
            # Определяем типы (dep, pos, tag) для текущих позиций с различиями
            diff_types = set()
            for index in diff_indices:
                if 1 <= index <= length:
                    diff_types.add('dep')
                elif length < index <= length * 2:
                    diff_types.add('pos')
                else:
                    diff_types.add('tag')
            
            # Формируем WHERE clause для проверки, не были ли паттерны уже отмодерированы по этим типам
            where_moderated = " AND ".join([f"moderated_{t} = FALSE" for t in diff_types])

            # Формируем сигнатуру для группировки (все части, КРОМЕ различающихся)
            signature_indices = [i for i in all_possible_indices if i not in diff_indices]
            if not signature_indices: continue # Не может быть 0 общих частей

            signature_sql = " || '_' || ".join([f"split_part(pattern_text, '_', {j})" for j in signature_indices])

            query = f"""
                WITH candidate_groups AS (
                    SELECT 
                        {signature_sql} as signature,
                        array_agg(id) as pattern_ids,
                        SUM(total_frequency) as group_frequency
                    FROM public.unique_patterns
                    WHERE phrase_length = {length} AND ({where_moderated})
                    GROUP BY signature
                    HAVING count(id) > 1
                )
                SELECT pattern_ids
                FROM candidate_groups
                ORDER BY group_frequency DESC
                LIMIT 1;
            """
            
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    result = cur.fetchone()
                    if result and result[0]:
                        return {
                            "pattern_ids": result[0],
                            "difference_level": n_diffs,
                            "difference_types": list(diff_types)
                        }
            except Exception as e:
                print(f"Ошибка при поиске кандидатов с {n_diffs} различиями: {e}")
                conn.rollback()
                # Продолжаем поиск со следующей комбинацией
    
    # --- Основная логика поиска ---
    for n in range(1, 4): # Ищем сначала с 1, потом 2, потом 3 отличиями
        print(f"Поиск кандидатов с {n} отличиями для длины {length}...")
        found_group = find_group_with_n_diffs(n)
        if found_group:
            return found_group
            
    return None # Ничего не найдено

def get_available_lengths_for_merging(conn):
    """Получает список длин, для которых есть необработанные паттерны."""
    if not conn: return []
    try:
        with conn.cursor() as cur:
            # Паттерн считается необработанным, если хотя бы один из флагов модерации FALSE
            cur.execute("""
                SELECT DISTINCT phrase_length 
                FROM unique_patterns 
                WHERE moderated_dep = FALSE OR moderated_pos = FALSE OR moderated_tag = FALSE
                ORDER BY phrase_length;
            """)
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении доступных длин: {e}")
        return []

def execute_multiple_merges(conn, merges):
    """
    Выполняет список операций слияния в одной транзакции.
    Обновлено для работы с новой схемой модерации.
    """
    if not conn or not merges: 
        return False, "Нет данных для выполнения."

    all_target_ids = [op['target'] for op in merges]
    
    try:
        with conn.cursor() as cur:
            print("Начало транзакции для множественного слияния...")
            
            for merge_op in merges:
                source_ids = merge_op.get('sources')
                target_id = merge_op.get('target')
                print(f"  - Выполнение слияния: {source_ids} -> {target_id}")
                
                cur.execute("SELECT deps, pos, tags, lemmas, tokens, morph FROM ngrams WHERE pattern_id = %s LIMIT 1;", (target_id,))
                target_data = cur.fetchone()
                if not target_data:
                    raise Exception(f"Не найдены n-граммы для целевого паттерна ID {target_id}.")
                target_deps, target_pos, target_tags, target_lemmas, target_tokens, target_morph = target_data
                
                update_query = """
                    UPDATE ngrams SET
                        pattern_id = %s, deps = %s, pos = %s, tags = %s,
                        lemmas = %s, tokens = %s, morph = %s
                    WHERE pattern_id = ANY(%s);
                """
                cur.execute(update_query, (
                    target_id, 
                    json.dumps(target_deps), json.dumps(target_pos), json.dumps(target_tags),
                    json.dumps(target_lemmas), json.dumps(target_tokens), json.dumps(target_morph), 
                    source_ids
                ))

                cur.execute("DELETE FROM unique_patterns WHERE id = ANY(%s);", (source_ids,))

            print(f"  - Пересчет статистики для целевых паттернов: {all_target_ids}")
            cur.execute("""
                UPDATE unique_patterns up
                SET 
                    total_frequency = agg.total_freq,
                    total_quantity = agg.total_qty
                FROM (
                    SELECT pattern_id, SUM(freq_mln) as total_freq, COUNT(id) as total_qty
                    FROM ngrams
                    WHERE pattern_id = ANY(%s)
                    GROUP BY pattern_id
                ) as agg
                WHERE up.id = agg.pattern_id;
            """, (all_target_ids,))

            # После успешного слияния и пересчета, помечаем целевой паттерн как полностью отмодерированный
            print(f"  - Пометка целевых паттернов как полностью обработанных: {all_target_ids}")
            cur.execute("""
                UPDATE unique_patterns 
                SET moderated_dep = TRUE, moderated_pos = TRUE, moderated_tag = TRUE, moderation_diff_level = 0
                WHERE id = ANY(%s);
            """, (all_target_ids,))

            conn.commit()
            print("Транзакция успешно закоммичена.")
            return True, "Все слияния успешно выполнены."

    except Exception as e:
        conn.rollback()
        print(f"Критическая ошибка во время множественного слияния: {e}")
        return False, str(e)


def get_available_lengths_for_merging(conn):
    """Получает список длин, для которых есть необработанные паттерны."""
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT phrase_length FROM unique_patterns WHERE merged = FALSE ORDER BY phrase_length;")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении доступных длин: {e}")
        return []

def get_patterns_data_by_ids(conn, pattern_ids):
    """Получает подробные данные для списка ID паттернов."""
    if not conn or not pattern_ids: return []
    try:
        with conn.cursor() as cur:
            query = """
                SELECT 
                    up.id, up.pattern_text, up.phrase_length, up.total_frequency, up.total_quantity,
                    (
                        SELECT jsonb_agg(jsonb_build_object('text', pe.example_text, 'freq', pe.example_frequency) ORDER BY pe.example_frequency DESC)
                        FROM (
                            SELECT example_text, example_frequency
                            FROM pattern_examples
                            WHERE pattern_id = up.id
                            ORDER BY example_frequency DESC
                            LIMIT 50
                        ) pe
                    ) as examples
                FROM unique_patterns up
                WHERE up.id = ANY(%s)
                ORDER BY up.total_frequency DESC;
            """
            cur.execute(query, (pattern_ids,))
            results = []
            for row in cur.fetchall():
                results.append({
                    "id": row[0],
                    "text": row[1],
                    "len": row[2],
                    "freq": row[3],
                    "qty": row[4],
                    "examples": row[5] or []
                })
            return results
    except Exception as e:
        print(f"Ошибка при получении данных паттернов: {e}")
        return []

def execute_multiple_merges(conn, merges):
    """
    Выполняет список операций слияния в одной транзакции.
    merges: список словарей, каждый вида {'sources': [...], 'target': ...}
    """
    if not conn or not merges: 
        return False, "Нет данных для выполнения."

    all_target_ids = [op['target'] for op in merges]
    all_source_ids = [sid for op in merges for sid in op['sources']]

    try:
        with conn.cursor() as cur:
            # Явно начинаем транзакцию
            print("Начало транзакции для множественного слияния...")
            
            for merge_op in merges:
                source_ids = merge_op.get('sources')
                target_id = merge_op.get('target')
                print(f"  - Выполнение слияния: {source_ids} -> {target_id}")
                
                # Выполняем обновление n-грамм
                cur.execute("SELECT deps, pos, tags, lemmas, tokens, morph FROM ngrams WHERE pattern_id = %s LIMIT 1;", (target_id,))
                target_data = cur.fetchone()
                if not target_data:
                    raise Exception(f"Не найдены n-граммы для целевого паттерна ID {target_id}.")
                target_deps, target_pos, target_tags, target_lemmas, target_tokens, target_morph = target_data
                
                update_query = """
                    UPDATE ngrams SET
                        pattern_id = %s, deps = %s, pos = %s, tags = %s,
                        lemmas = %s, tokens = %s, morph = %s
                    WHERE pattern_id = ANY(%s);
                """
                cur.execute(update_query, (
                    target_id, 
                    json.dumps(target_deps), json.dumps(target_pos), json.dumps(target_tags),
                    json.dumps(target_lemmas), json.dumps(target_tokens), json.dumps(target_morph), 
                    source_ids
                ))

                # Удаляем исходные паттерны
                cur.execute("DELETE FROM unique_patterns WHERE id = ANY(%s);", (source_ids,))

            # Пересчитываем статистику для всех затронутых целевых паттернов
            print(f"  - Пересчет статистики для целевых паттернов: {all_target_ids}")
            cur.execute("""
                UPDATE unique_patterns up
                SET 
                    total_frequency = agg.total_freq,
                    total_quantity = agg.total_qty
                FROM (
                    SELECT pattern_id, SUM(freq_mln) as total_freq, COUNT(id) as total_qty
                    FROM ngrams
                    WHERE pattern_id = ANY(%s)
                    GROUP BY pattern_id
                ) as agg
                WHERE up.id = agg.pattern_id;
            """, (all_target_ids,))

            # Помечаем целевые паттерны как обработанные
            print(f"  - Пометка целевых паттернов как обработанных: {all_target_ids}")
            cur.execute("UPDATE unique_patterns SET merged = TRUE WHERE id = ANY(%s);", (all_target_ids,))

            conn.commit()
            print("Транзакция успешно закоммичена.")
            return True, "Все слияния успешно выполнены."

    except Exception as e:
        conn.rollback()
        print(f"Критическая ошибка во время множественного слияния: {e}")
        return False, str(e)


# --- Функции для управления темами паттернов ---
def create_theme(conn, name, description=None, parent_theme_id=None):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO pattern_themes (name, description, parent_theme_id) VALUES (%s, %s, %s) RETURNING id;", (name, description, parent_theme_id))
            theme_id = cur.fetchone()[0]
            conn.commit()
            return theme_id
    except Exception as e:
        print(f"Ошибка при создании темы: {e}")
        conn.rollback()
        return False

def get_all_themes(conn):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, description, parent_theme_id FROM pattern_themes ORDER BY name;")
            return [{"id": r[0], "name": r[1], "description": r[2], "parent_theme_id": r[3]} for r in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении всех тем: {e}")
        return []

def get_theme_by_id(conn, theme_id):
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, description, parent_theme_id FROM pattern_themes WHERE id = %s;", (theme_id,))
            r = cur.fetchone()
            if r:
                return {"id": r[0], "name": r[1], "description": r[2], "parent_theme_id": r[3]}
            return None
    except Exception as e:
        print(f"Ошибка при получении темы по ID: {e}")
        return None

def update_theme(conn, theme_id, name, description, parent_theme_id):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("UPDATE pattern_themes SET name = %s, description = %s, parent_theme_id = %s WHERE id = %s;", (name, description, parent_theme_id, theme_id))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при обновлении темы: {e}")
        conn.rollback()
        return False

def delete_theme(conn, theme_id):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pattern_themes WHERE id = %s;", (theme_id,))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при удалении темы: {e}")
        conn.rollback()
        return False

# --- Функции для связывания паттернов с темами ---
def associate_pattern_with_theme(conn, pattern_id, theme_id):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO pattern_theme_associations (pattern_id, theme_id) VALUES (%s, %s) ON CONFLICT (pattern_id, theme_id) DO NOTHING;", (pattern_id, theme_id))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при связывании паттерна с темой: {e}")
        conn.rollback()
        return False

def get_themes_for_pattern(conn, pattern_id):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pt.id, pt.name FROM pattern_themes pt JOIN pattern_theme_associations pta ON pt.id = pta.theme_id WHERE pta.pattern_id = %s ORDER BY pt.name;", (pattern_id,))
            return [{"id": r[0], "name": r[1]} for r in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении тем для паттерна: {e}")
        return []

def get_patterns_for_theme(conn, theme_id):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT up.id, up.pattern_text FROM unique_patterns up JOIN pattern_theme_associations pta ON up.id = pta.pattern_id WHERE pta.theme_id = %s ORDER BY up.pattern_text;", (theme_id,))
            return [{"id": r[0], "text": r[1]} for r in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении паттернов для темы: {e}")
        return []

def remove_pattern_from_theme(conn, pattern_id, theme_id):
    if not conn: return False
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pattern_theme_associations WHERE pattern_id = %s AND theme_id = %s;", (pattern_id, theme_id))
            conn.commit()
            return True
    except Exception as e:
        print(f"Ошибка при удалении паттерна из темы: {e}")
        conn.rollback()
        return False


# --- Построение SQL ---
def build_where_clauses(blocks, block_id_to_skip=None, rule_id_to_skip=None, table_name="ngrams"):
    where_clauses = []
    for block in blocks:
        if block['id'] == block_id_to_skip and rule_id_to_skip is None: continue
        position = block['position']
        block_rules = []
        for rule in block['rules']:
            if block['id'] == block_id_to_skip and rule['id'] == rule_id_to_skip: continue
            if not rule['values']: continue

            operator = rule.get('operator', 'include')
            db_col_type = COLUMN_MAPPING.get(rule['type'])
            values = rule['values']
            
            if not db_col_type: continue # Should not happen with valid UI

            length_check = f"jsonb_array_length({table_name}.{db_col_type}) > {position}"

            if db_col_type == 'morph':
                # For morph, which is an array of arrays.
                # Escape double quotes in values to prevent errors.
                safe_values = [str(v).replace('"', '\\"') for v in values]
                conditions = ' OR '.join([f"{table_name}.morph->{position} @> '[\"{v}\"]'::jsonb" for v in safe_values])
                rule_logic = f"({conditions})"
            else:
                # For simple arrays (dep, pos, tag, token, lemma).
                # Escape single quotes in values to prevent SQL errors.
                safe_values = [str(v).replace("'", "''") for v in values]
                positional_values = ", ".join([f"'{v}'" for v in safe_values])
                rule_logic = f"{table_name}.{db_col_type}->>{position} IN ({positional_values})"

            # Apply operator
            if operator == 'exclude':
                block_rules.append(f"({length_check} AND NOT ({rule_logic}))")
            else: # include
                block_rules.append(f"({length_check} AND {rule_logic})")

        if block_rules:
            where_clauses.append(f"({' AND '.join(block_rules)})")
    return where_clauses

def execute_query(conn, query):
    """Выполняет основной запрос на получение фраз."""
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return cur.fetchall()
    except Exception as e:
        print(f"Ошибка выполнения запроса: {e}")
        return []
