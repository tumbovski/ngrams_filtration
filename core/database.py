import psycopg2
import json
import os
from dotenv import load_dotenv
import bcrypt

# Загружаем переменные окружения из .env файла
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
def get_all_unique_lengths(conn):
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT len FROM ngrams ORDER BY len;")
            return [row[0] for row in cur.fetchall()]
    except Exception as e:
        print(f"Ошибка при получении длин: {e}")
        return []

def get_unique_values_for_rule(conn, position, rule_type, selected_lengths, all_blocks, block_id_to_exclude, rule_id_to_exclude, min_frequency=0.0, min_quantity=0):
    if not conn: return []
    db_column_name = COLUMN_MAPPING.get(rule_type, rule_type)
    preceding_where_clauses = build_where_clauses(all_blocks, block_id_to_exclude, rule_id_to_exclude)
    
    if selected_lengths:
        preceding_where_clauses.append(f"len IN ({', '.join(map(str, selected_lengths))})")
    
    preceding_where_str = " AND " + " AND ".join(preceding_where_clauses) if preceding_where_clauses else ""
    base_where = f"jsonb_array_length(ngrams.{db_column_name}) > {position}"

    query_template = "SELECT {field}, SUM(freq_mln), COUNT(id) FROM ngrams WHERE {base_where} {preceding_where_str} GROUP BY 1 HAVING SUM(freq_mln) >= {min_frequency:.3f} AND COUNT(id) >= {min_quantity:d} ORDER BY 2 DESC;"
    if db_column_name == 'morph':
        field = f"jsonb_array_elements_text(ngrams.morph->{position})"
    else:
        field = f"ngrams.{db_column_name}->>{position}"
    
    query = query_template.format(field=field, base_where=base_where, preceding_where_str=preceding_where_str, min_frequency=min_frequency, min_quantity=min_quantity)
    
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            return [(r[0], r[1], r[2]) for r in cur.fetchall() if r[0] is not None]
    except Exception as e:
        print(f"Ошибка при получении уникальных значений: {e}")
        return []

def get_frequent_sequences(conn, sequence_type, phrase_length, filter_blocks, selected_lengths, limit=100):
    if not conn: return []
    db_column_name = COLUMN_MAPPING.get(sequence_type, sequence_type)
    
    if not (1 <= phrase_length <= 10): # Ограничение на длину фразы для безопасности и производительности
        return []

    select_parts = []
    group_by_parts = []
    for i in range(phrase_length):
        select_parts.append(f"ngrams.{db_column_name}->>{i}")
        group_by_parts.append(f"ngrams.{db_column_name}->>{i}")
    
    select_clause = ", ".join(select_parts)
    group_by_clause = ", ".join(group_by_parts)

    where_clauses = build_where_clauses(filter_blocks)
    if selected_lengths:
        where_clauses.append(f"len IN ({', '.join(map(str, selected_lengths))})")

    # Добавляем условие на длину фразы для текущего запроса
    where_clauses.append(f"len = {phrase_length}")
    where_clauses.append(f"jsonb_array_length(ngrams.{db_column_name}) = {phrase_length}")

    full_where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    query = f"""
        SELECT
            {select_clause},
            SUM(freq_mln) as total_frequency,
            COUNT(*) as quantity
        FROM
            ngrams
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

def get_suggestion_data(conn, selected_lengths, filter_blocks, min_frequency=0.0, min_quantity=0):
    """
    Получает данные для панели подсказок: доступные варианты фильтрации
    для каждой позиции, отсортированные по частотности.
    """
    if not conn or not selected_lengths:
        return {}

    max_len = max(selected_lengths) if selected_lengths else 0
    if max_len == 0:
        return {}

    base_where_clauses = build_where_clauses(filter_blocks)
    if selected_lengths:
        base_where_clauses.append(f"len IN ({', '.join(map(str, selected_lengths))})")
    
    base_where_str = " AND " + " AND ".join(base_where_clauses) if base_where_clauses else ""

    suggestion_types = ['dep', 'pos', 'tag', 'morph']
    
    union_parts = []
    for i in range(max_len):
        for rule_type in suggestion_types:
            is_filtered = any(
                rule['type'] == rule_type and rule['values'] and rule.get('operator', 'include') == 'include'
                for block in filter_blocks if block['position'] == i 
                for rule in block['rules']
            )
            
            if not is_filtered:
                db_column = COLUMN_MAPPING[rule_type]
                if rule_type == 'morph':
                    # Запрос для структуры "массив массивов строк" (для morph)
                    union_parts.append(f"""
                        SELECT 
                            {i} as position, 
                            '{rule_type}' as type, 
                            jsonb_array_elements_text(ngrams.{db_column}->{i}) as value, 
                            SUM(freq_mln) as frequency,
                            COUNT(id) as quantity
                        FROM ngrams
                        WHERE 
                            jsonb_array_length(ngrams.{db_column}) > {i} AND
                            jsonb_typeof(ngrams.{db_column}->{i}) = 'array'
                            {base_where_str}
                        GROUP BY 1, 2, 3
                        HAVING SUM(freq_mln) >= {min_frequency:.3f} AND COUNT(id) >= {min_quantity:d}
                    """)
                else:
                    # Запрос для структуры "массив строк" (для dep, pos, tag)
                    union_parts.append(f"""
                        SELECT 
                            {i} as position, 
                            '{rule_type}' as type, 
                            ngrams.{db_column}->>{i} as value, 
                            SUM(freq_mln) as frequency,
                            COUNT(id) as quantity
                        FROM ngrams
                        WHERE 
                            jsonb_array_length(ngrams.{db_column}) > {i}
                            {base_where_str}
                        GROUP BY 1, 2, 3
                        HAVING SUM(freq_mln) >= {min_frequency:.3f} AND COUNT(id) >= {min_quantity:d}
                    """)

    if not union_parts:
        return {} 

    full_query = " UNION ALL ".join(union_parts)
    full_query += " ORDER BY position, frequency DESC"

    try:
        with conn.cursor() as cur:
            cur.execute(full_query)
            results = cur.fetchall()
            
            suggestion_data = {}
            for pos, r_type, r_val, r_freq, r_qty in results:
                if r_val is None or r_val == '': continue
                
                if pos not in suggestion_data:
                    suggestion_data[pos] = []
                
                suggestion_data[pos].append({
                    "type": r_type,
                    "value": r_val,
                    "freq": r_freq,
                    "qty": r_qty
                })

            for pos in suggestion_data:
                unique_items = {item['value']: item for item in suggestion_data[pos]}.values()
                suggestion_data[pos] = sorted(list(unique_items), key=lambda x: x['freq'], reverse=True)

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

# --- Построение SQL ---
def build_where_clauses(blocks, block_id_to_skip=None, rule_id_to_skip=None):
    where_clauses = []
    for block in blocks:
        if block['id'] == block_id_to_skip and rule_id_to_skip is None: continue
        position = block['position']
        block_rules = []
        for rule in block['rules']:
            if block['id'] == block_id_to_skip and rule['id'] == rule_id_to_skip: continue
            if not rule['values']: continue
            db_col_type = COLUMN_MAPPING[rule['type']]
            values = rule['values']
            operator = rule.get('operator', 'include') # По умолчанию 'include'
            if db_col_type == 'morph':
                conditions = ' OR '.join([f"ngrams.morph->{position} @> '{json.dumps(v)}'::jsonb" for v in values])
                if operator == 'exclude':
                    block_rules.append(f"(jsonb_array_length(ngrams.morph) > {position} AND NOT ({conditions}))")
                else:
                    block_rules.append(f"(jsonb_array_length(ngrams.morph) > {position} AND ({conditions}))")
            else:
                vals_str = ", ".join([f"'{v}'" for v in values])
                if operator == 'exclude':
                    block_rules.append(f"(jsonb_array_length(ngrams.{db_col_type}) > {position} AND ngrams.{db_col_type}->>{position} NOT IN ({vals_str}))")
                else:
                    block_rules.append(f"(jsonb_array_length(ngrams.{db_col_type}) > {position} AND ngrams.{db_col_type}->>{position} IN ({vals_str}))")
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
