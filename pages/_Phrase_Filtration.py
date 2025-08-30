import streamlit as st
import json
import uuid
import pandas as pd
from core.database import (
    get_db_connection,
    get_all_unique_lengths,
    get_unique_values_for_rule,
    save_filter_set,
    load_filter_set_names,
    load_filter_set_by_name,
    delete_filter_set_by_name,
    save_block,
    load_block_names,
    load_block_by_name,
    delete_block_by_name,
    build_where_clauses,
    execute_query,
    get_frequent_sequences,
    get_suggestion_data,
    get_pattern_by_id, # This import will now work
    create_temp_table_for_session
)

# --- Управление состоянием ---
st.set_page_config(layout="wide", page_title="Phrase Filtration")

# Check authentication status
if 'logged_in' not in st.session_state or not st.session_state.logged_in:
    st.warning("Пожалуйста, войдите в систему, чтобы получить доступ к этой странице.")
    st.switch_page("Home.py")

if 'filter_blocks' not in st.session_state: st.session_state.filter_blocks = []
if 'selected_lengths' not in st.session_state: st.session_state.selected_lengths = []
if 'last_query' not in st.session_state: st.session_state.last_query = ""
if 'results' not in st.session_state: st.session_state.results = []

if 'current_filters_hash' not in st.session_state: st.session_state.current_filters_hash = None
if 'show_word_analysis' not in st.session_state: st.session_state.show_word_analysis = False
if 'min_frequency' not in st.session_state: st.session_state.min_frequency = 0.0
if 'min_quantity' not in st.session_state: st.session_state.min_quantity = 0
if 'temp_table_name' not in st.session_state: st.session_state.temp_table_name = None

# --- Подключение к БД и кэширование ---
@st.cache_resource
def init_connection():
    return get_db_connection()

conn = init_connection()

if not conn:
    st.error("Не удалось подключиться к базе данных. Проверьте настройки в .env файле и доступность сервера.")
    st.stop()

# --- Хелперы для кэширования ---
def make_hashable(obj):
    if isinstance(obj, dict):
        return frozenset((k, make_hashable(v)) for k, v in sorted(obj.items()))
    if isinstance(obj, list):
        return tuple(make_hashable(v) for v in obj)
    return obj

def make_mutable(obj):
    if isinstance(obj, frozenset):
        return {k: make_mutable(v) for k, v in obj}
    if isinstance(obj, tuple):
        return list(make_mutable(v) for v in obj)
    return obj

def format_number_with_spaces(number):
    if number == int(number):
        return f"{int(number):,}".replace(",", " ")
    else:
        return f"{number:,.2f}".replace(",", " ")

# --- Кэшируемые функции ---
@st.cache_data(ttl=3600)
def cached_get_all_unique_lengths():
    return get_all_unique_lengths(conn)

@st.cache_data(ttl=3600)
def cached_get_unique_values_for_rule(position, rule_type, selected_lengths_tuple, blocks_tuple, block_id_to_exclude, rule_id_to_exclude, min_frequency, min_quantity, table_name="ngrams"):
    all_blocks = make_mutable(blocks_tuple)
    selected_lengths = list(selected_lengths_tuple)
    return get_unique_values_for_rule(conn, position, rule_type, selected_lengths, all_blocks, block_id_to_exclude, rule_id_to_exclude, min_frequency, min_quantity, table_name)

@st.cache_data(ttl=3600)
def cached_load_filter_set_names():
    return load_filter_set_names(conn)

@st.cache_data(ttl=3600)
def cached_load_block_names():
    return load_block_names(conn)

@st.cache_data(ttl=3600)
def cached_get_frequent_sequences(sequence_type, phrase_length, filter_blocks_tuple, selected_lengths_tuple, table_name="ngrams"):
    mutable_filter_blocks = make_mutable(filter_blocks_tuple)
    mutable_selected_lengths = list(selected_lengths_tuple)
    return get_frequent_sequences(conn, sequence_type, phrase_length, mutable_filter_blocks, mutable_selected_lengths, table_name=table_name)

@st.cache_data(ttl=3600)
def cached_get_suggestion_data(selected_lengths_tuple, filter_blocks_tuple, min_frequency, min_quantity, table_name="ngrams"):
    selected_lengths = list(selected_lengths_tuple)
    filter_blocks = make_mutable(filter_blocks_tuple)
    return get_suggestion_data(conn, selected_lengths, filter_blocks, min_frequency, min_quantity, table_name)

@st.cache_data(ttl=3600)
def cached_get_pattern_by_id(pattern_id):
    return get_pattern_by_id(pattern_id)

# --- Функции-коллбэки и хендлеры ---
def clear_caches():
    cached_get_unique_values_for_rule.clear()
    cached_get_suggestion_data.clear()
    cached_get_frequent_sequences.clear()

def handle_length_change():
    st.session_state.selected_lengths = st.session_state.selected_lengths_widget
    max_len = max(st.session_state.selected_lengths) if st.session_state.selected_lengths else 0
    st.session_state.filter_blocks = [b for b in st.session_state.filter_blocks if b['position'] < max_len]
    
    # Clear caches and temp table when lengths change
    clear_caches()
    st.session_state.temp_table_name = None # Reset temp table

    if st.session_state.selected_lengths:
        with st.spinner("Создание временной таблицы для ускорения..."):
            table_name = create_temp_table_for_session(conn, st.session_state.selected_lengths)
            if table_name:
                st.session_state.temp_table_name = table_name
                st.toast("Временная таблица создана!", icon="✅")
            else:
                st.error("Не удалось создать временную таблицу.")

def add_block():
    new_block_id = str(uuid.uuid4())
    new_rule_id = str(uuid.uuid4())
    st.session_state.filter_blocks.append({'id': new_block_id, 'position': 0, 'rules': [{'id': new_rule_id, 'type': 'dep', 'values': [], 'operator': 'include'}]})

def remove_block(block_id):
    st.session_state.filter_blocks = [b for b in st.session_state.filter_blocks if b['id'] != block_id]
    clear_caches()

def add_rule(block_id):
    for block in st.session_state.filter_blocks:
        if block['id'] == block_id:
            block['rules'].append({'id': str(uuid.uuid4()), 'type': 'dep', 'values': [], 'operator': 'include'})
            break

def remove_rule(block_id, rule_id):
    for block in st.session_state.filter_blocks:
        if block['id'] == block_id:
            block['rules'] = [r for r in block['rules'] if r['id'] != rule_id]
            break
    clear_caches()

def handle_position_change(block_id):
    new_pos = st.session_state[f"pos_block_{block_id}"] - 1
    for block in st.session_state.filter_blocks:
        if block['id'] == block_id and block['position'] != new_pos:
            block['position'] = new_pos
            for rule in block['rules']:
                rule['values'] = []
            clear_caches()
            break

def handle_type_change(block_id, rule_id):
    new_type = st.session_state[f"type_{rule_id}"]
    for block in st.session_state.filter_blocks:
        if block['id'] == block_id:
            for rule in block['rules']:
                if rule['id'] == rule_id and rule['type'] != new_type:
                    rule['type'] = new_type
                    rule['values'] = []
                    rule['operator'] = rule.get('operator', 'include') 
                    clear_caches()
                    break
            break

def handle_values_change(block_id, rule_id, disp_opts):
    selected_disp = st.session_state[f"vals_{rule_id}"]
    new_values = [disp_opts[s] for s in selected_disp if s in disp_opts]
    for block in st.session_state.filter_blocks:
        if block['id'] == block_id:
            for rule in block['rules']:
                if rule['id'] == rule_id:
                    rule['values'] = new_values
                    clear_caches()
                    break
            break

def handle_operator_change(block_id, rule_id):
    new_operator = st.session_state[f"op_{rule_id}"]
    for block in st.session_state.filter_blocks:
        if block['id'] == block_id:
            for rule in block['rules']:
                if rule['id'] == rule_id and rule.get('operator', 'include') != new_operator:
                    rule['operator'] = new_operator
                    clear_caches()
                    break
            break

def replace_block(block_id, new_block_data):
    for i, block in enumerate(st.session_state.filter_blocks):
        if block['id'] == block_id:
            new_block_data['id'] = block_id
            st.session_state.filter_blocks[i] = new_block_data
            clear_caches()
            break

def toggle_filter_from_suggestion(position, rule_type, value):
    block_exists = False
    for block in st.session_state.filter_blocks:
        if block['position'] == position:
            block_exists = True
            rule_exists = False
            for rule in block['rules']:
                if rule['type'] == rule_type:
                    rule_exists = True
                    if value in rule['values']:
                        rule['values'].remove(value)
                    else:
                        rule['values'].append(value)
                    break
            if not rule_exists:
                block['rules'].append({'id': str(uuid.uuid4()), 'type': rule_type, 'values': [value]})
            break
    
    if not block_exists:
        st.session_state.filter_blocks.append({
            'id': str(uuid.uuid4()),
            'position': position,
            'rules': [{'id': str(uuid.uuid4()), 'type': rule_type, 'values': [value]}]
        })
    
    st.session_state.filter_blocks = [b for b in ({'id': b['id'], 'position': b['position'], 'rules': [r for r in b['rules'] if r['values']]} for b in st.session_state.filter_blocks) if b['rules']]
    st.session_state.filter_blocks.sort(key=lambda b: b['position'])
    clear_caches()

# --- Диалоговые окна ---
@st.dialog("Управление блоком")
def manage_block_dialog(block_id):
    block_to_manage = next((b for b in st.session_state.filter_blocks if b['id'] == block_id), None)
    if not block_to_manage: return

    st.subheader("Сохранить текущий блок")
    name_to_save = st.text_input("Имя шаблона блока")
    if st.button("Сохранить блок"):
        if name_to_save:
            clean_block = {k: v for k, v in block_to_manage.items() if k != 'id'}
            if save_block(conn, name_to_save, clean_block):
                st.toast("Шаблон блока сохранен!", icon="✅")
                cached_load_block_names.clear()
            else:
                st.error("Ошибка сохранения блока.")
        else:
            st.warning("Введите имя для шаблона.")
    
    st.markdown("---")
    st.subheader("Загрузить шаблон в текущий блок")
    saved_block_names = cached_load_block_names()
    selected_block_name = st.selectbox("Выберите шаблон", ["-- Выберите --"] + saved_block_names)
    
    load_b_col, del_b_col = st.columns(2)
    if load_b_col.button("Загрузить шаблон"):
        if selected_block_name != "-- Выберите --":
            loaded_block = load_block_by_name(conn, selected_block_name)
            if loaded_block:
                replace_block(block_id, loaded_block)
                st.rerun()
            else:
                st.error("Ошибка загрузки блока.")

    if del_b_col.button("Удалить шаблон"):
            if selected_block_name != "-- Выберите --":
                if delete_block_by_name(conn, selected_block_name):
                    cached_load_block_names.clear()
                    st.rerun()
                else:
                    st.error("Ошибка удаления шаблона.")

    if st.button("Закрыть"):
        st.rerun()

@st.dialog("Заполнить по шаблону")
def fill_sequence_dialog(sequence_type):
    if not st.session_state.selected_lengths:
        st.warning("Сначала выберите длину фразы.")
        if st.button("Закрыть"):
            st.rerun()
        return

    if len(st.session_state.selected_lengths) > 1:
        st.warning("Для заполнения по шаблону выберите только одну длину фразы.")
        if st.button("Закрыть"):
            st.rerun()
        return

    phrase_length = st.session_state.selected_lengths[0]
    st.write(f"Выберите частую последовательность {sequence_type} для длины {phrase_length}:")

    blocks_tuple = make_hashable(st.session_state.filter_blocks)
    selected_lengths_tuple = tuple(st.session_state.selected_lengths)

    table_to_use = st.session_state.get("temp_table_name") or "ngrams"
    sequences_data = cached_get_frequent_sequences(sequence_type, phrase_length, blocks_tuple, selected_lengths_tuple, table_name=table_to_use)
    
    options = []
    for seq in sequences_data:
        sequence_values = seq[:-2]
        frequency = float(seq[-2])
        quantity = seq[-1]
        options.append(f"{'_'.join(map(str,sequence_values))} (F: {format_number_with_spaces(frequency)}, Q: {format_number_with_spaces(quantity)})")

    selected_option = st.selectbox("Последовательность", options)

    if st.button("Заполнить"):
        if selected_option:
            selected_values_str = selected_option.split(' (F:')[0]
            selected_values = selected_values_str.split('_')

            if len(selected_values) != phrase_length:
                st.error("Выбранная последовательность не соответствует выбранной длине фразы.")
                return

            existing_blocks_by_position = {block['position']: block for block in st.session_state.filter_blocks}

            for i, val in enumerate(selected_values):
                position = i
                if position in existing_blocks_by_position:
                    block = existing_blocks_by_position[position]
                    found_rule = False
                    for rule in block['rules']:
                        if rule['type'] == sequence_type:
                            if val not in rule['values']:
                                rule['values'].append(val)
                            found_rule = True
                            break
                    if not found_rule:
                        block['rules'].append({'id': str(uuid.uuid4()), 'type': sequence_type, 'values': [val]})
                else:
                    st.session_state.filter_blocks.append({
                        'id': str(uuid.uuid4()),
                        'position': position,
                        'rules': [{'id': str(uuid.uuid4()), 'type': sequence_type, 'values': [val]}]
                    })
            
            st.session_state.filter_blocks.sort(key=lambda b: b['position'])
            clear_caches()
            st.toast("Блоки фильтров заполнены!", icon="✅")
            st.rerun()
    
    if st.button("Закрыть"):
        st.rerun()

@st.dialog("Загрузить паттерн по ID")
def load_pattern_by_id_dialog():
    pattern_id = st.number_input("Введите ID паттерна", min_value=1, step=1, value=None)
    if st.button("Загрузить паттерн"):
        if pattern_id and pattern_id > 0:
            pattern_data = cached_get_pattern_by_id(pattern_id)
            if pattern_data:
                pattern_text = pattern_data['text']
                phrase_length = pattern_data['len']
                parts = pattern_text.split('_')

                if len(parts) != phrase_length * 3:
                    st.error(f"Ошибка разбора паттерна: ожидалось {phrase_length * 3} частей, получено {len(parts)}.")
                    return

                deps = parts[0:phrase_length]
                poss = parts[phrase_length : 2 * phrase_length]
                tags = parts[2 * phrase_length : 3 * phrase_length]

                st.session_state.filter_blocks = []
                st.session_state.selected_lengths = [phrase_length]

                for i in range(phrase_length):
                    st.session_state.filter_blocks.append({
                        'id': str(uuid.uuid4()),
                        'position': i,
                        'rules': [
                            {'id': str(uuid.uuid4()), 'type': 'dep', 'values': [deps[i]]},
                            {'id': str(uuid.uuid4()), 'type': 'pos', 'values': [poss[i]]},
                            {'id': str(uuid.uuid4()), 'type': 'tag', 'values': [tags[i]]}
                        ]
                    })
                
                clear_caches()
                st.toast("Паттерн успешно загружен!", icon="✅")
                st.rerun()
            else:
                st.error(f"Паттерн с ID {pattern_id} не найден.")
        else:
            st.warning("Введите корректный ID.")

@st.dialog("Сгенерированный SQL-запрос")
def show_sql_dialog():
    st.code(st.session_state.last_query, language='sql')
    if st.button("Закрыть"):
        st.rerun()

@st.dialog("Сохранить набор фильтров")
def save_set_dialog():
    name_to_save = st.text_input("Имя набора")
    if st.button("Сохранить"):
        if name_to_save:
            if save_filter_set(conn, name_to_save, {"lengths": st.session_state.selected_lengths, "blocks": st.session_state.filter_blocks}):
                st.toast("Набор сохранен!", icon="✅")
                cached_load_filter_set_names.clear()
                st.rerun()
            else:
                st.error("Ошибка сохранения набора.")
        else:
            st.warning("Введите имя.")
    if st.button("Отмена"):
        st.rerun()

@st.dialog("Загрузить набор фильтров")
def load_set_dialog():
    saved_names = sorted(cached_load_filter_set_names(), key=str.lower)
    selected_name = st.selectbox("Выберите набор", ["-- Выберите --"] + saved_names)
    load_btn_col, del_btn_col = st.columns(2)
    if load_btn_col.button("Загрузить"):
        if selected_name != "-- Выберите --":
            loaded = load_filter_set_by_name(conn, selected_name)
            if loaded:
                st.session_state.selected_lengths = loaded.get("lengths", [])
                st.session_state.filter_blocks = loaded.get("blocks", [])
                clear_caches()
                st.rerun()
            else:
                st.error("Ошибка загрузки набора.")
    if del_btn_col.button("Удалить"):
        if selected_name != "-- Выберите --":
            if delete_filter_set_by_name(conn, selected_name):
                cached_load_filter_set_names.clear()
                st.rerun()
            else:
                st.error("Ошибка удаления набора.")
    if st.button("Отмена"):
        st.rerun()

# --- Основной интерфейс ---
st.title("Phrase Filtration")
main_col1, main_col2 = st.columns([2, 1.5])

with main_col1:
    st.subheader("Параметры фильтрации")

    # Row 1: Min Freq, Min Qty, Length
    row1_cols = st.columns([1, 1, 2])
    with row1_cols[0]:
        st.number_input("Мин. частотность (млн)", min_value=0.0, value=st.session_state.min_frequency, step=0.001, key="min_frequency_widget", on_change=lambda: setattr(st.session_state, 'min_frequency', st.session_state.min_frequency_widget))
    with row1_cols[1]:
        st.number_input("Мин. количество фраз", min_value=0, value=st.session_state.min_quantity, step=1, key="min_quantity_widget", on_change=lambda: setattr(st.session_state, 'min_quantity', st.session_state.min_quantity_widget))
    with row1_cols[2]:
        st.multiselect(
            "Длина фразы (токенов)",
            options=cached_get_all_unique_lengths(),
            default=st.session_state.selected_lengths,
            key="selected_lengths_widget",
            on_change=handle_length_change,
            label_visibility="visible"
        )

    # Row 2: DEP, POS, TAG, ID buttons
    row2_cols = st.columns(4)
    with row2_cols[0]:
        st.button("DEP", use_container_width=True, on_click=fill_sequence_dialog, args=("dep",))
    with row2_cols[1]:
        st.button("POS", use_container_width=True, on_click=fill_sequence_dialog, args=("pos",))
    with row2_cols[2]:
        st.button("TAG", use_container_width=True, on_click=fill_sequence_dialog, args=("tag",))
    with row2_cols[3]:
        st.button("ID", use_container_width=True, on_click=load_pattern_by_id_dialog)

    st.markdown("---")

    max_len = max(st.session_state.selected_lengths) if st.session_state.selected_lengths else 0
    pos_options = list(range(1, max_len + 1))

    table_to_use = st.session_state.get("temp_table_name") or "ngrams"

    for block in st.session_state.filter_blocks:
        expander_title = f"Позиция {block['position'] + 1}"
        with st.expander(expander_title, expanded=True):
            block_id = block['id']
            
            header_cols = st.columns([2, 1.3], vertical_alignment="bottom")
            
            if pos_options:
                current_pos_index = pos_options.index(block['position'] + 1) if (block['position'] + 1) in pos_options else 0
                header_cols[0].selectbox("Позиция", pos_options, index=current_pos_index, key=f"pos_block_{block_id}", on_change=handle_position_change, args=(block_id,))
            else:
                header_cols[0].warning("Выберите длину фразы для выбора позиции.")
            
            with header_cols[1]:
                btn_cols = st.columns(2, gap="small")
                if btn_cols[0].button("Управление", key=f"manage_block_{block_id}", help="Управление блоком", use_container_width=True):
                    manage_block_dialog(block_id)
                btn_cols[1].button("Удалить", on_click=remove_block, args=(block_id,), key=f"rem_block_{block_id}", help="Удалить блок", use_container_width=True)
            
            for rule in block['rules']:
                rule_id = rule['id']
                rule_cols = st.columns([1, 1, 3, 0.5])
                
                current_operator_index = ['include', 'exclude'].index(rule.get('operator', 'include'))
                rule_cols[0].radio("Оператор", ['include', 'exclude'], index=current_operator_index, key=f"op_{rule_id}", on_change=handle_operator_change, args=(block_id, rule_id), label_visibility="collapsed", horizontal=True)

                current_type_index = ['dep', 'pos', 'tag', 'token', 'lemma', 'morph'].index(rule['type'])
                rule_cols[1].selectbox("Тип", ['dep', 'pos', 'tag', 'token', 'lemma', 'morph'], index=current_type_index, key=f"type_{rule_id}", on_change=handle_type_change, args=(block_id, rule_id), label_visibility="collapsed")
                
                blocks_tuple = make_hashable(st.session_state.filter_blocks)
                selected_lengths_tuple = tuple(st.session_state.selected_lengths)
                unique_vals = cached_get_unique_values_for_rule(block['position'], rule['type'], selected_lengths_tuple, blocks_tuple, block_id, rule_id, st.session_state.min_frequency, st.session_state.min_quantity, table_name=table_to_use)
                
                disp_opts = {f"{v[0]} (F:{format_number_with_spaces(v[1])}, Q:{format_number_with_spaces(v[2])})" if v[1] is not None else f"{v[0]} (Q:{format_number_with_spaces(v[2])})" : v[0] for v in unique_vals}
                default_disp = [k for k, v in disp_opts.items() if v in rule['values']]
                
                rule_cols[2].multiselect("Значения", list(disp_opts.keys()), default=default_disp, key=f"vals_{rule_id}", on_change=handle_values_change, kwargs=dict(block_id=block_id, rule_id=rule_id, disp_opts=disp_opts), label_visibility="collapsed")

                rule_cols[3].button("🗑️", on_click=remove_rule, args=(block_id, rule_id), key=f"rem_rule_{rule_id}")
            
            st.button("➕ Добавить правило", on_click=add_rule, args=(block_id,), key=f"add_rule_{block_id}")

    st.button("Добавить блок фильтров", on_click=add_block, use_container_width=True)

    sql_col, save_set_col, load_set_col = st.columns(3)

    if sql_col.button("SQL", use_container_width=True):
        if st.session_state.last_query:
            show_sql_dialog()

    if save_set_col.button("Сохранить набор", use_container_width=True):
        save_set_dialog()

    if load_set_col.button("Загрузить набор", use_container_width=True):
        load_set_dialog()
    st.markdown("---")

    # --- Панель подсказок ---
    if st.session_state.selected_lengths:
        selected_lengths_tuple = tuple(st.session_state.selected_lengths)
        
        # Создаем версию блоков фильтров, которая включает только правила со значениями.
        # Это будет использоваться в качестве ключа кэша для данных подсказок.
        active_filter_blocks = []
        for block in st.session_state.filter_blocks:
            active_rules = [rule for rule in block['rules'] if rule['values']]
            if active_rules:
                # Нам нужно сохранить структуру блока, но только с активными правилами
                active_block = block.copy()
                active_block['rules'] = active_rules
                active_filter_blocks.append(active_block)

        filter_blocks_tuple_for_suggestions = make_hashable(active_filter_blocks)
        
        table_to_use = st.session_state.get("temp_table_name") or "ngrams"
        suggestion_data = cached_get_suggestion_data(selected_lengths_tuple, filter_blocks_tuple_for_suggestions, st.session_state.min_frequency, st.session_state.min_quantity, table_name=table_to_use)

        if not suggestion_data:
            with st.expander("Подсказки для фильтрации", expanded=True):
                st.info("Нет доступных вариантов для дальнейшей фильтрации.")
        else:
            suggestions_by_type_and_pos = {'dep': {}, 'pos': {}, 'tag': {}, 'morph': {}}
            for position, suggestions in suggestion_data.items():
                for s in suggestions:
                    if s['type'] in suggestions_by_type_and_pos:
                        if position not in suggestions_by_type_and_pos[s['type']]:
                            suggestions_by_type_and_pos[s['type']][position] = []
                        suggestions_by_type_and_pos[s['type']][position].append(s)

            active_filters = set()
            for b in st.session_state.filter_blocks:
                for r in b['rules']:
                    for v in r['values']:
                        active_filters.add((b['position'], r['type'], v))

            type_names = {
                'dep': "Параметры фильтрации DEP",
                'pos': "Параметры фильтрации POS",
                'tag': "Параметры фильтрации TAG",
                'morph': "Параметры фильтрации MORPH"
            }

            for s_type, pos_dict in suggestions_by_type_and_pos.items():
                if pos_dict:
                    with st.expander(type_names.get(s_type, s_type.upper()), expanded=False):
                        
                        sorted_positions = sorted(pos_dict.keys())
                        num_columns = max(len(sorted_positions), 1)
                        cols = st.columns(num_columns)

                        for i, position in enumerate(sorted_positions):
                            with cols[i % num_columns]:
                                st.markdown(f"**Позиция {position + 1}**")
                                for s in pos_dict[position]:
                                    is_checked = (position, s['type'], s['value']) in active_filters
                                    key = f"suggest_{position}_{s['type']}_{s['value']}"
                                    label = f"{s['value']}  \nF: {format_number_with_spaces(s['freq'])}  \nQ: {format_number_with_spaces(s['qty'])}" 
                                    
                                    st.checkbox(
                                        label, 
                                        value=is_checked, 
                                        key=key, 
                                        on_change=toggle_filter_from_suggestion, 
                                        args=(position, s['type'], s['value'])
                                    )
    
    
def _run_query():
    
    if not st.session_state.selected_lengths:
        st.session_state.results = []
        st.session_state.last_query = ""
        return
    
    has_active_filters = any(rule['values'] for block in st.session_state.filter_blocks for rule in block['rules'])

    if not st.session_state.filter_blocks or not has_active_filters:
        st.session_state.results = []
        st.session_state.last_query = ""
        return

    table_to_use = st.session_state.get("temp_table_name") or "ngrams"
    where_clauses = build_where_clauses(st.session_state.filter_blocks, table_name=table_to_use)

    # Add min_frequency filter
    if st.session_state.min_frequency > 0:
        where_clauses.append(f"{table_to_use}.freq_mln >= {float(st.session_state.min_frequency)}")

    # The main WHERE clause for lengths is only needed when querying the main ngrams table
    # (the temp table is already filtered by length).
    if table_to_use == "ngrams":
        where_clauses.insert(0, f"len IN ({', '.join(map(str, st.session_state.selected_lengths))})")

    full_where_clause = f"WHERE {' AND '.join(where_clauses)}" if where_clauses else ""

    query = f"""
        SELECT text, freq_mln, tokens
        FROM {table_to_use}
        {full_where_clause}
        ORDER BY freq_mln DESC;
    """
    st.session_state.last_query = query.strip()
    results = execute_query(conn, st.session_state.last_query)
    if results is not None:
        st.session_state.results = results
    else:
        st.session_state.results = []
        st.error("Ошибка выполнения запроса к базе данных.")

# --- Автоматическое обновление результатов ---
current_filters_state = {
    "lengths": st.session_state.selected_lengths,
    "blocks": st.session_state.filter_blocks
}
current_filters_hash = hash(make_hashable(current_filters_state))

if st.session_state.current_filters_hash != current_filters_hash:
    st.session_state.current_filters_hash = current_filters_hash
    _run_query()

with main_col2:
    if st.session_state.results:
        total_frequency = sum(res[1] for res in st.session_state.results)
        total_quantity = len(st.session_state.results)
        st.markdown(f"### <small>F: {format_number_with_spaces(total_frequency)}, Q: {format_number_with_spaces(total_quantity)}</small>", unsafe_allow_html=True)

    if st.session_state.results:
        swapped_results = [(res[1], res[0]) for res in st.session_state.results]
        df_results = pd.DataFrame(swapped_results, columns=["Частотность (млн)", "Фраза"])
        
        st.dataframe(
            df_results,
            column_config={
                "Частотность (млн)": st.column_config.NumberColumn(
                    width="small", format="localized"
                ),
                "Фраза": st.column_config.TextColumn(
                    width="large"
                )
            },
            use_container_width=True,
            height=800,
            hide_index=True
        )
        
        if st.button("Показать анализ слов по позициям"):
            st.session_state.show_word_analysis = not st.session_state.show_word_analysis
        
        if st.session_state.show_word_analysis:
            with st.expander("Анализ слов по позициям", expanded=True):
                position_word_count = {}
                for _, _, tokens_list in st.session_state.results:
                    words = tokens_list
                    for i, word in enumerate(words):
                        if i not in position_word_count:
                            position_word_count[i] = {}
                        position_word_count[i][word] = position_word_count[i].get(word, 0) + 1

                num_columns = 7
                sorted_positions = sorted(position_word_count.keys())
                
                cols = st.columns(min(len(sorted_positions), num_columns))

                for i, position in enumerate(sorted_positions):
                    with cols[i % num_columns]:
                        word_counts = position_word_count[position]
                        total_words_in_position = sum(word_counts.values())
                        st.markdown(f"**Позиция {position + 1} ({format_number_with_spaces(total_words_in_position)})**")
                        sorted_words = sorted(word_counts.items(), key=lambda item: item[1], reverse=True)
                        for word, count in sorted_words:
                            st.markdown(f"- {word} ({format_number_with_spaces(count)})")
