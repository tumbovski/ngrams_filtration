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
    get_frequent_sequences # Добавляем импорт новой функции
)

# --- Управление состоянием ---
st.set_page_config(layout="wide", page_title="Фильтрация фраз")

if 'filter_blocks' not in st.session_state: st.session_state.filter_blocks = []
if 'selected_lengths' not in st.session_state: st.session_state.selected_lengths = []
if 'last_query' not in st.session_state: st.session_state.last_query = ""
if 'results' not in st.session_state: st.session_state.results = []
if 'show_word_analysis' not in st.session_state: st.session_state.show_word_analysis = False

# --- Подключение к БД и кэширование ---
@st.cache_resource
def init_connection():
    return get_db_connection()

conn = init_connection()

if not conn:
    st.error("Не удалось подключиться к базе данных. Проверьте настройки в .env файле и доступность сервера.")
    st.stop()

# Оборачиваем функции работы с БД в кэшируемые streamlit-функции
@st.cache_data(ttl=3600)
def cached_get_all_unique_lengths():
    return get_all_unique_lengths(conn)

@st.cache_data(ttl=3600)
def cached_get_unique_values_for_rule(position, rule_type, selected_lengths_tuple, blocks_tuple, block_id_to_exclude, rule_id_to_exclude):
    # Рекурсивно преобразуем хешируемую структуру обратно в стандартные dict и list
    def make_mutable(obj):
        if isinstance(obj, frozenset):
            return dict((k, make_mutable(v)) for k, v in obj)
        if isinstance(obj, tuple):
            return list(make_mutable(v) for v in obj)
        return obj

    all_blocks = make_mutable(blocks_tuple)
    selected_lengths = list(selected_lengths_tuple)
    return get_unique_values_for_rule(conn, position, rule_type, selected_lengths, all_blocks, block_id_to_exclude, rule_id_to_exclude)

@st.cache_data(ttl=3600)
def cached_load_filter_set_names():
    return load_filter_set_names(conn)

@st.cache_data(ttl=3600)
def cached_load_block_names():
    return load_block_names(conn)

@st.cache_data(ttl=3600)
def cached_get_frequent_sequences(sequence_type, phrase_length, filter_blocks_tuple, selected_lengths_tuple):
    # Рекурсивно преобразуем хешируемую структуру обратно в стандартные dict и list
    def make_mutable(obj):
        if isinstance(obj, frozenset):
            return dict((k, make_mutable(v)) for k, v in obj)
        if isinstance(obj, tuple):
            return list(make_mutable(v) for v in obj)
        return obj

    mutable_filter_blocks = make_mutable(filter_blocks_tuple)
    mutable_selected_lengths = list(selected_lengths_tuple)

    return get_frequent_sequences(conn, sequence_type, phrase_length, mutable_filter_blocks, mutable_selected_lengths)

# --- Основной интерфейс ---

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

    blocks_tuple = tuple(
        frozenset({
            "id": b["id"],
            "position": b["position"],
            "rules": tuple(
                frozenset({
                    "id": r["id"],
                    "type": r["type"],
                    "values": tuple(r["values"])
                }.items())
                for r in b["rules"]
            )
        }.items())
        for b in st.session_state.filter_blocks
    )
    selected_lengths_tuple = tuple(st.session_state.selected_lengths)

    sequences_data = cached_get_frequent_sequences(sequence_type, phrase_length, blocks_tuple, selected_lengths_tuple)
    
    options = []
    for seq in sequences_data:
        # Последний элемент в seq - это частотность
        sequence_values = seq[:-1]
        frequency = seq[-1]
        options.append(f"{'_'.join(sequence_values)} (Частотность: {frequency:.3f})")

    selected_option = st.selectbox("Последовательность", options)

    if st.button("Заполнить"):
        if selected_option:
            # Извлекаем значения последовательности из выбранной опции
            selected_values_str = selected_option.split(' (Частотность:')[0]
            selected_values = selected_values_str.split('_')

            if len(selected_values) != phrase_length:
                st.error("Выбранная последовательность не соответствует выбранной длине фразы.")
                return

            # Вместо очистки, будем добавлять или обновлять блоки
            # Создаем временный словарь для быстрого доступа к блокам по позиции
            existing_blocks_by_position = {block['position']: block for block in st.session_state.filter_blocks}

            for i, val in enumerate(selected_values):
                position = i
                if position in existing_blocks_by_position:
                    # Блок на этой позиции уже существует, обновляем его
                    block = existing_blocks_by_position[position]
                    found_rule = False
                    for rule in block['rules']:
                        if rule['type'] == sequence_type:
                            if val not in rule['values']:
                                rule['values'].append(val)
                            found_rule = True
                            break
                    if not found_rule:
                        # Нет правила такого типа в существующем блоке, добавляем новое
                        new_rule_id = str(uuid.uuid4())
                        block['rules'].append({
                            'id': new_rule_id,
                            'type': sequence_type,
                            'values': [val]
                        })
                else:
                    # Блока на этой позиции нет, создаем новый
                    new_block_id = str(uuid.uuid4())
                    new_rule_id = str(uuid.uuid4())
                    st.session_state.filter_blocks.append({
                        'id': new_block_id,
                        'position': position,
                        'rules': [{
                            'id': new_rule_id,
                            'type': sequence_type,
                            'values': [val]
                        }]
                    })
            
            # Сортируем блоки по позиции, чтобы UI отображался корректно
            st.session_state.filter_blocks.sort(key=lambda b: b['position'])
            st.toast("Блоки фильтров заполнены!", icon="✅")
            st.rerun()
    
    if st.button("Закрыть"):
        st.rerun()

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
    saved_names = cached_load_filter_set_names()
    selected_name = st.selectbox("Выберите набор", ["-- Выберите --"] + saved_names)
    load_btn_col, del_btn_col = st.columns(2)
    if load_btn_col.button("Загрузить"):
        if selected_name != "-- Выберите --":
            loaded = load_filter_set_by_name(conn, selected_name)
            if loaded:
                st.session_state.selected_lengths = loaded.get("lengths", [])
                st.session_state.filter_blocks = loaded.get("blocks", [])
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

# --- Подключение к БД и кэширование ---
st.title("Фильтрация фраз")
main_col1, main_col2 = st.columns([2, 1.5])

with main_col1:
    st.subheader("Параметры фильтрации")
    
    def handle_length_change():
        st.session_state.selected_lengths = st.session_state.selected_lengths_widget
        max_len = max(st.session_state.selected_lengths) if st.session_state.selected_lengths else 0
        st.session_state.filter_blocks = [b for b in st.session_state.filter_blocks if b['position'] < max_len]
        cached_get_unique_values_for_rule.clear()

    def handle_position_change(block_id):
        new_pos = st.session_state[f"pos_block_{block_id}"] - 1
        for block in st.session_state.filter_blocks:
            if block['id'] == block_id:
                if block['position'] != new_pos:
                    block['position'] = new_pos
                    for rule in block['rules']:
                        rule['values'] = []
                    cached_get_unique_values_for_rule.clear()
                break

    def handle_type_change(block_id, rule_id):
        new_type = st.session_state[f"type_{rule_id}"]
        for block in st.session_state.filter_blocks:
            if block['id'] == block_id:
                for rule in block['rules']:
                    if rule['id'] == rule_id:
                        if rule['type'] != new_type:
                            rule['type'] = new_type
                            rule['values'] = []
                            cached_get_unique_values_for_rule.clear()
                        break
                break

    def handle_values_change(block_id, rule_id, disp_opts):
        selected_disp = st.session_state[f"vals_{rule_id}"]
        new_values = [disp_opts[s] for s in selected_disp]
        for block in st.session_state.filter_blocks:
            if block['id'] == block_id:
                for rule in block['rules']:
                    if rule['id'] == rule_id:
                        rule['values'] = new_values
                        break
                break

    def add_block():
        new_block_id = str(uuid.uuid4())
        new_rule_id = str(uuid.uuid4())
        st.session_state.filter_blocks.append({'id': new_block_id, 'position': 0, 'rules': [{'id': new_rule_id, 'type': 'dep', 'values': []}]})
    
    def remove_block(block_id):
        st.session_state.filter_blocks = [b for b in st.session_state.filter_blocks if b['id'] != block_id]

    def add_rule(block_id):
        for block in st.session_state.filter_blocks:
            if block['id'] == block_id:
                new_rule_id = str(uuid.uuid4())
                block['rules'].append({'id': new_rule_id, 'type': 'dep', 'values': []})
                break
    
    def remove_rule(block_id, rule_id):
        for block in st.session_state.filter_blocks:
            if block['id'] == block_id:
                block['rules'] = [r for r in block['rules'] if r['id'] != rule_id]
                break

    def replace_block(block_id, new_block_data):
        for i, block in enumerate(st.session_state.filter_blocks):
            if block['id'] == block_id:
                new_block_data['id'] = block_id
                st.session_state.filter_blocks[i] = new_block_data
                cached_get_unique_values_for_rule.clear()
                break

    st.multiselect(
        "Длина фразы (токенов)",
        options=cached_get_all_unique_lengths(),
        default=st.session_state.selected_lengths,
        key="selected_lengths_widget",
        on_change=handle_length_change
    )

    # Кнопки для заполнения по шаблону
    fill_cols = st.columns(3)
    if fill_cols[0].button("Заполнить DEP", use_container_width=True):
        fill_sequence_dialog("dep")
    if fill_cols[1].button("Заполнить POS", use_container_width=True):
        fill_sequence_dialog("pos")
    if fill_cols[2].button("Заполнить TAG", use_container_width=True):
        fill_sequence_dialog("tag")

    st.markdown("---")

    max_len = max(st.session_state.selected_lengths) if st.session_state.selected_lengths else 0
    pos_options = list(range(1, max_len + 1))

    for block in st.session_state.filter_blocks:
        with st.container(border=True):
            block_id = block['id']
            
            header_cols = st.columns([2, 1.3], vertical_alignment="bottom")
            
            if pos_options: # Only render selectbox if options are available
                current_pos_index = pos_options.index(block['position'] + 1) if (block['position'] + 1) in pos_options else 0
                header_cols[0].selectbox("Позиция", pos_options, index=current_pos_index, key=f"pos_block_{block_id}", on_change=handle_position_change, args=(block_id,))
            else:
                header_cols[0].warning("Выберите длину фразы для выбора позиции.")
            
            with header_cols[1]:
                btn_cols = st.columns(2, gap="small")
                if btn_cols[0].button("Управление", key=f"manage_block_{block_id}", help="Управление блоком", use_container_width=True):
                    manage_block_dialog(block_id)
                btn_cols[1].button("Удалить", on_click=remove_block, args=(block_id,), key=f"rem_block_{block_id}", help="Удалить блок", use_container_width=True)
            
            st.markdown("--- ")

            for rule in block['rules']:
                rule_id = rule['id']
                rule_cols = st.columns([1, 2, 0.5])
                
                current_type_index = ['dep', 'pos', 'tag', 'token', 'lemma', 'morph'].index(rule['type'])
                rule_cols[0].selectbox("Тип", ['dep', 'pos', 'tag', 'token', 'lemma', 'morph'], index=current_type_index, key=f"type_{rule_id}", on_change=handle_type_change, args=(block_id, rule_id), label_visibility="collapsed")
                
                blocks_tuple = tuple(
                    frozenset({
                        "id": b["id"],
                        "position": b["position"],
                        "rules": tuple(
                            frozenset({
                                "id": r["id"],
                                "type": r["type"],
                                "values": tuple(r["values"])
                            }.items())
                            for r in b["rules"]
                        )
                    }.items())
                    for b in st.session_state.filter_blocks
                )
                selected_lengths_tuple = tuple(st.session_state.selected_lengths)
                unique_vals = cached_get_unique_values_for_rule(block['position'], rule['type'], selected_lengths_tuple, blocks_tuple, block_id, rule_id)
                
                disp_opts = {f"{v[0]} (Фраз: {v[2]}, F: {v[1]:.3f})" if v[1] is not None else f"{v[0]} (Фраз: {v[2]})" : v[0] for v in unique_vals}
                default_disp = [k for k, v in disp_opts.items() if v in rule['values']]
                
                rule_cols[1].multiselect("Значения", list(disp_opts.keys()), default=default_disp, key=f"vals_{rule_id}", on_change=handle_values_change, kwargs=dict(block_id=block_id, rule_id=rule_id, disp_opts=disp_opts), label_visibility="collapsed")

                rule_cols[2].button("🗑️", on_click=remove_rule, args=(block_id, rule_id), key=f"rem_rule_{rule_id}")
            
            st.button("➕ Добавить правило", on_click=add_rule, args=(block_id,), key=f"add_rule_{block_id}")

    st.button("Добавить блок фильтров", on_click=add_block, use_container_width=True)
    st.markdown("---")

    apply_col, sql_col, save_set_col, load_set_col = st.columns(4)
    apply_button = apply_col.button("Применить фильтры", type="primary", use_container_width=True)

    if sql_col.button("SQL", use_container_width=True):
        if st.session_state.last_query:
            show_sql_dialog()

    if save_set_col.button("Сохранить набор", use_container_width=True):
        save_set_dialog()

    if load_set_col.button("Загрузить набор", use_container_width=True):
        load_set_dialog()

if apply_button:
    st.session_state.show_word_analysis = False
    if not st.session_state.selected_lengths:
        st.warning("Выберите хотя бы одну длину фразы.")
    elif not st.session_state.filter_blocks or not any(rule['values'] for block in st.session_state.filter_blocks for rule in block['rules']):
        st.warning("Добавьте и настройте хотя бы один фильтр.")
    else:
        where_clauses = build_where_clauses(st.session_state.filter_blocks)
        query = f"""
            SELECT text, freq_mln
            FROM ngrams
            WHERE len IN ({', '.join(map(str, st.session_state.selected_lengths))})
            {'AND ' + ' AND '.join(where_clauses) if where_clauses else ''}
            ORDER BY frequency DESC;
        """
        st.session_state.last_query = query.strip()
        results = execute_query(conn, st.session_state.last_query)
        if results is not None:
            st.session_state.results = results
        else:
            st.session_state.results = []
            st.error("Ошибка выполнения запроса к базе данных.")

with main_col2:
    if st.session_state.results:
        st.markdown(f"### Результаты <small>({len(st.session_state.results)})</small>", unsafe_allow_html=True)
    else:
        st.subheader("Результаты")

    if st.session_state.results:
        # Меняем порядок данных для соответствия новому порядку столбцов
        swapped_results = [(res[1], res[0]) for res in st.session_state.results]
        df_results = pd.DataFrame(swapped_results, columns=["Частотность (млн)", "Фраза"])
        
        st.dataframe(
            df_results,
            column_config={
                "Частотность (млн)": st.column_config.NumberColumn(
                    width="small", format="%.3f"
                ),
                "Фраза": st.column_config.TextColumn(
                    width="large"
                )
            },
            use_container_width=True,
            height=800,
            hide_index=True
        )
        
        if st.button("Анализ слов по позициям"):
            st.session_state.show_word_analysis = True

if st.session_state.show_word_analysis and st.session_state.results:
    st.subheader("Анализ слов по позициям")

    @st.cache_data
    def analyze_words_by_position(_results_tuple):
        results = list(_results_tuple)
        position_word_count = {}
        # Считаем количество вхождений каждого слова на каждой позиции локально
        for phrase_text, _ in results:  # Игнорируем глобальную частотность
            words = phrase_text.split()
            for i, word in enumerate(words):
                if i not in position_word_count:
                    position_word_count[i] = {}
                position_word_count[i][word] = position_word_count[i].get(word, 0) + 1
        return position_word_count

    word_analysis_data = analyze_words_by_position(tuple(map(tuple, st.session_state.results)))
    max_position = max(word_analysis_data.keys()) if word_analysis_data else -1
    
    if max_position > -1:
        tables_per_row = 7  # Увеличиваем количество таблиц в ряду до 7
        positions = sorted(word_analysis_data.keys())

        for i in range(0, len(positions), tables_per_row):
            cols = st.columns(tables_per_row)
            row_positions = positions[i:i + tables_per_row]

            for j, position in enumerate(row_positions):
                with cols[j]:
                    st.markdown(f"**Позиция {position + 1}**")
                    if position in word_analysis_data:
                        sorted_words = sorted(word_analysis_data[position].items(), key=lambda item: item[1], reverse=True)
                        # Меняем порядок столбцов: сначала Количество, потом Слово
                        df_pos = pd.DataFrame([(count, word) for word, count in sorted_words], columns=["Количество", "Слово"])
                        
                        st.dataframe(
                            df_pos,
                            column_config={
                                "Количество": st.column_config.NumberColumn(format="%d", width="small"),
                                "Слово": st.column_config.TextColumn(width="small")
                            },
                            use_container_width=True,
                            height=300,
                            hide_index=True
                        )
                    else:
                        st.info("Нет данных")
