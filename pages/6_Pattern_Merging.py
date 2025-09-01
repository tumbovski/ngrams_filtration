import streamlit as st
import pandas as pd
from core.database import (
    get_db_connection,
    find_next_merge_candidate_group,
    get_patterns_data_by_ids,
    execute_multiple_merges,
    mark_patterns_as_skipped,
    get_available_lengths_for_merging
)

st.set_page_config(page_title="Слияние паттернов", layout="wide")

# --- Инициализация состояния ---
if 'current_merge_group' not in st.session_state:
    st.session_state.current_merge_group = None
if 'planned_merges' not in st.session_state:
    st.session_state.planned_merges = []
if 'selected_length' not in st.session_state:
    st.session_state.selected_length = None

conn = get_db_connection()

# --- Функции-помощники для UI ---
def add_merge_to_plan():
    source_ids = st.session_state.get('merge_source_select', [])
    target_id = st.session_state.get('merge_target_select')
    if not source_ids or not target_id:
        st.warning("Нужно выбрать и исходные, и целевой паттерны.")
        return
    if target_id in source_ids:
        st.error("Целевой паттерн не может быть одновременно и исходным.")
        return
    
    all_planned_ids = {p_id for merge in st.session_state.planned_merges for p_id in merge['sources']} \
                    | {merge['target'] for merge in st.session_state.planned_merges}
    
    if any(pid in all_planned_ids for pid in source_ids) or target_id in all_planned_ids:
        st.error("Один из выбранных паттернов уже участвует в другом запланированном слиянии.")
        return

    st.session_state.planned_merges.append({
        "sources": source_ids,
        "target": target_id
    })

def clear_plan():
    st.session_state.planned_merges = []

def clear_current_group():
    st.session_state.current_merge_group = None
    st.session_state.planned_merges = []

# --- Основная логика --- #
st.title("Слияние паттернов")
st.warning("**Внимание!** Этот раздел выполняет необратимые изменения в базе данных.")

available_lengths = get_available_lengths_for_merging(conn)
st.selectbox(
    "Доступные длины:", 
    options=available_lengths, 
    key='selected_length', 
    index=None, 
    placeholder="Выберите длину...",
    on_change=clear_current_group # Сбрасываем группу при смене длины
)

# Шаг 2: Модерация группы
if st.session_state.selected_length:

    if not st.session_state.current_merge_group:
        with st.spinner(f"Идет поиск кандидатов длиной {st.session_state.selected_length}..."):
            st.session_state.current_merge_group = find_next_merge_candidate_group(conn, st.session_state.selected_length)

    if not st.session_state.current_merge_group:
        st.success(f"✅ Все доступные кандидаты для длины {st.session_state.selected_length} были обработаны!")
    else:
        group = st.session_state.current_merge_group
        pattern_ids = group['pattern_ids']
        
        with st.spinner("Загрузка данных для паттернов..."):
            patterns_data = get_patterns_data_by_ids(conn, pattern_ids)
            patterns_map = {p['id']: p for p in patterns_data}

        st.markdown("---")

        # --- Логика для сравнения и выделения различий ---
        num_tokens = st.session_state.selected_length
        keys = ['dep', 'pos', 'tag']
        parsed_patterns = {}

        # Шаг 1: Разобрать все паттерны в группе
        for p in patterns_data:
            all_parts = p['text'].split('_')
            if len(all_parts) >= num_tokens * 3:
                parsed_patterns[p['id']] = {
                    'dep': all_parts[0:num_tokens],
                    'pos': all_parts[num_tokens:num_tokens*2],
                    'tag': all_parts[num_tokens*2:num_tokens*3]
                }
            else:
                # Сохраняем в сыром виде, если разобрать не удалось
                parsed_patterns[p['id']] = {'raw': p['text']}

        # Шаг 2: Найти индексы, в которых есть различия
        diff_indices = {key: set() for key in keys}
        if len(patterns_data) > 1:
            for key in keys:
                for i in range(num_tokens):
                    # Собираем значения для данной позиции у всех паттернов
                    values = set()
                    for p in patterns_data:
                        # Убеждаемся, что паттерн был разобран и имеет нужный ключ и индекс
                        if 'raw' not in parsed_patterns[p['id']] and i < len(parsed_patterns[p['id']][key]):
                            values.add(parsed_patterns[p['id']][key][i])
                    
                    # Если значений больше одного, значит, есть различие
                    if len(values) > 1:
                        diff_indices[key].add(i)
        # --- Конец логики сравнения ---

        patterns_per_row = 3
        chunked_patterns = [patterns_data[i:i + patterns_per_row] for i in range(0, len(patterns_data), patterns_per_row)]

        for row_patterns in chunked_patterns:
            cols = st.columns(patterns_per_row)
            for i, p_data in enumerate(row_patterns):
                with cols[i]:
                    st.subheader(f"Паттерн #{p_data['id']}")

                    # --- Формирование вывода с выделением ---
                    current_parsed = parsed_patterns.get(p_data['id'])
                    if not current_parsed or 'raw' in current_parsed:
                        # Если паттерн не был разобран, показываем как есть
                        st.code(p_data['text'], language='bash')
                    else:
                        lines_for_code = []
                        for key in keys:
                            line_parts = []
                            for idx, token in enumerate(current_parsed[key]):
                                if idx in diff_indices[key]:
                                    line_parts.append(f"[{token}]") # Выделяем скобками
                                else:
                                    line_parts.append(token)
                            lines_for_code.append("_".join(line_parts))
                        
                        code_string = "\n".join(lines_for_code)
                        st.code(code_string, language='bash')
                    # --- Конец блока отображения ---

                    st.markdown(f"**F:** {p_data['freq']:.2f} | **Q:** {p_data['qty']}")
                    if p_data['examples']:
                        df_examples = pd.DataFrame(p_data['examples'])
                        df_examples.rename(columns={'text': 'Фраза', 'freq': 'Частотность (ipm)'}, inplace=True)
                        st.dataframe(df_examples, hide_index=True, use_container_width=True)
                    else:
                        st.write("Примеры не найдены.")

        with st.container(border=True):
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                target_select = st.selectbox("Основной", options=list(patterns_map.keys()), format_func=lambda pid: f"#{pid}: {patterns_map[pid]['text']}", key="merge_target_select", index=None)
            with col2:
                source_select = st.multiselect("Поглощаемый", options=list(patterns_map.keys()), format_func=lambda pid: f"#{pid}: {patterns_map[pid]['text']}", key="merge_source_select")
            with col3:
                st.write("")
                st.button("Добавить в план", on_click=add_merge_to_plan, use_container_width=True)

        if st.session_state.planned_merges:
            st.subheader("План слияния для этой группы")
            for i, merge in enumerate(st.session_state.planned_merges):
                with st.container(border=True):
                    st.write(f"**Операция #{i+1}**")
                    target_text = patterns_map[merge['target']]['text']
                    st.write(f"🎯 **Цель:** `#{merge['target']}`: *{target_text}*")
                    st.write("**Источники (будут удалены и заменены):**")
                    for source_id in merge['sources']:
                        source_text = patterns_map[source_id]['text']
                        st.write(f"- `#{source_id}`: *{source_text}*")
            st.button("Очистить план", on_click=clear_plan, use_container_width=True)

        st.markdown("---")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("✅ Выполнить все запланированные слияния", use_container_width=True, type="primary", disabled=not st.session_state.planned_merges):
                with st.spinner("Выполняется слияние... Это может занять много времени."):
                    success, message = execute_multiple_merges(conn, st.session_state.planned_merges)
                    if success:
                        st.success(f"Слияние успешно завершено! {message}")
                        clear_current_group()
                        st.rerun()
                    else:
                        st.error(f"Ошибка при слиянии: {message}")

        with col2:
            if st.button("➡️ Пропустить эту группу паттернов", use_container_width=True):
                with st.spinner("Обновление статуса паттернов..."):
                    group_info = st.session_state.current_merge_group
                    diff_level = group_info.get("difference_level", 0)
                    diff_types = group_info.get("difference_types", [])
                    
                    mark_patterns_as_skipped(conn, pattern_ids, diff_level, diff_types)

                st.success(f"Группа пропущена (уровень: {diff_level}, типы: {diff_types}). Паттерны {pattern_ids} больше не будут появляться в подобных поисках.")
                clear_current_group()
                st.rerun()