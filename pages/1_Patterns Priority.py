import streamlit as st
import pandas as pd
from core.database import get_db_connection, get_next_unmoderated_pattern, count_unmoderated_patterns, get_examples_by_pattern_id, save_moderation_record, process_moderation_submission

st.set_page_config(layout="wide", page_title="Pattern Moderation")

# Check authentication status
if 'logged_in' not in st.session_state or not st.session_state.logged_in:
    st.warning("Пожалуйста, войдите в систему, чтобы получить доступ к этой странице.")
    st.switch_page("Home.py")

conn = get_db_connection()

if not conn:
    st.error("Не удалось подключиться к базе данных. Проверьте настройки в .env файле и доступность сервера.")
    st.stop()

# Phrase Length Options
phrase_lengths_options = list(range(2, 13)) # 2 to 12

# --- Session State Management ---
st.session_state.setdefault('selected_phrase_length', phrase_lengths_options[0])
st.session_state.setdefault('min_total_frequency', 0)
st.session_state.setdefault('min_total_quantity', 0)
st.session_state.setdefault('current_pattern_to_moderate', None)
st.session_state.setdefault('remaining_patterns_count', 0)
st.session_state.setdefault('current_ngrams', None)
st.session_state.setdefault('moderation_rating', None) # No default rating
st.session_state.setdefault('moderation_comment', '')
st.session_state.setdefault('moderation_tag', '')

# --- Helper Functions ---
def load_next_pattern(skipped_pattern_id=None):
    """Загружает следующий паттерн, опционально исключая только что пропущенный."""
    # Сбрасываем поля перед загрузкой нового паттерна
    st.session_state.moderation_rating = None
    st.session_state.moderation_comment = ''
    st.session_state.moderation_tag = ''
    
    if st.session_state.selected_phrase_length and st.session_state.user_id:
        min_freq = st.session_state.get('min_total_frequency', 0)
        min_qty = st.session_state.get('min_total_quantity', 0)
        
        pattern = get_next_unmoderated_pattern(
            conn, 
            st.session_state.user_id, 
            st.session_state.selected_phrase_length,
            min_total_frequency=min_freq,
            min_total_quantity=min_qty,
            pattern_id_to_exclude=skipped_pattern_id # Передаем ID для исключения
        )
        st.session_state.current_pattern_to_moderate = pattern
        
        st.session_state.remaining_patterns_count = count_unmoderated_patterns(
            conn, st.session_state.user_id, st.session_state.selected_phrase_length,
            min_total_frequency=min_freq,
            min_total_quantity=min_qty
        )
        
        if pattern:
            st.session_state.current_ngrams = get_examples_by_pattern_id(conn, pattern['id'])
        else:
            st.session_state.current_ngrams = None
    else:
        st.session_state.current_pattern_to_moderate = None
        st.session_state.remaining_patterns_count = 0
        st.session_state.current_ngrams = None

def apply_filters_and_reload():
    """Применяет фильтры и перезагружает паттерн."""
    st.session_state.min_total_frequency = st.session_state.min_freq_input
    st.session_state.min_total_quantity = st.session_state.min_qty_input
    load_next_pattern()

def handle_phrase_length_change():
    st.session_state.selected_phrase_length = st.session_state.phrase_length_selector
    load_next_pattern()

def on_rating_change():
    """Callback, который срабатывает при выборе оценки."""
    rating = st.session_state.moderation_rating
    if rating is None:
        return

    comment = st.session_state.moderation_comment
    tag = st.session_state.moderation_tag
    pattern_id = st.session_state.current_pattern_to_moderate['id']
    user_id = st.session_state.user_id

    if save_moderation_record(conn, pattern_id, user_id, rating, comment, tag):
        process_moderation_submission(conn, pattern_id)
        st.toast(f"Оценка '{rating}' принята!", icon="✅")
        load_next_pattern()
    else:
        st.error("Ошибка при сохранении модерации.")

def format_number_with_spaces(number):
    try:
        num = float(number)
        if num == int(num):
            return f"{int(num):,}".replace(",", " ")
        else:
            return f"{num:,.2f}".replace(",", " ")
    except (ValueError, TypeError):
        return number

# --- Main UI ---
st.title("Приоритет модерации паттернов")

# Load initial pattern if not already loaded
if 'current_pattern_to_moderate' not in st.session_state or st.session_state.current_pattern_to_moderate is None:
    load_next_pattern()

# Define pattern here, so it's available to both columns
pattern = st.session_state.current_pattern_to_moderate

# Main layout with two columns
ngrams_table_col, moderation_details_col = st.columns([1, 2])

with ngrams_table_col:
    if pattern:
        if st.session_state.get('current_ngrams'):
            df_ngrams = pd.DataFrame(st.session_state.current_ngrams, columns=["Частотность (млн)", "Фраза"])
            df_ngrams = df_ngrams[["Частотность (млн)", "Фраза"]] # Reorder columns
            st.dataframe(
                df_ngrams, 
                use_container_width=True, 
                hide_index=True, 
                height=654,
                column_config={
                    "Фраза": st.column_config.Column(width="small"),
                    "Частотность (млн)": st.column_config.Column(width="small"),
                },
                key=f"ngrams_table_{pattern['id']}"
            )
        else:
            st.info("Нет фраз, соответствующих этому паттерну.")
    else:
        st.info("Выберите длину паттерна, чтобы начать модерацию.")


with moderation_details_col:
    with st.expander("Показать/скрыть фильтры"):
        col1, col2, col3 = st.columns(3)
        with col1:
            st.selectbox(
                "Длина паттерна:",
                options=phrase_lengths_options,
                key="phrase_length_selector",
                on_change=handle_phrase_length_change,
                index=phrase_lengths_options.index(st.session_state.selected_phrase_length) if st.session_state.selected_phrase_length in phrase_lengths_options else 0
            )
        with col2:
            st.number_input(
                "Минимальная суммарная частотность:",
                min_value=0,
                step=10,
                key="min_freq_input",
                value=st.session_state.min_total_frequency
            )
        with col3:
            st.number_input(
                "Минимальное количество фраз:",
                min_value=0,
                step=1,
                key="min_qty_input",
                value=st.session_state.min_total_quantity
            )
        
        if st.button("Применить фильтры", use_container_width=True):
            apply_filters_and_reload()

    if pattern:
        st.caption(f"Осталось: {format_number_with_spaces(st.session_state.remaining_patterns_count)}")
        st.write(f"**Паттерн:** `{pattern['pattern_text']}`")
        st.markdown(f"**ID:** {pattern['id']} | **F:** {format_number_with_spaces(pattern['total_frequency'])} | **Q:** {format_number_with_spaces(pattern['total_quantity'])}")

        st.markdown("---")
        
        user_id = st.session_state.user_id
        if user_id is None:
            st.warning("Не удалось получить ID пользователя. Пожалуйста, войдите снова.")
        else:
            st.text_area("Комментарий", key="moderation_comment")
            st.text_input("Тег/Тип", key="moderation_tag")
            
            st.radio(
                "Оценка", 
                options=[1, 2, 3, 4, 5], 
                horizontal=True, 
                key="moderation_rating",
                on_change=on_rating_change,
                index=None # Убираем выбор по умолчанию
            )
            
            st.markdown("---")
            # Передаем ID текущего паттерна в callback кнопки "Пропустить"
            st.button("Пропустить", use_container_width=True, on_click=load_next_pattern, args=(pattern['id'],))

    else:
        st.info("Паттерны, соответствующие заданным фильтрам, не найдены или уже отмодерированы.")
        st.info("Попробуйте изменить фильтры или выбрать другую длину паттерна.")
        if st.button("Проверить снова"):
            load_next_pattern()

