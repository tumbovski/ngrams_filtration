import streamlit as st
import pandas as pd
from core.database import get_db_connection, get_next_unmoderated_pattern, count_unmoderated_patterns, get_ngrams_by_pattern_text, save_moderation_record, process_moderation_submission

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
st.session_state.setdefault('current_pattern_to_moderate', None)
st.session_state.setdefault('remaining_patterns_count', 0)

# --- Helper Functions ---
def load_next_pattern():
    if st.session_state.selected_phrase_length and st.session_state.user_id:
        st.session_state.current_pattern_to_moderate = get_next_unmoderated_pattern(
            conn, st.session_state.user_id, st.session_state.selected_phrase_length
        )
        st.session_state.remaining_patterns_count = count_unmoderated_patterns(
            conn, st.session_state.user_id, st.session_state.selected_phrase_length
        )
    else:
        st.session_state.current_pattern_to_moderate = None
        st.session_state.remaining_patterns_count = 0

def handle_phrase_length_change():
    st.session_state.selected_phrase_length = st.session_state.phrase_length_selector # Update session state
    st.session_state.current_pattern_to_moderate = None # Reset current pattern
    load_next_pattern()

def submit_moderation_action(pattern_id, user_id, rating, comment, tag):
    if save_moderation_record(conn, pattern_id, user_id, rating, comment, tag):
        process_moderation_submission(conn, pattern_id) # Process final rating/tag/comment
        st.success("Модерация успешно сохранена!")
        load_next_pattern() # Load next pattern
        st.rerun()
    else:
        st.error("Ошибка при сохранении модерации.")

# --- Main UI ---
st.title("Модерация паттернов")

# Phrase Length Selection
phrase_lengths_options = list(range(2, 13)) # 2 to 12
selected_length_ui = st.selectbox(
    "Выберите длину паттерна для модерации:",
    options=phrase_lengths_options,
    key="phrase_length_selector",
    on_change=handle_phrase_length_change,
    index=phrase_lengths_options.index(st.session_state.selected_phrase_length) if st.session_state.selected_phrase_length in phrase_lengths_options else 0
)

# Update session state if selectbox value changes
if selected_length_ui != st.session_state.selected_phrase_length:
    st.session_state.selected_phrase_length = selected_length_ui
    handle_phrase_length_change()

# Load initial pattern if length is selected and no pattern is loaded
if st.session_state.selected_phrase_length and st.session_state.current_pattern_to_moderate is None:
    load_next_pattern()
    st.rerun()

# Main layout with two columns
moderation_details_col, ngrams_table_col = st.columns([2, 1])

with moderation_details_col:
    if st.session_state.current_pattern_to_moderate:
        pattern = st.session_state.current_pattern_to_moderate
        st.subheader(f"Паттерн: {pattern['pattern_text']}")
        st.write(f"Длина фразы: {pattern['phrase_length']}")
        st.write(f"Общая частотность: {pattern['total_frequency']:.3f}")
        st.write(f"Общее количество фраз: {pattern['total_quantity']}")
        st.write(f"Осталось неотмодерированных паттернов: {st.session_state.remaining_patterns_count}")

        st.markdown("---")
        st.subheader("Ваша модерация")
        
        user_id = st.session_state.user_id
        if user_id is None:
            st.warning("Не удалось получить ID пользователя. Пожалуйста, войдите снова.")
        else:
            rating = st.radio("Оценка", options=[1, 2, 3, 4, 5], index=2, horizontal=True)
            comment = st.text_area("Комментарий")
            tag = st.text_input("Тег/Тип")

            if st.button("Готово"):
                submit_moderation_action(pattern['id'], user_id, rating, comment, tag)

    else:
        st.info("Выберите длину паттерна, чтобы начать модерацию.")

with ngrams_table_col:
    if st.session_state.current_pattern_to_moderate:
        pattern = st.session_state.current_pattern_to_moderate
        st.subheader("Фразы, соответствующие паттерну")
        ngrams_for_pattern = get_ngrams_by_pattern_text(conn, pattern['pattern_text'])
        if ngrams_for_pattern:
            df_ngrams = pd.DataFrame(ngrams_for_pattern, columns=["Фраза", "Частотность (млн)"])
            st.dataframe(df_ngrams, use_container_width=True, hide_index=True, height=600)
        else:
            st.info("Нет фраз, соответствующих этому паттерну.")
