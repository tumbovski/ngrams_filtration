import streamlit as st
import pandas as pd
from core.database import (
    get_db_connection,
    get_user_by_login,
    get_moderation_history,
    update_moderation_entry,
    process_moderation_submission,
    get_examples_by_pattern_id
)

st.set_page_config(layout="wide", page_title="Moderation History")

# Check authentication status
if 'logged_in' not in st.session_state or not st.session_state.logged_in or 'user_login' not in st.session_state:
    st.warning("Пожалуйста, войдите в систему, чтобы получить доступ к этой странице.")
    st.switch_page("Home.py")

st.title("История моих модераций")

# --- Database Connection ---
@st.cache_resource
def init_connection():
    return get_db_connection()

conn = init_connection()

if not conn:
    st.error("Не удалось подключиться к базе данных. Проверьте настройки в .env файле и доступность сервера.")
    st.stop()

# --- Get User ID ---
current_user = get_user_by_login(conn, st.session_state.user_login)
if not current_user:
    st.error("Не удалось получить данные пользователя. Пожалуйста, попробуйте войти снова.")
    st.stop()
user_id = current_user['id']

# --- Helper for refreshing history ---
def refresh_moderation_history():
    st.session_state.moderation_history = get_moderation_history(conn, user_id)

# Initialize or refresh history
if 'moderation_history' not in st.session_state:
    refresh_moderation_history()

# --- Edit/Save Logic ---
if 'editing_entry_id' not in st.session_state:
    st.session_state.editing_entry_id = None
if 'show_phrases_for_pattern' not in st.session_state:
    st.session_state.show_phrases_for_pattern = {}

def set_editing_entry(entry_id):
    st.session_state.editing_entry_id = entry_id

def save_edited_entry(entry_id, new_rating, new_comment, new_tag):
    if update_moderation_entry(conn, entry_id, new_rating, new_comment, new_tag):
        st.toast("Запись успешно обновлена!", icon="✅")
        st.session_state.editing_entry_id = None # Exit edit mode
        refresh_moderation_history() # Refresh data
        st.rerun() # Rerun to update UI
    else:
        st.error("Ошибка при обновлении записи.")

def cancel_edit():
    st.session_state.editing_entry_id = None

def toggle_phrase_display(entry_id):
    if entry_id not in st.session_state.show_phrases_for_pattern:
        st.session_state.show_phrases_for_pattern[entry_id] = False
    st.session_state.show_phrases_for_pattern[entry_id] = not st.session_state.show_phrases_for_pattern[entry_id]

# --- Display Moderation History ---
if not st.session_state.moderation_history:
    st.info("У вас пока нет записей модерации.")
else:
    for entry in st.session_state.moderation_history:
        entry_id = entry['id']
        
        with st.expander(f"Паттерн: {entry['pattern_text']} (ID: {entry_id})", expanded=(st.session_state.editing_entry_id == entry_id)):
            if st.session_state.editing_entry_id == entry_id:
                # Edit mode
                st.subheader("Редактирование записи")
                
                new_rating = st.slider("Оценка", 1, 5, entry['rating'], key=f"rating_{entry_id}")
                new_comment = st.text_area("Комментарий", entry['comment'], key=f"comment_{entry_id}")
                new_tag = st.text_input("Тег", entry['tag'], key=f"tag_{entry_id}")

                col1, col2 = st.columns(2)
                with col1:
                    st.button("Сохранить", on_click=save_edited_entry, args=(entry_id, new_rating, new_comment, new_tag), key=f"save_{entry_id}")
                with col2:
                    st.button("Отмена", on_click=cancel_edit, key=f"cancel_{entry_id}")
            else:
                # View mode
                st.write(f"**Оценка:** {entry['rating']}")
                st.write(f"**Комментарий:** {entry['comment'] if entry['comment'] else 'Нет'}")
                st.write(f"**Тег:** {entry['tag'] if entry['tag'] else 'Нет'}")
                
                col_edit, col_phrases = st.columns([0.2, 0.8]) # Use columns for buttons
                with col_edit:
                    st.button("Редактировать", on_click=set_editing_entry, args=(entry_id,), key=f"edit_{entry_id}")
                with col_phrases:
                    # Button to show/hide phrases
                    show_phrases = st.session_state.show_phrases_for_pattern.get(entry_id, False)
                    button_label = "Показать фразы" if not show_phrases else "Скрыть фразы"
                    if st.button(button_label, key=f"toggle_phrases_{entry_id}"):
                        toggle_phrase_display(entry_id)

                if show_phrases:
                    st.subheader("Фразы, соответствующие паттерну")
                    # Fetch phrases here using entry['pattern_id']
                    phrases_data = get_examples_by_pattern_id(conn, entry['pattern_id'])
                    if phrases_data:
                        df_phrases = pd.DataFrame(phrases_data, columns=["Фраза", "Частотность (млн)"])
                        st.dataframe(df_phrases, use_container_width=True, hide_index=True, height=300) # Smaller height for history page
                    else:
                        st.info("Нет фраз, соответствующих этому паттерну.")
