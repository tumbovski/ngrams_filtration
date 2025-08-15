import streamlit as st
import pandas as pd
from core.database import (
    get_db_connection,
    get_user_by_login,
    get_moderation_history,
    update_moderation_entry,
    process_moderation_submission,
    get_examples_by_pattern_id,
    delete_moderation_record # Импортируем новую функцию
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

# --- UI State Management ---
if 'editing_entry_id' not in st.session_state:
    st.session_state.editing_entry_id = None
if 'show_phrases_for_pattern' not in st.session_state:
    st.session_state.show_phrases_for_pattern = {}

# --- Callback Functions ---
def set_editing_entry(entry_id):
    st.session_state.editing_entry_id = entry_id
    # When entering edit mode, hide phrases for all patterns to avoid clutter
    st.session_state.show_phrases_for_pattern = {}


def save_edited_entry(entry_id, new_rating, new_comment, new_tag):
    if update_moderation_entry(conn, entry_id, new_rating, new_comment, new_tag):
        st.toast("Запись успешно обновлена!", icon="✅")
        st.session_state.editing_entry_id = None # Exit edit mode
        refresh_moderation_history() # Refresh data
    else:
        st.error("Ошибка при обновлении записи.")

def cancel_edit():
    st.session_state.editing_entry_id = None

def delete_entry(entry_id):
    """Callback to delete a moderation record."""
    success, pattern_id = delete_moderation_record(conn, entry_id)
    if success:
        st.toast(f"Запись {entry_id} удалена.", icon="🗑️")
        if pattern_id:
            # Recalculate stats for the affected pattern
            process_moderation_submission(conn, pattern_id)
        refresh_moderation_history()
        # Ensure we exit edit mode if the deleted entry was being edited
        if st.session_state.editing_entry_id == entry_id:
            st.session_state.editing_entry_id = None
    else:
        st.error("Ошибка при удалении записи.")

def toggle_phrase_display(entry_id):
    """Toggle the visibility of the phrase list for a given entry."""
    st.session_state.show_phrases_for_pattern[entry_id] = not st.session_state.show_phrases_for_pattern.get(entry_id, False)


# --- Display Moderation History ---
if not st.session_state.moderation_history:
    st.info("У вас пока нет записей модерации.")
else:
    for entry in st.session_state.moderation_history:
        entry_id = entry['id']
        is_editing = st.session_state.editing_entry_id == entry_id
        
        with st.expander(f"Паттерн: {entry['pattern_text']} (ID записи: {entry_id})", expanded=is_editing):
            if is_editing:
                # --- Edit Mode ---
                st.subheader("Редактирование записи")
                
                new_rating = st.slider("Оценка", 1, 5, entry['rating'], key=f"rating_{entry_id}")
                new_comment = st.text_area("Комментарий", entry['comment'], key=f"comment_{entry_id}")
                new_tag = st.text_input("Тег", entry['tag'], key=f"tag_{entry_id}")

                col1, col2, _ = st.columns([1, 1, 5])
                with col1:
                    st.button("Сохранить", on_click=save_edited_entry, args=(entry_id, new_rating, new_comment, new_tag), key=f"save_{entry_id}", use_container_width=True)
                with col2:
                    st.button("Отмена", on_click=cancel_edit, key=f"cancel_{entry_id}", use_container_width=True)
            else:
                # --- View Mode ---
                date_str = f"| **Дата:** {entry['submitted_at'].strftime('%Y-%m-%d %H:%M:%S')}" if entry.get('submitted_at') else ""
                st.write(f"**Оценка:** {entry['rating']} {date_str}")
                st.write(f"**Комментарий:** {entry['comment'] if entry['comment'] else 'Нет'}")
                st.write(f"**Тег:** {entry['tag'] if entry['tag'] else 'Нет'}")
                
                st.markdown("---")
                
                col_edit, col_delete, col_phrases, _ = st.columns([1.2, 1, 1.5, 4])
                with col_edit:
                    st.button("Редактировать", on_click=set_editing_entry, args=(entry_id,), key=f"edit_{entry_id}", use_container_width=True)
                
                with col_delete:
                    st.button("Удалить", on_click=delete_entry, args=(entry_id,), key=f"delete_{entry_id}", type="primary", use_container_width=True)

                with col_phrases:
                    show_phrases = st.session_state.show_phrases_for_pattern.get(entry_id, False)
                    button_label = "Показать фразы" if not show_phrases else "Скрыть фразы"
                    st.button(button_label, key=f"toggle_phrases_{entry_id}", on_click=toggle_phrase_display, args=(entry_id,), use_container_width=True)

                if st.session_state.show_phrases_for_pattern.get(entry_id, False):
                    st.subheader("Фразы, соответствующие паттерну")
                    phrases_data = get_examples_by_pattern_id(conn, entry['pattern_id'])
                    if phrases_data:
                        df_phrases = pd.DataFrame(phrases_data, columns=["Фраза", "Частотность (млн)"])
                        # Меняем порядок столбцов и отключаем растягивание по ширине
                        st.dataframe(df_phrases[["Частотность (млн)", "Фраза"]], use_container_width=False, hide_index=True, height=300)
                    else:
                        st.info("Нет фраз, соответствующих этому паттерну.")


if st.button("Обновить историю"):
    refresh_moderation_history()
    st.rerun()