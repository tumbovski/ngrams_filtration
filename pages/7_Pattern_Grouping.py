import streamlit as st
import pandas as pd
from core.database import (
    get_db_connection,
    create_theme,
    get_all_themes,
    update_theme,
    delete_theme,
    associate_pattern_with_theme,
    get_themes_for_pattern,
    remove_pattern_from_theme,
    get_pattern_by_id,
    get_moderation_history
)

st.set_page_config(page_title="Группировка паттернов", layout="wide")

conn = get_db_connection()

# --- Вспомогательные функции для обновления UI ---
def refresh_themes():
    st.session_state.all_themes = get_all_themes(conn)

def refresh_pattern_associations(pattern_id):
    st.session_state.current_pattern_associations = get_themes_for_pattern(conn, pattern_id)

def load_pattern_from_moderated_list(pattern_id):
    st.session_state.current_pattern_id = pattern_id
    st.session_state.current_pattern_data = get_pattern_by_id(conn, pattern_id)
    if st.session_state.current_pattern_data:
        refresh_pattern_associations(pattern_id)
        st.success(f"Паттерн #{pattern_id} загружен из списка отмодерированных.")
    else:
        st.error(f"Паттерн с ID {pattern_id} не найден.")
        st.session_state.current_pattern_id = None
        st.session_state.current_pattern_data = None
        st.session_state.current_pattern_associations = []

# --- Инициализация состояния ---
if 'all_themes' not in st.session_state:
    refresh_themes()
if 'current_pattern_id' not in st.session_state: st.session_state.current_pattern_id = None
if 'current_pattern_data' not in st.session_state: st.session_state.current_pattern_data = None
if 'current_pattern_associations' not in st.session_state: st.session_state.current_pattern_associations = []

# --- Заголовок страницы ---
st.title("Группировка паттернов по темам")

# --- Раздел управления темами ---
st.header("Управление темами")

# Форма для создания новой темы
with st.expander("Создать новую тему", expanded=False):
    with st.form("new_theme_form", clear_on_submit=True):
        new_theme_name = st.text_input("Название темы", key="new_theme_name")
        new_theme_description = st.text_area("Описание темы (необязательно)", key="new_theme_description")
        
        # Выбор родительской темы
        parent_theme_options = [("Нет", None)] + [(t['name'], t['id']) for t in st.session_state.all_themes]
        selected_parent_theme_name = st.selectbox("Родительская тема (необязательно)", options=[opt[0] for opt in parent_theme_options], format_func=lambda x: x, key="new_theme_parent")
        selected_parent_theme_id = next((opt[1] for opt in parent_theme_options if opt[0] == selected_parent_theme_name), None)

        submitted = st.form_submit_button("Создать тему")
        if submitted:
            if new_theme_name:
                if create_theme(conn, new_theme_name, new_theme_description, selected_parent_theme_id):
                    st.success(f"Тема '{new_theme_name}' успешно создана!")
                    refresh_themes()
                    st.rerun()
                else:
                    st.error("Ошибка при создании темы. Возможно, тема с таким названием уже существует.")
            else:
                st.warning("Название темы не может быть пустым.")

# Отображение существующих тем
if st.session_state.all_themes:
    st.subheader("Существующие темы")
    themes_df = pd.DataFrame(st.session_state.all_themes)
    themes_df['parent_theme_name'] = themes_df['parent_theme_id'].apply(lambda x: next((t['name'] for t in st.session_state.all_themes if t['id'] == x), None))
    themes_df_display = themes_df[['id', 'name', 'description', 'parent_theme_name']]
    themes_df_display.columns = ['ID', 'Название', 'Описание', 'Родительская тема']

    st.dataframe(themes_df_display, use_container_width=True, hide_index=True)

    # Форма для редактирования/удаления тем
    st.subheader("Редактировать / Удалить тему")
    with st.form("edit_delete_theme_form"):
        theme_to_edit_id = st.selectbox("Выберите тему для редактирования или удаления", options=[t['id'] for t in st.session_state.all_themes], format_func=lambda x: next((t['name'] for t in st.session_state.all_themes if t['id'] == x), str(x)))
        
        current_theme_data = next((t for t in st.session_state.all_themes if t['id'] == theme_to_edit_id), None)
        if current_theme_data:
            edited_name = st.text_input("Название", value=current_theme_data['name'], key="edit_theme_name")
            edited_description = st.text_area("Описание", value=current_theme_data['description'], key="edit_theme_description")
            
            # Выбор родительской темы для редактирования
            edit_parent_theme_options = [("Нет", None)] + [(t['name'], t['id']) for t in st.session_state.all_themes if t['id'] != theme_to_edit_id] # Нельзя выбрать себя как родителя
            initial_parent_name = next((opt[0] for opt in edit_parent_theme_options if opt[1] == current_theme_data['parent_theme_id']), "Нет")
            edited_parent_theme_name = st.selectbox("Родительская тема", options=[opt[0] for opt in edit_parent_theme_options], format_func=lambda x: x, index=[opt[0] for opt in edit_parent_theme_options].index(initial_parent_name), key="edit_theme_parent")
            edited_parent_theme_id = next((opt[1] for opt in edit_parent_theme_options if opt[0] == edited_parent_theme_name), None)

            col_edit, col_delete = st.columns(2)
            with col_edit:
                update_submitted = st.form_submit_button("Обновить тему")
            with col_delete:
                delete_submitted = st.form_submit_button("Удалить тему", type="secondary")

            if update_submitted:
                if edited_name:
                    if update_theme(conn, theme_to_edit_id, edited_name, edited_description, edited_parent_theme_id):
                        st.success(f"Тема '{edited_name}' успешно обновлена!")
                        refresh_themes()
                        st.rerun()
                    else:
                        st.error("Ошибка при обновлении темы. Возможно, тема с таким названием уже существует.")
                else:
                    st.warning("Название темы не может быть пустым.")
            
            if delete_submitted:
                if delete_theme(conn, theme_to_edit_id):
                    st.success(f"Тема '{current_theme_data['name']}' успешно удалена!")
                    refresh_themes()
                    st.rerun()
                else:
                    st.error("Ошибка при удалении темы.")
        else:
            st.info("Выберите тему для редактирования или удаления.")

st.markdown("---")

# --- Раздел группировки паттернов ---
st.header("Группировка паттернов")

# --- Отображение отмодерированных паттернов из истории модерации ---
st.subheader("Отмодерированные паттерны (из вашей истории модерации)")

if 'user_id' not in st.session_state:
    st.warning("Пожалуйста, войдите в систему, чтобы просмотреть историю модерации.")
else:
    user_id = st.session_state.user_id
    moderation_history = get_moderation_history(conn, user_id)

    if moderation_history:
        # Создаем DataFrame из истории модерации
        history_df = pd.DataFrame(moderation_history)
        
        # Группируем по pattern_id, чтобы получить уникальные паттерны
        # и берем последнюю оценку (или максимальную, если нужно)
        # Для простоты возьмем последнюю запись для каждого паттерна
        unique_patterns_from_history = history_df.sort_values(by='submitted_at', ascending=False).drop_duplicates(subset=['pattern_id'], keep='first')
        
        # Сортируем по оценке в убывающем порядке
        unique_patterns_from_history = unique_patterns_from_history.sort_values(by='rating', ascending=False)

        moderated_patterns_df_display = unique_patterns_from_history[['pattern_id', 'pattern_text', 'rating', 'comment', 'submitted_at']]
        moderated_patterns_df_display.columns = ['ID Паттерна', 'Текст Паттерна', 'Ваша Оценка', 'Комментарий', 'Дата Модерации']
        
        st.dataframe(
            moderated_patterns_df_display,
            use_container_width=True,
            hide_index=True,
            selection_mode="single-row",
            key="moderated_patterns_table"
        )

        selected_rows = st.session_state.moderated_patterns_table['selection']
        if selected_rows:
            selected_pattern_id = selected_rows[0]['ID Паттерна']
            if st.button(f"Загрузить паттерн #{selected_pattern_id} для группировки"):
                load_pattern_from_moderated_list(selected_pattern_id)
        else:
            st.info("Выберите паттерн из списка выше, чтобы загрузить его для группировки.")
    else:
        st.info("В вашей истории модерации нет записей.")

st.markdown("---")

# Выбор паттерна для группировки
with st.form("select_pattern_form"):
    pattern_id_input = st.number_input("Введите ID паттерна", min_value=1, format="%d", key="pattern_id_input")
    load_pattern_button = st.form_submit_button("Загрузить паттерн")

    if load_pattern_button and pattern_id_input:
        st.session_state.current_pattern_id = pattern_id_input
        st.session_state.current_pattern_data = get_pattern_by_id(conn, pattern_id_input)
        if st.session_state.current_pattern_data:
            refresh_pattern_associations(pattern_id_input)
            st.success(f"Паттерн #{pattern_id_input} загружен.")
        else:
            st.error(f"Паттерн с ID {pattern_id_input} не найден.")
            st.session_state.current_pattern_id = None
            st.session_state.current_pattern_data = None
            st.session_state.current_pattern_associations = []

if st.session_state.current_pattern_data:
    st.subheader(f"Выбранный паттерн: #{st.session_state.current_pattern_data['id']}")
    st.code(st.session_state.current_pattern_data['text'], language='bash')
    st.write(f"Длина: {st.session_state.current_pattern_data['len']}")
    st.write(f"Частота: {st.session_state.current_pattern_data['freq']:.2f}")
    st.write(f"Количество: {st.session_state.current_pattern_data['qty']}")

    # Управление ассоциациями
    st.subheader("Связать с темой")
    if st.session_state.all_themes:
        available_themes_for_association = [t for t in st.session_state.all_themes if t['id'] not in [assoc['id'] for assoc in st.session_state.current_pattern_associations]]
        
        if available_themes_for_association:
            theme_to_associate_id = st.selectbox("Выберите тему для связывания", options=[t['id'] for t in available_themes_for_association], format_func=lambda x: next((t['name'] for t in available_themes_for_association if t['id'] == x), str(x)), key="theme_to_associate_select")
            if st.button("Связать паттерн с выбранной темой"):
                if associate_pattern_with_theme(conn, st.session_state.current_pattern_id, theme_to_associate_id):
                    st.success(f"Паттерн #{st.session_state.current_pattern_id} связан с темой '{next((t['name'] for t in st.session_state.all_themes if t['id'] == theme_to_associate_id), '')}'.")
                    refresh_pattern_associations(st.session_state.current_pattern_id)
                    st.rerun()
                else:
                    st.error("Ошибка при связывании паттерна с темой.")
        else:
            st.info("Все доступные темы уже связаны с этим паттерном.")
    else:
        st.info("Нет доступных тем. Создайте их в разделе 'Управление темами'.")

    st.subheader("Текущие ассоциации паттерна")
    if st.session_state.current_pattern_associations:
        associations_df = pd.DataFrame(st.session_state.current_pattern_associations)
        associations_df.columns = ['ID', 'Название темы']
        st.dataframe(associations_df, use_container_width=True, hide_index=True)

        theme_to_remove_id = st.selectbox("Выберите тему для отвязки", options=[t['id'] for t in st.session_state.current_pattern_associations], format_func=lambda x: next((t['name'] for t in st.session_state.current_pattern_associations if t['id'] == x), str(x)), key="theme_to_remove_select")
        if st.button("Отвязать паттерн от выбранной темы"):
            if remove_pattern_from_theme(conn, st.session_state.current_pattern_id, theme_to_remove_id):
                st.success(f"Паттерн #{st.session_state.current_pattern_id} отвязан от темы '{next((t['name'] for t in st.session_state.all_themes if t['id'] == theme_to_remove_id), '')}'.")
                refresh_pattern_associations(st.session_state.current_pattern_id)
                st.rerun()
            else:
                st.error("Ошибка при отвязке паттерна от темы.")
    else:
        st.info("Этот паттерн пока не связан ни с одной темой.")
