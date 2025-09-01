import streamlit as st
import pandas as pd
from core.database import get_db_connection, get_category_tree, get_patterns_for_category

st.set_page_config(page_title="Категории паттернов", layout="wide")

conn = get_db_connection()

# --- Функции для отображения ---

def display_category_tree(categories, level=0):
    """Рекурсивно отображает дерево категорий с помощью st.expander."""
    for category in categories:
        indent = "&nbsp;" * 4 * level
        expander_label = f"{indent}📁 {category['name']}"
        with st.expander(expander_label):
            st.markdown(f"**Описание:** {category.get('description', 'Нет')}")
            
            # Кнопка для выбора категории
            if st.button(f"Показать паттерны в '{category['name']}'", key=f"select_cat_{category['id']}"):
                st.session_state.selected_category_id = category['id']
                st.session_state.selected_category_name = category['name']

            # Рекурсивный вызов для дочерних категорий
            if category['children']:
                display_category_tree(category['children'], level + 1)

# --- Инициализация состояния ---
if 'category_tree' not in st.session_state:
    st.session_state.category_tree = get_category_tree(conn)
if 'selected_category_id' not in st.session_state:
    st.session_state.selected_category_id = None
if 'selected_category_name' not in st.session_state:
    st.session_state.selected_category_name = None

# --- Основная часть страницы ---
st.title("Иерархия категорий паттернов")

st.write("На этой странице вы можете просматривать иерархию категорий и паттерны, которые были автоматически классифицированы в них.")

if st.button("Обновить дерево категорий"):
    st.session_state.category_tree = get_category_tree(conn)
    st.rerun()

col1, col2 = st.columns([1, 2])

with col1:
    st.header("Дерево категорий")
    if st.session_state.category_tree:
        display_category_tree(st.session_state.category_tree)
    else:
        st.warning("Категории не найдены. Запустите скрипт классификации.")

with col2:
    st.header("Паттерны в категории")
    if st.session_state.selected_category_id:
        st.subheader(f"Категория: **{st.session_state.selected_category_name}**")
        
        patterns = get_patterns_for_category(conn, st.session_state.selected_category_id)
        
        if patterns:
            df = pd.DataFrame(patterns)
            df_display = df[['id', 'text', 'freq', 'qty']]
            df_display.columns = ['ID', 'Текст паттерна', 'Частота (ipm)', 'Кол-во']
            st.dataframe(df_display, use_container_width=True, hide_index=True)
        else:
            st.info("В этой категории нет паттернов.")
    else:
        st.info("Выберите категорию слева, чтобы увидеть связанные с ней паттерны.")
