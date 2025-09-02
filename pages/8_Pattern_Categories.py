import streamlit as st
import pandas as pd
from core.database import (
    get_db_connection, 
    get_category_tree, 
    get_pattern_by_id,
    get_examples_by_pattern_id, 
    count_patterns_for_category,
    get_patterns_for_category
)

st.set_page_config(page_title="Категории паттернов", layout="wide")

conn = get_db_connection()

# --- Кэширование данных ---

@st.cache_data(ttl=3600)
def cached_get_pattern_by_id(pattern_id):
    """Кэшированная функция для получения данных паттерна по ID."""
    return get_pattern_by_id(pattern_id)

@st.cache_data(ttl=3600)
def cached_get_examples_by_pattern_id(pattern_id):
    """Кэшированная функция для получения примеров фраз."""
    db_conn = get_db_connection()
    if not db_conn: return []
    try:
        examples = get_examples_by_pattern_id(db_conn, pattern_id)
        return [{"text": row[0], "freq": row[1]} for row in examples]
    finally:
        if db_conn: db_conn.close()

@st.cache_data(ttl=3600)
def cached_count_patterns_for_category(category_id):
    """Кэшированная функция для подсчета паттернов в категории."""
    # Используем прямое подключение, т.к. основное 'conn' может быть занято
    with get_db_connection() as db_conn:
        return count_patterns_for_category(db_conn, category_id) if db_conn else 0

# --- Функции для отображения ---

def display_category_tree(categories, level=0):
    """Рекурсивно отображает дерево категорий с помощью st.expander."""
    for category in categories:
        indent = "&nbsp;" * 4 * level
        expander_label = f"{indent}📁 {category['name']}"
        with st.expander(expander_label):
            st.caption(f"{category.get('description', 'Нет описания')}")
            # Кнопка для выбора категории
            if st.button(f"Выбрать '{category['name']}'", key=f"select_cat_{category['id']}"):
                st.session_state.selected_category_id = category['id']
                st.session_state.selected_category_name = category['name']
                # Сбрасываем ID паттерна при выборе новой категории
                st.session_state.pattern_id_input = None
                # Сбрасываем страницу при выборе новой категории
                st.session_state.current_page = 1
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
if 'pattern_id_input' not in st.session_state:
    st.session_state.pattern_id_input = None
if 'current_page' not in st.session_state:
    st.session_state.current_page = 1

# --- Основная часть страницы ---
st.title("Анализ паттернов по категориям")

st.write("На этой странице вы можете просматривать иерархию категорий, список паттернов в них, а также анализировать конкретные паттерны, вводя их ID.")

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
    # --- Раздел паттернов в категории ---
    st.header("Паттерны в категории")
    if st.session_state.selected_category_id:
        category_name = st.session_state.selected_category_name
        category_id = st.session_state.selected_category_id
        
        with st.spinner(f"Подсчет паттернов в категории '{category_name}'..."):
            pattern_count = cached_count_patterns_for_category(category_id)
        
        st.info(f"Выбрана категория: **{category_name}**. В ней найдено **{pattern_count:,}** паттернов.".replace(',', ' '))

        if pattern_count > 0:
            PAGE_SIZE = 50
            total_pages = (pattern_count + PAGE_SIZE - 1) // PAGE_SIZE

            # Убедимся, что текущая страница в допустимых пределах
            if st.session_state.current_page > total_pages:
                st.session_state.current_page = total_pages
            
            with st.spinner("Загрузка списка паттернов..."):
                patterns = get_patterns_for_category(conn, category_id, page=st.session_state.current_page, page_size=PAGE_SIZE)

            if patterns:
                df = pd.DataFrame(patterns)
                df_display = df[['id', 'text', 'freq', 'qty']]
                df_display.columns = ['ID', 'Текст паттерна', 'Частота (ipm)', 'Кол-во']
                st.dataframe(df_display, use_container_width=True, hide_index=True)

                # Пагинация
                page_cols = st.columns([1.5, 1.5, 1, 5])
                
                def prev_page(): st.session_state.current_page -= 1
                def next_page(): st.session_state.current_page += 1

                page_cols[0].button("◀️ Назад", on_click=prev_page, disabled=st.session_state.current_page <= 1, use_container_width=True)
                page_cols[1].button("Вперед ▶️", on_click=next_page, disabled=st.session_state.current_page >= total_pages, use_container_width=True)
                with page_cols[2]:
                    st.write(f"Стр. {st.session_state.current_page} / {total_pages}")
            else:
                st.info("На этой странице нет паттернов.")
    else:
        st.info("Выберите категорию слева, чтобы увидеть список паттернов.")

    st.divider()

    # --- Раздел анализа по ID ---
    with st.container(border=True):
        st.subheader("Анализ паттерна по ID")
        pattern_id = st.number_input("Введите ID паттерна:", min_value=1, step=1, value=None, key="pattern_id_input")

        if pattern_id:
            with st.spinner(f"Загрузка данных для паттерна ID {pattern_id}..."):
                pattern_data = cached_get_pattern_by_id(pattern_id)
            
            if pattern_data:
                st.markdown(f"##### Паттерн: `{pattern_data['text']}`")
                st.markdown(f"**ID:** {pattern_data['id']} | **F:** {pattern_data['freq']:.2f} | **Q:** {pattern_data['qty']}")
                
                if pattern_data.get('categories'):
                    categories_str = ", ".join(pattern_data['categories'])
                    st.markdown(f"**Присвоенные категории:** {categories_str}")
                else:
                    st.markdown("**Категории не присвоены.**")
                
                with st.expander("Показать примеры фраз"):
                    with st.spinner("Загрузка примеров..."):
                        examples = cached_get_examples_by_pattern_id(pattern_id)
                    
                    if examples:
                        df_examples = pd.DataFrame(examples)
                        df_examples.rename(columns={'text': 'Фраза', 'freq': 'Частотность (ipm)'}, inplace=True)
                        st.dataframe(df_examples, use_container_width=True, hide_index=True)
                    else:
                        st.info("Примеры фраз для этого паттерна не найдены.")
            else:
                st.error(f"Паттерн с ID {pattern_id} не найден.")
