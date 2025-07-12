import streamlit as st
from core.database import get_db_connection, authenticate_user, get_user_by_id

st.set_page_config(
    page_title="Главная",
    page_icon="👋",
    layout="wide"
)

# Initialize session state for login
if 'logged_in' not in st.session_state: st.session_state.logged_in = False
if 'user_role' not in st.session_state: st.session_state.user_role = None
if 'user_nickname' not in st.session_state: st.session_state.user_nickname = None
if 'user_login' not in st.session_state: st.session_state.user_login = None
if 'user_id' not in st.session_state: st.session_state.user_id = None

conn = get_db_connection()

# Auto-login from URL query parameter
if not st.session_state.logged_in:
    query_params = st.query_params
    if "user_id" in query_params:
        user_id_from_url = query_params["user_id"][0]
        user = get_user_by_id(conn, user_id_from_url)
        if user and user['status'] == 'active':
            st.session_state.logged_in = True
            st.session_state.user_role = user['role']
            st.session_state.user_nickname = user['nickname']
            st.session_state.user_login = user['login']
            st.session_state.user_id = user['id']
            st.success(f"Автоматический вход: Добро пожаловать, {user['nickname']}!")
            st.query_params.user_id = user['id']
        else:
            st.query_params.user_id = None # Clear invalid user_id

def login_user(username, password, remember_me):
    user = authenticate_user(conn, username, password)
    if user:
        if user['status'] == 'active':
            st.session_state.logged_in = True
            st.session_state.user_role = user['role']
            st.session_state.user_nickname = user['nickname']
            st.session_state.user_login = user['login']
            st.session_state.user_id = user['id']
            st.success(f"Добро пожаловать, {user['nickname']}!")
            if remember_me:
                st.query_params.user_id = user['id']
            else:
                st.query_params.user_id = None
            st.rerun()
        else:
            st.error("Ваш аккаунт отключен. Обратитесь к администратору.")
    else:
        st.error("Неверный логин или пароль.")

def logout_user():
    st.session_state.logged_in = False
    st.session_state.user_role = None
    st.session_state.user_nickname = None
    st.session_state.user_login = None
    st.session_state.user_id = None
    st.experimental_set_query_params(user_id=None) # Clear user_id from URL
    st.info("Вы вышли из системы.")
    st.rerun()

if not st.session_state.logged_in:
    st.title("Вход в приложение")
    login = st.text_input("Логин")
    password = st.text_input("Пароль", type="password")
    remember_me = st.checkbox("Запомнить меня")
    if st.button("Войти"):
        login_user(login, password, remember_me)
else:
    st.title(f"Добро пожаловать, {st.session_state.user_nickname}!")
    st.sidebar.success("Выберите страницу для работы.")

    if st.session_state.user_role == 'admin':
        st.sidebar.page_link("pages/_Admin_Panel.py", label="Панель администратора", icon="⚙️")

    st.markdown(
        """
        Это приложение предназначено для интерактивной фильтрации и анализа синтаксических n-грамм.

        **👈 Выберите страницу из меню слева**, чтобы начать работу.

        ### Что можно делать?
        - **Фильтрация фраз:** Конструируйте сложные фильтры для поиска фраз по лингвистическим признакам.
        - **Анализ результатов:** Изучайте частотность слов на разных позициях в найденных фразах.
        """
    )
    st.sidebar.button("Выйти", on_click=logout_user)
