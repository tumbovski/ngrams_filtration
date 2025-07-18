import streamlit as st
from core.database import get_db_connection, authenticate_user, get_user_by_id
from streamlit_cookies_manager import CookieManager

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
cookies = CookieManager()

# Auto-login from URL query parameter or cookie
if not st.session_state.logged_in:
    user_id_from_cookie = cookies.get('user_id')
    
    user_id_to_check = None

    # Prioritize cookie over URL query param for auto-login
    if user_id_from_cookie and user_id_from_cookie.isdigit():
        user_id_to_check = int(user_id_from_cookie)
    else:
        query_params = st.query_params
        if "user_id" in query_params and query_params["user_id"][0].isdigit():
            user_id_to_check = int(query_params["user_id"][0])

    if user_id_to_check:
        user = get_user_by_id(conn, user_id_to_check)
        if user and user['status'] == 'active':
            st.session_state.logged_in = True
            st.session_state.user_role = user['role']
            st.session_state.user_nickname = user['nickname']
            st.session_state.user_login = user['login']
            st.session_state.user_id = user['id']
            st.success(f"Автоматический вход: Добро пожаловать, {user['nickname']}!")
            # Ensure cookie is set if auto-logged in via URL
            if not user_id_from_cookie:
                cookies['user_id'] = str(user['id'])
                cookies.save()
        else:
            # Clear invalid user_id from cookie and URL
            if user_id_from_cookie:
                cookies.delete('user_id')
                cookies.save()
            if "user_id" in st.query_params:
                del st.query_params["user_id"]

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
                cookies['user_id'] = str(user['id'])
                cookies.save()
            else:
                del cookies['user_id']
                cookies.save()
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
    del cookies['user_id'] # Clear user_id from cookie
    cookies.save()
    if "user_id" in st.query_params: # Also clear from URL if present
        del st.query_params["user_id"]
    st.info("Вы вышли из системы.")

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

    # Conditional page links
    if st.session_state.logged_in:
        st.sidebar.page_link("pages/_Phrase_Filtration.py", label="Phrase Filtration", icon="🔍")
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
