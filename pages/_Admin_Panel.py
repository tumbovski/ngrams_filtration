import streamlit as st
from core.database import get_db_connection, get_all_moderators, update_user_status, update_user_details, add_user
import bcrypt

conn = get_db_connection()

st.set_page_config(page_title="Панель администратора", layout="wide")

if not st.session_state.logged_in or st.session_state.user_role != 'admin':
    st.warning("У вас нет прав доступа к этой странице.")
    st.stop()

st.title("Панель администратора")

st.subheader("Управление модераторами")

def refresh_moderators():
    st.session_state.moderators = get_all_moderators(conn)

if 'moderators' not in st.session_state:
    refresh_moderators()

if st.session_state.moderators:
    df_moderators = st.dataframe(
        st.session_state.moderators,
        key="moderators_table",
        hide_index=True,
        column_config={
            "id": st.column_config.NumberColumn("ID", disabled=True),
            "login": st.column_config.TextColumn("Логин", disabled=True),
            "nickname": st.column_config.TextColumn("Никнейм", required=True),
            "role": st.column_config.SelectboxColumn("Роль", options=["moderator", "admin"], required=True),
            "status": st.column_config.SelectboxColumn("Статус", options=["active", "disabled"], required=True),
        },
        
        use_container_width=True
    )

    # Handle changes from the dataframe
    for i, row in enumerate(st.session_state.moderators):
        col1, col2, col3, col4, col5, col6 = st.columns([1, 2, 2, 1, 1, 1])
        with col1:
            st.write(row["id"])
        with col2:
            st.write(row["login"])
        with col3:
            new_nickname = st.text_input("Никнейм", value=row["nickname"], key=f"nickname_{row['id']}", label_visibility="collapsed")
        with col4:
            new_role = st.selectbox("Роль", options=["moderator", "admin"], index=["moderator", "admin"].index(row["role"]), key=f"role_{row['id']}", label_visibility="collapsed")
        with col5:
            new_status = st.selectbox("Статус", options=["active", "disabled"], index=["active", "disabled"].index(row["status"]), key=f"status_{row['id']}", label_visibility="collapsed")
        with col6:
            if st.button("Обновить", key=f"update_{row['id']}"):
                if update_user_details(conn, row["id"], new_nickname, role=new_role):
                    if update_user_status(conn, row["id"], new_status):
                        st.success(f"Пользователь {row['login']} обновлен.")
                        refresh_moderators()
                        st.rerun()
                    else:
                        st.error(f"Ошибка при обновлении статуса пользователя {row['login']}.")
                else:
                    st.error(f"Ошибка при обновлении данных пользователя {row['login']}.")
            if st.button("Сбросить пароль", key=f"reset_password_{row['id']}"):
                new_password = st.text_input("Новый пароль", type="password", key=f"new_password_{row['id']}")
                if new_password:
                    if update_user_details(conn, row["id"], row["nickname"], password=new_password, role=row["role"]):
                        st.success(f"Пароль для {row['login']} сброшен.")
                        refresh_moderators()
                        st.rerun()
                    else:
                        st.error(f"Ошибка при сбросе пароля для {row['login']}.")
                else:
                    st.warning("Введите новый пароль.")

else:
    st.info("Модераторы не найдены.")

st.markdown("--- ")

st.subheader("Создать новый аккаунт")
with st.form("new_user_form"):
    new_login = st.text_input("Логин")
    new_nickname = st.text_input("Никнейм")
    new_password = st.text_input("Пароль", type="password")
    new_role = st.selectbox("Роль", options=["moderator", "admin"])
    
    submitted = st.form_submit_button("Создать аккаунт")
    if submitted:
        if new_login and new_nickname and new_password:
            if add_user(conn, new_login, new_nickname, new_password, new_role, 'active'):
                st.success(f"Аккаунт {new_login} создан. Передайте пароль пользователю.")
                refresh_moderators()
                st.rerun()
            else:
                st.error("Ошибка при создании аккаунта. Возможно, логин уже занят.")
        else:
            st.warning("Пожалуйста, заполните все поля.")
