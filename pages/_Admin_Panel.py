import streamlit as st
import pandas as pd
from core.database import get_db_connection, get_all_moderators, update_user_status, update_user_details, add_user
import bcrypt

conn = get_db_connection()

st.set_page_config(page_title="Панель администратора", layout="wide")

if not st.session_state.logged_in or st.session_state.user_role != 'admin':
    st.warning("У вас нет прав доступа к этой странице.")
    st.switch_page("Home.py")

st.title("Панель администратора")

st.subheader("Управление модераторами")

def refresh_moderators():
    data = get_all_moderators(conn)
    st.session_state.moderators = pd.DataFrame(data) if data else pd.DataFrame()

if 'moderators' not in st.session_state:
    refresh_moderators()

if not st.session_state.moderators.empty:
    edited_df = st.data_editor(
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
        num_rows="dynamic",
        use_container_width=True
    )

    if not isinstance(st.session_state.moderators, pd.DataFrame):
        st.session_state.moderators = pd.DataFrame(st.session_state.moderators)
    if edited_df.equals(st.session_state.moderators):
        st.info("Изменений нет.")
    else:
        st.session_state.moderators = edited_df
        st.success("Изменения сохранены.")
        # Process edited rows
        for i, row in edited_df.iterrows():
            original_row_df = st.session_state.moderators[st.session_state.moderators['id'] == row['id']]
            if not original_row_df.empty:
                original_row = original_row_df.iloc[0] # Get the first (and only) matching row as a Series
            else:
                original_row = None # No matching original row found
            if original_row is not None:
                if original_row["nickname"] != row["nickname"] or \
                   original_row["role"] != row["role"] or \
                   original_row["status"] != row["status"]:
                    if update_user_details(conn, row["id"], row["nickname"], role=row["role"]):
                        if update_user_status(conn, row["id"], row["status"]):
                            st.success(f"Пользователь {row["login"]} обновлен.")
                        else:
                            st.error(f"Ошибка при обновлении статуса пользователя {row["login"]}.")
                    else:
                        st.error(f"Ошибка при обновлении данных пользователя {row["login"]}.")
        refresh_moderators()
        st.rerun()

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
