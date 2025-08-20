import streamlit as st
import pandas as pd
from core.database import get_db_connection, get_db_engine

# --- Helper Functions ---

def get_relaxed_signature(full_signature, length):
    """Extracts the POS+TAG parts of a signature."""
    parts = full_signature.split('_')
    if len(parts) < 3 * length:
        parts.extend([''] * (3 * length - len(parts)))
    pos = parts[length:2*length]
    tags = parts[2*length:3*length]
    return '_'.join(pos + tags)

# --- Data Loading & Caching ---

@st.cache_data(ttl=3600)
def get_pattern_by_id(pattern_id):
    conn = get_db_connection()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, pattern_text, phrase_length, total_frequency, total_quantity FROM unique_patterns WHERE id = %s", (pattern_id,))
            p = cur.fetchone()
            if p:
                # Рассчитываем и добавляем relaxed_signature для удобства
                relaxed_sig = get_relaxed_signature(p[1], p[2])
                return {"id": p[0], "text": p[1], "len": p[2], "freq": p[3], "qty": p[4], "relaxed_sig": relaxed_sig}
            return None
    finally:
        if conn: conn.close()

@st.cache_data(ttl=3600)
def get_pattern_examples(pattern_id):
    """Fetches example phrases for a given pattern ID."""
    conn = get_db_connection()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT example_text, example_frequency FROM pattern_examples WHERE pattern_id = %s ORDER BY example_frequency DESC LIMIT 50;", (pattern_id,))
            examples = cur.fetchall()
            return [{"example_text": row[0], "example_frequency": row[1]} for row in examples]
    finally:
        if conn: conn.close()

@st.cache_data(ttl=3600)
def get_most_frequent_pattern_for_relaxed_sig(relaxed_sig):
    """Finds the single most frequent concrete pattern for a given relaxed signature."""
    conn = get_db_connection()
    if not conn or not relaxed_sig: return None
    try:
        query = "SELECT id, pattern_text, phrase_length, total_frequency, total_quantity FROM unique_patterns WHERE relaxed_signature = %s ORDER BY total_frequency DESC LIMIT 1"
        with conn.cursor() as cur:
            cur.execute(query, (relaxed_sig,))
            p = cur.fetchone()
            if p:
                return {"id": p[0], "text": p[1], "len": p[2], "freq": p[3], "qty": p[4], "relaxed_sig": relaxed_sig}
            return None
    finally:
        if conn: conn.close()

@st.cache_data(show_spinner=True, ttl=3600)
def find_constructions_relaxed(source_pattern_id):
    """Finds patterns that can be constructed with the source pattern using relaxed relations."""
    before_results = []
    after_results = []

    source_pattern = get_pattern_by_id(source_pattern_id)
    if not source_pattern: return [], []

    source_relaxed_sig = source_pattern['relaxed_sig']

    conn = get_db_connection()
    if not conn: return [], []
    try:
        with conn.cursor() as cur:
            # Find patterns that can be glued BEFORE (B + A = C)
            # Here, A is child_2, B is child_1, C is parent
            query_before = """
                SELECT prr.parent_pattern_id, prr.child_1_relaxed_signature
                FROM pattern_relations_relaxed prr
                WHERE prr.child_2_relaxed_signature = %s;
            """
            cur.execute(query_before, (source_relaxed_sig,))
            for parent_c_id, child_b_relaxed_sig in cur.fetchall():
                pattern_c = get_pattern_by_id(parent_c_id)
                if pattern_c:
                    partner_b = get_most_frequent_pattern_for_relaxed_sig(child_b_relaxed_sig)
                    if partner_b:
                        before_results.append({'partner': partner_b, 'result': pattern_c})

            # Find patterns that can be glued AFTER (A + B = C)
            # Here, A is child_1, B is child_2, C is parent
            query_after = """
                SELECT prr.parent_pattern_id, prr.child_2_relaxed_signature
                FROM pattern_relations_relaxed prr
                WHERE prr.child_1_relaxed_signature = %s;
            """
            cur.execute(query_after, (source_relaxed_sig,))
            for parent_c_id, child_b_relaxed_sig in cur.fetchall():
                pattern_c = get_pattern_by_id(parent_c_id)
                if pattern_c:
                    partner_b = get_most_frequent_pattern_for_relaxed_sig(child_b_relaxed_sig)
                    if partner_b:
                        after_results.append({'partner': partner_b, 'result': pattern_c})
    finally:
        if conn: conn.close()

    # Sort results by frequency of the result pattern and limit to top 10
    before_results = sorted(before_results, key=lambda x: x['result']['freq'], reverse=True)[:10]
    after_results = sorted(after_results, key=lambda x: x['result']['freq'], reverse=True)[:10]
    
    return before_results, after_results

# --- UI ---
st.set_page_config(page_title="Конструктор паттернов", layout="wide")
st.title("Конструктор паттернов")
st.info("Выберите паттерн, и система найдет, с какими другими паттернами его можно 'склеить' (по ослабленной сигнатуре), чтобы получить новый, более длинный существующий паттерн.")

pattern_id_input = st.number_input("Введите ID исходного паттерна:", min_value=1, step=1, value=None)

if pattern_id_input:
    source_pattern = get_pattern_by_id(pattern_id_input)

    if not source_pattern:
        st.error(f"Паттерн с ID {pattern_id_input} не найден.")
    else:
        st.header(f"Исходный паттерн: {get_relaxed_signature(source_pattern['text'], source_pattern['len'])}")
        st.write(f"(F: {source_pattern['freq']:,.2f}, Q: {source_pattern['qty']:,}; ID: {source_pattern['id']})".replace(',', ' '))

        before_patterns, after_patterns = find_constructions_relaxed(source_pattern['id'])
        st.divider()

        col1, col2 = st.columns(2)

        with col1:
            st.subheader(f"Склеиваются ПЕРЕД ({len(before_patterns)} шт.)")
            for item in before_patterns:
                partner = item['partner']
                result = item['result']
                with st.expander(f"Партнер: {get_relaxed_signature(partner['text'], partner['len'])}"):
                    st.markdown(f"**Партнер (Б):** {get_relaxed_signature(partner['text'], partner['len'])} (F: {partner['freq']:,.2f}, Q: {partner['qty']:,})".replace(',', ' '))
                    st.markdown(f"**Результат (В):** {get_relaxed_signature(result['text'], result['len'])} (F: {result['freq']:,.2f}, Q: {result['qty']:,})".replace(',', ' '))
                    st.markdown("**Примеры для результата (В):**")
                    examples = get_pattern_examples(result['id'])
                    if examples:
                        df_ex = pd.DataFrame(examples)
                        df_ex.rename(columns={'example_text': 'Пример', 'example_frequency': 'F'}, inplace=True)
                        st.dataframe(df_ex, hide_index=True)
                    else: st.info("Примеры не найдены.")

        with col2:
            st.subheader(f"Склеиваются ПОСЛЕ ({len(after_patterns)} шт.)")
            for item in after_patterns:
                partner = item['partner']
                result = item['result']
                with st.expander(f"Партнер: {get_relaxed_signature(partner['text'], partner['len'])}"):
                    st.markdown(f"**Партнер (Б):** {get_relaxed_signature(partner['text'], partner['len'])} (F: {partner['freq']:,.2f}, Q: {partner['qty']:,})".replace(',', ' '))
                    st.markdown(f"**Результат (В):** {get_relaxed_signature(result['text'], result['len'])} (F: {result['freq']:,.2f}, Q: {result['qty']:,})".replace(',', ' '))
                    st.markdown("**Примеры для результата (В):**")
                    examples = get_pattern_examples(result['id'])
                    if examples:
                        df_ex = pd.DataFrame(examples)
                        df_ex.rename(columns={'example_text': 'Пример', 'example_frequency': 'F'}, inplace=True)
                        st.dataframe(df_ex, hide_index=True)
                    else: st.info("Примеры не найдены.")