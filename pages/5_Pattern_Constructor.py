import streamlit as st
import pandas as pd
from core.database import get_db_connection, get_pattern_by_id, get_relaxed_signature

# --- Helper Functions ---

# --- Data Loading & Caching ---

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

@st.cache_data(show_spinner=True, ttl=3600)
def find_constructions_relaxed(source_pattern_id):
    """
    Finds patterns that can be constructed with the source pattern using relaxed relations.
    This version is optimized to use a single query with JOINs and CTEs to avoid the N+1 problem.
    """
    source_pattern = get_pattern_by_id(source_pattern_id)
    if not source_pattern:
        return [], []

    source_relaxed_sig = source_pattern['relaxed_sig']
    conn = get_db_connection()
    if not conn:
        st.error("Database connection failed.")
        return [], []

    # This CTE finds the most frequent concrete pattern for each relaxed signature.
    # It's used in both queries to avoid N+1 lookups.
    base_query = """
        WITH frequent_partners AS (
            SELECT
                id, pattern_text, phrase_length, total_frequency, total_quantity, relaxed_signature,
                ROW_NUMBER() OVER(PARTITION BY relaxed_signature ORDER BY total_frequency DESC) as rn
            FROM unique_patterns
        )
        SELECT
            fp.id as partner_id, fp.pattern_text as partner_text, fp.phrase_length as partner_len,
            fp.total_frequency as partner_freq, fp.total_quantity as partner_qty, fp.relaxed_signature as partner_relaxed_sig,
            up_c.id as result_id, up_c.pattern_text as result_text, up_c.phrase_length as result_len,
            up_c.total_frequency as result_freq, up_c.total_quantity as result_qty, up_c.relaxed_signature as result_relaxed_sig
        FROM
            pattern_relations_relaxed prr
        JOIN
            unique_patterns up_c ON prr.parent_pattern_id = up_c.id
        JOIN
            frequent_partners fp ON prr.{join_column} = fp.relaxed_signature
        WHERE
            prr.{where_column} = %s AND fp.rn = 1
        ORDER BY
            up_c.total_frequency DESC
        LIMIT 10;
    """

    before_results = []
    after_results = []

    try:
        with conn.cursor() as cur:
            # Find patterns that can be glued BEFORE (B + A = C)
            query_before = base_query.format(
                join_column='child_1_relaxed_signature',
                where_column='child_2_relaxed_signature'
            )
            cur.execute(query_before, (source_relaxed_sig,))
            for row in cur.fetchall():
                partner_b = {"id": row[0], "text": row[1], "len": row[2], "freq": row[3], "qty": row[4], "relaxed_sig": row[5]}
                pattern_c = {"id": row[6], "text": row[7], "len": row[8], "freq": row[9], "qty": row[10], "relaxed_sig": row[11]}
                before_results.append({'partner': partner_b, 'result': pattern_c})

            # Find patterns that can be glued AFTER (A + B = C)
            query_after = base_query.format(
                join_column='child_2_relaxed_signature',
                where_column='child_1_relaxed_signature'
            )
            cur.execute(query_after, (source_relaxed_sig,))
            for row in cur.fetchall():
                partner_b = {"id": row[0], "text": row[1], "len": row[2], "freq": row[3], "qty": row[4], "relaxed_sig": row[5]}
                pattern_c = {"id": row[6], "text": row[7], "len": row[8], "freq": row[9], "qty": row[10], "relaxed_sig": row[11]}
                after_results.append({'partner': partner_b, 'result': pattern_c})
    except Exception as e:
        st.error(f"An error occurred while finding constructions: {e}")
    finally:
        if conn: conn.close()

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

        with st.expander("Показать примеры фраз для исходного паттерна"):
            source_examples = get_pattern_examples(source_pattern['id'])
            if source_examples:
                df_source_examples = pd.DataFrame(source_examples)
                df_source_examples.rename(columns={'example_text': 'Пример', 'example_frequency': 'F'}, inplace=True)
                st.dataframe(df_source_examples, hide_index=True, use_container_width=True)
            else:
                st.info("Примеры фраз для исходного паттерна не найдены.")

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