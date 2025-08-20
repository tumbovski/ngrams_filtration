import streamlit as st
import pandas as pd
from core.database import get_db_connection

# --- Helper Functions ---

def glue_signatures(sig1, len1, sig2, len2):
    """Glues two full pattern signatures together."""
    parts1 = sig1.split('_')
    parts2 = sig2.split('_')

    dep1 = parts1[0:len1]
    pos1 = parts1[len1:2*len1]
    tags1 = parts1[2*len1:3*len1]

    dep2 = parts2[0:len2]
    pos2 = parts2[len2:2*len2]
    tags2 = parts2[2*len2:3*len2]

    glued_dep = dep1 + dep2
    glued_pos = pos1 + pos2
    glued_tags = tags1 + tags2

    return '_'.join(glued_dep + glued_pos + glued_tags)

# --- Data Loading & Caching ---

@st.cache_resource(ttl=3600)
def load_all_patterns_data():
    """Loads all patterns into a dictionary and a set for fast lookups."""
    conn = get_db_connection()
    if not conn: return {}, set()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, pattern_text, phrase_length, total_frequency, total_quantity FROM unique_patterns")
            all_patterns = cur.fetchall()
            patterns_map = {p[1]: {"id": p[0], "text": p[1], "len": p[2], "freq": p[3], "qty": p[4]} for p in all_patterns}
            patterns_set = set(patterns_map.keys())
            return patterns_map, patterns_set
    finally:
        if conn: conn.close()

@st.cache_data(ttl=3600)
def get_pattern_by_id(pattern_id):
    conn = get_db_connection()
    if not conn: return None
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id, pattern_text, phrase_length, total_frequency, total_quantity FROM unique_patterns WHERE id = %s", (pattern_id,))
            p = cur.fetchone()
            if p:
                return {"id": p[0], "text": p[1], "len": p[2], "freq": p[3], "qty": p[4]}
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

@st.cache_data(show_spinner=True, ttl=3600)
def find_constructions(source_pattern, all_patterns_map, all_patterns_set):
    """Finds all patterns that can be constructed with the source pattern."""
    before_results = []
    after_results = []
    sig_a = source_pattern['text']
    len_a = source_pattern['len']

    for sig_b, pattern_b in all_patterns_map.items():
        len_b = pattern_b['len']
        
        # Check for B + A = C
        glued_before = glue_signatures(sig_b, len_b, sig_a, len_a)
        if glued_before in all_patterns_set:
            pattern_c = all_patterns_map[glued_before]
            before_results.append({'partner': pattern_b, 'result': pattern_c})

        # Check for A + B = C
        glued_after = glue_signatures(sig_a, len_a, sig_b, len_b)
        if glued_after in all_patterns_set:
            pattern_c = all_patterns_map[glued_after]
            after_results.append({'partner': pattern_b, 'result': pattern_c})

    # Sort results by frequency of the partner pattern
    before_results = sorted(before_results, key=lambda x: x['partner']['freq'], reverse=True)
    after_results = sorted(after_results, key=lambda x: x['partner']['freq'], reverse=True)
    
    return before_results, after_results

# --- UI ---
st.set_page_config(page_title="Конструктор паттернов", layout="wide")
st.title("Конструктор паттернов")
st.info("Выберите паттерн, и система найдет, с какими другими паттернами его можно 'склеить', чтобы получить новый, более длинный существующий паттерн.")

patterns_map, patterns_set = load_all_patterns_data()

pattern_id_input = st.number_input("Введите ID исходного паттерна:", min_value=1, step=1)

if pattern_id_input:
    source_pattern = get_pattern_by_id(pattern_id_input)

    if not source_pattern:
        st.error(f"Паттерн с ID {pattern_id_input} не найден.")
    else:
        st.header(f"Исходный паттерн: {source_pattern['text']}")
        st.write(f"(F: {source_pattern['freq']:,.2f}, Q: {source_pattern['qty']:,})".replace(',', ' '))

        before_patterns, after_patterns = find_constructions(source_pattern, patterns_map, patterns_set)
        st.divider()

        col1, col2 = st.columns(2)

        with col1:
            st.subheader(f"Склеиваются ПЕРЕД ({len(before_patterns)} шт.)")
            for item in before_patterns:
                partner = item['partner']
                result = item['result']
                with st.expander(f"Паттерн: {partner['text']}"):
                    st.markdown(f"**Партнер (Б):** {partner['text']} (F: {partner['freq']:,.2f}, Q: {partner['qty']:,})".replace(',', ' '))
                    st.markdown(f"**Результат (В):** {result['text']} (F: {result['freq']:,.2f}, Q: {result['qty']:,})".replace(',', ' '))
                    st.markdown("**Примеры для партнера (Б):**")
                    examples = get_pattern_examples(partner['id'])
                    if examples:
                        df_ex = pd.DataFrame(examples)
                        st.dataframe(df_ex)
                    else: st.info("Примеры не найдены.")

        with col2:
            st.subheader(f"Склеиваются ПОСЛЕ ({len(after_patterns)} шт.)")
            for item in after_patterns:
                partner = item['partner']
                result = item['result']
                with st.expander(f"Паттерн: {partner['text']}"):
                    st.markdown(f"**Партнер (Б):** {partner['text']} (F: {partner['freq']:,.2f}, Q: {partner['qty']:,})".replace(',', ' '))
                    st.markdown(f"**Результат (В):** {result['text']} (F: {result['freq']:,.2f}, Q: {result['qty']:,})".replace(',', ' '))
                    st.markdown("**Примеры для партнера (Б):**")
                    examples = get_pattern_examples(partner['id'])
                    if examples:
                        df_ex = pd.DataFrame(examples)
                        st.dataframe(df_ex)
                    else: st.info("Примеры не найдены.")
