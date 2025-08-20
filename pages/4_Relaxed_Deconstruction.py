import streamlit as st
import pandas as pd
from core.database import get_db_connection, get_relaxed_signature

# --- Helper Function ---

# --- Data Loading and Caching ---

@st.cache_data(ttl=3600)
def get_patterns_by_relaxed_sig(relaxed_sig, limit=5):
    """
    Fetches the top N most frequent concrete patterns for a given relaxed signature.
    This is highly optimized to use the database index and avoids loading all patterns into memory.
    """
    if not relaxed_sig:
        return []
    conn = get_db_connection()
    if not conn:
        return []
    try:
        query = """
            SELECT id, pattern_text, phrase_length, total_frequency, total_quantity
            FROM unique_patterns
            WHERE relaxed_signature = %s
            ORDER BY total_frequency DESC
            LIMIT %s;
        """
        with conn.cursor() as cur:
            cur.execute(query, (relaxed_sig, limit))
            patterns = cur.fetchall()
            return [{"id": p[0], "text": p[1], "len": p[2], "freq": p[3], "qty": p[4]} for p in patterns]
    except Exception as e:
        st.error(f"Error fetching patterns for signature {relaxed_sig}: {e}")
        return []
    finally:
        if conn: conn.close()

@st.cache_data(ttl=3600)
def get_available_parent_lengths():
    """Gets a list of available lengths for parent patterns."""
    conn = get_db_connection()
    if not conn: return []
    try:
        query = "SELECT DISTINCT up.phrase_length FROM unique_patterns up JOIN pattern_relations_relaxed prr ON up.id = prr.parent_pattern_id ORDER BY up.phrase_length;"
        df = pd.read_sql(query, conn)
        return df['phrase_length'].tolist()
    except Exception as e:
        st.error(f"Error getting available lengths: {e}")
        return []
    finally:
        if conn: conn.close()

@st.cache_data(ttl=3600)
def get_relaxed_parent_patterns(selected_length=None):
    """Fetches patterns filtered by length with specific formatting."""
    conn = get_db_connection()
    if conn is None or not selected_length: return pd.DataFrame()
    try:
        query = "SELECT DISTINCT up.id, up.pattern_text, up.phrase_length, up.total_frequency, up.total_quantity FROM unique_patterns up JOIN pattern_relations_relaxed prr ON up.id = prr.parent_pattern_id WHERE up.phrase_length = %(length)s ORDER BY up.total_frequency DESC;"
        df = pd.read_sql(query, conn, params={'length': selected_length})
        df['display_label'] = df.apply(
            lambda row: f"(ID: {row['id']}) {get_relaxed_signature(row['pattern_text'], row['phrase_length'])} (F: {row['total_frequency']:,.2f}, Q: {row['total_quantity']:,})".replace(',', ' '),
            axis=1
        )
        return df[['id', 'display_label']].set_index('id')
    except Exception as e:
        st.error(f"Error getting parent patterns: {e}")
        return pd.DataFrame()
    finally:
        if conn: conn.close()

@st.cache_data(ttl=3600)
def get_relaxed_children(parent_id):
    """Fetches the relaxed children signatures for a given parent."""
    conn = get_db_connection()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT child_1_relaxed_signature, child_2_relaxed_signature, split_position FROM pattern_relations_relaxed WHERE parent_pattern_id = %s", (parent_id,))
            return cur.fetchall()
    finally:
        if conn: conn.close()

@st.cache_data(ttl=3600)
def get_pattern_examples(pattern_id):
    """Fetches example phrases, with a slow fallback."""
    conn = get_db_connection()
    if not conn: return []
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT example_text, example_frequency FROM pattern_examples WHERE pattern_id = %s ORDER BY example_frequency DESC LIMIT 50;", (pattern_id,))
            fast_examples = cur.fetchall()
            if fast_examples: return [{"example_text": row[0], "example_frequency": row[1]} for row in fast_examples]
        with st.spinner('Кэш пуст, выполняю медленный поиск...'):
            pattern_text = None
            with conn.cursor() as cur:
                cur.execute("SELECT pattern_text FROM unique_patterns WHERE id = %s", (pattern_id,))
                result = cur.fetchone()
                if result: pattern_text = result[0]
            if pattern_text:
                slow_query = "SELECT n.text, n.freq_mln FROM ngrams n WHERE (SELECT STRING_AGG(elem->>0, '_') FROM jsonb_array_elements(n.deps) AS elem) || '_' || (SELECT STRING_AGG(elem->>0, '_') FROM jsonb_array_elements(n.pos) AS elem) || '_' || (SELECT STRING_AGG(elem->>0, '_') FROM jsonb_array_elements(n.tags) AS elem) = %s ORDER BY n.freq_mln DESC LIMIT 50;"
                with conn.cursor() as cur:
                    cur.execute(slow_query, (pattern_text,))
                    slow_examples = cur.fetchall()
                    return [{"example_text": row[0], "example_frequency": row[1]} for row in slow_examples]
        return []
    finally:
        if conn: conn.close()

def generate_graphviz_chart(parent_label, child_relations):
    """Generates a Graphviz DOT string for the deconstruction tree."""
    dot_lines = ['digraph {', '    rankdir=LR;', '    node [shape=box, style="rounded,filled", fillcolor=lightgrey];']
    parent_node_id = f'"parent_{parent_label}"'
    dot_lines.append(f'{parent_node_id} [label="{parent_label}", fillcolor=lightblue];')
    for i, item in enumerate(child_relations):
        child1_sig, child2_sig, split_pos = item['relation']
        split_node_id = f'"split_{i}"'
        dot_lines.append(f'{split_node_id} [label="Split @ {split_pos}\nScore: {item['score']:.2f}", shape=circle, fillcolor=orange];')
        dot_lines.append(f'{parent_node_id} -> {split_node_id};')
        child1_node_id = f'"child1_{i}_{child1_sig}"'
        child2_node_id = f'"child2_{i}_{child2_sig}"'
        dot_lines.append(f'{child1_node_id} [label="{child1_sig}"];')
        dot_lines.append(f'{child2_node_id} [label="{child2_sig}"];')
        dot_lines.append(f'{split_node_id} -> {child1_node_id};')
        dot_lines.append(f'{split_node_id} -> {child2_node_id};')
    dot_lines.append('}')
    return '\n'.join(dot_lines)

def display_deconstruction(pattern_id):
    """The main display logic for a given pattern ID, fetching data on-demand."""
    parent_info_query = "SELECT pattern_text, phrase_length, total_frequency, total_quantity FROM unique_patterns WHERE id = %s"
    conn = get_db_connection()
    if not conn: return
    try:
        parent_info_df = pd.read_sql(parent_info_query, conn, params=(pattern_id,))
        if parent_info_df.empty:
            st.error(f"Паттерн с ID {pattern_id} не найден.")
            return
        parent_info = parent_info_df.iloc[0]
    finally:
        conn.close()

    pattern_signature_only = get_relaxed_signature(parent_info['pattern_text'], parent_info['phrase_length'])
    id_and_stats = f"F: {parent_info['total_frequency']:,.2f}; Q: {parent_info['total_quantity']:,}; ID: {pattern_id}".replace(',', ' ')

    # Define parent_label here for use in generate_graphviz_chart
    parent_label = f"Паттерн: {pattern_signature_only}\n{id_and_stats}"

    st.subheader(f"Паттерн: {pattern_signature_only}")
    st.markdown(f"<p style='font-size: medium;'>{id_and_stats}</p>", unsafe_allow_html=True)
    
    parent_examples = get_pattern_examples(pattern_id)
    if parent_examples:
        parent_df = pd.DataFrame(parent_examples)
        parent_df.rename(columns={'example_text': 'Пример фразы', 'example_frequency': 'Частота (F)'}, inplace=True)
        st.dataframe(parent_df, hide_index=True)
    else:
        st.info("Примеры не найдены.")
    child_relations = get_relaxed_children(pattern_id)
    if not child_relations:
        st.warning("Для этого паттерна не найдено разложений.")
    else:
        scored_relations = []
        for rel in child_relations:
            child1_sig, child2_sig, _ = rel
            matching_patterns1 = get_patterns_by_relaxed_sig(child1_sig, limit=1)
            freq1 = matching_patterns1[0]['freq'] if matching_patterns1 else 0
            matching_patterns2 = get_patterns_by_relaxed_sig(child2_sig, limit=1)
            freq2 = matching_patterns2[0]['freq'] if matching_patterns2 else 0
            score = max(freq1, freq2)
            scored_relations.append({'relation': rel, 'score': score})
        
        sorted_relations = sorted(scored_relations, key=lambda x: x['score'], reverse=True)
        st.subheader("Графическое представление разложения")
        dot_string = generate_graphviz_chart(parent_label, sorted_relations)
        st.graphviz_chart(dot_string)
        st.subheader("Детализация и примеры фраз (отсортировано по 'силе' связи)")
        for i, item in enumerate(sorted_relations):
            child1_sig, child2_sig, split_pos = item['relation']
            st.markdown(f"--- ")
            st.markdown(f"#### Вариант разложения №{i+1} (позиция: {split_pos}, оценка: {item['score']:.2f})")
            col1, col2 = st.columns(2)
            with col1:
                st.markdown(f"**Левый дочерний паттерн (POS+TAG):** `{child1_sig}`")
                matching_patterns1 = get_patterns_by_relaxed_sig(child1_sig, limit=5)
                if not matching_patterns1:
                    st.warning("Не найдено реальных паттернов для этой сигнатуры.")
                else:
                    st.markdown(f"Топ-5 реальных паттернов по частоте:")
                    for pattern in matching_patterns1[:5]:
                        relaxed_child_text = get_relaxed_signature(pattern['text'], pattern['len'])
                        with st.expander(f"(ID: {pattern['id']}) {relaxed_child_text} (F: {pattern['freq']:,.2f}, Q: {pattern['qty']:,})".replace(',',' ')):
                            st.markdown("**Примеры фраз:**")
                            examples = get_pattern_examples(pattern['id'])
                            if examples:
                                df_examples = pd.DataFrame(examples)
                                df_examples.rename(columns={'example_text': 'Пример фразы', 'example_frequency': 'Частота (F)'}, inplace=True)
                                st.dataframe(df_examples, hide_index=True)
                            else: st.info("Примеры не найдены.")
            with col2:
                st.markdown(f"**Правый дочерний паттерн (POS+TAG):** `{child2_sig}`")
                matching_patterns2 = get_patterns_by_relaxed_sig(child2_sig, limit=5)
                if not matching_patterns2:
                    st.warning("Не найдено реальных паттернов для этой сигнатуры.")
                else:
                    st.markdown(f"Топ-5 реальных паттернов по частоте:")
                    for pattern in matching_patterns2[:5]:
                        relaxed_child_text = get_relaxed_signature(pattern['text'], pattern['len'])
                        with st.expander(f"(ID: {pattern['id']}) {relaxed_child_text} (F: {pattern['freq']:,.2f}, Q: {pattern['qty']:,})".replace(',',' ')):
                            st.markdown("**Примеры фраз:**")
                            examples = get_pattern_examples(pattern['id'])
                            if examples:
                                df_examples = pd.DataFrame(examples)
                                df_examples.rename(columns={'example_text': 'Пример фразы', 'example_frequency': 'Частота (F)'}, inplace=True)
                                st.dataframe(df_examples, hide_index=True)
                            else: st.info("Примеры не найдены.")

# --- UI ---
st.set_page_config(page_title="Relaxed Deconstruction", layout="wide")
st.title("Анализ 'ослабленных' связей паттернов")
st.info("Здесь связи ищутся по совпадению только частей речи (POS) и тегов, игнорируя зависимости (DEP).")

col_len, col_id_input = st.columns([0.7, 0.3])

with col_len:
    available_lengths = get_available_parent_lengths()
    if not available_lengths:
        st.warning("В базе данных не найдено 'ослабленных' разложений. Запустите соответствующий скрипт анализа.")
        selected_len = None # Ensure selected_len is None if no lengths available
    else:
        selected_len = st.selectbox("Шаг 1: Выберите длину исходного паттерна", options=available_lengths, key="len_selector")

with col_id_input:
    direct_id_input = st.number_input("Или введите ID:", min_value=1, step=1, value=None, key="id_direct_input")

selected_id_for_display = None
if direct_id_input:
    selected_id_for_display = direct_id_input
elif selected_len:
    parent_patterns_df = get_relaxed_parent_patterns(selected_len)
    if not parent_patterns_df.empty:
        selected_id_from_list = st.selectbox(
            label="Шаг 2: Выберите родительский паттерн для анализа (отсортировано по частоте)",
            options=parent_patterns_df.index,
            format_func=lambda x: parent_patterns_df.loc[x, 'display_label'],
            key="pattern_selector"
        )
        selected_id_for_display = selected_id_from_list
    else:
        st.info("Не найдено паттернов для выбранной длины.")

st.markdown("--- ")

if selected_id_for_display:
    display_deconstruction(selected_id_for_display)
else:
    st.info("Выберите паттерн или введите ID для начала анализа.")
