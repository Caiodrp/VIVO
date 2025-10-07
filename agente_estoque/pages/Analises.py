# -*- coding: utf-8 -*-
# Agente_ST.py — UI minimalista/local
# - Carrega o motor e o modelo LLM ao abrir (warm-up).
# - Regex + pandas fazem tudo; LLM só reescreve UMA frase no final (opcional via toggle).
# - Renderiza gráficos (Plotly) quando o engine retornar "chart" no result (SEM IA).

import os
import time
import tempfile
import streamlit as st
import pandas as pd
import plotly.express as px  # <- para gráficos
import psycopg2
from sqlalchemy import create_engine
from Engine import StockEngine, EngineConfig

# ============================
# Config da página
# ============================
st.set_page_config(
    page_title="Agente de Estoque",
    page_icon="📦",
    layout="wide",
)

# ============================
# Estado & Inicialização
# ============================
if "engine" not in st.session_state:
    with st.spinner("Carregando motor e modelo local"):
        # Carrega o modelo agora e faz warm-up para evitar atraso na 1ª resposta
        st.session_state.engine = StockEngine(
            EngineConfig(
                use_llm=True,          # mantém o modelo carregado em memória
                warmup_on_init=True,   # aquece (1 token)
                warmup_max_tokens=1,
                n_ctx=512,             # contexto pequeno = mais rápido
                n_threads=6,           # ajuste ao seu i5
                max_tokens=36,         # UMA frase curta para reescrever
                temperature=0.1
            )
        )

if "df" not in st.session_state:
    st.session_state.df = None


# ============================
# Helpers
# ============================
def _load_excel_to_df(uploaded_file) -> pd.DataFrame:
    """
    Lê o Excel (UploadedFile). Tenta direto; se falhar, salva temporariamente.
    Em seguida aplica normalizações/classificações do engine.
    """
    engine = st.session_state.engine
    # 1) File-like direto
    try:
        df_raw = engine.read_excel(uploaded_file, sheet_name=0)
    except Exception:
        # 2) Fallback: salva temp
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp.write(uploaded_file.getbuffer())
                tmp_path = tmp.name
            df_raw = engine.read_excel(tmp_path, sheet_name=0)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                try:
                    os.remove(tmp_path)
                except Exception:
                    pass

    # Normalização/classificação conforme engine
    df = engine.classify_supplier_by_description(df_raw)
    return df


def _load_db_to_df() -> pd.DataFrame:
    """
    Conecta ao banco de dados PostgreSQL e carrega dados da tabela EstoqueEND em um DataFrame.
    """
    try:
        # Configurações do banco de dados
        db_config = {
            'dbname': 'OperacaoVIVO',
            'user': 'postgres',
            'password': os.getenv('DB_PASSWORD'), 
            'host': 'localhost',
            'port': 5432
        }

        # Verificar se a variável de ambiente foi configurada corretamente
        if db_config['password'] is None:
            raise ValueError("A senha do banco de dados não foi encontrada na variável de ambiente 'DB_PASSWORD'.")

        # Criar a string de conexão com o PostgreSQL
        engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

        # Carregar dados da tabela estoque_end
        query = 'SELECT * FROM estoque_end'
        df = pd.read_sql(query, engine)

        # Normalização/classificação conforme engine
        df = st.session_state.engine.classify_supplier_by_description(df)
        return df

    except Exception as e:
        st.error(f"Erro ao carregar dados do banco de dados: {e}")
        return pd.DataFrame()


def _render_chart(chart: dict):
    """
    Renderiza gráficos do Plotly com base no hint retornado pelo engine.
    chart = {
        "type": "bar" | "pie",
        "df": <pd.DataFrame>,
        # bar:
        "x": <str>, "y": <str>,
        # pie:
        "names": <str>, "values": <str>,
        "title": <str>
    }
    """
    if not chart or not isinstance(chart, dict):
        return

    st.write("**Visualização:**")
    try:
        ctype = chart.get("type")
        cdf = chart.get("df")
        title = chart.get("title", "")
        if not isinstance(cdf, pd.DataFrame) or cdf.empty:
            st.info("Sem dados suficientes para o gráfico.")
            return

        if ctype == "bar":
            x = chart.get("x"); y = chart.get("y")
            if x not in cdf.columns or y not in cdf.columns:
                st.info("Configuração de gráfico de barras inválida.")
                return
            # Ordena por Y desc para leitura melhor
            cdf_plot = cdf.sort_values(by=y, ascending=False)
            fig = px.bar(cdf_plot, x=x, y=y, text=y, title=title, template="plotly_white")
            fig.update_traces(textposition="outside")
            fig.update_layout(xaxis_title=x, yaxis_title=y, uniformtext_minsize=10, uniformtext_mode="hide")
            st.plotly_chart(fig, use_container_width=True)

        elif ctype == "pie":
            names = chart.get("names"); values = chart.get("values")
            if names not in cdf.columns or values not in cdf.columns:
                st.info("Configuração de gráfico de pizza inválida.")
                return
            fig = px.pie(cdf, names=names, values=values, title=title, hole=0.35, template="plotly_white")
            # Deixa legível com muitas fatias
            fig.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig, use_container_width=True)

        else:
            st.info("Tipo de gráfico não suportado.")
    except Exception as e:
        st.info(f"Não foi possível renderizar o gráfico: {e}")


# ============================
# Sidebar (Upload / Banco de Dados)
# ============================
with st.sidebar:
    st.header("Fonte de Dados")
    data_source = st.radio(
        "Selecione a fonte de dados",
        ("Banco de Dados", "Upload Planilha (.xlsx)")
    )

    if data_source == "Upload Planilha (.xlsx)":
        st.header("📤 Upload da Planilha (.xlsx)")
        uploaded = st.file_uploader("Selecione o arquivo", type=["xlsx"])

        if uploaded is not None:
            with st.spinner("Lendo e preparando dados..."):
                st.session_state.df = _load_excel_to_df(uploaded)
            st.success("Planilha carregada e processada!")

    elif data_source == "Banco de Dados":
        with st.spinner("Carregando dados do banco de dados..."):
            st.session_state.df = _load_db_to_df()
        st.success("Dados do banco de dados carregados e processados!")


# ============================
# Corpo principal
# ============================
st.title("Oi, sou seu Agente de Estoque. Como posso lhe ajudar?")

# Toggle para usar IA apenas na resposta final
usar_ia = st.checkbox(
    "Resposta elaborada (IA)",
    value=False,
    help=(
        "Quando desativado: resposta imediata (regex + pandas). "
        "Quando ativado: a IA reformula apenas a frase final (os cálculos continuam em pandas)."
    )
)

question = st.text_input(
    "Digite sua pergunta sobre a planilha carregada:"
)

consultar = st.button("Consultar", type="primary")

if consultar:
    if st.session_state.df is None:
        st.warning("Faça o upload de uma planilha .xlsx na barra lateral ou selecione o banco de dados antes de consultar.")
    elif not question.strip():
        st.info("Digite uma pergunta e clique em Consultar.")
    else:
        t0 = time.time()
        # IA só na frase final se 'usar_ia' estiver ligado
        result = st.session_state.engine.answer(
            st.session_state.df,
            question.strip(),
            narrate=usar_ia  # 👈 chave: IA apenas na narrativa final
        )
        dt = time.time() - t0

        # st.write(f"⏱️ Tempo total: **{dt:.2f}s**")
        st.markdown(f"**Resposta:** {result.get('text', '')}")

        # Gráfico (se houver hint)
        chart = result.get("chart")
        if chart:
            _render_chart(chart)

        # Recorte / Resultados (se houver)
        table: pd.DataFrame = result.get("table", pd.DataFrame())
        if table is not None and not table.empty:
            st.write("**Recorte/Resultados:**")
            st.dataframe(table, use_container_width=True)

        # (Opcional) Detalhes de desempenho
        timings = result.get("timings", {})
        if timings:
            with st.expander("⏱️ Detalhes de desempenho (parse/pandas/LLM)"):
                st.json(timings, expanded=False)

# ============================
# Estética leve (opcional)
# ============================
hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
"""
st.markdown(hide_streamlit_style, unsafe_allow_html=True)