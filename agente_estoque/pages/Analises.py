# -*- coding: utf-8 -*-
"""Agente_ST.py — UI minimalista/local

- Carrega o motor e o modelo LLM ao abrir (warm-up).
- Regex + pandas fazem tudo; LLM só reescreve UMA frase no final (opcional via toggle).
- Renderiza gráficos (Plotly) quando o engine retornar "chart" no result (sem IA).
"""
from __future__ import annotations

import os
import time
import tempfile
from pathlib import Path
from typing import Any, Dict, Optional

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

from Engine import StockEngine, EngineConfig  # mantém a API existente

# ============================
# Config da página
# ============================
st.set_page_config(
    page_title="Agente de Estoque",
    page_icon="📦",
    layout="wide",
)

PAGE_TITLE = "Agente de Estoque"
SESSION_ENGINE_KEY = "engine"
SESSION_DF_KEY = "df"


# ============================
# Estado & Inicialização
# ============================
if SESSION_ENGINE_KEY not in st.session_state:
    with st.spinner("Carregando motor e modelo local…"):
        # Carrega e aquece o modelo para evitar atraso na 1ª resposta
        st.session_state[SESSION_ENGINE_KEY] = StockEngine(
            EngineConfig(
                use_llm=True,          # mantém o modelo carregado em memória
                warmup_on_init=True,   # aquece (1 token)
                warmup_max_tokens=1,
                n_ctx=512,             # contexto menor = mais rápido
                n_threads=6,           # ajuste ao seu i5 (ou use os.cpu_count())
                max_tokens=36,         # UMA frase curta para reescrever
                temperature=0.1,
            )
        )

if SESSION_DF_KEY not in st.session_state:
    st.session_state[SESSION_DF_KEY] = None


# ============================
# Helpers
# ============================
def _load_excel_to_df(uploaded_file: Any) -> pd.DataFrame:
    """Lê o Excel de `uploaded_file`. Tenta via file-like; se falhar, salva temporário.

    Em seguida, aplica normalizações/classificações do engine.
    Mantém a API e semântica do código original.

    Parâmetros
    ----------
    uploaded_file : Any
        Objeto retornado por `st.file_uploader` (file-like).

    Retorna
    -------
    pd.DataFrame
        DataFrame já classificado pelo engine.
    """
    engine = st.session_state[SESSION_ENGINE_KEY]

    # 1) Tenta ler direto do file-like
    try:
        df_raw = engine.read_excel(uploaded_file, sheet_name=0)
    except Exception:
        # 2) Fallback: salva como arquivo temporário .xlsx
        tmp_path: Optional[Path] = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp.write(uploaded_file.getbuffer())
                tmp_path = Path(tmp.name)
            df_raw = engine.read_excel(str(tmp_path), sheet_name=0)
        finally:
            if tmp_path and tmp_path.exists():
                try:
                    tmp_path.unlink(missing_ok=True)
                except Exception:
                    # Falha ao apagar o temporário não é crítica para o fluxo.
                    pass

    # Normalização/classificação conforme engine
    df = engine.classify_supplier_by_description(df_raw)
    return df


def _resolve_db_password() -> str:
    """Resolve a senha do banco a partir de `st.secrets` ou variável de ambiente.

    Ordem de resolução:
    1. `st.secrets["DB_PASSWORD"]`
    2. `os.getenv("DB_PASSWORD")`

    Retorna
    -------
    str
        A senha encontrada.

    Levanta
    -------
    ValueError
        Se nenhuma fonte tiver a senha configurada.
    """
    # Preferir st.secrets se existir
    try:
        secrets_pw = st.secrets.get("DB_PASSWORD")  # type: ignore[attr-defined]
        if secrets_pw:
            return str(secrets_pw)
    except Exception:
        # st.secrets pode não estar disponível (modo local sem secrets.toml)
        pass

    env_pw = os.getenv("DB_PASSWORD")
    if env_pw:
        return env_pw

    raise ValueError(
        "A senha do banco não foi encontrada. Defina `st.secrets['DB_PASSWORD']` "
        "ou a variável de ambiente 'DB_PASSWORD'."
    )


def _load_db_to_df() -> pd.DataFrame:
    """Conecta ao PostgreSQL e carrega dados de `estoque_end` para um DataFrame.

    Retorna
    -------
    pd.DataFrame
        DataFrame já classificado pelo engine.

    Observações
    -----------
    - Usa `sqlalchemy` com driver `psycopg2`.
    - Não faz cache propositalmente, pois inventário costuma ser dinâmico.
      (Se quiser, dá para aplicar `@st.cache_data(ttl=60)` no `pd.read_sql`.)
    """
    try:
        db_config: Dict[str, Any] = {
            "dbname": "OperacaoVIVO",
            "user": "postgres",
            "password": _resolve_db_password(),
            "host": "localhost",
            "port": 5432,
        }

        # String de conexão
        url = (
            f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        )
        engine_sa = create_engine(url)

        query = "SELECT * FROM estoque_end"
        df = pd.read_sql(query, engine_sa)

        # Normalização/classificação conforme engine
        df = st.session_state[SESSION_ENGINE_KEY].classify_supplier_by_description(df)
        return df

    except Exception as e:
        # Mensagem amigável para o usuário; detalhes técnicos ficam no log/terminal
        st.error(f"Erro ao carregar dados do banco de dados: {e}")
        return pd.DataFrame()


def _render_chart(chart: Dict[str, Any]) -> None:
    """Renderiza gráficos Plotly com base no hint retornado pelo engine.

    Parâmetros
    ----------
    chart : dict
        Dicionário esperado:
        - "type": {"bar", "pie"}
        - "df": pd.DataFrame
        - Para "bar": "x": str, "y": str
        - Para "pie": "names": str, "values": str
        - "title": str (opcional)
    """
    if not chart or not isinstance(chart, dict):
        return

    st.write("**Visualização:**")
    try:
        ctype = chart.get("type")
        cdf: Any = chart.get("df")
        title = chart.get("title", "")

        if not isinstance(cdf, pd.DataFrame) or cdf.empty:
            st.info("Sem dados suficientes para o gráfico.")
            return

        if ctype == "bar":
            x, y = chart.get("x"), chart.get("y")
            if not all(col in cdf.columns for col in (x, y)):
                st.info("Configuração de gráfico de barras inválida.")
                return

            # Ordena por Y desc para leitura melhor
            cdf_plot = cdf.sort_values(by=y, ascending=False)
            fig = px.bar(
                cdf_plot,
                x=x,
                y=y,
                text=y,
                title=title,
                template="plotly_white",
            )
            fig.update_traces(textposition="outside")
            fig.update_layout(
                xaxis_title=x,
                yaxis_title=y,
                uniformtext_minsize=10,
                uniformtext_mode="hide",
                margin=dict(l=16, r=16, t=48, b=16),
            )
            st.plotly_chart(fig, use_container_width=True)

        elif ctype == "pie":
            names, values = chart.get("names"), chart.get("values")
            if not all(col in cdf.columns for col in (names, values)):
                st.info("Configuração de gráfico de pizza inválida.")
                return

            fig = px.pie(
                cdf,
                names=names,
                values=values,
                title=title,
                hole=0.35,
                template="plotly_white",
            )
            # Mais legível com muitas fatias
            fig.update_traces(textposition="inside", textinfo="percent+label")
            fig.update_layout(margin=dict(l=16, r=16, t=48, b=16))
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
        ("Banco de Dados", "Upload Planilha (.xlsx)"),
        index=0,
        help="Escolha entre carregar do PostgreSQL ou fazer upload de um .xlsx",
    )

    if data_source == "Upload Planilha (.xlsx)":
        st.header("📤 Upload da Planilha (.xlsx)")
        uploaded = st.file_uploader("Selecione o arquivo", type=["xlsx"])

        if uploaded is not None:
            with st.spinner("Lendo e preparando dados…"):
                st.session_state[SESSION_DF_KEY] = _load_excel_to_df(uploaded)
            st.success("Planilha carregada e processada!")

    elif data_source == "Banco de Dados":
        with st.spinner("Carregando dados do banco de dados…"):
            st.session_state[SESSION_DF_KEY] = _load_db_to_df()
        st.success("Dados do banco de dados carregados e processados!")


# ============================
# Corpo principal
# ============================
st.title("Oi, sou seu Agente de Estoque. Como posso lhe ajudar?")

# Resumo rápido dos dados carregados (se houver)
df_atual = st.session_state[SESSION_DF_KEY]
if isinstance(df_atual, pd.DataFrame) and not df_atual.empty:
    st.caption(f"Dados carregados: {len(df_atual):,} linhas × {len(df_atual.columns)} colunas".replace(",", "."))

# Formulário para evitar re-execução a cada tecla e agrupar submissão
with st.form(key="form_consulta", clear_on_submit=False):
    usar_ia = st.checkbox(
        "Resposta elaborada (IA)",
        value=False,
        help=(
            "Quando desativado: resposta imediata (regex + pandas). "
            "Quando ativado: a IA reformula apenas a frase final (os cálculos continuam em pandas)."
        ),
    )

    question = st.text_input("Digite sua pergunta sobre a planilha carregada:")
    consultar = st.form_submit_button("Consultar", type="primary")

if consultar:
    if st.session_state[SESSION_DF_KEY] is None:
        st.warning("Faça o upload de uma planilha .xlsx na barra lateral ou selecione o banco de dados antes de consultar.")
    elif not question.strip():
        st.info("Digite uma pergunta e clique em Consultar.")
    else:
        try:
            t0 = time.perf_counter()
            result: Dict[str, Any] = st.session_state[SESSION_ENGINE_KEY].answer(
                st.session_state[SESSION_DF_KEY],
                question.strip(),
                narrate=usar_ia,  # IA apenas na narrativa final
            )
            dt = time.perf_counter() - t0

            st.markdown(f"**Resposta:** {result.get('text', '')}")
            st.caption(f"⏱️ Tempo total: {dt:.2f}s")

            # Gráfico (se houver hint)
            chart = result.get("chart")
            if chart:
                _render_chart(chart)

            # Recorte / Resultados (se houver)
            table: pd.DataFrame = result.get("table", pd.DataFrame())
            if isinstance(table, pd.DataFrame) and not table.empty:
                st.write("**Recorte/Resultados:**")
                st.dataframe(table, use_container_width=True)

            # (Opcional) Detalhes de desempenho
            timings = result.get("timings", {})
            if isinstance(timings, dict) and timings:
                with st.expander("⏱️ Detalhes de desempenho (parse/pandas/LLM)"):
                    st.json(timings, expanded=False)

        except Exception as e:
            st.error(f"Não foi possível processar a consulta: {e}")


# ============================
# Estética leve (opcional)
# ============================
hide_streamlit_style = """
    <style>
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    </style>
"""
