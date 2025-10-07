
# -*- coding: utf-8 -*-
import os
import tempfile
import streamlit as st
import pandas as pd
import openpyxl
import re
from io import BytesIO

from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ============================
# Config da página
# ============================
st.set_page_config(
    page_title="Agente de Estoque",
    page_icon="📦",
    layout="wide",
)

# ============================
# Session State
# ============================
if 'df' not in st.session_state:
    st.session_state.df = pd.DataFrame()
if 'comandos_acumulados' not in st.session_state:
    st.session_state.comandos_acumulados = []
if 'df_source' not in st.session_state:
    st.session_state.df_source = None  # 'db' ou 'upload'

# ============================
# Constantes
# ============================
TABLE_NAME = 'estoque_end'  # ✅ padronizado, minúsculo e sem aspas
SCHEMA_NAME = 'public'

# ============================
# Helpers
# ============================
def _normalize_blanks(df: pd.DataFrame) -> pd.DataFrame:
    """
    Converte strings compostas apenas por espaços em '', e NaN para ''.
    Evite preencher com ' ' (espaço), pois quebra lógicas de igualdade.
    """
    return df.replace(r'^\s+$', '', regex=True).fillna('')

def aplicar_regra_zero_vazio(df: pd.DataFrame) -> pd.DataFrame:
    """
    Se QTD == 0 ⇒ SKU = 'VAZIO' e DESCRIÇÃO = 'VAZIO'.
    Garante QTD numérica não-negativa.
    """
    df = df.copy()
    if 'QTD' not in df.columns:
        return df
    df['QTD'] = pd.to_numeric(df['QTD'], errors='coerce').fillna(0).astype(int)
    df['QTD'] = df['QTD'].clip(lower=0)  # nunca negativo
    zero_mask = df['QTD'].eq(0)
    if 'SKU' in df.columns:
        df.loc[zero_mask, 'SKU'] = 'VAZIO'
    if 'DESCRIÇÃO' in df.columns:
        df.loc[zero_mask, 'DESCRIÇÃO'] = 'VAZIO'
    return df

def _get_engine():
    """
    Constrói engine do Postgres. Requer DB_PASSWORD no ambiente.
    """
    db_config = {
        'dbname': 'OperacaoVIVO',
        'user': 'postgres',
        'password': os.getenv('DB_PASSWORD'),
        'host': 'localhost',
        'port': 5432
    }
    if not db_config['password']:
        raise ValueError("A senha do banco não foi encontrada na variável de ambiente 'DB_PASSWORD'.")
    return create_engine(
        f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}:{db_config['port']}/{db_config['dbname']}",
        echo=False
    )

def atualizar_endereco(df: pd.DataFrame) -> pd.DataFrame:
    """
    Padroniza FILA, TORRE, NÍVEL como strings upper/strip e recalcula ENDEREÇO = FILA-TORRE-NÍVEL.
    Mantém os nomes de colunas com acentos (compatível com sua planilha).
    """
    df = df.copy()
    required = ['FILA', 'TORRE', 'NÍVEL']
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes: {missing}")

    df['FILA'] = df['FILA'].astype(str).str.upper().str.strip()
    df['TORRE'] = df['TORRE'].astype(str).str.upper().str.strip()
    df['NÍVEL'] = df['NÍVEL'].astype(str).str.upper().str.strip()
    df['ENDEREÇO'] = df['FILA'] + '-' + df['TORRE'] + '-' + df['NÍVEL']
    return df

def _load_excel_to_df(uploaded_file) -> pd.DataFrame:
    """
    Lê Excel usando openpyxl, normaliza vazios e recalcula ENDEREÇO.
    """
    try:
        uploaded_file.seek(0)
        df_raw = pd.read_excel(uploaded_file, sheet_name=0, engine='openpyxl', dtype=str)
    except Exception:
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(suffix=".xlsx", delete=False) as tmp:
                tmp.write(uploaded_file.getbuffer())
                tmp_path = tmp.name
            df_raw = pd.read_excel(tmp_path, sheet_name=0, engine='openpyxl', dtype=str)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

    df = _normalize_blanks(df_raw)
    df = atualizar_endereco(df)
    df = aplicar_regra_zero_vazio(df)
    return df

def _load_db_to_df() -> pd.DataFrame:
    """
    Lê do Postgres (public.estoque_end), normaliza vazios e recalcula ENDEREÇO.
    """
    try:
        engine = _get_engine()
        with engine.connect() as conn:
            df = pd.read_sql(text(f'SELECT * FROM {SCHEMA_NAME}.{TABLE_NAME}'), conn)
        df = _normalize_blanks(df)
        df = atualizar_endereco(df)
        df = aplicar_regra_zero_vazio(df)
        return df
    except Exception as e:
        st.error(f"Erro ao carregar dados do banco de dados: {e}")
        return pd.DataFrame()

def save_to_db(df: pd.DataFrame):
    """
    Salva o DataFrame em public.estoque_end (replace, transacional).
    """
    try:
        engine = _get_engine()
        df_to_save = df.copy()
        df_to_save = aplicar_regra_zero_vazio(df_to_save)

        with engine.begin() as conn:  # transação: commit/rollback automático
            df_to_save.to_sql(
                TABLE_NAME,
                con=conn,
                if_exists='replace',
                index=False,
                method='multi',
                schema=SCHEMA_NAME
            )
        st.success("Dados registrados com sucesso no banco de dados.")
    except SQLAlchemyError as e:
        st.error(f"Erro ao salvar dados no banco de dados: {e}")

def drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicidades por ENDEREÇO + SKU (mais seguro do que em todas as colunas).
    """
    subset_cols = [c for c in ['ENDEREÇO', 'SKU'] if c in df.columns]
    if subset_cols:
        return df.drop_duplicates(subset=subset_cols).reset_index(drop=True)
    return df.drop_duplicates().reset_index(drop=True)

def regra_sku_vazio(df: pd.DataFrame, enderecos_afetados=None) -> pd.DataFrame:
    """
    Em cada ENDEREÇO, se existir qualquer SKU não-vazio (≠ 'VAZIO' e ≠ ''),
    remove linhas cujo SKU é 'VAZIO' (ou vazio lógico) naquele endereço.
    Se enderecos_afetados for passado, restringe a remoção a eles.
    """
    df = df.copy()
    if 'SKU' not in df.columns or 'ENDEREÇO' not in df.columns:
        return df

    sku_norm = df['SKU'].fillna('').astype(str).str.strip().str.upper()
    end_norm = df['ENDEREÇO'].fillna('').astype(str).str.strip().str.upper()

    is_vazio_logico = sku_norm.eq('VAZIO') | sku_norm.eq('')
    has_non_vazio = (~is_vazio_logico).groupby(end_norm).transform('any')
    drop_mask = is_vazio_logico & has_non_vazio

    if enderecos_afetados:
        alvo = {e.strip().upper() for e in enderecos_afetados}
        drop_mask = drop_mask & end_norm.isin(alvo)

    return df.loc[~drop_mask].reset_index(drop=True)

# ============================
# Core de comandos
# ============================
def process_command(df: pd.DataFrame, comando: str):
    """
    Comandos suportados:
      - "mudar sku <SKU> do/no endereço <A-B-C> para o <X-Y-Z>"
      - "trocar sku <SKU1> do/no endereço <A-B-C> com o <SKU2> do/no endereço <X-Y-Z>"
      - "tirar <QTD> <SKU> do/no endereço <A-B-C>"
      - "add <QTD> <SKU> no endereço <A-B-C>"
      - "limpar endereço <A-B-C>"
    Retorna (df_atualizado, mensagem, enderecos_tocados:set).
    """
    comando = (comando or '').lower().strip()
    enderecos_tocados = set()
    df = df.copy()

    # Garante colunas mínimas
    cols_min = ['SKU', 'DESCRIÇÃO', 'FILA', 'TORRE', 'NÍVEL', 'ENDEREÇO', 'QTD']
    for c in cols_min:
        if c not in df.columns:
            df[c] = ''

    # Normalização básica de tipos string
    df['SKU'] = df['SKU'].astype(str)
    df['DESCRIÇÃO'] = df['DESCRIÇÃO'].astype(str)
    df['ENDEREÇO'] = df['ENDEREÇO'].astype(str)

    # ------------------- MUDAR -------------------
    match_mudar = re.match(
        r"^mudar\s+sku\s+(\S+)\s+(?:do|no)\s+endereço\s+(\S+)\s+para\s+(?:o|a)\s+(\S+)$",
        comando
    )
    if match_mudar:
        sku, origem, destino = match_mudar.groups()
        sku_u = sku.upper().strip()
        origem = origem.upper().strip()
        destino = destino.upper().strip()
        enderecos_tocados.update([origem, destino])

        # Partes do destino
        dest_parts = destino.split("-")
        if len(dest_parts) < 2:
            return df, "Endereço de destino inválido.", enderecos_tocados

        fila_d = dest_parts[0].upper()
        torre_d = dest_parts[1].upper()
        nivel_d = dest_parts[2].upper() if len(dest_parts) > 2 and dest_parts[2] else ''

        # Linhas de origem (SKU no endereço de origem)
        cond_origem = (df['SKU'].str.upper() == sku_u) & (df['ENDEREÇO'].str.upper() == origem)
        if not cond_origem.any():
            return df, f"SKU {sku} no endereço {origem} não encontrado.", enderecos_tocados

        # QTD total a mover e descrição de referência
        qtd_move = pd.to_numeric(df.loc[cond_origem, 'QTD'], errors='coerce').fillna(0).astype(int).sum()
        desc_ref = df.loc[cond_origem, 'DESCRIÇÃO'].astype(str).values[0] if 'DESCRIÇÃO' in df.columns else ''

        # Condições de destino
        cond_dest = (df['ENDEREÇO'].str.upper() == destino)
        cond_dest_vazio = cond_dest & (df['SKU'].str.upper() == 'VAZIO')
        cond_dest_same = cond_dest & (df['SKU'].str.upper() == sku_u)
        cond_dest_occupied = cond_dest & (~df['SKU'].str.upper().isin(['', 'VAZIO', sku_u]))

        # Se destino está ocupado por outro SKU, avisa e não executa
        if cond_dest_occupied.any():
            return df, (
                f"Endereço {destino} já ocupado por outro SKU. "
                f"Use 'trocar' ou 'limpar endereço {destino}' antes de mover."
            ), enderecos_tocados

        # Aplicar no destino
        if cond_dest_same.any():
            # Soma QTD se já existir o mesmo SKU no destino
            df.loc[cond_dest_same, 'QTD'] = (
                pd.to_numeric(df.loc[cond_dest_same, 'QTD'], errors='coerce').fillna(0).astype(int) + int(qtd_move)
            )
            # Garante coordenadas corretas no destino
            df.loc[cond_dest_same, ['FILA', 'TORRE', 'NÍVEL']] = [fila_d, torre_d, nivel_d]

        elif cond_dest_vazio.any():
            # Converte VAZIO do destino em SKU alvo
            df.loc[cond_dest_vazio, ['SKU', 'DESCRIÇÃO', 'FILA', 'TORRE', 'NÍVEL', 'QTD']] = [
                sku_u, desc_ref, fila_d, torre_d, nivel_d, int(qtd_move)
            ]

        else:
            # Cria nova linha no destino
            nova_linha = {
                'SKU': sku_u,
                'DESCRIÇÃO': desc_ref,
                'FILA': fila_d,
                'TORRE': torre_d,
                'NÍVEL': nivel_d,
                'ENDEREÇO': destino,
                'QTD': int(qtd_move)
            }
            df = pd.concat([df, pd.DataFrame([nova_linha])], ignore_index=True)

        # Origem vira placeholder VAZIO com QTD 0
        df.loc[cond_origem, ['QTD', 'SKU', 'DESCRIÇÃO']] = [0, 'VAZIO', 'VAZIO']

        # Recalcula ENDEREÇO pelas coordenadas
        df = atualizar_endereco(df)
        return df, f"SKU {sku} movido de {origem} para {destino}.", enderecos_tocados

    # ------------------- TROCAR -------------------
    match_trocar = re.match(r"^trocar\s+sku\s+(\S+)\s+(?:do|no)\s+endereço\s+(\S+)\s+com\s+o\s+(\S+)\s+(?:do|no)\s+endereço\s+(\S+)$", comando)
    if match_trocar:
        sku1, endereco1, sku2, endereco2 = match_trocar.groups()
        endereco1 = endereco1.upper().strip()
        endereco2 = endereco2.upper().strip()
        enderecos_tocados.update([endereco1, endereco2])

        cond1 = (df['SKU'].str.upper() == sku1.upper()) & (df['ENDEREÇO'].str.upper() == endereco1)
        cond2 = (df['SKU'].str.upper() == sku2.upper()) & (df['ENDEREÇO'].str.upper() == endereco2)
        if cond1.any() and cond2.any():
            fila1, torre1, nivel1 = df.loc[cond1, ['FILA', 'TORRE', 'NÍVEL']].values[0]
            fila2, torre2, nivel2 = df.loc[cond2, ['FILA', 'TORRE', 'NÍVEL']].values[0]
            df.loc[cond1, ['FILA', 'TORRE', 'NÍVEL']] = [str(fila2).upper(), str(torre2).upper(), str(nivel2).upper()]
            df.loc[cond2, ['FILA', 'TORRE', 'NÍVEL']] = [str(fila1).upper(), str(torre1).upper(), str(nivel1).upper()]
            df = atualizar_endereco(df)
            return df, f"SKUs {sku1} e {sku2} trocados entre {endereco1} e {endereco2}.", enderecos_tocados
        return df, "Não foi possível encontrar ambos os SKUs e endereços.", enderecos_tocados

    # ------------------- TIRAR -------------------
    match_tirar = re.match(r"^tirar\s+(\d+)\s+(\S+)\s+(?:do|no)\s+endereço\s+(\S+)$", comando)
    if match_tirar:
        qtd, sku, endereco = match_tirar.groups()
        qtd = int(qtd)
        endereco = endereco.upper().strip()
        enderecos_tocados.add(endereco)

        cond = (df['SKU'].str.upper() == sku.upper()) & (df['ENDEREÇO'].str.upper() == endereco)
        if cond.any():
            df.loc[cond, 'QTD'] = pd.to_numeric(df.loc[cond, 'QTD'], errors='coerce').fillna(0).astype(int) - qtd
            df.loc[cond, 'QTD'] = df.loc[cond, 'QTD'].clip(lower=0)
            return df, f"{qtd} unidades retiradas do SKU {sku} no endereço {endereco}.", enderecos_tocados
        return df, "SKU e endereço não encontrados.", enderecos_tocados

    # ------------------- ADD -------------------
    match_add = re.match(r"^add\s+(\d+)\s+(\S+)\s+no\s+endereço\s+(\S+)$", comando)
    if match_add:
        qtd, sku, endereco = match_add.groups()
        qtd = int(qtd)
        endereco = endereco.upper().strip()
        enderecos_tocados.add(endereco)

        cond = (df['SKU'].str.upper() == sku.upper()) & (df['ENDEREÇO'].str.upper() == endereco)
        cond_vazio = (df['SKU'].str.upper() == "VAZIO") & (df['ENDEREÇO'].str.upper() == endereco)

        if cond.any():
            df.loc[cond, 'QTD'] = pd.to_numeric(df.loc[cond, 'QTD'], errors='coerce').fillna(0).astype(int) + qtd
            return df, f"{qtd} unidades adicionadas ao SKU {sku} no endereço {endereco}.", enderecos_tocados
        elif cond_vazio.any():
            desc_cond = df['SKU'].str.upper() == sku.upper()
            if desc_cond.any():
                descricao = df.loc[desc_cond, 'DESCRIÇÃO'].values[0]
                qtd_vazio = pd.to_numeric(df.loc[cond_vazio, 'QTD'], errors='coerce').fillna(0).astype(int).sum()
                df.loc[cond_vazio, 'SKU'] = sku.upper()
                df.loc[cond_vazio, 'DESCRIÇÃO'] = descricao
                df.loc[cond_vazio, 'QTD'] = qtd_vazio + qtd
                return df, f"SKU {sku} adicionado ao endereço {endereco} com {qtd} unidades (sobrescrevendo VAZIO).", enderecos_tocados
            else:
                return df, f"Descrição não encontrada para SKU {sku}.", enderecos_tocados
        else:
            sku_cond = df['SKU'].str.upper() == sku.upper()
            if sku_cond.any():
                descricao = df.loc[sku_cond, 'DESCRIÇÃO'].values[0]
                parts = endereco.split('-')
                fila = parts[0].upper() if len(parts) > 0 else ''
                torre = parts[1].upper() if len(parts) > 1 else ''
                nivel = parts[2].upper() if len(parts) > 2 else ''
                nova_linha = {
                    'SKU': sku.upper(),
                    'DESCRIÇÃO': descricao,
                    'FILA': fila,
                    'TORRE': torre,
                    'NÍVEL': nivel,
                    'ENDEREÇO': endereco,
                    'QTD': qtd
                }
                df = pd.concat([df, pd.DataFrame([nova_linha])], ignore_index=True)
                return df, f"Novo SKU {sku} adicionado ao endereço {endereco} com {qtd} unidades.", enderecos_tocados
            return df, "SKU não encontrado para copiar a descrição.", enderecos_tocados

    # ------------------- LIMPAR -------------------
    match_limpar = re.match(r"^limpar\s+endereço\s+(\S+)$", comando)
    if match_limpar:
        endereco = match_limpar.group(1).upper().strip()
        enderecos_tocados.add(endereco)

        # ✅ Normaliza ENDEREÇO para remover TODAS as variações (espaços, case)
        end_norm = df['ENDEREÇO'].fillna('').astype(str).str.strip().str.upper()
        df = df.loc[~end_norm.eq(endereco)].copy()

        # Monta placeholder VAZIO para o endereço solicitado
        parts = endereco.split("-")
        fila = parts[0].upper() if len(parts) > 0 else ''
        torre = parts[1].upper() if len(parts) > 1 else ''
        nivel = parts[2].upper() if len(parts) > 2 else ''

        nova_linha = {
            'SKU': 'VAZIO',
            'DESCRIÇÃO': 'VAZIO',
            'FILA': fila,
            'TORRE': torre,
            'NÍVEL': nivel,
            'ENDEREÇO': endereco,
            'QTD': 0
        }
        df = pd.concat([df, pd.DataFrame([nova_linha])], ignore_index=True)

        # Garante ENDEREÇO recalculado a partir das colunas base
        df = atualizar_endereco(df)

        return df, f"Endereço {endereco} limpo e marcado como VAZIO.", enderecos_tocados

def sort_endereco_key(endereco):
    if isinstance(endereco, str):
        parts = endereco.strip().upper().split('-')
        letra = parts[0] if len(parts) > 0 and parts[0] else 'ZZZ'
        torre = int(parts[1]) if len(parts) > 1 and parts[1].isdigit() else 9999
        nivel = int(parts[2]) if len(parts) > 2 and parts[2].isdigit() else 9999
        return (letra, torre, nivel)
    return ('ZZZ', 9999, 9999)

def reorganizar_e_salvar(df, ws):
    """
    Ordena por ENDEREÇO (A-1-1, A-1-2, ...), remove a coluna ENDEREÇO ao salvar,
    e escreve de volta nas colunas originais (pula a coluna ENDEREÇO ao escrever).
    """
    df_sorted = df.copy()
    df_sorted['__sort_key__'] = df_sorted['ENDEREÇO'].apply(sort_endereco_key)
    df_sorted = df_sorted.sort_values('__sort_key__').drop(columns='__sort_key__')
    df_to_save = df_sorted.drop(columns=['ENDEREÇO'])

    col_names = [cell.value for cell in ws[1]]
    for i, row in enumerate(df_to_save.itertuples(index=False), start=2):
        col_idx = 0
        for j, value in enumerate(row, start=1):
            col_name = col_names[j-1]
            if col_name == "ENDEREÇO":
                col_idx += 1
            ws.cell(row=i, column=j + col_idx, value=value)
    return df_sorted

# ============================
# INTERFACE STREAMLIT
# ============================
st.title("MOVIMENTAR ESTOQUE")

with st.sidebar:
    # Uploader e botão de carregar BD juntos
    uploaded_file = st.file_uploader("Faça upload da sua planilha (.xlsx)", type=["xlsx"])
    download_placeholder = st.empty()

    if st.button("Carregar do Banco de Dados"):
        st.session_state.df = _load_db_to_df()
        st.session_state.df_source = 'db'
        if not st.session_state.df.empty:
            st.success("Dados do banco de dados carregados.")
        else:
            st.warning("Não foi possível carregar os dados do banco de dados.")

    # Botão global de salvar no banco (independente da origem atual do DF)
    if st.button("Registrar alterações no Banco de Dados"):
        if not st.session_state.df.empty:
            save_to_db(st.session_state.df)
        else:
            st.warning("Nada para salvar: DataFrame vazio.")

# Upload → carrega, normaliza e atualiza estado (não precisa trocar de aba)
if uploaded_file is not None:
    st.session_state.df = _load_excel_to_df(uploaded_file)
    st.session_state.df_source = 'upload'
    st.success("Planilha carregada.")

# Entrada de comando
comando = st.text_input("Digite o comando:")

if st.button("Aplicar alteração"):
    if not st.session_state.df.empty:
        df_proc, msg, enderecos_tocados = process_command(st.session_state.df, comando)

        # Normaliza vazios ANTES das regras
        df_proc = _normalize_blanks(df_proc)

        # Aplica a regra QTD==0 => VAZIO antes da regra de remoção
        df_proc = aplicar_regra_zero_vazio(df_proc)

        # Regras auxiliares
        df_proc = drop_duplicates(df_proc)                         # subset ENDEREÇO+SKU
        df_proc = regra_sku_vazio(df_proc, enderecos_afetados=enderecos_tocados)
        df_proc = atualizar_endereco(df_proc)

        # Atualiza estado
        st.session_state.df = df_proc

        # Log
        st.session_state.comandos_acumulados.append((comando, msg))
        st.success(msg)
        st.dataframe(df_proc, use_container_width=True)
    else:
        st.warning("Nenhum dado foi carregado. Por favor, faça o upload de uma planilha ou carregue os dados do banco de dados.")

if st.checkbox("Mostrar comandos acumulados"):
    st.write("Comandos acumulados:")
    for comando_txt, msg_txt in st.session_state.comandos_acumulados:
        st.write(f"- {comando_txt}: {msg_txt}")

# Saída: Upload → Excel atualizado para download
if uploaded_file and not st.session_state.df.empty:
    try:
        uploaded_file.seek(0)
        wb = openpyxl.load_workbook(uploaded_file)
        ws = wb.active

        df_sorted = reorganizar_e_salvar(st.session_state.df, ws)

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        with st.sidebar:
            download_placeholder.download_button(
                label="Baixar planilha atualizada",
                data=output.getvalue(),
                file_name="Estoque_atualizado.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    except Exception as e:
        st.error(f"Erro ao gerar planilha atualizada: {e}")