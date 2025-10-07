# -*- coding: utf-8 -*-
import os
import json
import pickle
from pathlib import Path
from typing import Dict, Tuple, Optional

import numpy as np
import pandas as pd
import streamlit as st
from pandas.tseries.offsets import BusinessDay, MonthEnd

# Para unpickle/treino de modelos
from neuralforecast import NeuralForecast
from neuralforecast.models import NHITS

# DB
try:
    import sqlalchemy as sa
except Exception:
    sa = None  # Mensagem amigável se tentar treinar sem sqlalchemy/psycopg2

# =========================
# CONFIG BÁSICA DA PÁGINA
# =========================
st.set_page_config(
    page_title="Previsão D+1 — CD Belo Horizonte",
    page_icon="📦",
    layout="wide"
)

st.title("📦 Previsão D+1 — CD Belo Horizonte")

# =======================================
# CAMINHOS
# =======================================
BASE_ROOT = Path(r"C:\Users\40418197\Desktop\ML Pedidos")
PIPELINE_ROOT = BASE_ROOT / "pipeline"
RUNTIME_DIR = BASE_ROOT / "runtime_store"
RUNTIME_DIR.mkdir(parents=True, exist_ok=True)

HIST_PATH = RUNTIME_DIR / "history.csv"
PRED_LOG = RUNTIME_DIR / "preds_log.csv"

DEFAULT_PIPELINE_DIR = PIPELINE_ROOT / "MG_2025-10-01_1316"

# =======================
# FUNÇÕES AUXILIARES
# =======================
def ensure_business_day(ds: pd.Timestamp) -> pd.Timestamp:
    """Garante um dia útil (Seg–Sex). Não contempla feriados."""
    ds = pd.to_datetime(ds).floor("D")
    while ds.weekday() > 4:  # 5=sáb, 6=dom
        ds += BusinessDay(1)
    return ds

def is_business_month_end_last3(ds) -> int:
    """1 se 'ds' é um dos 3 últimos dias úteis do mês; 0 caso contrário."""
    ds = pd.to_datetime(ds).floor("D")
    mstart = ds.replace(day=1)
    mend = (mstart + MonthEnd(1))
    bidx = pd.bdate_range(mstart, mend, freq="B")
    if len(bidx) == 0:
        return 0
    return int(ds in set(bidx[-3:]))

def add_exogs(df: pd.DataFrame) -> pd.DataFrame:
    """Adiciona exógenas de calendário a um df com ['unique_id','ds',('y')]."""
    out = df.copy()
    out["ds"] = pd.to_datetime(out["ds"]).dt.floor("D")
    out["dow_num"] = out["ds"].dt.dayofweek
    out["mes_num"] = out["ds"].dt.month
    out["fechamento"] = out["ds"].apply(is_business_month_end_last3).astype(int)
    return out

def to_bin_index_centered(y, bin_size=200):
    """Índice de faixa via arredondamento ao múltiplo de BIN_SIZE mais próximo."""
    y = np.maximum(0, np.asarray(y))
    return np.floor((y + bin_size/2) / bin_size).astype(int)

def bin_label_from_idx(idx, bin_size=200):
    a = int(idx * bin_size)
    b = int((idx + 1) * bin_size - 1)
    return f"{a}–{b}"

def load_history() -> pd.DataFrame:
    if HIST_PATH.exists():
        return pd.read_csv(HIST_PATH, parse_dates=["ds"])
    return pd.DataFrame(columns=["unique_id", "ds", "y"])

def append_history_row(ds, values: dict):
    """values = {'Simcard': int, 'Terminal': int}"""
    ds = pd.to_datetime(ds).floor("D")
    rows = [{"unique_id": uid, "ds": ds, "y": int(v)} for uid, v in values.items()]
    df_new = pd.DataFrame(rows)
    df_old = load_history()
    df_all = (
        pd.concat([df_old, df_new], ignore_index=True)
          .drop_duplicates(subset=["unique_id", "ds"], keep="last")
          .sort_values(["unique_id", "ds"])
    )
    df_all.to_csv(HIST_PATH, index=False)

def append_pred_log(df_preds: pd.DataFrame):
    """Salva/atualiza log de previsões."""
    df_preds = df_preds.copy()
    df_preds["ts_logged"] = pd.Timestamp.now()
    if PRED_LOG.exists():
        old = pd.read_csv(PRED_LOG, parse_dates=["ds", "ts_logged"])
        out = (
            pd.concat([old, df_preds], ignore_index=True)
              .drop_duplicates(subset=["unique_id", "ds", "run_dir"], keep="last")
        )
    else:
        out = df_preds
    out.to_csv(PRED_LOG, index=False)

def update_bias_ewma(old_bias: float, error: float, alpha: float = 0.3) -> float:
    """EWMA do viés: novo = (1-alpha)*antigo + alpha*(erro do dia)."""
    if pd.isna(old_bias):
        old_bias = 0.0
    return float((1 - alpha) * old_bias + alpha * error)

# =======================
# CARREGAR PIPELINE
# =======================
@st.cache_resource(show_spinner=True)
def load_pipeline(pipeline_dir: Path) -> Tuple[dict, Dict[str, object], Dict[str, float], Path]:
    """
    Retorna: (config, models, biases, run_dir)
    - config: dict
    - models: dict[uid] -> NeuralForecast carregado (via pickle)
    - biases: dict[uid] -> float
    """
    run_dir = Path(pipeline_dir)
    if not run_dir.exists():
        raise FileNotFoundError(f"Pipeline não encontrado: {run_dir}")

    # Config
    with open(run_dir / "config.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    # Modelos
    models = {}
    for uid in config["TIPOS_OK"]:
        pkl_path = run_dir / f"{uid}.pkl"
        if not pkl_path.exists():
            raise FileNotFoundError(f"Modelo não encontrado: {pkl_path}")
        with open(pkl_path, "rb") as f:
            models[uid] = pickle.load(f)

    # Biases
    biases_path = run_dir / "biases.json"
    if biases_path.exists():
        with open(biases_path, "r", encoding="utf-8") as f:
            biases = json.load(f)
    else:
        biases = {uid: 0.0 for uid in config["TIPOS_OK"]}

    return config, models, biases, run_dir

# =======================
# ETL (reuso no re-treino DB)
# =======================
def etl_build_Y_df(df_raw: pd.DataFrame,
                   UF_ALVO: str = "MG",
                   TIPOS_OK: Optional[list] = None) -> pd.DataFrame:
    """
    Replica sua ETL para gerar Y_df com dias úteis e features.
    Requer colunas: ['DT NF','TIPO MAT','MES','UF'].
    """
    TIPOS_OK = TIPOS_OK or ["Simcard", "Terminal"]
    COLS_KEEP = ["DT NF", "TIPO MAT", "MES", "UF"]

    missing = set(COLS_KEEP) - set(df_raw.columns)
    if missing:
        raise ValueError(f"Colunas obrigatórias ausentes no dataset: {missing}")

    df = df_raw[COLS_KEEP].copy()
    df = df.query("UF == @UF_ALVO and `TIPO MAT` in @TIPOS_OK").copy()

    df["DT NF"] = pd.to_datetime(df["DT NF"])
    df["dia"] = df["DT NF"].dt.floor("D")

    daily = (df.groupby(["TIPO MAT", "dia"], as_index=False)
               .size().rename(columns={"size": "y"}))

    def make_business_series(g):
        g = g.set_index("dia").sort_index()
        bidx = pd.date_range(g.index.min(), g.index.max(), freq="B")
        g = g.reindex(bidx)
        g.index.name = "ds"
        g["y"] = g["y"].fillna(0).astype(int)
        g["dow_num"] = g.index.dayofweek
        g["mes_num"] = g.index.month
        g["fechamento"] = 0
        for _, gi in g.groupby(g.index.to_period('M')):
            last3 = gi.index.sort_values()[-3:]
            g.loc[last3, "fechamento"] = 1
        return g.reset_index()

    TIPOS_OK = list(TIPOS_OK)
    series = {}
    for t in TIPOS_OK:
        g = daily[daily["TIPO MAT"] == t][["dia", "y"]].copy()
        if g.empty:
            raise ValueError(f"Sem dados para o tipo '{t}' após filtro UF={UF_ALVO}.")
        series[t] = make_business_series(g)
        series[t]["unique_id"] = t

    Y_df = pd.concat([series[t] for t in TIPOS_OK], ignore_index=True)[
        ["unique_id", "ds", "y", "dow_num", "mes_num", "fechamento"]
    ].sort_values(["unique_id", "ds"]).reset_index(drop=True)

    return Y_df

# ==============================
# PREVISÃO D+1 (produção)
# ==============================
def predict_next_bday_for_all_uids(pipeline_dir: Path,
                                   history_df: pd.DataFrame = None,
                                   return_all_cols: bool = False,
                                   verbose: bool = False) -> pd.DataFrame:
    """
    - Usa histórico local (runtime) para ancorar D+1 e prover janela de contexto.
    - Prevê D+1 por UID com exógenas, aplica bias e classifica em faixas (BIN_SIZE).
    - Loga em preds_log.csv.
    """
    config, models, biases, run_dir = load_pipeline(pipeline_dir)
    BIN_SIZE = int(config.get("BIN_SIZE", 200))

    hist = load_history() if history_df is None else history_df.copy()
    if hist.empty:
        raise ValueError("Histórico vazio. Registre pelo menos um dia de valores reais antes de prever D+1.")

    out_rows = []
    for uid in config["TIPOS_OK"]:
        g = hist[hist["unique_id"] == uid].dropna(subset=["y"]).sort_values("ds")
        if g.empty:
            raise ValueError(f"Sem histórico para UID '{uid}'. Registre valores reais primeiro.")

        last_ds = pd.to_datetime(g["ds"].max()).floor("D")
        ds_next = ensure_business_day(last_ds + BusinessDay(1))

        input_size = int(config.get("cfg_modelos", {}).get(uid, {}).get("input_size", 32))
        g_tail = g.iloc[-max(input_size, 40):].copy()

        df_hist_exog = add_exogs(g_tail[["unique_id", "ds", "y"]])
        futr_next = add_exogs(pd.DataFrame({"unique_id": [uid], "ds": [ds_next]}))

        nf = models[uid]
        pred = nf.predict(
            df=df_hist_exog[["unique_id", "ds", "y", "dow_num", "mes_num", "fechamento"]],
            futr_df=futr_next[["unique_id", "ds", "dow_num", "mes_num", "fechamento"]]
        )

        pred_col = next(c for c in pred.columns if c not in ["unique_id", "ds"])
        pred = pred.rename(columns={pred_col: "yhat"}).sort_values("ds")

        bias = float(biases.get(uid, 0.0))
        pred["yhat_cal"] = pred["yhat"] + bias
        idx0 = to_bin_index_centered(pred["yhat"].values, BIN_SIZE)[0]
        idx1 = to_bin_index_centered(pred["yhat_cal"].values, BIN_SIZE)[0]
        pred["faixa"] = bin_label_from_idx(idx0, BIN_SIZE)
        pred["faixa_cal"] = bin_label_from_idx(idx1, BIN_SIZE)

        if return_all_cols:
            pred = pred.merge(futr_next, on=["unique_id", "ds"], how="left")

        pred["run_dir"] = str(run_dir)
        out_rows.append(pred)

        if verbose:
            st.info(f"{uid}: D+1 {ds_next.date()} → faixa {pred['faixa_cal'].iloc[0]}")

    result = (
        pd.concat(out_rows, ignore_index=True)
          .sort_values(["unique_id", "ds"])
          .reset_index(drop=True)
    )
    append_pred_log(result[["unique_id", "ds", "yhat", "yhat_cal", "faixa", "faixa_cal", "run_dir"]])
    return result

def eod_submit_and_predict(ds_real,
                           real_simcard: int,
                           real_terminal: int,
                           alpha_bias: float,
                           pipeline_dir: Path) -> pd.DataFrame:
    """
    Fluxo EOD: registra reais, atualiza bias (se houver previsão logada) e prevê D+1.
    """
    ds_real = pd.to_datetime(ds_real).floor("D")

    # 1) histórico (inteiros)
    append_history_row(ds_real, {"Simcard": int(real_simcard), "Terminal": int(real_terminal)})

    # 2) bias (EWMA) usando a previsão logada do dia, se houver
    config, _, biases, run_dir = load_pipeline(pipeline_dir)
    if PRED_LOG.exists():
        log = pd.read_csv(PRED_LOG, parse_dates=["ds"])
        todays = log[log["ds"] == ds_real]
        if not todays.empty:
            y_true_map = {"Simcard": int(real_simcard), "Terminal": int(real_terminal)}
            new_biases = {}
            for uid in config["TIPOS_OK"]:
                row = todays[todays["unique_id"] == uid]
                if not row.empty:
                    yhat_cal = float(row["yhat_cal"].iloc[0])
                    err = float(y_true_map[uid] - yhat_cal)
                    old = float(biases.get(uid, 0.0))
                    new_biases[uid] = update_bias_ewma(old, err, alpha_bias)
                else:
                    new_biases[uid] = float(biases.get(uid, 0.0))
            with open(Path(run_dir) / "biases.json", "w", encoding="utf-8") as f:
                json.dump(new_biases, f, ensure_ascii=False, indent=2)

    # 3) previsão D+1
    prev_next = predict_next_bday_for_all_uids(pipeline_dir=run_dir, verbose=False)
    return prev_next

# =======================
# HELPERS DE APRESENTAÇÃO
# =======================
def render_range_messages(df: pd.DataFrame):
    """Mostra a data D+1 e as faixas (intervalos) para cada UID."""
    if df is None or df.empty:
        st.warning("Sem resultados para exibir.")
        return

    ds_next = pd.to_datetime(df["ds"].iloc[0]).date()
    st.markdown(f"**Data da previsão (D+1):** {ds_next.strftime('%d/%m/%Y')}")

    name_map = {"Simcard": "simcards", "Terminal": "terminal"}
    for uid in df["unique_id"].unique():
        row = df.loc[df["unique_id"] == uid].iloc[0]
        faixa_label = row["faixa_cal"] if "faixa_cal" in row and pd.notna(row["faixa_cal"]) else row.get("faixa", "")
        try:
            a_str, b_str = str(faixa_label).replace("–", "-").split("-")
            a, b = int(a_str), int(b_str)
            st.markdown(
                f"**previsão para {name_map.get(uid, uid.lower())}:** "
                f"entre **{a}** e **{b}** pedidos  _(faixa: {faixa_label})_"
            )
        except Exception:
            st.markdown(f"**previsão para {name_map.get(uid, uid.lower())}:** faixa **{faixa_label}**")

# =======================
# RE-TREINO (POSTGRES)
# =======================
def _get_engine():
    """
    Construir engine do Postgres. Requer DB_PASSWORD no ambiente.
    """
    db_config = {
        'dbname': 'OperacaoVIVO',
        'user': 'postgres',
        'password': os.getenv('DB_PASSWORD'),
        'host': 'localhost',
        'port': 5432
    }
    if not db_config['password']:
        raise RuntimeError("Variável de ambiente DB_PASSWORD não encontrada.")
    # Monta a URL SQLAlchemy
    url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@" \
          f"{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
    if sa is None:
        raise RuntimeError("Pacote 'sqlalchemy' não está instalado.")
    return sa.create_engine(url)

def _retrain_from_postgres(pipeline_dir: Path, keep_bias_flag: bool = True) -> str:
    """
    Lê 'pedidos' do Postgres, executa ETL, treina NHITS por UID conforme o config atual
    e salva um novo pipeline versionado. Retorna o path do novo pipeline.
    """
    # 0) Carrega config/bias atuais para herdar parâmetros
    config_old, _, biases_old, _ = load_pipeline(pipeline_dir)
    TIPOS_OK = config_old["TIPOS_OK"]
    UF_ALVO = config_old.get("UF_ALVO", "MG")
    H = int(config_old.get("H", 1))
    cfg_modelos = config_old.get("cfg_modelos", {})

    # 1) Ler do Postgres — tabela 'pedidos'
    engine = _get_engine()
    # Atenção: colunas com espaço — usar aspas duplas
    cols = '"DT NF", "TIPO MAT", "MES", "UF"'
    query = f'SELECT {cols} FROM pedidos'
    df_raw = pd.read_sql(query, engine, parse_dates=["DT NF"])
    engine.dispose()

    if df_raw.empty:
        raise ValueError("Consulta ao banco retornou vazio (tabela 'pedidos').")

    # 2) ETL → Y_df
    Y_df = etl_build_Y_df(df_raw, UF_ALVO=UF_ALVO, TIPOS_OK=TIPOS_OK)

    # 3) Treinar NHITS por UID
    forecasters = {}
    for uid in TIPOS_OK:
        params = cfg_modelos.get(uid, {"input_size": 32, "max_steps": 400, "dropout": 0.0})
        model = NHITS(
            input_size=params["input_size"],
            h=H,
            max_steps=params["max_steps"],
            batch_size=32,
            scaler_type='robust',
            dropout_prob_theta=params.get("dropout", 0.0),
            futr_exog_list=["dow_num","mes_num","fechamento"],
        )
        nf = NeuralForecast(models=[model], freq='B')
        df_u = Y_df[Y_df["unique_id"] == uid][["unique_id","ds","y","dow_num","mes_num","fechamento"]]
        if df_u.empty:
            raise ValueError(f"Histórico vazio para {uid} após ETL.")
        nf.fit(df=df_u)
        forecasters[uid] = nf

    # 4) Salvar novo pipeline versionado
    RUN_ID = f"{UF_ALVO}_{pd.Timestamp.today().strftime('%Y-%m-%d_%H%M')}"
    NEW_DIR = PIPELINE_ROOT / RUN_ID
    NEW_DIR.mkdir(parents=True, exist_ok=True)

    for uid, nf in forecasters.items():
        with open(NEW_DIR / f"{uid}.pkl", "wb") as f:
            pickle.dump(nf, f)

    # Copia config antigo (mantendo hiperparâmetros)
    config_new = dict(config_old)
    config_new["TIPOS_OK"] = TIPOS_OK
    with open(NEW_DIR / "config.json", "w", encoding="utf-8") as f:
        json.dump(config_new, f, ensure_ascii=False, indent=2)

    # Biases: mantém ou zera
    out_biases = biases_old if keep_bias_flag else {uid: 0.0 for uid in TIPOS_OK}
    with open(NEW_DIR / "biases.json", "w", encoding="utf-8") as f:
        json.dump(out_biases, f, ensure_ascii=False, indent=2)

    return str(NEW_DIR)

# =======================
# UI — SIDEBAR
# =======================
st.sidebar.header("Configuração")

# Pipelines disponíveis
pipeline_dirs = sorted([p for p in PIPELINE_ROOT.glob("*") if p.is_dir()], key=lambda p: p.name, reverse=True)
default_index = pipeline_dirs.index(DEFAULT_PIPELINE_DIR) if DEFAULT_PIPELINE_DIR in pipeline_dirs else 0

selected_pipeline = st.sidebar.selectbox(
    "Escolha o pipeline",
    options=pipeline_dirs,
    index=default_index if pipeline_dirs else 0,
    format_func=lambda p: p.name
)

# Slider do bias (inicial 0,20)
alpha_bias = st.sidebar.slider("Alpha do Bias (EWMA)", min_value=0.05, max_value=0.9, value=0.20, step=0.05)
st.sidebar.caption("Bias corrige tendência do modelo com média móvel exponencial dos erros.")

# Re-treino simples (Postgres)
st.sidebar.subheader("Treinamento — Banco (Postgres)")
keep_bias = st.sidebar.checkbox("Manter bias atual após re-treino", value=True)

if st.sidebar.button("🔁 Re-treinar Modelo"):
    try:
        with st.sidebar.status("Re-treinando a partir do Postgres...", expanded=True) as status:
            new_dir = _retrain_from_postgres(selected_pipeline, keep_bias_flag=keep_bias)
            status.update(label=f"✅ Novo pipeline salvo em: {new_dir}", state="complete", expanded=False)
        st.success(f"Novo pipeline salvo: `{Path(new_dir).name}`. Selecione-o no menu ou recarregue a página para usar.")
    except Exception as e:
        st.sidebar.error(f"Falha no re-treino: {e}")

# Carrega pipeline selecionado (se nenhum re-treino foi disparado)
with st.spinner("Carregando pipeline..."):
    try:
        config, models, biases, run_dir = load_pipeline(selected_pipeline)
        st.success(f"Pipeline carregado: `{run_dir.name}`")
    except Exception as e:
        st.error(f"Erro ao carregar pipeline: {e}")
        st.stop()

st.divider()

# =======================
# SEÇÃO — EOD PRIMEIRO
# =======================
st.subheader("Registrar Pedidos do dia e prever de Amanhã")

today_default = pd.Timestamp.today().floor("D")
# Formulário: inputs + submit DENTRO do bloco
with st.form(key="eod_form"):
    colA, colB, colC = st.columns([1.2, 1, 1])
    with colA:
        ds_real = st.date_input("Data do dia (real)", value=today_default, format="DD/MM/YYYY")
    with colB:
        real_sim = st.number_input("Real — Simcard", min_value=0, value=0, step=1)
    with colC:
        real_term = st.number_input("Real — Terminal", min_value=0, value=0, step=1)

    submitted = st.form_submit_button("Registrar EOD e Prever D+1")

# Ação após submit
if submitted:
    try:
        prev_next = eod_submit_and_predict(
            ds_real=ds_real,
            real_simcard=int(real_sim),
            real_terminal=int(real_term),
            alpha_bias=float(alpha_bias),
            pipeline_dir=run_dir
        )
        st.success("EOD registrado e previsão D+1 gerada.")
        render_range_messages(prev_next)
    except Exception as e:
        st.error(f"Falha no EOD/Previsão: {e}")

st.divider()

# # =======================
# # SEÇÃO — PREVER SEM REGISTRAR (opcional)
# # =======================
# st.subheader("Prever D+1 (sem enviar real agora)")
# if st.button("Prever D+1 agora"):
#     try:
#         prev = predict_next_bday_for_all_uids(pipeline_dir=run_dir, verbose=True)
#         st.success("Previsão gerada e logada em `preds_log.csv`.")
#         render_range_messages(prev)
#     except Exception as e:
#         st.error(f"Falha ao prever D+1: {e}")

# st.divider()

# # =======================
# # SEÇÃO — HISTÓRICO (apenas se existir)
# # =======================
# hist = load_history()
# if not hist.empty:
#     st.subheader("📜 Histórico (history.csv)")
#     st.dataframe(hist.sort_values(["unique_id", "ds"]).tail(20), use_container_width=True)

st.caption("• A previsão D+1 usa o próximo dia útil após o último `ds` do histórico. ")
