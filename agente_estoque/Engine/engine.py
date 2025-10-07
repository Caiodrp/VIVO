# -*- coding: utf-8 -*-
"""
engine.py — pandas decide (rápido); LLM só entra no final para narrar.
Implementa "PROJETA ⇒ FILTRA": primeiro nome de coluna citado é a SAÍDA,
o segundo é a COLUNA de filtro, seguida do(s) valor(es).

Exemplos:
- "Endereços do SKU TGSA56224000?" => out=endereço; filtro=sku; valor=TGSA56224000
- "SKUs do endereço A-2-2?"       => out=sku; filtro=endereço; valor=A-2-2

Mantidas funções clássicas: qts fornecedor, skus por fila/torre, total geral.
Gráficos (sem IA): dicas em result["chart"] para o Streamlit renderizar via Plotly.
"""

from dataclasses import dataclass
from typing import Optional, Dict, List, Tuple
import os
import re
import time
import pandas as pd

try:
    from llama_cpp import Llama  # type: ignore
except ImportError:
    Llama = None


# ============================
# Configuração
# ============================
@dataclass
class EngineConfig:
    use_llm: bool = True
    model_path: Optional[str] = None
    n_ctx: int = 2048               # mantendo o contexto em 2048 para não sobrecarregar a RAM
    n_threads: int = 12             # uso máximo das threads disponíveis
    temperature: float = 0.1
    max_tokens: int = 256           # adequado para respostas concisas
    # Parâmetros específicos para narração (não afetam o motor pandas)
    narration_max_tokens: int = 256
    narration_temperature: float = 0.2
    # Pré-carregamento e aquecimento (para reduzir cold start)
    warmup_on_init: bool = True
    warmup_max_tokens: int = 1


class StockEngine:
    # ---- Sinônimos por coluna (singular/plural/variações) ----
    COL_SYNONYMS = {
        "sku": ["sku", "skus", "material", "materiais", "código", "codigo", "códigos", "codigos", "item", "itens", "id", "ids"],
        "descricao": ["descrição", "descricao", "descrições", "descricoes", "desc", "nome", "nomes"],
        "fornecedor": ["fornecedor", "fornecedores", "fabricante", "fabricantes", "marca", "marcas", "vendor", "vendors"],
        "qtd": ["qtd", "quantidade", "quantidades", "qtd_estoque", "qtd."],
        "endereco": ["endereço", "endereco", "endereços", "enderecos", "local", "locais", "lugar", "lugares", "onde", "end"],
        "fila": ["fila", "filas", "rua", "corredor", "corredores"],
        "torre": ["torre", "torres", "estante", "estantes", "coluna", "colunas"],
        "nivel": ["nível", "nivel", "níveis", "niveis", "prateleira", "prateleiras", "andar", "andares"],
    }

    LABEL_SING = {
        "sku": "SKU", "descricao": "descrição", "fornecedor": "fornecedor", "qtd": "quantidade",
        "endereco": "endereço", "fila": "fila", "torre": "torre", "nivel": "nível",
    }
    LABEL_PLUR = {
        "sku": "SKUs", "descricao": "descrições", "fornecedor": "fornecedores",
        "endereco": "Endereços", "fila": "Filas", "torre": "Torres", "nivel": "Níveis",
    }

    def __init__(self, cfg: EngineConfig):
        """
        Inicializa o motor:
        - Guarda cfg
        - Se use_llm=True: carrega o modelo do cfg.model_path ou de ../Models/*.gguf
        - Faz warm-up opcional para evitar latência na 1ª resposta
        """
        self.cfg = cfg
        self.llm = None

        if not self.cfg.use_llm:
            return  # modo turbo (sem IA)

        # 1) Caminho do modelo, se informado
        model = self.cfg.model_path
        if model:
            model = os.path.abspath(os.path.expanduser(str(model)))

        # 2) Caso não informado, procura no diretório ../Models (fixo)
        if not model or not os.path.isfile(model):
            # ../Models relativo a Engine/engine.py
            pkg_dir = os.path.dirname(os.path.abspath(__file__))   # .../Engine
            base_dir = os.path.dirname(pkg_dir)                    # raiz do projeto
            models_dir = os.path.join(base_dir, "Models")

            if os.path.isdir(models_dir):
                ggufs = [os.path.join(models_dir, f) for f in os.listdir(models_dir) if f.lower().endswith(".gguf")]
                ggufs.sort()
                if ggufs:
                    model = ggufs[0]

        # 3) Carrega a LLM (se houver binding e arquivo)
        if model and Llama is not None and os.path.isfile(model):
            try:
                self.llm = Llama(
                    model_path=model,
                    n_ctx=self.cfg.n_ctx,
                    n_threads=self.cfg.n_threads,
                    verbose=False,
                )
                # 4) Warm-up opcional (1 token) para evitar cold start
                if getattr(self.cfg, "warmup_on_init", False):
                    try:
                        self.llm.create_completion(
                            prompt="ok",
                            max_tokens=getattr(self.cfg, "warmup_max_tokens", 1),
                            temperature=0.0,
                            stop=["\n"],  # ok só no warm-up
                        )
                    except Exception:
                        pass
            except Exception:
                self.llm = None
        else:
            # Sem modelo/binding -> segue sem IA
            self.llm = None

    # ============================
    # I/O & Normalização
    # ============================
    def read_excel(self, src, sheet_name=0):
        return pd.read_excel(src, sheet_name=sheet_name, engine="openpyxl")

    def classify_supplier_by_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Normalizações:
        - QTD numérica (VAZIO/vazio => 0)
        - Derivar FILA/TORRE/NIVEL a partir de ENDEREÇO (padrão F-T-N), se necessário
        """
        df = df.copy()
        cols = self._detect_columns(df)

        # QTD → número
        qtd_col = cols.get("qtd")
        if qtd_col:
            df[qtd_col] = (
                df[qtd_col].astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^\d\.\-]", "", regex=True)
                .replace({"": "0", "VAZIO": "0", "vazio": "0"})
            )
            df[qtd_col] = pd.to_numeric(df[qtd_col], errors="coerce").fillna(0).astype(int)

        # Derivar FILA/TORRE/NIVEL a partir de ENDEREÇO
        end_col = cols.get("endereco")
        if end_col and (cols.get("fila") is None or cols.get("torre") is None or cols.get("nivel") is None):
            df = self._derive_from_endereco(df, end_col)

        return df

    # ============================
    # Colunas & Utils
    # ============================
    def _find_col(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        cols_lower = {c.lower(): c for c in df.columns}
        for name in candidates:
            if name.lower() in cols_lower:
                return cols_lower[name.lower()]
        for c in df.columns:  # fallback por inclusão parcial
            if any(n.lower() in c.lower() for n in candidates):
                return c
        return None

    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        return {
            "qtd": self._find_col(df, self.COL_SYNONYMS["qtd"]),
            "fornecedor": self._find_col(df, self.COL_SYNONYMS["fornecedor"]),
            "sku": self._find_col(df, self.COL_SYNONYMS["sku"]),
            "descricao": self._find_col(df, self.COL_SYNONYMS["descricao"]),
            "endereco": self._find_col(df, self.COL_SYNONYMS["endereco"]),
            "fila": self._find_col(df, self.COL_SYNONYMS["fila"]),
            "torre": self._find_col(df, self.COL_SYNONYMS["torre"]),
            "nivel": self._find_col(df, self.COL_SYNONYMS["nivel"]),
        }

    def _derive_from_endereco(self, df: pd.DataFrame, end_col: str) -> pd.DataFrame:
        df = df.copy()
        if end_col not in df.columns:
            return df
        for c in ("FILA", "TORRE", "NIVEL"):
            if c not in df.columns:
                df[c] = pd.NA

        def _split_end(s: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
            if not isinstance(s, str):
                return (None, None, None)
            parts = re.split(r"[\s\-]+", s.strip())
            if len(parts) >= 3:
                return (parts[0] or None, parts[1] or None, parts[2] or None)
            m = re.match(r"^\s*([A-Za-z])\s*[- ]?\s*(\d{1,2})(?:\s*[- ]\s*(\d{1,2}))?\s*$", s)
            if m:
                return (m.group(1), m.group(2), m.group(3))
            return (None, None, None)

        parsed = [_split_end(str(x)) for x in df[end_col].astype(str).fillna("").tolist()]
        df.loc[:, "FILA"]  = [p[0] for p in parsed]
        df.loc[:, "TORRE"] = [p[1] for p in parsed]
        df.loc[:, "NIVEL"] = [p[2] for p in parsed]
        return df

    def _normalize_token(self, s: str) -> str:
        return re.sub(r"[\s\-_/]", "", str(s)).casefold()

    # === Extractors ===
    def _extract_sku_from_question(self, df: pd.DataFrame, sku_col: Optional[str], question: str) -> List[str]:
        if not sku_col:
            return []
        skus_norm = {self._normalize_token(x): str(x) for x in df[sku_col].dropna().astype(str).unique()}
        tokens = re.findall(r"[A-Za-z0-9\-_/]{4,}", question)
        hits = []
        for t in tokens:
            t_norm = self._normalize_token(t)
            if t_norm in skus_norm:
                hits.append(skus_norm[t_norm])
        return list(dict.fromkeys(hits))

    def _extract_address_tokens(self, question: str) -> List[str]:
        toks = re.findall(r"\b[A-Za-z]\s*-\s*\d{1,2}(?:\s*-\s*\d{1,2})?\b", question, flags=re.IGNORECASE)
        return [re.sub(r"\s*", "", t) for t in toks]

    def _extract_simple_value(self, question: str, key: str) -> List[str]:
        """
        Para fila/torre/nivel/fornecedor/descricao:
        - Primeiro tenta valores entre aspas (simples/duplas/tipográficas).
        - Senão, captura sequência de palavras após o sinônimo (permitindo espaços),
          até pontuação forte ou fim da frase.
        """
        q = question

        # 1) Aspas simples, duplas e tipográficas
        quoted = re.findall(r"\"“”‘’'[\"“”‘’']", q)
        if quoted:
            return [v.strip() for v in quoted if v and v.strip()]

        # 2) Captura de sequência após sinônimo (aceita acentos)
        synonyms = self.COL_SYNONYMS.get(key, [])
        for syn in synonyms:
            m = re.search(
                rf"{re.escape(syn)}\s*(?:[:=]\s*)?([A-Za-zÀ-ÿ0-9\-/_.\s]{{2,}}?)(?=[,.;\n\r]|$)",
                q,
                flags=re.IGNORECASE
            )
            if m:
                val = m.group(1).strip()
                if val:
                    return [val]

        # 3) Fallback: último token alfanumérico
        last = re.findall(r"[A-Za-z0-9\-_/]{2,}", q)
        return [last[-1]] if last else []

    # ============================
    # Parser: PROJETA⇒FILTRA + intents clássicas
    # ============================
    def _match_first_col_key(self, q: str) -> Optional[Tuple[str, Tuple[int,int], str]]:
        """Retorna (col_key, (start,end), matched_text) da primeira coluna citada na frase."""
        best = None
        for key, syns in self.COL_SYNONYMS.items():
            for syn in syns:
                for m in re.finditer(rf"\b{re.escape(syn)}\b", q, flags=re.IGNORECASE):
                    pos = (m.start(), m.end())
                    if best is None or pos[0] < best[1][0]:
                        best = (key, pos, m.group(0))
        return best

    def _match_next_col_key(self, q: str, start_after: int) -> Optional[Tuple[str, Tuple[int,int], str]]:
        best = None
        sub = q[start_after:]
        off = start_after
        for key, syns in self.COL_SYNONYMS.items():
            for syn in syns:
                for m in re.finditer(rf"\b{re.escape(syn)}\b", sub, flags=re.IGNORECASE):
                    pos = (m.start()+off, m.end()+off)
                    if best is None or pos[0] < best[1][0]:
                        best = (key, pos, m.group(0))
        return best

    def _parse_projection_query(self, q: str) -> Optional[Dict]:
        """
        PROJETA⇒FILTRA:
        "<Saída> do/de/da <Filtro> <valor(es)>"
        """
        qn = q.strip()
        first = self._match_first_col_key(qn)
        if not first:
            return None
        out_key, (s0, e0), _ = first

        second = self._match_next_col_key(qn, e0)
        if not second:
            return None
        filt_key, (s1, e1), _ = second

        # valores brutos (refinados depois com o DF)
        values: List[str] = []
        if filt_key == "endereco":
            values = self._extract_address_tokens(qn)
        elif filt_key in ("fila", "torre", "nivel", "fornecedor", "descricao"):
            values = self._extract_simple_value(qn, filt_key)

        return {"intent": "project_filter", "out_key": out_key, "filter_key": filt_key, "raw_values": values}

    def _parse_intent(self, q: str) -> Dict:
        # 1) PROJETA⇒FILTRA
        pf = self._parse_projection_query(q)
        if pf:
            return pf

        # 2) Clássicas
        ql = q.lower().strip()
        if any(t in ql for t in ["estoque total", "total geral", "soma geral"]):
            return {"intent": "total_geral"}

        # soma entre SKUs (pizza)
        if any(k in ql for k in ["soma", "total", "somar", "quantidade"]) and any(k in ql for k in ["sku", "skus", "itens", "materiais"]):
            return {"intent": "sum_skus"}

        if ("onde" in ql) or ("em qual endereço" in ql) or ("em qual endereco" in ql):
            return {"intent": "enderecos_por_sku"}  # legado

        if (("qual" in ql) or ("quais" in ql)) and any(t in ql for t in ["endereço", "endereco", "local", "lugar"]):
            return {"intent": "skus_por_endereco"}  # legado

        m = re.search(r"skus?\s+na\s+fila\s+([a-z0-9\-]+)\s+torre\s+([a-z0-9\-]+)", ql)
        if m:
            return {"intent": "skus_por_fila_torre", "fila": m.group(1), "torre": m.group(2)}

        m = re.search(r"skus?\s+na\s+fila\s+([a-z0-9\-]+)", ql)
        if m:
            return {"intent": "skus_por_fila", "fila": m.group(1)}

        m = re.search(r"\bqts?\s+(.+?)\s+no\s+estoque\b", ql)
        if m:
            return {"intent": "qtd_por_fornecedor", "fornecedor": m.group(1).strip(" ?")}

        if any(w in ql for w in ["qtd", "quantidade", "soma", "quanto tem", "qts", "quantos", "média", "media"]):
            return {"intent": "sum_generic"}

        return {"intent": "unknown"}

    # ============================
    # LLM — reescrita de UMA frase e intro explicativa (listas)
    # ============================
    def _narrate_sentence(self, sentence: str, allow_multi: bool = False, max_tokens: Optional[int] = None) -> str:
        """Reescreve a frase de forma natural. Se LLM desligada, devolve a própria frase.
        allow_multi=True permite 1–3 frases; caso contrário, 1 frase.
        """
        if not (self.cfg.use_llm and self.llm):
            return sentence
        instr = "Reescreva em pt-BR de forma natural e objetiva, preservando exatamente as mesmas informações. "
        if allow_multi:
            instr += "Use entre 1 e 3 frases. Não invente dados."
        else:
            instr += "Use 1 frase. Não invente dados."
        prompt = f"{instr}\n\nFrase:\n{sentence}\n\nReescrita:\n"
        try:
            out = self.llm.create_completion(
                prompt=prompt,
                max_tokens=max_tokens or max(self.cfg.narration_max_tokens // 3, 96),
                temperature=max(self.cfg.temperature, self.cfg.narration_temperature),
                top_p=0.9,
                repeat_penalty=1.1,
                # sem 'stop' para não cortar cedo
            )
            text = out["choices"][0]["text"].strip()
            return text if text else sentence
        except Exception:
            return sentence

    def _most_common_text(self, s: pd.Series, topn: int = 1) -> List[str]:
        """Retorna os valores textuais mais frequentes (limpos) de uma série."""
        s = s.dropna().astype(str).str.strip()
        if s.empty:
            return []
        vc = s.value_counts()
        vals = vc.index.tolist()[:topn]
        return [v[:120] for v in vals if v]

    def _format_addr_label(self, addr: str) -> str:
        """Remove espaços e hífens pendurados do endereço, ex.: 'O-5-' -> 'O-5'."""
        t = re.sub(r"\s+", "", str(addr))
        t = re.sub(r"[-]+$", "", t)
        return t

    def _narrate_intro_explanatory(self, payload_explain: Dict[str, str]) -> str:
        """
        Gera um pequeno parágrafo (1–3 frases) explicando o contexto:
        - SKU, descrição (se houver), fornecedor/marca (se houver)
        - Total de unidades, nº de endereços
        - Destaque: endereço com maior concentração
        NÃO inclui a lista detalhada; a lista é anexada depois de forma determinística.
        """
        # Frase determinística caso LLM desligada:
        base = []
        sku = payload_explain.get("sku")
        desc = payload_explain.get("descricao")
        forn = payload_explain.get("fornecedor")
        total = payload_explain.get("total_unidades")
        n_end = payload_explain.get("num_enderecos")
        top_loc = payload_explain.get("top_endereco")
        top_qtd = payload_explain.get("top_qtd")

        if sku:
            if desc and forn:
                base.append(f"O SKU {sku} ({desc}, {forn})")
            elif desc:
                base.append(f"O SKU {sku} ({desc})")
            elif forn:
                base.append(f"O SKU {sku} ({forn})")
            else:
                base.append(f"O SKU {sku}")
        if n_end and total:
            base.append(f"está distribuído em {n_end} endereço(s), somando {total} unidades")
        elif n_end:
            base.append(f"está distribuído em {n_end} endereço(s)")
        if top_loc and top_qtd:
            base.append(f"com maior concentração em {top_loc} ({top_qtd} unidade(s))")

        deterministic = ". ".join(base).strip() + "."
        if not (self.cfg.use_llm and self.llm):
            return deterministic

        prompt = (
            "Reescreva em pt-BR de forma natural e informativa, em 1 a 3 frases, "
            "mantendo exatamente os mesmos dados e sem incluir listas detalhadas. Não invente informações.\n\n"
            f"Texto-base:\n{deterministic}\n\nReescrita:\n"
        )
        try:
            out = self.llm.create_completion(
                prompt=prompt,
                max_tokens=max(self.cfg.narration_max_tokens // 2, 128),
                temperature=max(self.cfg.temperature, self.cfg.narration_temperature),
                top_p=0.9,
                repeat_penalty=1.1,
            )
            text = out["choices"][0]["text"].strip()
            return text if text else deterministic
        except Exception:
            return deterministic

    # ============================
    # Schema mínimo (somente campos usados)
    # ============================
    def _schema_min(self, df: pd.DataFrame, cols: Dict[str, Optional[str]], used: List[str]) -> str:
        def _typ(col):
            if col is None or col not in df.columns:
                return "ausente"
            dt = str(df[col].dtype)
            return "numérico" if ("int" in dt or "float" in dt) else "texto"
        return "\n".join(f"- {cols.get(k) or k.upper()} ({_typ(cols.get(k))})" for k in used)

    # ============================
    # Helper: remover duplicadas preservando ordem (para colunas da table)
    # ============================
    def _unique_preserve(self, seq: List[Optional[str]]) -> List[str]:
        """Remove duplicadas preservando a ordem e ignorando None."""
        seen = set()
        out: List[str] = []
        for x in seq:
            if not x:
                continue
            if x not in seen:
                out.append(x)
                seen.add(x)
        return out

    # ============================
    # Funções pandas — PROJETA⇒FILTRA (genérica) e clássicas
    # ============================
    def _fn_project_filter(self, df: pd.DataFrame, cols: Dict[str, str], out_key: str, filt_key: str, question: str, raw_values: List[str]):
        out_col  = cols.get(out_key)
        fk_col   = cols.get(filt_key)
        qtd_col  = cols.get("qtd")

        if not out_col or not fk_col:
            return False, "Colunas ausentes para a consulta.", {}, pd.DataFrame(), self._schema_min(df, cols, [out_key, filt_key])

        # Extração de valores com DF em mãos
        if filt_key == "sku":
            values = self._extract_sku_from_question(df, fk_col, question)
        elif filt_key == "endereco":
            values = self._extract_address_tokens(question) or raw_values
        else:
            values = raw_values or self._extract_simple_value(question, filt_key)

        if not values:
            return False, f"Valor de filtro para {self.LABEL_SING.get(filt_key, filt_key)} não identificado.", {}, pd.DataFrame(), self._schema_min(df, cols, [filt_key])

        # Máscara por tipo
        if filt_key == "sku":
            mask = df[fk_col].astype(str).str.lower().isin([v.lower() for v in values])
        else:
            patt = "|".join(re.escape(v) for v in values)
            mask = df[fk_col].astype(str).str.contains(patt, case=False, na=False)

        df_f = df[mask]
        if df_f.empty:
            return False, f"Nenhum resultado para {self.LABEL_SING.get(filt_key, filt_key)} = {', '.join(values)}.", {}, pd.DataFrame(), self._schema_min(df, cols, [filt_key])

        # Agrupa por out_col (soma QTD se disponível) e formata itens
        if qtd_col:
            g = df_f.groupby([out_col], dropna=False)[qtd_col].sum().reset_index().sort_values(qtd_col, ascending=False)
            def _fmt_out(v):
                return self._format_addr_label(v) if out_key == "endereco" else str(v)
            items = [f"{_fmt_out(r[out_col])} (qtd {int(r[qtd_col])})" for _, r in g.iterrows()]
        else:
            uniq = df_f[out_col].dropna().astype(str).unique().tolist()
            items = [self._format_addr_label(u) if out_key == "endereco" else u for u in uniq]
            g = pd.DataFrame()  # dummy

        list_str = "; ".join(items)
        out_label = self.LABEL_PLUR.get(out_key, out_col)
        filt_label = self.LABEL_SING.get(filt_key, fk_col)
        template = "Os {out_label} do(a) {filt_label} {filt_value} são: {list_str}."
        payload  = {"out_label": out_label, "filt_label": filt_label, "filt_value": ", ".join(values), "list_str": list_str}

        # ===== Enriquecimento para narração explicativa (endereços do SKU) =====
        if out_key == "endereco" and filt_key == "sku":
            sku_col = cols.get("sku")
            desc_col = cols.get("descricao")
            forn_col = cols.get("fornecedor")

            payload_explain = {}
            payload_explain["sku"] = ", ".join(values)
            if desc_col:
                tops = self._most_common_text(df_f[desc_col], topn=1)
                if tops:
                    payload_explain["descricao"] = tops[0]
            if forn_col:
                tops = self._most_common_text(df_f[forn_col], topn=1)
                if tops:
                    payload_explain["fornecedor"] = tops[0]
            if qtd_col:
                total = int(pd.to_numeric(df_f[qtd_col], errors="coerce").fillna(0).sum())
                payload_explain["total_unidades"] = str(total)
            payload_explain["num_enderecos"] = str(len(g) if qtd_col else len(items))
            if qtd_col and isinstance(g, pd.DataFrame) and not g.empty:
                top_row = g.iloc[0]
                payload_explain["top_endereco"] = self._format_addr_label(str(top_row[out_col]))
                payload_explain["top_qtd"] = str(int(top_row[qtd_col]))

            payload["__explain"] = payload_explain
            payload["__intent"]  = "enderecos_por_sku"

        # ====== Chart hints ======
        if qtd_col and isinstance(g, pd.DataFrame) and not g.empty:
            # Caso 1: Endereços do SKU -> Barras por endereço
            if out_key == "endereco" and filt_key == "sku":
                chart_df = g.copy()
                chart_df[out_col] = chart_df[out_col].astype(str).map(self._format_addr_label)
                payload["__chart"] = {
                    "type": "bar",
                    "df": chart_df[[out_col, qtd_col]],
                    "x": out_col,
                    "y": qtd_col,
                    "title": f"QTD do SKU {', '.join(values)} por endereço"
                }

            # Caso 2: SKUs do endereço/fila/torre -> Pizza por SKU
            if out_key == "sku" and filt_key in ("endereco", "fila", "torre", "nivel"):
                payload["__chart"] = {
                    "type": "pie",
                    "df": g[[cols.get("sku") or out_col, qtd_col]],
                    "names": cols.get("sku") or out_col,
                    "values": qtd_col,
                    "title": f"Distribuição de QTD por SKU"
                }

        # >>>>>> Tabela de apoio (ENRIQUECIDA + DEDUP) <<<<<<
        raw_keep = [
            out_col, fk_col,
            cols.get("descricao"),
            cols.get("fornecedor"),
            cols.get("endereco"),
            cols.get("fila"), cols.get("torre"), cols.get("nivel"),
            qtd_col
        ]
        keep_cols = self._unique_preserve(raw_keep)
        table = df_f.loc[:, keep_cols].drop_duplicates().head(5000)

        schema = self._schema_min(df, cols, [out_key, filt_key, "qtd"])
        return True, template, payload, table, schema

    def _fn_qtd_por_fornecedor(self, df: pd.DataFrame, cols: Dict[str, str], fornecedor: str):
        qtd_col = cols["qtd"]
        forn_col = cols.get("fornecedor")
        desc_col = cols.get("descricao")
        fornecedor_re = re.escape(fornecedor)

        df_f = pd.DataFrame()
        if forn_col:
            df_f = df[df[forn_col].astype(str).str.contains(fornecedor_re, case=False, na=False)]
        if df_f.empty and desc_col:
            df_f = df[df[desc_col].astype(str).str.contains(fornecedor_re, case=False, na=False)]

        total = int(pd.to_numeric(df_f[qtd_col], errors="coerce").fillna(0).sum()) if not df_f.empty else 0
        template = "Total de unidades no estoque para {fornecedor}: {total}."
        payload  = {"fornecedor": fornecedor, "total": str(total)}
        table = df_f[[c for c in [cols.get("sku"), cols.get("descricao"), forn_col, qtd_col, cols.get("endereco")] if c]].head(2000) if not df_f.empty else pd.DataFrame()
        schema = self._schema_min(df, cols, ["fornecedor", "qtd", "sku", "descricao", "endereco"])
        return True, template, payload, table, schema

    def _fn_skus_por_fila(self, df: pd.DataFrame, cols: Dict[str, str], fila: str):
        sku_col = cols.get("sku"); fila_col = cols.get("fila"); qtd_col = cols["qtd"]
        if not (fila_col and sku_col):
            return False, "Colunas de FILA ou SKU ausentes.", {}, pd.DataFrame(), self._schema_min(df, cols, ["fila", "sku"])
        df_f = df[df[fila_col].astype(str).str.contains(re.escape(fila), case=False, na=False)]
        if df_f.empty:
            return False, f"Nenhum SKU encontrado na fila {fila}.", {}, pd.DataFrame(), self._schema_min(df, cols, ["fila"])

        g = df_f.groupby([sku_col], dropna=False)[qtd_col].sum().reset_index().sort_values(qtd_col, ascending=False)
        list_str = "; ".join(f"{str(r[sku_col])} (qtd {int(r[qtd_col])})" for _, r in g.iterrows())
        template = "Na fila {fila}, os SKUs encontrados são: {list_str}."
        payload  = {"fila": fila, "list_str": list_str}

        # >>>>>> Tabela de apoio (ENRIQUECIDA + DEDUP) <<<<<<
        extra = [cols.get("descricao"), cols.get("fornecedor"), cols.get("endereco")]
        base_cols = self._unique_preserve([sku_col, fila_col, qtd_col] + extra)
        table = df_f.loc[:, base_cols].head(5000)
        schema = self._schema_min(df, cols, ["fila", "sku", "qtd"])

        # Chart: pizza por SKU
        payload["__chart"] = {
            "type": "pie",
            "df": g[[sku_col, qtd_col]],
            "names": sku_col,
            "values": qtd_col,
            "title": f"Distribuição de QTD por SKU na fila {fila}"
        }
        return True, template, payload, table, schema

    def _fn_skus_por_fila_torre(self, df: pd.DataFrame, cols: Dict[str, str], fila: str, torre: str):
        sku_col = cols.get("sku"); fila_col = cols.get("fila"); torre_col = cols.get("torre"); qtd_col = cols["qtd"]
        if not (fila_col and torre_col and sku_col):
            return False, "Colunas de FILA/TORRE ou SKU ausentes.", {}, pd.DataFrame(), self._schema_min(df, cols, ["fila", "torre", "sku"])
        df_f = df[
            df[fila_col].astype(str).str.contains(re.escape(fila), case=False, na=False) &
            df[torre_col].astype(str).str.contains(re.escape(torre), case=False, na=False)
        ]
        if df_f.empty:
            return False, f"Nenhum SKU encontrado na fila {fila} torre {torre}.", {}, pd.DataFrame(), self._schema_min(df, cols, ["fila", "torre"])

        g = df_f.groupby([sku_col], dropna=False)[qtd_col].sum().reset_index().sort_values(qtd_col, ascending=False)
        list_str = "; ".join(f"{str(r[sku_col])} (qtd {int(r[qtd_col])})" for _, r in g.iterrows())
        template = "Na fila {fila}, torre {torre}, os SKUs encontrados são: {list_str}."
        payload  = {"fila": fila, "torre": torre, "list_str": list_str}

        # >>>>>> Tabela de apoio (ENRIQUECIDA + DEDUP) <<<<<<
        extra = [cols.get("descricao"), cols.get("fornecedor"), cols.get("endereco")]
        base_cols = self._unique_preserve([sku_col, fila_col, torre_col, qtd_col] + extra)
        table = df_f.loc[:, base_cols].head(5000)
        schema = self._schema_min(df, cols, ["fila", "torre", "sku", "qtd"])

        # Chart: pizza por SKU
        payload["__chart"] = {
            "type": "pie",
            "df": g[[sku_col, qtd_col]],
            "names": sku_col,
            "values": qtd_col,
            "title": f"Distribuição de QTD por SKU – fila {fila}, torre {torre}"
        }
        return True, template, payload, table, schema

    def _fn_total_geral(self, df: pd.DataFrame, cols: Dict[str, str]):
        qtd_col = cols["qtd"]
        total = int(pd.to_numeric(df[qtd_col], errors="coerce").fillna(0).sum())
        template = "O estoque total (todas as linhas) soma {total} unidades."
        payload  = {"total": str(total)}
        table = pd.DataFrame()
        schema = self._schema_min(df, cols, ["qtd"])
        return True, template, payload, table, schema

    def _fn_sum_por_skus(self, df: pd.DataFrame, cols: Dict[str, str], question: str):
        sku_col = cols.get("sku")
        qtd_col = cols.get("qtd")
        if not (sku_col and qtd_col):
            return False, "Colunas de SKU ou QTD ausentes.", {}, pd.DataFrame(), self._schema_min(df, cols, ["sku", "qtd"])
        skus = self._extract_sku_from_question(df, sku_col, question)
        if len(skus) < 2:
            return False, "Informe 2 ou mais SKUs para somarmos as quantidades.", {}, pd.DataFrame(), self._schema_min(df, cols, ["sku", "qtd"])

        df_f = df[df[sku_col].astype(str).str.lower().isin([s.lower() for s in skus])]
        if df_f.empty:
            return False, f"SKUs não encontrados: {', '.join(skus)}.", {}, pd.DataFrame(), self._schema_min(df, cols, ["sku"])

        g = df_f.groupby([sku_col], dropna=False)[qtd_col].sum().reset_index().sort_values(qtd_col, ascending=False)
        total = int(g[qtd_col].sum())
        list_str = "; ".join(f"{str(r[sku_col])} (qtd {int(r[qtd_col])})" for _, r in g.iterrows())
        template = "Soma de quantidades para os SKUs selecionados: total {total} unidades. Por SKU: {list_str}."
        payload  = {"total": str(total), "list_str": list_str}

        # Chart hint: pizza por SKU
        payload["__chart"] = {
            "type": "pie",
            "df": g[[sku_col, qtd_col]],
            "names": sku_col,
            "values": qtd_col,
            "title": "Distribuição de QTD por SKU (selecionados)"
        }

        # >>>>>> Tabela de apoio (ENRIQUECIDA + DEDUP) <<<<<<
        extra = [cols.get("descricao"), cols.get("fornecedor"), cols.get("endereco")]
        base_cols = self._unique_preserve([sku_col, qtd_col] + extra)
        table = df_f.loc[:, base_cols].head(5000)
        schema = self._schema_min(df, cols, ["sku", "qtd"])
        return True, template, payload, table, schema

    # ============================
    # Narração baseada em evidências (tabela de apoio)
    # ============================
    def _build_evidence_from_table(self, table: pd.DataFrame, cols: Dict[str, Optional[str]], max_rows: int = 12) -> str:
        if table is None or table.empty:
            return "Nenhuma linha encontrada."
        show = table.copy()
        keys = ["sku", "descricao", "fornecedor", "qtd", "endereco", "fila", "torre", "nivel"]
        keep = [cols.get(k) for k in keys if cols.get(k) and cols.get(k) in show.columns]
        if keep:
            show = show[keep]
        show = show.head(max_rows)

        # Acesso seguro (linha pode ter rótulos duplicados externamente)
        def _val_from_row(row: pd.Series, col: Optional[str]):
            if not col or col not in row.index:
                return None
            v = row[col]
            # Se houver índice duplicado, row[col] vira Series -> pega o primeiro valor
            if isinstance(v, pd.Series):
                v = v.iloc[0] if not v.empty else None
            return v

        lines = []
        for _, row in show.iterrows():
            parts = []
            def add(label, key):
                col = cols.get(key)
                v = _val_from_row(row, col)
                if v is not None and pd.notna(v):
                    parts.append(f"{label}={str(v).strip()}")

            add("SKU", "sku")
            add("DESCRIÇÃO", "descricao")
            add("FORNECEDOR", "fornecedor")
            add("QTD", "qtd")

            v_end = _val_from_row(row, cols.get("endereco"))
            if v_end is not None and pd.notna(v_end):
                parts.append(f"ENDEREÇO={str(v_end).strip()}")
            else:
                for k in ["fila", "torre", "nivel"]:
                    vk = _val_from_row(row, cols.get(k))
                    if vk is not None and pd.notna(vk):
                        parts.append(f"{k.upper()}={str(vk).strip()}")

            if parts:
                lines.append(" • " + "; ".join(parts))
        return "\n".join(lines) if lines else "Sem colunas relevantes para evidência."

    def _narrate_results_from_table(self, question: str, table: pd.DataFrame, enhanced: bool = True,
                                    max_rows: int = 12, max_tokens: Optional[int] = 256) -> str: # aumentei para 256 tokens
        if not (self.cfg.use_llm and self.llm):
            if table is None or table.empty:
                return f"Resultado para: {question.strip()}\nNenhum resultado encontrado."
            return f"Resultado para: {question.strip()}\n{len(table)} linha(s) (amostra) na tabela de apoio."

        cols_map = self._detect_columns(table)
        evidence = self._build_evidence_from_table(table, cols_map, max_rows=max_rows)

        # Resumo conciso com cálculo de total e inclusão de descrições/fabricantes
        summary_lines = []
        total_units = 0
        for _, row in table.iterrows():
            sku = row[cols_map["sku"]]
            descricao = row.get(cols_map["descricao"], "Descrição não disponível")
            qtd = int(row[cols_map["qtd"]]) if pd.notna(row[cols_map["qtd"]]) else 0
            total_units += qtd

            summary_lines.append(f"SKU {sku}: {descricao} com um total de {qtd} unidades.")

            if len(summary_lines) >= max_rows:
                break

        summary_text = "\n".join(summary_lines)
        total_text = f"\nNo total, o endereço possui {total_units} unidades."

        style = "Resuma brevemente incluindo as descrições e quantidades dos SKUs."
        prompt = f"""Você é um assistente que narra resultados de consulta de estoque.
    Regras:
    - Não invente valores. Use APENAS as EVIDÊNCIAS.
    - Inclua detalhes como descrição e fabricante.
    - Informe o total de unidades de forma clara.
    - Escreva em pt-BR.

    PERGUNTA:
    {question.strip()}

    EVIDÊNCIAS (até 12 linhas):
    {summary_text}
    {total_text}

    # RESPOSTA:"""

        try:
            out = self.llm.create_completion(
                prompt=prompt,
                max_tokens=max_tokens,
                temperature=max(self.cfg.temperature, self.cfg.narration_temperature),
                top_p=0.9,
                repeat_penalty=1.1,
            )
            text = (out.get("choices") or [{}])[0].get("text", "").strip()
            if not text:
                return "Nenhum resultado encontrado." if (table is None or table.empty) else f"{len(table)} resultado(s) (amostra)."
            return text
        except Exception as e:
            if table is None or table.empty:
                return f"Nenhum resultado encontrado. (LLM falhou: {e})"
            return f"{len(table)} resultado(s). (LLM falhou: {e})"

    # ============================
    # Resposta principal (LLM só no final)
    # ============================
    def answer(self, df: pd.DataFrame, question: str, narrate: bool = True) -> Dict:
        """
        Retorna: {text, table, chart, timings}
        - narrate=False: frase determinística (sem IA)
        - narrate=True : IA narra de forma elaborada com base nas evidências (tabela de apoio)
        """
        t_all0 = time.perf_counter()
        result = {"text": "", "table": pd.DataFrame(), "timings": {}}
        df = df.copy()

        # Detecta colunas / normaliza
        cols = self._detect_columns(df)
        if cols.get("qtd") is None:
            result["text"] = "Não encontrei a coluna de quantidade (QTD)."
            result["timings"]["total_ms"] = round((time.perf_counter() - t_all0) * 1000, 1)
            return result

        df = self.classify_supplier_by_description(df)
        cols = self._detect_columns(df)

        # Parse (regex → intenção)
        t0 = time.perf_counter()
        intent_info = self._parse_intent(question)
        intent = intent_info.get("intent")
        t1 = time.perf_counter()

        # Execução pandas
        t2 = time.perf_counter()
        if intent == "project_filter":
            success, template, payload, table, schema = self._fn_project_filter(
                df, cols, intent_info["out_key"], intent_info["filter_key"], question, intent_info.get("raw_values", [])
            )
        elif intent == "qtd_por_fornecedor":
            success, template, payload, table, schema = self._fn_qtd_por_fornecedor(
                df, cols, intent_info.get("fornecedor", "")
            )
        elif intent == "skus_por_fila":
            success, template, payload, table, schema = self._fn_skus_por_fila(
                df, cols, intent_info.get("fila", "")
            )
        elif intent == "skus_por_fila_torre":
            success, template, payload, table, schema = self._fn_skus_por_fila_torre(
                df, cols, intent_info.get("fila", ""), intent_info.get("torre", "")
            )
        elif intent == "total_geral":
            success, template, payload, table, schema = self._fn_total_geral(df, cols)
        elif intent == "enderecos_por_sku":
            success, template, payload, table, schema = self._fn_project_filter(df, cols, "endereco", "sku", question, [])
        elif intent == "skus_por_endereco":
            success, template, payload, table, schema = self._fn_project_filter(df, cols, "sku", "endereco", question, [])
        elif intent == "sum_skus":
            success, template, payload, table, schema = self._fn_sum_por_skus(df, cols, question)
        elif intent == "sum_generic":
            result["text"] = "Pergunta genérica. Especifique **o que listar** (ex.: endereços, SKUs) e **por qual coluna filtrar** (ex.: SKU, endereço)."
            result["timings"] = {
                "parse_ms": round((t1 - t0) * 1000, 1),
                "pandas_ms": 0.0,
                "llm_ms": 0.0,
                "total_ms": round((time.perf_counter() - t_all0) * 1000, 1),
            }
            return result
        else:
            result["text"] = (
                "Não entendi completamente. Tente:\n"
                "- Endereços do SKU TGSA57412000?\n"
                "- SKUs do endereço A-2-2?\n"
                "- skus na fila A torre 2?\n"
                "- qts Apple no estoque?\n"
                "- estoque total\n"
                "- soma de quantidade entre TGSA56224000 TGSA509B4000"
            )
            result["timings"] = {
                "parse_ms": round((t1 - t0) * 1000, 1),
                "pandas_ms": 0.0,
                "llm_ms": 0.0,
                "total_ms": round((time.perf_counter() - t_all0) * 1000, 1),
            }
            return result
        t3 = time.perf_counter()

        # ---------- Montagem da resposta final ----------
        llm_ms = 0.0

        list_str   = (payload or {}).get("list_str", "").strip()
        payload_ex = (payload or {}).get("__explain", {})

        # Frase determinística base (compatibilidade com UI antiga)
        if list_str:
            out_label  = (payload or {}).get("out_label", "")
            filt_label = (payload or {}).get("filt_label", "")
            filt_value = (payload or {}).get("filt_value", "")
            intro = f"Os {out_label} do(a) {filt_label} {filt_value} são:"
            base_sentence = f"{intro} {list_str}"
        else:
            base_sentence = template.format_map({k: str(v) for k, v in (payload or {}).items()})

        final_sentence = base_sentence

        # Narração com IA (elaborada com evidências da tabela)
        if narrate and self.cfg.use_llm:
            t4 = time.perf_counter()
            body = self._narrate_results_from_table(question, table, enhanced=False, max_tokens=256)  # aumentado os tokens
            final_sentence = f"{body}".strip()
            llm_ms = round((time.perf_counter() - t4) * 1000, 1)

        result["text"] = final_sentence
        result["table"] = table
        result["chart"] = (payload or {}).get("__chart")

        return result