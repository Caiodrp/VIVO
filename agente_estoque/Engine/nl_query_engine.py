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
    n_ctx: int = 2048               # contexto ampliado para narração com evidências
    n_threads: int = 8
    temperature: float = 0.1
    max_tokens: int = 64            # motor base; narração usa limites próprios
    # Parâmetros específicos para narração (não afetam o motor pandas)
    narration_max_tokens: int = 96
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
                                    force_max_tokens: Optional[int] = None) -> str:
        if not (self.cfg.use_llm and self.llm):
            if table is None or table.empty:
                return f"Resultado para: {question.strip()}\nNenhum resultado encontrado."
            return f"Resultado para: {question.strip()}\n{len(table)} linha(s) (amostra) na tabela de apoio."

        cols_map = self._detect_columns(table)
        evidence = self._build_evidence_from_table(table, cols_map, max_rows=12)

        style = ("Elabore com naturalidade e clareza, citando descrição, fornecedor e endereço quando fizer sentido. "
                 "Se houver muitos itens, resuma padrões.") if enhanced else \
                "Responda de forma breve e objetiva, sem floreios."

        prompt = f"""Você é um assistente que narra resultados de consulta de estoque.
Regras:
- Não invente valores. Use APENAS as EVIDÊNCIAS.
- Se não houver dados, diga isso claramente.
- {style}
- Escreva em pt-BR.

PERGUNTA:
{question.strip()}

EVIDÊNCIAS (até 12 linhas):
{evidence}

# RESPOSTA:"""

        try:
            out = self.llm.create_completion(
                prompt=prompt,
                max_tokens=force_max_tokens or self.cfg.narration_max_tokens,
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
            result["text"] = "Não encontrei coluna de quantidade (QTD)."
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
            # Se houver contexto adicional (__explain), criamos um "lead" curto
            if payload_ex:
                lead = self._narrate_intro_explanatory(payload_ex)
            else:
                lead = self._narrate_sentence(base_sentence, allow_multi=True, max_tokens=128)
            body = self._narrate_results_from_table(question, table, enhanced=True)
            final_sentence = f"{lead}\n\n{body}".strip()
            llm_ms = round((time.perf_counter() - t4) * 1000, 1)

        result["text"] = final_sentence
        result["table"] = table
        result["chart"] = (payload or {}).get("__chart")

        return result

# # -*- coding: utf-8 -*-
# from dataclasses import dataclass
# from typing import Dict, Optional, List, Tuple
# import pandas as pd
# import re
# import time
# import unicodedata

# # ============================
# # Configuração & utilitários
# # ============================
# @dataclass
# class EngineConfig:
#     use_llm: bool = False     # mantém o comportamento determinístico por padrão
#     narration_max_tokens: int = 256
#     temperature: float = 0.2
#     narration_temperature: float = 0.4
# def strip_accents(s: str) -> str:
#     if not isinstance(s, str):
#         s = str(s)
#     return ''.join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != 'Mn')

# def norm_token(s: str) -> str:
#     s = strip_accents(s)
#     s = re.sub(r"[\s\-_/.]", "", s, flags=re.IGNORECASE)
#     return s.casefold()

# # ============================
# # Núcleo do motor
# # ============================
# class NLQueryEngine:
#     # Sinônimos configuráveis (edição num lugar só)
#     COL_SYNONYMS: Dict[str, List[str]] = {
#         "qtd":        ["qtd", "quantidade", "qtde", "qts", "qte"],
#         "sku":        ["sku", "skus", "item", "material", "codigo", "código"],
#         "descricao":  ["descricao", "descrição", "produto", "nome"],
#         "fornecedor": ["fornecedor", "marca", "vendor", "fabricante"],
#         "endereco":   ["endereco", "enderecos", "endereços", "endereço", "local", "posicao", "posição", "lugar"],
#         "fila":       ["fila", "filas"],
#         "torre":      ["torre", "coluna", "torres", "colunas"],
#         "nivel":      ["nivel", "nível", "andar", "prateleira"],
#     }

#     LABEL_SING = {
#         "sku": "SKU", "fornecedor": "fornecedor", "descricao": "descrição",
#         "endereco": "endereço", "fila": "fila", "torre": "torre", "nivel": "nível"
#     }
#     LABEL_PLUR = {
#         "sku": "SKUs", "fornecedor": "fornecedores", "descricao": "descrições",
#         "endereco": "endereços", "fila": "filas", "torre": "torres", "nivel": "níveis"
#     }

#     # Regex pré-compilados básicos
#     RE_ADDR_INLINE = re.compile(r"\b([A-Za-z])\s*-\s*(\d{1,2})(?:\s*-\s*(\d{1,2}))?\b", flags=re.IGNORECASE)
#     RE_TOKEN_4PLUS  = re.compile(r"[A-Za-z0-9\-_/.]{4,}")
#     # Captura conteúdo entre aspas simples/duplas, inclusive aspas tipográficas (curvas)
#     RE_QUOTES = re.compile(r'(["“”‘’\'])([^"“”‘’\']+)\1')


#     def __init__(self, cfg: EngineConfig = EngineConfig(), summarize_fn=None):
#         self.cfg = cfg
#         # summarize_fn: Callable[[str, pd.DataFrame], str]  -> hook para LLM local (ou None)
#         self.summarize_fn = summarize_fn

#         # Compila mapa sinônimos → key (para parse rápido)
#         self._syn_pat = []
#         for key, syns in self.COL_SYNONYMS.items():
#             for s in syns:
#                 self._syn_pat.append((key, re.compile(rf"\b{re.escape(s)}s?\b", flags=re.IGNORECASE)))

#     # ============================
#     # I/O & Normalização
#     # ============================
#     def read_excel(self, src, sheet_name=0) -> pd.DataFrame:
#         return pd.read_excel(src, sheet_name=sheet_name, engine="openpyxl")

#     def _find_col(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
#         cols_lower = {c.lower(): c for c in df.columns}
#         for name in candidates:
#             if name.lower() in cols_lower:
#                 return cols_lower[name.lower()]
#         # fallback: substring
#         for c in df.columns:
#             if any(name.lower() in c.lower() for name in candidates):
#                 return c
#         return None

#     def _detect_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
#         get = lambda key: self._find_col(df, self.COL_SYNONYMS[key])
#         return {
#             "qtd": get("qtd"),
#             "sku": get("sku"),
#             "descricao": get("descricao"),
#             "fornecedor": get("fornecedor"),
#             "endereco": get("endereco"),
#             "fila": get("fila"),
#             "torre": get("torre"),
#             "nivel": get("nivel"),
#         }

#     def _derive_from_endereco(self, df: pd.DataFrame, end_col: str) -> pd.DataFrame:
#         if end_col not in df.columns:
#             return df
#         # garante colunas
#         for c in ("FILA", "TORRE", "NIVEL"):
#             if c not in df.columns:
#                 df[c] = pd.NA

#         def split_addr(s: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
#             if not isinstance(s, str):
#                 return (None, None, None)
#             s2 = s.strip()
#             # 1) A-2-3 / A-2
#             m = self.RE_ADDR_INLINE.search(s2)
#             if m:
#                 return (m.group(1), m.group(2), m.group(3))
#             # 2) tokens separados por espaço/hífen
#             parts = re.split(r"[\s\-]+", s2)
#             if len(parts) >= 3:
#                 return (parts[0] or None, parts[1] or None, parts[2] or None)
#             return (None, None, None)

#         parsed = [split_addr(str(x)) for x in df[end_col].astype(str).fillna("").tolist()]
#         df.loc[:, "FILA"]  = [p[0] for p in parsed]
#         df.loc[:, "TORRE"] = [p[1] for p in parsed]
#         df.loc[:, "NIVEL"] = [p[2] for p in parsed]
#         return df

#     def normalize_df(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Optional[str]]]:
#         df = df.copy()
#         cols = self._detect_columns(df)

#         # QTD → número
#         qtd_col = cols.get("qtd")
#         if qtd_col:
#             ser = (
#                 df[qtd_col].astype(str)
#                 .str.replace(",", ".", regex=False)
#                 .str.replace(r"[^\d\.\-]", "", regex=True)
#                 .replace({"": "0", "VAZIO": "0", "vazio": "0"})
#             )
#             df[qtd_col] = pd.to_numeric(ser, errors="coerce").fillna(0).astype(int)

#         # Derivação FILA/TORRE/NIVEL
#         if cols.get("endereco") and (not cols.get("fila") or not cols.get("torre") or not cols.get("nivel")):
#             df = self._derive_from_endereco(df, cols["endereco"])
#             # após derivar, atualiza mapeamento se as colunas novas existem
#             cols = self._detect_columns(df)
#             # preferir derivadas padrão em maiúsculo se original ausente
#             cols["fila"]  = cols["fila"]  or ("FILA" if "FILA" in df.columns else None)
#             cols["torre"] = cols["torre"] or ("TORRE" if "TORRE" in df.columns else None)
#             cols["nivel"] = cols["nivel"] or ("NIVEL" if "NIVEL" in df.columns else None)

#         return df, cols
#     # ============================
#     # Parsing (intents)
#     # ============================
#     def _first_col_key(self, q: str) -> Optional[Tuple[str, Tuple[int, int]]]:
#         found = []
#         for key, pat in self._syn_pat:
#             m = pat.search(q)
#             if m:
#                 found.append((key, (m.start(), m.end())))
#         if not found:
#             return None
#         found.sort(key=lambda t: t[1][0])
#         return found[0]

#     def _next_col_key(self, q: str, start_after: int) -> Optional[Tuple[str, Tuple[int, int]]]:
#         sub = q[start_after:]
#         off = start_after
#         found = []
#         for key, pat in self._syn_pat:
#             m = pat.search(sub)
#             if m:
#                 found.append((key, (m.start() + off, m.end() + off)))
#         if not found:
#             return None
#         found.sort(key=lambda t: t[1][0])
#         return found[0]

#     def _extract_values(self, q: str, key: str, df: Optional[pd.DataFrame] = None, col: Optional[str] = None) -> List[str]:
#         q = q.strip()

#         # 1) valores entre aspas
#         quoted = [v.strip() for v in self.RE_QUOTES.findall(q)]
#         if quoted:
#             return [v for v in quoted if v]

#         # 2) após sinônimo: "fornecedor Apple", "fila A", etc.
#         for syn in self.COL_SYNONYMS.get(key, []):

#             m = re.search(
#                 rf"{re.escape(syn)}s?\s*(?:[:=]\s*)?([A-Za-zÀ-ÿ0-9\-/_.\s]{{1,}}?)(?=[,.;\n\r]|$)",
#                 q, flags=re.IGNORECASE
#             )

#             if m:
#                 val = m.group(1).strip()
#                 if val:
#                     return [val]

#         # 3) tokens 4+ (útil para SKU)
#         if key == "sku" and df is not None and col:
#             skus_norm = {norm_token(x): str(x) for x in df[col].dropna().astype(str).unique()}
#             hits = []
#             for t in self.RE_TOKEN_4PLUS.findall(q):
#                 t_norm = norm_token(t)
#                 if t_norm in skus_norm:
#                     hits.append(skus_norm[t_norm])
#             return list(dict.fromkeys(hits))

#         # 4) fallback: último token 4+
#         toks = self.RE_TOKEN_4PLUS.findall(q)
#         return [toks[-1]] if toks else []

#     def _extract_addresses(self, q: str) -> List[str]:
#         # Captura padrões do tipo A-2-3 / A-2
#         toks = [f"{m.group(1)}-{m.group(2)}" + (f"-{m.group(3)}" if m.group(3) else "") for m in self.RE_ADDR_INLINE.finditer(q)]
#         # Remove espaços extras
#         return [re.sub(r"\s+", "", t) for t in toks]

#     def _parse_intent(self, q: str) -> Dict:
#         ql = strip_accents(q).lower().strip()

#         # PROJETA ⇒ FILTRA: "<Saída> do/de <Filtro> ..."
#         first = self._first_col_key(ql)
#         if first:
#             out_key, (_, e0) = first
#             second = self._next_col_key(ql, e0)
#             if second:
#                 filt_key, _ = second
#                 return {"intent": "project_filter", "out_key": out_key, "filter_key": filt_key}

#         # Clássicas
#         if any(t in ql for t in ["estoque total", "total geral", "soma geral"]):
#             return {"intent": "total_geral"}

#         # soma entre SKUs (menção a soma/quantidade + SKU/materiais)
#         if any(k in ql for k in ["soma", "total", "somar", "quantidade"]) and any(k in ql for k in ["sku", "skus", "itens", "materiais"]):
#             return {"intent": "sum_skus"}

#         # legados / curto-circuitos
#         if "onde" in ql or "em qual endereco" in ql or "em qual endereço" in ql:
#             return {"intent": "enderecos_por_sku"}
#         if (("qual" in ql) or ("quais" in ql)) and any(t in ql for t in ["endereco", "endereço", "local", "lugar"]):
#             return {"intent": "skus_por_endereco"}

#         # padrões "skus na fila X torre Y"
#         m = re.search(r"skus?\s+na\s+fila\s+([a-z0-9\-]+)\s+torre\s+([a-z0-9\-]+)", ql)
#         if m:
#             return {"intent": "skus_por_fila_torre", "fila": m.group(1), "torre": m.group(2)}
#         m = re.search(r"skus?\s+na\s+fila\s+([a-z0-9\-]+)", ql)
#         if m:
#             return {"intent": "skus_por_fila", "fila": m.group(1)}

#         if re.search(r"\bqts?\s+(.+?)\s+no\s+estoque\b", ql):
#             fornecedor = re.search(r"\bqts?\s+(.+?)\s+no\s+estoque\b", ql).group(1)
#             return {"intent": "qtd_por_fornecedor", "fornecedor": fornecedor}

#         if any(w in ql for w in ["qtd", "quantidade", "soma", "quanto tem", "qts", "quantos", "media", "média"]):
#             return {"intent": "sum_generic"}

#         return {"intent": "unknown"}

#     # ============================
#     # Execução (pandas)
#     # ============================
#     def _schema_min(self, df: pd.DataFrame, cols: Dict[str, Optional[str]], used: List[str]) -> str:
#         def typ(col):
#             if col is None or col not in df.columns: return "ausente"
#             d = str(df[col].dtype)
#             return "numérico" if ("int" in d or "float" in d) else "texto"
#         return "\n".join(f"- {cols.get(k) or k.upper()} ({typ(cols.get(k))})" for k in used)

#     def _format_addr(self, addr: str) -> str:
#         t = re.sub(r"\s+", "", str(addr))
#         return re.sub(r"-+$", "", t)

#     def _most_common_text(self, s: pd.Series, topn=1) -> List[str]:
#         s = s.dropna().astype(str).str.strip()
#         if s.empty: return []
#         vals = s.value_counts().index.tolist()[:topn]
#         return [v[:120] for v in vals if v]

#     def _project_filter(self, df: pd.DataFrame, cols: Dict[str, str], out_key: str, filt_key: str, q: str) -> Tuple[bool, str, Dict, pd.DataFrame, str]:
#         out_col = cols.get(out_key)
#         fk_col  = cols.get(filt_key)
#         qtd_col = cols.get("qtd")
#         if not out_col or not fk_col:
#             return False, "Colunas ausentes para a consulta.", {}, pd.DataFrame(), self._schema_min(df, cols, [out_key, filt_key])

#         # extrai valores de filtro
#         if filt_key == "endereco":
#             values = self._extract_addresses(q)
#             if not values:
#                 values = self._extract_values(q, "endereco")
#         elif filt_key == "sku":
#             values = self._extract_values(q, "sku", df, fk_col)
#         else:
#             values = self._extract_values(q, filt_key)
#         if not values:
#             return False, f"Valor de filtro para {self.LABEL_SING.get(filt_key, filt_key)} não identificado.", {}, pd.DataFrame(), self._schema_min(df, cols, [filt_key])

#         # aplica máscara
#         if filt_key == "sku":
#             mask = df[fk_col].astype(str).str.casefold().isin([v.lower() for v in values])
#         else:
#             patt = "|".join(re.escape(v) for v in values)
#             mask = df[fk_col].astype(str).str.contains(patt, case=False, na=False)
#         df_f = df[mask]
#         if df_f.empty:
#             return False, f"Nenhum resultado para {self.LABEL_SING.get(filt_key, fk_col)} = {', '.join(values)}.", {}, pd.DataFrame(), self._schema_min(df, cols, [filt_key])

#         # agrega ou lista
#         if qtd_col:
#             g = (
#                 df_f.groupby([out_col], dropna=False)[qtd_col]
#                 .sum().reset_index().sort_values(qtd_col, ascending=False)
#             )
#             items = [f"{(self._format_addr(r[out_col]) if out_key=='endereco' else str(r[out_col]))} (qtd {int(r[qtd_col])})"
#                      for _, r in g.iterrows()]
#         else:
#             uniq = df_f[out_col].dropna().astype(str).unique().tolist()
#             g = pd.DataFrame()
#             items = [self._format_addr(u) if out_key == "endereco" else u for u in uniq]

#         list_str = "; ".join(items)
#         payload = {
#             "out_label": self.LABEL_PLUR.get(out_key, out_col),
#             "filt_label": self.LABEL_SING.get(filt_key, fk_col),
#             "filt_value": ", ".join(values),
#             "list_str": list_str
#         }
#         template = "Os {out_label} do(a) {filt_label} {filt_value} são: {list_str}."

#         # enriquecimento explicativo (endereços do SKU)
#         if out_key == "endereco" and filt_key == "sku":
#             payload_explain = {"sku": ", ".join(values)}
#             if cols.get("descricao"):
#                 tops = self._most_common_text(df_f[cols["descricao"]], 1)
#                 if tops: payload_explain["descricao"] = tops[0]
#             if cols.get("fornecedor"):
#                 tops = self._most_common_text(df_f[cols["fornecedor"]], 1)
#                 if tops: payload_explain["fornecedor"] = tops[0]
#             if qtd_col:
#                 total = int(pd.to_numeric(df_f[qtd_col], errors="coerce").fillna(0).sum())
#                 payload_explain["total_unidades"] = str(total)
#             payload_explain["num_enderecos"] = str(len(g) if not g.empty else len(items))
#             if qtd_col and isinstance(g, pd.DataFrame) and not g.empty:
#                 top_row = g.iloc[0]
#                 payload_explain["top_endereco"] = self._format_addr(str(top_row[out_col]))
#                 payload_explain["top_qtd"] = str(int(top_row[qtd_col]))
#             payload["__explain"] = payload_explain
#             payload["__intent"] = "enderecos_por_sku"

#         # chart hints
#         if cols.get("qtd"):
#             if out_key == "endereco" and filt_key == "sku" and isinstance(g, pd.DataFrame) and not g.empty:
#                 g2 = g.copy()
#                 g2[out_col] = g2[out_col].astype(str).map(self._format_addr)
#                 payload["__chart"] = {"type": "bar", "df": g2[[out_col, qtd_col]], "x": out_col, "y": qtd_col,
#                                       "title": f"QTD do SKU {', '.join(values)} por endereço"}
#             if out_key == "sku" and filt_key in ("endereco", "fila", "torre", "nivel") and isinstance(g, pd.DataFrame) and not g.empty:
#                 payload["__chart"] = {"type": "pie", "df": g[[cols.get("sku") or out_col, qtd_col]],
#                                       "names": cols.get("sku") or out_col, "values": qtd_col,
#                                       "title": "Distribuição de QTD por SKU"}

#         # tabela de apoio
#         keep = []
#         for k in [out_col, fk_col, cols.get("descricao"), cols.get("fornecedor"),
#                   cols.get("endereco"), cols.get("fila"), cols.get("torre"), cols.get("nivel"), cols.get("qtd")]:
#             if k and k not in keep: keep.append(k)
#         table = df_f.loc[:, keep].drop_duplicates().head(5000)
#         schema = self._schema_min(df, cols, [out_key, filt_key, "qtd"])
#         return True, template, payload, table, schema

#     def _total_geral(self, df: pd.DataFrame, cols: Dict[str, str]):
#         qtd_col = cols.get("qtd")
#         if not qtd_col: 
#             return False, "Coluna de quantidade ausente.", {}, pd.DataFrame(), self._schema_min(df, cols, ["qtd"])
#         total = int(pd.to_numeric(df[qtd_col], errors="coerce").fillna(0).sum())
#         return True, "Estoque total (soma de unidades): {total}.", {"total": str(total)}, pd.DataFrame(), self._schema_min(df, cols, ["qtd"])

#     def _qtd_por_fornecedor(self, df: pd.DataFrame, cols: Dict[str, str], fornecedor: str):
#         qtd_col = cols.get("qtd"); forn_col = cols.get("fornecedor"); desc_col = cols.get("descricao")
#         fornecedor_re = re.escape(fornecedor)
#         df_f = pd.DataFrame()
#         if forn_col:
#             df_f = df[df[forn_col].astype(str).str.contains(fornecedor_re, case=False, na=False)]
#         if df_f.empty and desc_col:
#             df_f = df[df[desc_col].astype(str).str.contains(fornecedor_re, case=False, na=False)]
#         total = int(pd.to_numeric(df_f[qtd_col], errors="coerce").fillna(0).sum()) if not df_f.empty else 0
#         table = df_f[[c for c in [cols.get("sku"), desc_col, forn_col, qtd_col, cols.get("endereco")] if c]].head(2000) if not df_f.empty else pd.DataFrame()
#         return True, "Total de unidades no estoque para {fornecedor}: {total}.", {"fornecedor": fornecedor, "total": str(total)}, table, self._schema_min(df, cols, ["fornecedor","qtd","sku","descricao","endereco"])

#     def _skus_por_fila(self, df: pd.DataFrame, cols: Dict[str, str], fila: str):
#         return self._project_filter(df, cols, out_key="sku", filt_key="fila", q=f"fila {fila}")

#     def _skus_por_fila_torre(self, df: pd.DataFrame, cols: Dict[str, str], fila: str, torre: str):
#         # filtra direto e agrupa por SKU
#         sku_col = cols.get("sku"); fila_col = cols.get("fila"); torre_col = cols.get("torre"); qtd_col = cols.get("qtd")
#         if not (sku_col and fila_col and torre_col and qtd_col):
#             return False, "Colunas de FILA/TORRE/SKU/QTD ausentes.", {}, pd.DataFrame(), self._schema_min(df, cols, ["fila","torre","sku","qtd"])
#         df_f = df[
#             df[fila_col].astype(str).str.contains(re.escape(fila), case=False, na=False) &
#             df[torre_col].astype(str).str.contains(re.escape(torre), case=False, na=False)
#         ]
#         if df_f.empty:
#             return False, f"Nenhum SKU encontrado na fila {fila} torre {torre}.", {}, pd.DataFrame(), self._schema_min(df, cols, ["fila","torre"])
#         g = df_f.groupby([sku_col], dropna=False)[qtd_col].sum().reset_index().sort_values(qtd_col, ascending=False)
#         list_str = "; ".join(f"{str(r[sku_col])} (qtd {int(r[qtd_col])})" for _, r in g.iterrows())
#         payload = {"fila": fila, "torre": torre, "list_str": list_str, "__chart": {"type":"pie","df":g[[sku_col,qtd_col]],"names":sku_col,"values":qtd_col,"title":f"Distribuição de QTD por SKU – fila {fila}, torre {torre}"}}
#         table_cols = [c for c in [sku_col, fila_col, torre_col, qtd_col, cols.get("descricao"), cols.get("fornecedor"), cols.get("endereco")] if c]
#         table = df_f.loc[:, table_cols].drop_duplicates().head(5000)
#         return True, "Na fila {fila}, torre {torre}, os SKUs encontrados são: {list_str}.", payload, table, self._schema_min(df, cols, ["fila","torre","sku","qtd"])

#     # ============================
#     # Narração (determinística + hook para LLM local)
#     # ============================
#     def _narrate_intro_explanatory(self, ex: Dict[str, str]) -> str:
#         base = []
#         sku = ex.get("sku"); desc = ex.get("descricao"); forn = ex.get("fornecedor")
#         total = ex.get("total_unidades"); n_end = ex.get("num_enderecos")
#         top_loc = ex.get("top_endereco"); top_qtd = ex.get("top_qtd")
#         if sku:
#             if desc and forn: base.append(f"O SKU {sku} ({desc}, {forn})")
#             elif desc:        base.append(f"O SKU {sku} ({desc})")
#             elif forn:        base.append(f"O SKU {sku} ({forn})")
#             else:             base.append(f"O SKU {sku}")
#         if n_end and total: base.append(f"está distribuído em {n_end} endereço(s), somando {total} unidades")
#         elif n_end:         base.append(f"está distribuído em {n_end} endereço(s)")
#         if top_loc and top_qtd: base.append(f"com maior concentração em {top_loc} ({top_qtd} unidade(s))")
#         return (". ".join(base).strip() + ".") if base else ""

#     def _compose_final_text(self, question: str, template: str, payload: Dict, table: pd.DataFrame) -> str:
#         list_str = (payload or {}).get("list_str", "").strip()
#         if list_str:
#             intro = f"Os {payload.get('out_label','')} do(a) {payload.get('filt_label','')} {payload.get('filt_value','')} são:"
#             base_sentence = f"{intro} {list_str}"
#         else:
#             base_sentence = template.format_map({k: str(v) for k, v in (payload or {}).items()})

#         # Sem LLM → determinístico
#         if not self.cfg.use_llm or self.summarize_fn is None:
#             lead = self._narrate_intro_explanatory(payload.get("__explain", {})) if payload.get("__explain") else base_sentence
#             return lead if payload.get("__explain") else base_sentence

#         # Com LLM local (hook)
#         try:
#             lead = self._narrate_intro_explanatory(payload.get("__explain", {})) or base_sentence
#             body = self.summarize_fn(question=lead, table=table)
#             return f"{lead}\n\n{body}".strip()
#         except Exception:
#             return base_sentence

#     # ============================
#     # API principal
#     # ============================
#     def answer(self, df: pd.DataFrame, question: str, narrate: bool = True) -> Dict:
#         t0 = time.perf_counter()
#         result = {"text": "", "table": pd.DataFrame(), "chart": None, "timings": {}}

#         # Detecta colunas / normaliza
#         df, cols = self.normalize_df(df)
#         if cols.get("qtd") is None:
#             result["text"] = "Não encontrei coluna de quantidade (QTD)."
#             result["timings"]["total_ms"] = round((time.perf_counter() - t0) * 1000, 1)
#             return result

#         # Parse
#         tp0 = time.perf_counter()
#         intent_info = self._parse_intent(question)
#         intent = intent_info.get("intent")
#         tp1 = time.perf_counter()

#         # Executa
#         if intent == "project_filter":
#             ok, template, payload, table, schema = self._project_filter(df, cols, intent_info["out_key"], intent_info["filter_key"], question)
#         elif intent == "qtd_por_fornecedor":
#             ok, template, payload, table, schema = self._qtd_por_fornecedor(df, cols, intent_info.get("fornecedor",""))
#         elif intent == "skus_por_fila":
#             ok, template, payload, table, schema = self._skus_por_fila(df, cols, intent_info.get("fila",""))
#         elif intent == "skus_por_fila_torre":
#             ok, template, payload, table, schema = self._skus_por_fila_torre(df, cols, intent_info.get("fila",""), intent_info.get("torre",""))
#         elif intent == "total_geral":
#             ok, template, payload, table, schema = self._total_geral(df, cols)
#         elif intent == "enderecos_por_sku":
#             ok, template, payload, table, schema = self._project_filter(df, cols, "endereco", "sku", question)
#         elif intent == "skus_por_endereco":
#             ok, template, payload, table, schema = self._project_filter(df, cols, "sku", "endereco", question)
#         elif intent == "sum_skus":
#             # você pode implementar uma versão simples somando QTD de SKUs mencionados
#             ok, template, payload, table, schema = self._project_filter(df, cols, "endereco", "sku", question)
#         elif intent == "sum_generic":
#             result["text"] = "Pergunta genérica. Especifique **o que listar** (ex.: endereços, SKUs) e **por qual coluna filtrar** (ex.: SKU, endereço)."
#             result["timings"] = {"parse_ms": round((tp1-tp0)*1000,1), "pandas_ms": 0.0, "total_ms": round((time.perf_counter()-t0)*1000,1)}
#             return result
#         else:
#             result["text"] = (
#                 "Não entendi completamente. Tente:\n"
#                 "- Endereços do SKU TGSA57412000?\n"
#                 "- SKUs do endereço A-2-2?\n"
#                 "- skus na fila A torre 2?\n"
#                 "- qts Apple no estoque?\n"
#                 "- estoque total\n"
#                 "- soma de quantidade entre TGSA56224000 TGSA509B4000"
#             )
#             result["timings"] = {"parse_ms": round((tp1-tp0)*1000,1), "pandas_ms": 0.0, "total_ms": round((time.perf_counter()-t0)*1000,1)}
#             return result

#         # Monta resposta
#         result["table"] = table
#         result["chart"] = (payload or {}).get("__chart")
#         text = self._compose_final_text(question, template, payload, table) if narrate else (template.format_map(payload) if payload else template)
#         result["text"] = text

#         return result
