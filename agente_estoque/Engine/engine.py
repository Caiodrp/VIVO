# -*- coding: utf-8 -*-
"""
engine.py — pandas decide (rápido); LLM só entra no final para narrar.
Implementa "PROJETA ⇒ FILTRA": primeiro nome de coluna citado é a SAÍDA,
o segundo é a COLUNA de filtro, seguida do(s) valor(es).

Exemplos:
- "Endereços do SKU TGSA56224000?" => out=endereço; filtro=sku; valor=TGSA56224000
- "SKUs do endereço A-2-2?"       => out=sku; filtro=endereço; valor=A-2-2

"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import os
import re
import time

import numpy as np
import pandas as pd

try:
    from llama_cpp import Llama  # type: ignore
except ImportError:  # pragma: no cover
    Llama = None


# ============================
# Configuração
# ============================
@dataclass
class EngineConfig:
    """Configurações de execução do motor.

    Atributos:
        use_llm: Se True, utiliza LLM para narração.
        model_path: Caminho opcional para o modelo .gguf.
        n_ctx: Tamanho de contexto para o modelo.
        n_threads: Número de threads para inferência.
        temperature: Temperatura padrão (respostas curtas).
        max_tokens: Tokens máximos para respostas simples.
        narration_max_tokens: Tokens máximos para narração final.
        narration_temperature: Temperatura para narração.
        warmup_on_init: Se True, faz warm-up de 1 token no init.
        warmup_max_tokens: Tokens usados no warm-up.
    """
    use_llm: bool = True
    model_path: Optional[str] = None
    n_ctx: int = 2048
    n_threads: int = 12
    temperature: float = 0.1
    max_tokens: int = 256
    narration_max_tokens: int = 256
    narration_temperature: float = 0.2
    warmup_on_init: bool = True
    warmup_max_tokens: int = 1


class StockEngine:
    """Motor de consultas ao estoque com pandas (rápido) e narração opcional via LLM."""

    # ---- Sinônimos por coluna (singular/plural/variações) ----
    COL_SYNONYMS: Dict[str, List[str]] = {
        "sku": ["sku", "skus", "material", "materiais", "código", "codigo", "códigos", "codigos", "item", "itens", "id", "ids"],
        "descricao": ["descrição", "descricao", "descrições", "descricoes", "desc", "nome", "nomes"],
        "fornecedor": ["fornecedor", "fornecedores", "fabricante", "fabricantes", "marca", "marcas", "vendor", "vendors"],
        "qtd": ["qtd", "quantidade", "quantidades", "qtd_estoque", "qtd."],
        "endereco": ["endereço", "endereco", "endereços", "enderecos", "local", "locais", "lugar", "lugares", "onde", "end"],
        "fila": ["fila", "filas", "rua", "corredor", "corredores"],
        "torre": ["torre", "torres", "estante", "estantes", "coluna", "colunas"],
        "nivel": ["nível", "nivel", "níveis", "niveis", "prateleira", "prateleiras", "andar", "andares"],
    }

    LABEL_SING: Dict[str, str] = {
        "sku": "SKU", "descricao": "descrição", "fornecedor": "fornecedor", "qtd": "quantidade",
        "endereco": "endereço", "fila": "fila", "torre": "torre", "nivel": "nível",
    }
    LABEL_PLUR: Dict[str, str] = {
        "sku": "SKUs", "descricao": "descrições", "fornecedor": "fornecedores",
        "endereco": "Endereços", "fila": "Filas", "torre": "Torres", "nivel": "Níveis",
    }

    # ----------------------------
    # Construtor / LLM bootstrap
    # ----------------------------
    def __init__(self, cfg: EngineConfig) -> None:
        """Inicializa o motor e carrega o modelo LLM, se configurado."""
        self.cfg = cfg
        self.llm: Optional[Llama] = None

        if not self.cfg.use_llm:
            return  # modo turbo (sem IA)

        # 1) Caminho do modelo informado
        model: Optional[Path] = None
        if self.cfg.model_path:
            p = Path(os.path.expanduser(str(self.cfg.model_path))).resolve()
            model = p if p.is_file() else None

        # 2) Caso não informado, procura em ../Models/*.gguf
        if model is None:
            pkg_dir = Path(__file__).resolve().parent   # .../Engine
            base_dir = pkg_dir.parent                   # raiz do projeto
            models_dir = base_dir / "Models"
            if models_dir.is_dir():
                ggufs = sorted([p for p in models_dir.iterdir() if p.suffix.lower() == ".gguf"])
                if ggufs:
                    model = ggufs[0]

        # 3) Carrega a LLM (se houver binding e arquivo)
        if model and Llama is not None and model.is_file():
            try:
                self.llm = Llama(
                    model_path=str(model),
                    n_ctx=self.cfg.n_ctx,
                    n_threads=self.cfg.n_threads,
                    verbose=False,
                )
                # 4) Warm-up (1 token) para evitar cold start
                if getattr(self.cfg, "warmup_on_init", False):
                    try:
                        _ = self.llm.create_completion(
                            prompt="ok",
                            max_tokens=getattr(self.cfg, "warmup_max_tokens", 1),
                            temperature=0.0,
                            stop=["\n"],
                        )
                    except Exception:
                        pass
            except Exception:
                self.llm = None  # segue sem IA

    # ============================
    # I/O & Normalização
    # ============================
    def read_excel(self, src, sheet_name: int | str = 0) -> pd.DataFrame:
        """Lê Excel com openpyxl (garantido no ambiente)."""
        return pd.read_excel(src, sheet_name=sheet_name, engine="openpyxl")

    def classify_supplier_by_description(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normaliza QTD e deriva FILA/TORRE/NIVEL a partir de ENDEREÇO, se necessário.

        Regras:
        - QTD numérica (strings com vírgula → ponto; remove não-dígitos; 'VAZIO' → 0).
        - Se faltar alguma de FILA/TORRE/NIVEL e existir ENDEREÇO, tenta derivar.
        """
        df = df.copy()
        cols = self._detect_columns(df)

        # QTD → número
        qtd_col = cols.get("qtd")
        if qtd_col:
            ser = (
                df[qtd_col].astype(str)
                .str.replace(",", ".", regex=False)
                .str.replace(r"[^\d\.\-]", "", regex=True)
                .replace({"": "0", "VAZIO": "0", "vazio": "0"})
            )
            df[qtd_col] = pd.to_numeric(ser, errors="coerce").fillna(0).astype(int)

        # Derivar FILA/TORRE/NIVEL a partir de ENDEREÇO
        end_col = cols.get("endereco")
        if end_col and (cols.get("fila") is None or cols.get("torre") is None or cols.get("nivel") is None):
            df = self._derive_from_endereco(df, end_col)

        return df

    # ============================
    # Colunas & Utils
    # ============================
    @staticmethod
    def _strip_accents(s: str) -> str:
        import unicodedata
        nfkd = unicodedata.normalize("NFD", s)
        return "".join(ch for ch in nfkd if unicodedata.category(ch) != "Mn")

    @classmethod
    def _norm_col_name(cls, s: str) -> str:
        """Normaliza nome de coluna para matching robusto."""
        s0 = str(s).strip().casefold()
        s1 = cls._strip_accents(s0)
        return re.sub(r"[\s\-_]+", "", s1)

    def _find_col(self, df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
        """Encontra a primeira coluna cujo nome case com os candidatos (robusto a acentos)."""
        if df is None or df.empty or not df.columns.size:
            return None

        # Mapas normalizados
        norm_map = {self._norm_col_name(c): c for c in df.columns}
        cand_norm = [self._norm_col_name(x) for x in candidates]

        # 1) Igualdade exata (normalizada)
        for c in cand_norm:
            if c in norm_map:
                return norm_map[c]

        # 2) Token match (ex.: "quantidade" não deve casar com "quantidade_minima")
        tokens_map: Dict[str, str] = {}
        for c in df.columns:
            toks = re.split(r"[\s\-_]+", self._strip_accents(str(c)).casefold())
            for t in toks:
                tokens_map.setdefault(t, c)
        for c in cand_norm:
            if c in tokens_map:
                return tokens_map[c]

        # 3) Fallback por inclusão parcial (mais permissivo)
        for c in df.columns:
            cn = self._norm_col_name(c)
            if any(part in cn for part in cand_norm):
                return c

        return None

    def _detect_columns(self, df: pd.DataFrame) -> Dict[str, Optional[str]]:
        """Detecta nomes reais das colunas com base nos sinônimos."""
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
        """Deriva FILA/TORRE/NIVEL a partir de um campo ENDEREÇO no padrão F-T-N."""
        df = df.copy()
        if end_col not in df.columns:
            return df

        for c in ("FILA", "TORRE", "NIVEL"):
            if c not in df.columns:
                df[c] = pd.NA

        def _split_end(s: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
            if not isinstance(s, str):
                return (None, None, None)
            s = s.strip()
            parts = re.split(r"[\s\-]+", s)
            if len(parts) >= 3:
                return (parts[0] or None, parts[1] or None, parts[2] or None)
            m = re.match(r"^\s*([A-Za-z])\s*[- ]?\s*(\d{1,2})(?:\s*[- ]\s*(\d{1,2}))?\s*$", s)
            if m:
                return (m.group(1), m.group(2), m.group(3))
            return (None, None, None)

        parsed = [_split_end(str(x)) for x in df[end_col].astype(str).fillna("").tolist()]
        df.loc[:, "FILA"] = [p[0] for p in parsed]
        df.loc[:, "TORRE"] = [p[1] for p in parsed]
        df.loc[:, "NIVEL"] = [p[2] for p in parsed]
        return df

    @staticmethod
    def _normalize_token(s: str) -> str:
        """Normaliza tokens removendo separadores comuns e aplicando casefold."""
        return re.sub(r"[\s\-_/]", "", str(s)).casefold()

    # === Extractors ===
    def _extract_sku_from_question(self, df: pd.DataFrame, sku_col: Optional[str], question: str) -> List[str]:
        """Extrai SKUs mencionados na pergunta cruzando com o DF para evitar falsos positivos."""
        if not sku_col or sku_col not in df.columns:
            return []
        skus_norm = {self._normalize_token(x): str(x) for x in df[sku_col].dropna().astype(str).unique()}
        tokens = re.findall(r"[A-Za-z0-9\-_/]{4,}", question or "")
        hits = []
        for t in tokens:
            t_norm = self._normalize_token(t)
            if t_norm in skus_norm:
                hits.append(skus_norm[t_norm])
        # Remove duplicados preservando ordem
        return list(dict.fromkeys(hits))

    @staticmethod
    def _extract_address_tokens(question: str) -> List[str]:
        """Extrai endereços no padrão F-##-##, permitindo espaços opcionais."""
        toks = re.findall(r"\b[A-Za-z]\s*-\s*\d{1,2}(?:\s*-\s*\d{1,2})?\b", question or "", flags=re.IGNORECASE)
        return [re.sub(r"\s*", "", t) for t in toks]

    def _extract_simple_value(self, question: str, key: str) -> List[str]:
        """Extrai valores simples (fila/torre/nivel/fornecedor/descricao) da pergunta.

        Estratégia:
        1) Primeiro tenta valores entre aspas (", ', “ ”, ‘ ’).
        2) Em seguida, captura sequência após o sinônimo, até pontuação forte.
        3) Fallback: usa o último token alfanumérico de 2+ caracteres.
        """
        q = question or ""

        # 1) Conteúdos entre aspas
        quoted_patterns = [
            r'"([^"]+)"',
            r"'([^']+)'",
            r"“([^”]+)”",
            r"‘([^’]+)’",
        ]
        for pat in quoted_patterns:
            qs = re.findall(pat, q)
            if qs:
                return [v.strip() for v in qs if v and v.strip()]

        # 2) Captura de sequência após sinônimo (aceita acentos)
        synonyms = self.COL_SYNONYMS.get(key, [])
        for syn in synonyms:
            m = re.search(
                rf"{re.escape(syn)}\s*(?:[:=]\s*)?([A-Za-zÀ-ÿ0-9\-/_.\s]{{2,}}?)(?=[,.;\n\r]|$)",
                q,
                flags=re.IGNORECASE,
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
    def _match_first_col_key(self, q: str) -> Optional[Tuple[str, Tuple[int, int], str]]:
        """Retorna (col_key, (start,end), matched_text) da primeira coluna citada na frase."""
        best = None
        for key, syns in self.COL_SYNONYMS.items():
            for syn in syns:
                for m in re.finditer(rf"\b{re.escape(syn)}\b", q or "", flags=re.IGNORECASE):
                    pos = (m.start(), m.end())
                    if best is None or pos[0] < best[1][0]:
                        best = (key, pos, m.group(0))
        return best

    def _match_next_col_key(self, q: str, start_after: int) -> Optional[Tuple[str, Tuple[int, int], str]]:
        """Retorna a próxima coluna citada após o índice fornecido."""
        best = None
        sub = (q or "")[start_after:]
        off = start_after
        for key, syns in self.COL_SYNONYMS.items():
            for syn in syns:
                for m in re.finditer(rf"\b{re.escape(syn)}\b", sub, flags=re.IGNORECASE):
                    pos = (m.start() + off, m.end() + off)
                    if best is None or pos[0] < best[1][0]:
                        best = (key, pos, m.group(0))
        return best

    def _parse_projection_query(self, q: str) -> Optional[Dict]:
        """Parser PROJETA ⇒ FILTRA: '<Saída> do/de/da <Filtro> <valor(es)>'."""
        qn = (q or "").strip()
        first = self._match_first_col_key(qn)
        if not first:
            return None
        out_key, (_s0, e0), _ = first

        second = self._match_next_col_key(qn, e0)
        if not second:
            return None
        filt_key, (_s1, _e1), _ = second

        # valores brutos (refinados depois com o DF)
        values: List[str] = []
        if filt_key == "endereco":
            values = self._extract_address_tokens(qn)
        elif filt_key in ("fila", "torre", "nivel", "fornecedor", "descricao"):
            values = self._extract_simple_value(qn, filt_key)
        elif filt_key == "sku":
            # coleta bruta; será revalidado contra o DF na execução
            values = re.findall(r"[A-Za-z0-9\-_/]{4,}", qn)

        return {"intent": "project_filter", "out_key": out_key, "filter_key": filt_key, "raw_values": values}

    def _parse_intent(self, q: str) -> Dict:
        """Classifica a intenção da pergunta."""
        # 1) PROJETA⇒FILTRA
        pf = self._parse_projection_query(q)
        if pf:
            return pf

        # 2) Clássicas (ordem importa — intents mais específicas primeiro)
        ql = (q or "").lower().strip()
        if any(t in ql for t in ["estoque total", "total geral", "soma geral"]):
            return {"intent": "total_geral"}

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

        Args:
            sentence: Texto base.
            allow_multi: Se True, permite 1–3 frases; senão, 1 frase.
            max_tokens: Tokens máximos da resposta.

        Returns:
            Texto reescrito ou original em fallback.
        """
        if not (self.cfg.use_llm and self.llm):
            return sentence

        instr = "Reescreva em pt-BR de forma natural e objetiva, preservando exatamente as mesmas informações. "
        instr += "Use entre 1 e 3 frases. Não invente dados." if allow_multi else "Use 1 frase. Não invente dados."
        prompt = f"{instr}\n\nFrase:\n{sentence}\n\nReescrita:\n"

        try:
            out = self.llm.create_completion(
                prompt=prompt,
                max_tokens=max_tokens or max(self.cfg.narration_max_tokens // 3, 96),
                temperature=max(self.cfg.temperature, self.cfg.narration_temperature),
                top_p=0.9,
                repeat_penalty=1.1,
            )
            choices = out.get("choices") if isinstance(out, dict) else None
            text = (choices or [{}])[0].get("text", "").strip() if isinstance(choices, list) else ""
            return text or sentence
        except Exception:
            return sentence

    @staticmethod
    def _most_common_text(s: pd.Series, topn: int = 1) -> List[str]:
        """Retorna os valores textuais mais frequentes (limpos) de uma série."""
        s = s.dropna().astype(str).str.strip()
        if s.empty:
            return []
        vc = s.value_counts()
        vals = vc.index.tolist()[:topn]
        return [v[:120] for v in vals if v]

    @staticmethod
    def _format_addr_label(addr: str) -> str:
        """Remove espaços e hífens pendurados do endereço, ex.: 'O-5-' -> 'O-5'."""
        t = re.sub(r"\s+", "", str(addr))
        t = re.sub(r"-+$", "", t)
        t = re.sub(r"-{2,}", "-", t)
        return t

    def _narrate_intro_explanatory(self, payload_explain: Dict[str, str]) -> str:
        """Gera pequeno parágrafo (1–3 frases) explicando o contexto (determinístico/LLM)."""
        base: List[str] = []
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

        deterministic = (". ".join(base).strip() + ".") if base else ""

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
            choices = out.get("choices") if isinstance(out, dict) else None
            text = (choices or [{}])[0].get("text", "").strip() if isinstance(choices, list) else ""
            return text or deterministic
        except Exception:
            return deterministic

    # ============================
    # Schema mínimo (somente campos usados)
    # ============================
    def _schema_min(self, df: pd.DataFrame, cols: Dict[str, Optional[str]], used: List[str]) -> str:
        """Retorna um schema textual mínimo das colunas realmente usadas."""

        def _typ(col_name: Optional[str]) -> str:
            if col_name is None or col_name not in df.columns:
                return "ausente"
            dt = str(df[col_name].dtype)
            return "numérico" if ("int" in dt or "float" in dt) else "texto"

        lines = [f"- {cols.get(k) or k.upper()} ({_typ(cols.get(k))})" for k in used]
        return "\n".join(lines)

    # ============================
    # Helper: remover duplicadas preservando ordem (para colunas da table)
    # ============================
    @staticmethod
    def _unique_preserve(seq: List[Optional[str]]) -> List[str]:
        """Remove duplicadas preservando a ordem e ignorando None."""
        return list(dict.fromkeys(x for x in seq if x))

    # ============================
    # Funções pandas — PROJETA⇒FILTRA (genérica) e clássicas
    # ============================
    def _fn_project_filter(
        self,
        df: pd.DataFrame,
        cols: Dict[str, Optional[str]],
        out_key: str,
        filt_key: str,
        question: str,
        raw_values: List[str],
    ):
        """Implementa PROJETA ⇒ FILTRA para qualquer par (out_key, filt_key)."""
        out_col = cols.get(out_key)
        fk_col = cols.get(filt_key)
        qtd_col = cols.get("qtd")

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
            return (
                False,
                f"Valor de filtro para {self.LABEL_SING.get(filt_key, filt_key)} não identificado.",
                {},
                pd.DataFrame(),
                self._schema_min(df, cols, [filt_key]),
            )

        # Máscara por tipo
        if filt_key == "sku":
            col_norm = df[fk_col].astype(str).str.casefold()
            values_norm = {str(v).casefold() for v in values}
            mask = col_norm.isin(values_norm)
        else:
            # Se poucos valores, usar regex OR; se muitos, combinar máscaras.
            if len(values) <= 8:
                patt = "|".join(re.escape(v) for v in values)
                mask = df[fk_col].astype(str).str.contains(patt, case=False, na=False)
            else:
                masks = [df[fk_col].astype(str).str.contains(re.escape(v), case=False, na=False) for v in values]
                mask = np.logical_or.reduce(masks) if masks else pd.Series(False, index=df.index)

        df_f = df[mask]
        if df_f.empty:
            return (
                False,
                f"Nenhum resultado para {self.LABEL_SING.get(filt_key, filt_key)} = {', '.join(values)}.",
                {},
                pd.DataFrame(),
                self._schema_min(df, cols, [filt_key]),
            )

        # Agrupa por out_col (soma QTD se disponível) e formata itens
        if qtd_col:
            g = (
                df_f.groupby([out_col], dropna=False)[qtd_col]
                .sum()
                .reset_index()
                .sort_values(qtd_col, ascending=False, ignore_index=True)
            )

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
        payload = {"out_label": out_label, "filt_label": filt_label, "filt_value": ", ".join(values), "list_str": list_str}

        # ===== Enriquecimento para narração explicativa (endereços do SKU) =====
        if out_key == "endereco" and filt_key == "sku":
            sku_col = cols.get("sku")
            desc_col = cols.get("descricao")
            forn_col = cols.get("fornecedor")

            payload_explain: Dict[str, str] = {}
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
            payload["__intent"] = "enderecos_por_sku"

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
                    "title": f"QTD do SKU {', '.join(values)} por endereço",
                }

            # Caso 2: SKUs do endereço/fila/torre -> Pizza por SKU
            if out_key == "sku" and filt_key in ("endereco", "fila", "torre", "nivel"):
                payload["__chart"] = {
                    "type": "pie",
                    "df": g[[cols.get("sku") or out_col, qtd_col]],
                    "names": cols.get("sku") or out_col,
                    "values": qtd_col,
                    "title": "Distribuição de QTD por SKU",
                }

        # >>>>>> Tabela de apoio (ENRIQUECIDA + DEDUP) <<<<<<
        raw_keep = [
            out_col,
            fk_col,
            cols.get("descricao"),
            cols.get("fornecedor"),
            cols.get("endereco"),
            cols.get("fila"),
            cols.get("torre"),
            cols.get("nivel"),
            qtd_col,
        ]
        keep_cols = self._unique_preserve(raw_keep)
        table = df_f.loc[:, keep_cols].drop_duplicates().head(5000)

        schema = self._schema_min(df, cols, [out_key, filt_key, "qtd"])
        return True, template, payload, table, schema

    def _fn_qtd_por_fornecedor(self, df: pd.DataFrame, cols: Dict[str, Optional[str]], fornecedor: str):
        """Totaliza quantidade para um fornecedor (ou fallback pela descrição)."""
        qtd_col = cols.get("qtd")
        if qtd_col is None:
            return False, "Coluna de quantidade (QTD) ausente.", {}, pd.DataFrame(), self._schema_min(df, cols, ["qtd"])

        forn_col = cols.get("fornecedor")
        desc_col = cols.get("descricao")
        fornecedor_re = re.escape(fornecedor or "")

        df_f = pd.DataFrame()
        if forn_col:
            df_f = df[df[forn_col].astype(str).str.contains(fornecedor_re, case=False, na=False)]
        if df_f.empty and desc_col:
            df_f = df[df[desc_col].astype(str).str.contains(fornecedor_re, case=False, na=False)]

        total = int(pd.to_numeric(df_f[qtd_col], errors="coerce").fillna(0).sum()) if not df_f.empty else 0
        template = "Total de unidades no estoque para {fornecedor}: {total}."
        payload = {"fornecedor": fornecedor, "total": str(total)}

        cols_show = [c for c in [cols.get("sku"), cols.get("descricao"), forn_col, qtd_col, cols.get("endereco")] if c]
        table = df_f[cols_show].head(2000) if (cols_show and not df_f.empty) else pd.DataFrame()
        schema = self._schema_min(df, cols, ["fornecedor", "qtd", "sku", "descricao", "endereco"])
        return True, template, payload, table, schema

    def _fn_skus_por_fila(self, df: pd.DataFrame, cols: Dict[str, Optional[str]], fila: str):
        """Lista SKUs por FILA, somando quantidades."""
        sku_col, fila_col, qtd_col = cols.get("sku"), cols.get("fila"), cols.get("qtd")
        if not (fila_col and sku_col and qtd_col):
            return False, "Colunas de FILA, SKU ou QTD ausentes.", {}, pd.DataFrame(), self._schema_min(df, cols, ["fila", "sku", "qtd"])

        df_f = df[df[fila_col].astype(str).str.contains(re.escape(fila or ""), case=False, na=False)]
        if df_f.empty:
            return False, f"Nenhum SKU encontrado na fila {fila}.", {}, pd.DataFrame(), self._schema_min(df, cols, ["fila"])

        g = (
            df_f.groupby([sku_col], dropna=False)[qtd_col]
            .sum()
            .reset_index()
            .sort_values(qtd_col, ascending=False, ignore_index=True)
        )
        list_str = "; ".join(f"{str(r[sku_col])} (qtd {int(r[qtd_col])})" for _, r in g.iterrows())
        template = "Na fila {fila}, os SKUs encontrados são: {list_str}."
        payload = {"fila": fila, "list_str": list_str}

        # >>>>>> Tabela de apoio (ENRIQUECIDA) <<<<<<
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
            "title": f"Distribuição de QTD por SKU na fila {fila}",
        }
        return True, template, payload, table, schema

    def _fn_skus_por_fila_torre(self, df: pd.DataFrame, cols: Dict[str, Optional[str]], fila: str, torre: str):
        """Lista SKUs por (FILA, TORRE), somando quantidades."""
        sku_col, fila_col, torre_col, qtd_col = cols.get("sku"), cols.get("fila"), cols.get("torre"), cols.get("qtd")
        if not (fila_col and torre_col and sku_col and qtd_col):
            return (
                False,
                "Colunas de FILA/TORRE, SKU ou QTD ausentes.",
                {},
                pd.DataFrame(),
                self._schema_min(df, cols, ["fila", "torre", "sku", "qtd"]),
            )
        df_f = df[
            df[fila_col].astype(str).str.contains(re.escape(fila or ""), case=False, na=False)
            & df[torre_col].astype(str).str.contains(re.escape(torre or ""), case=False, na=False)
        ]
        if df_f.empty:
            return (
                False,
                f"Nenhum SKU encontrado na fila {fila} torre {torre}.",
                {},
                pd.DataFrame(),
                self._schema_min(df, cols, ["fila", "torre"]),
            )

        g = (
            df_f.groupby([sku_col], dropna=False)[qtd_col]
            .sum()
            .reset_index()
            .sort_values(qtd_col, ascending=False, ignore_index=True)
        )
        list_str = "; ".join(f"{str(r[sku_col])} (qtd {int(r[qtd_col])})" for _, r in g.iterrows())
        template = "Na fila {fila}, torre {torre}, os SKUs encontrados são: {list_str}."
        payload = {"fila": fila, "torre": torre, "list_str": list_str}

        # >>>>>> Tabela de apoio (ENRIQUECIDA) <<<<<<
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
            "title": f"Distribuição de QTD por SKU – fila {fila}, torre {torre}",
        }
        return True, template, payload, table, schema

    def _fn_total_geral(self, df: pd.DataFrame, cols: Dict[str, Optional[str]]):
        """Soma total de unidades (todas as linhas)."""
        qtd_col = cols.get("qtd")
        if qtd_col is None:
            return False, "Coluna de quantidade (QTD) ausente.", {}, pd.DataFrame(), self._schema_min(df, cols, ["qtd"])

        total = int(pd.to_numeric(df[qtd_col], errors="coerce").fillna(0).sum())
        template = "O estoque total (todas as linhas) soma {total} unidades."
        payload = {"total": str(total)}
        table = pd.DataFrame()
        schema = self._schema_min(df, cols, ["qtd"])
        return True, template, payload, table, schema

    def _fn_sum_por_skus(self, df: pd.DataFrame, cols: Dict[str, Optional[str]], question: str):
        """Soma quantidades para uma lista de SKUs mencionados na pergunta."""
        sku_col, qtd_col = cols.get("sku"), cols.get("qtd")
        if not (sku_col and qtd_col):
            return False, "Colunas de SKU ou QTD ausentes.", {}, pd.DataFrame(), self._schema_min(df, cols, ["sku", "qtd"])

        skus = self._extract_sku_from_question(df, sku_col, question)
        if len(skus) < 2:
            return False, "Informe 2 ou mais SKUs para somarmos as quantidades.", {}, pd.DataFrame(), self._schema_min(df, cols, ["sku", "qtd"])

        df_f = df[df[sku_col].astype(str).str.lower().isin([s.lower() for s in skus])]
        if df_f.empty:
            return False, f"SKUs não encontrados: {', '.join(skus)}.", {}, pd.DataFrame(), self._schema_min(df, cols, ["sku"])

        g = (
            df_f.groupby([sku_col], dropna=False)[qtd_col]
            .sum()
            .reset_index()
            .sort_values(qtd_col, ascending=False, ignore_index=True)
        )
        total = int(g[qtd_col].sum())
        list_str = "; ".join(f"{str(r[sku_col])} (qtd {int(r[qtd_col])})" for _, r in g.iterrows())
        template = "Soma de quantidades para os SKUs selecionados: total {total} unidades. Por SKU: {list_str}."
        payload = {"total": str(total), "list_str": list_str}

        # Chart hint: pizza por SKU
        payload["__chart"] = {
            "type": "pie",
            "df": g[[sku_col, qtd_col]],
            "names": sku_col,
            "values": qtd_col,
            "title": "Distribuição de QTD por SKU (selecionados)",
        }

        # >>>>>> Tabela de apoio (ENRIQUECIDA) <<<<<<
        extra = [cols.get("descricao"), cols.get("fornecedor"), cols.get("endereco")]
        base_cols = self._unique_preserve([sku_col, qtd_col] + extra)
        table = df_f.loc[:, base_cols].head(5000)
        schema = self._schema_min(df, cols, ["sku", "qtd"])
        return True, template, payload, table, schema

    # ============================
    # Narração baseada em evidências (tabela de apoio)
    # ============================
    def _build_evidence_from_table(self, table: pd.DataFrame, cols: Dict[str, Optional[str]], max_rows: int = 12) -> str:
        """Constrói linhas de evidência legíveis a partir da tabela de apoio."""
        if table is None or table.empty:
            return "Nenhuma linha encontrada."

        show = table.copy()
        keys = ["sku", "descricao", "fornecedor", "qtd", "endereco", "fila", "torre", "nivel"]
        keep = [cols.get(k) for k in keys if cols.get(k) and cols.get(k) in show.columns]
        if keep:
            show = show[keep]
        show = show.head(max_rows)

        def _val_from_row(row: pd.Series, col: Optional[str]):
            if not col or col not in row.index:
                return None
            v = row[col]
            return v.iloc[0] if isinstance(v, pd.Series) else v

        lines = []
        for _, row in show.iterrows():
            parts: List[str] = []

            def add(label: str, key: str) -> None:
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

    def _narrate_results_from_table(
        self,
        question: str,
        table: pd.DataFrame,
        enhanced: bool = True,
        max_rows: int = 12,
        max_tokens: Optional[int] = 256,
    ) -> str:
        """Gera narração final com base na tabela (LLM); caso desligada, devolve resumo determinístico."""
        if not (self.cfg.use_llm and self.llm):
            if table is None or table.empty:
                return f"Resultado para: {question.strip()}\nNenhum resultado encontrado."
            return f"Resultado para: {question.strip()}\n{len(table)} linha(s) (amostra) na tabela de apoio."

        cols_map = self._detect_columns(table)
        # Sumário conciso para evidências (seguro a colunas ausentes)
        sku_c = cols_map.get("sku")
        desc_c = cols_map.get("descricao")
        forn_c = cols_map.get("fornecedor")
        qtd_c = cols_map.get("qtd")

        summary_lines: List[str] = []
        total_units = 0

        if table is not None and not table.empty and (sku_c or desc_c or forn_c or qtd_c):
            for _, row in table.head(max_rows).iterrows():
                sku = str(row[sku_c]) if (sku_c and sku_c in row) and pd.notna(row[sku_c]) else "—"
                descricao = str(row[desc_c]) if (desc_c and desc_c in row) and pd.notna(row[desc_c]) else "Descrição não disponível"
                fornecedor = str(row[forn_c]) if (forn_c and forn_c in row) and pd.notna(row[forn_c]) else None
                qtd = int(row[qtd_c]) if (qtd_c and qtd_c in row and pd.notna(row[qtd_c])) else 0
                total_units += qtd

                line = f"SKU {sku}: {descricao}"
                if fornecedor:
                    line += f" | Fornecedor: {fornecedor}"
                line += f" | QTD: {qtd}"
                summary_lines.append(line)
        summary_text = "\n".join(summary_lines)
        total_text = f"\nNo total, há {total_units} unidade(s)."

        prompt = f"""Você é um assistente que narra resultados de consulta de estoque.
Regras:
- Não invente valores. Use APENAS as EVIDÊNCIAS.
- Inclua detalhes como descrição e fabricante (se houver).
- Informe o total de unidades de forma clara.
- Escreva em pt-BR.

PERGUNTA:
{(question or '').strip()}

EVIDÊNCIAS (até {max_rows} linhas):
{summary_text}
{total_text}

# RESPOSTA:"""

        try:
            out = self.llm.create_completion(
                prompt=prompt,
                max_tokens=max_tokens or self.cfg.narration_max_tokens,
                temperature=max(self.cfg.temperature, self.cfg.narration_temperature),
                top_p=0.9,
                repeat_penalty=1.1,
            )
            choices = out.get("choices") if isinstance(out, dict) else None
            text = (choices or [{}])[0].get("text", "").strip() if isinstance(choices, list) else ""
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
        """Executa a consulta e retorna {text, table, chart, timings}.

        Args:
            df: DataFrame do estoque.
            question: Pergunta em linguagem natural.
            narrate: Se True, usa LLM para narrar com base na tabela de apoio.

        Returns:
            Dict com chaves: text (str), table (DataFrame), chart (dict|None), timings (dict).
        """
        t_all0 = time.perf_counter()
        result: Dict[str, object] = {"text": "", "table": pd.DataFrame(), "chart": None, "timings": {}}
        df = df.copy()

        # Detecta colunas / normaliza
        cols = self._detect_columns(df)
        if cols.get("qtd") is None:
            result["text"] = "Não encontrei a coluna de quantidade (QTD)."
            result["timings"] = {"parse_ms": 0.0, "pandas_ms": 0.0, "llm_ms": 0.0, "total_ms": round((time.perf_counter() - t_all0) * 1000, 1)}
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
            success, template, payload, table, schema = self._fn_qtd_por_fornecedor(df, cols, intent_info.get("fornecedor", ""))
        elif intent == "skus_por_fila":
            success, template, payload, table, schema = self._fn_skus_por_fila(df, cols, intent_info.get("fila", ""))
        elif intent == "skus_por_fila_torre":
            success, template, payload, table, schema = self._fn_skus_por_fila_torre(df, cols, intent_info.get("fila", ""), intent_info.get("torre", ""))
        elif intent == "total_geral":
            success, template, payload, table, schema = self._fn_total_geral(df, cols)
        elif intent == "enderecos_por_sku":  # legado
            success, template, payload, table, schema = self._fn_project_filter(df, cols, "endereco", "sku", question, [])
        elif intent == "skus_por_endereco":  # legado
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
        list_str = (payload or {}).get("list_str", "").strip()
        # payload_ex = (payload or {}).get("__explain", {})  # reservado para futura intro explicativa

        # Frase determinística base (compatibilidade com UI antiga)
        if list_str:
            out_label = (payload or {}).get("out_label", "")
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
            body = self._narrate_results_from_table(question, table, enhanced=False, max_tokens=256)
            final_sentence = f"{body}".strip()
            llm_ms = round((time.perf_counter() - t4) * 1000, 1)

        result["text"] = final_sentence
        result["table"] = table
        result["chart"] = (payload or {}).get("__chart")

        # Timings
        result["timings"] = {
            "parse_ms": round((t1 - t0) * 1000, 1),
            "pandas_ms": round((t3 - t2) * 1000, 1),
            "llm_ms": llm_ms,
            "total_ms": round((time.perf_counter() - t_all0) * 1000, 1),
        }
        return result
