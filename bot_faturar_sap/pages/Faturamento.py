import streamlit as st
import subprocess
import os
import tempfile
import json

def main():
    st.title("Faturar SAP")

    if st.button("🔄 Reset"):
        for key in list(st.session_state.keys()):
            del st.session_state[key]

    if st.button("🧹 Limpar"):
        for key in ["fornecimento", "serial_input", "numero_volumes", "numero_materiais"]:
            st.session_state[key] = "" if "serial" in key or "fornecimento" in key else 1

    # Inicialização das variáveis de estado
    if "fornecimento" not in st.session_state:
        st.session_state["fornecimento"] = ""
    if "serial_input" not in st.session_state:
        st.session_state["serial_input"] = ""
    if "numero_volumes" not in st.session_state:
        st.session_state["numero_volumes"] = 1
    if "numero_materiais" not in st.session_state:
        st.session_state["numero_materiais"] = 1
    if "valores_linhas" not in st.session_state:
        st.session_state["valores_linhas"] = []

    fornecimento = st.text_input("Número do fornecimento:", key="fornecimento")
    numero_volumes = st.number_input("Número de volumes:", min_value=1, key="numero_volumes")
    numero_materiais = st.number_input("Quantidade de materiais diferentes:", min_value=1, key="numero_materiais")

    if st.button("Entrar Pedido"):
        caminho_script_entrada = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Atendimento", "script_entrada.py"))
        with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as tmp:
            json_path = tmp.name

        args = [
            "python", caminho_script_entrada,
            fornecimento,
            str(numero_volumes),
            str(numero_materiais),
            json_path,
        ]

        process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        process.communicate()

        if process.returncode == 0:
            st.success("✅ Entrada do pedido realizada com sucesso!")
            st.session_state["json_path"] = json_path

            if os.path.getsize(json_path) > 0:
                with open(json_path, "r") as f:
                    dados = json.load(f)
                    st.session_state.valores_linhas = dados
            else:
                st.error("O arquivo JSON está vazio. Verifique o script de entrada no SAP.")

    serial_input = st.text_area("Escaneie os seriais:", height=200, key="serial_input")
    seriais_unicos = list(set([s.strip() for s in serial_input.split("\n") if s.strip()]))

    quantidade_esperada = sum(item["valor"] for item in st.session_state.get("valores_linhas", []))
    st.write(f"Seriais únicos inseridos: {len(seriais_unicos)} / {quantidade_esperada}")

    if len(seriais_unicos) == quantidade_esperada:
        if st.button("Faturar"):
            script_faturar = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Atendimento", "script_faturar.py"))
            args = [
                "python", script_faturar,
                fornecimento,
                ",".join(seriais_unicos),
                str(numero_materiais),
                json.dumps(st.session_state.valores_linhas)
            ]

            process = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            process.communicate()

            if process.returncode == 0:
                st.success("Faturamento concluído com sucesso!")

            if "json_path" in st.session_state:
                try:
                    os.remove(st.session_state["json_path"])
                    del st.session_state["json_path"]
                except Exception as e:
                    st.warning(f"⚠️ Erro ao tentar apagar arquivo temporário: {e}")
    else:
        diferenca = len(seriais_unicos) - quantidade_esperada
        if diferenca > 0:
            st.error(f"⚠️ Há {diferenca} seriais a mais do que o esperado.")
        else:
            st.error(f"⚠️ Faltam {-diferenca} seriais para completar o faturamento.")

if __name__ == "__main__":
    main()
