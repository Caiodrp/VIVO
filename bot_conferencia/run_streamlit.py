import os
import sys

def executar_streamlit():
    # Diretório do executável
    dir_do_exe = os.path.dirname(sys.executable)
    print(f"Diretório do executável: {dir_do_exe}")

    # Subir um nível, pois os arquivos estão fora da pasta dist/
    dir_base = os.path.dirname(dir_do_exe)
    print(f"Diretório base: {dir_base}")

    # Caminho do arquivo Inicio.py
    caminho_inicio = os.path.join(dir_base, "Inicio.py")
    print(f"Caminho do arquivo Inicio.py: {caminho_inicio}")

    # Verifique se o arquivo Inicio.py existe
    if not os.path.isfile(caminho_inicio):
        print(f"Arquivo Inicio.py não encontrado em {dir_base}")
    else:
        # Navegar até o diretório base
        os.chdir(dir_base)
        print(f"Diretório atual: {os.getcwd()}")

        # Abrir o CMD e executar o comando Streamlit
        os.system('cmd /k "streamlit run Inicio.py"')

if __name__ == "__main__":
    executar_streamlit()