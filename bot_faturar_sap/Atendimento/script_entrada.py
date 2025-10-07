import sys
import json
import time
import pyperclip
import pyautogui
import win32com.client
import pygetwindow as gw

def limpa_area_transferencia():
    pyperclip.copy("")
    time.sleep(0.3)

def selecionar_linhas(session, numero_materiais):
    tabela_id = "wnd[0]/usr/tabsTAXI_TABSTRIP_OVERVIEW/tabpT\\01/ssubSUBSCREEN_BODY:SAPMV50A:1102/tblSAPMV50ATC_LIPS_OVER"
    for linha in range(numero_materiais):
        session.findById(tabela_id).getAbsoluteRow(linha).selected = True
        time.sleep(0.1)

def obter_valor_coluna(session, linha):
    celula_id = f"wnd[0]/usr/tabsTAXI_TABSTRIP_OVERVIEW/tabpT\\01/ssubSUBSCREEN_BODY:SAPMV50A:1102/tblSAPMV50ATC_LIPS_OVER/txtLIPSD-G_LFIMG[2,{linha}]"
    session.findById(celula_id).setFocus()
    session.findById(celula_id).caretPosition = 1
    limpa_area_transferencia()
    pyautogui.hotkey("ctrl", "a")
    pyautogui.hotkey("ctrl", "c")
    time.sleep(0.1)
    return pyperclip.paste().strip()

def processar(session, fornecimento, numero_volumes, numero_materiais, json_saida_path):
    session.findById("wnd[0]/usr/ctxtLIKP-VBELN").text = fornecimento
    session.findById("wnd[0]").sendVKey(0)

    session.findById("wnd[0]/usr/tabsTAXI_TABSTRIP_OVERVIEW/tabpT\\01/ssubSUBSCREEN_BODY:SAPMV50A:1102/txtLIKP-ANZPK").text = str(numero_volumes)
    session.findById("wnd[0]").sendVKey(0)

    selecionar_linhas(session, numero_materiais)
    dados = []
    time.sleep(2)

    for linha in range(numero_materiais):
        valor = obter_valor_coluna(session, linha)
        if valor:
            dados.append({"linha": linha, "valor": int(valor)})

    with open(json_saida_path, "w") as f:
        json.dump(dados, f)

    print("Script entrada finalizado com sucesso.")

if __name__ == "__main__":
    fornecimento = sys.argv[1]
    numero_volumes = int(sys.argv[2])
    numero_materiais = int(sys.argv[3])
    json_saida_path = sys.argv[4]

    sap_app = win32com.client.GetObject("SAPGUI").GetScriptingEngine
    session = sap_app.Children(0).Children(0)
    processar(session, fornecimento, numero_volumes, numero_materiais, json_saida_path)