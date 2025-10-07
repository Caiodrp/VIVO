import sys  
import json  
import time 
import pyperclip 
import win32com.client  # Para automação de aplicativos Windows, como o SAP

def limpa_area_transferencia():
    pyperclip.copy("")
    time.sleep(0.2) 

def executar_faturamento(session, seriais, dados_linhas):
    seriais_str = "\r\n".join(seriais)
    limpa_area_transferencia()
    pyperclip.copy(seriais_str) 
    time.sleep(0.2)  

    for item in dados_linhas:
        linha = item["linha"] 
        valor = item["valor"] 

        print(f"Processando linha {linha} com valor {valor}") 

        session.findById("wnd[0]/mbar/menu[3]/menu[3]").select()
        session.findById("wnd[1]/tbar[0]/btn[6]").press()
        session.findById("wnd[0]/usr/btn%_SERNR_%_APP_%-VALU_PUSH").press()
        session.findById("wnd[1]/tbar[0]/btn[24]").press()

        if valor == 1:
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/"
                             "tblSAPLALDBSINGLE/txtRSCSEL_255-SLOW_I[1,0]").setFocus()
            session.findById("wnd[1]/usr/tabsTAB_STRIP/tabpSIVA/ssubSCREEN_HEADER:SAPLALDB:3010/"
                             "tblSAPLALDBSINGLE/txtRSCSEL_255-SLOW_I[1,0]").caretPosition = 0
            session.findById("wnd[1]/tbar[0]/btn[8]").press()
            session.findById("wnd[0]/tbar[1]/btn[8]").press()
            session.findById("wnd[1]/tbar[0]/btn[8]").press()
        else:
            session.findById("wnd[1]/tbar[0]/btn[8]").press()
            session.findById("wnd[0]/tbar[1]/btn[8]").press()
            session.findById("wnd[0]/tbar[1]/btn[5]").press()
            session.findById("wnd[0]/tbar[1]/btn[47]").press()
            session.findById("wnd[1]/tbar[0]/btn[8]").press()

    session.findById("wnd[0]/tbar[1]/btn[20]").press() 
    print("Faturamento concluído.")  

if __name__ == "__main__":
    fornecimento = sys.argv[1]  
    seriais = sys.argv[2].split(",") 
    numero_materiais = int(sys.argv[3])
    dados_linhas = json.loads(sys.argv[4])

    sap_app = win32com.client.GetObject("SAPGUI").GetScriptingEngine
    session = sap_app.Children(0).Children(0)
    executar_faturamento(session, seriais, dados_linhas)
