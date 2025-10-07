import streamlit as st
 
# Definir o template
st.set_page_config(page_title='Auxiliar de Faturamento',
                   page_icon='📦',
                   layout='wide')
 
def main():
    # Apresenta a imagem na barra lateral da aplicação
    st.sidebar.markdown("<div style='display: flex; justify-content: center; margin-top: 100px;'>", unsafe_allow_html=True)
    st.sidebar.image("img/logo_vivo.png", use_container_width=True)
    st.sidebar.markdown("</div>", unsafe_allow_html=True)
 
    # Título centralizado
    st.write(
        '<div style="display:flex; align-items:center; justify-content:center;">'
        '<h1 style="font-size:4.5rem;">Auxiliar de Faturamento</h1>'
        '</div>',
        unsafe_allow_html=True
    )
 
    # Subtítulo
    st.write(
        '<div style="display:flex; align-items:center; justify-content:center;">'
        '<h2 style="font-size:2.5rem;">RPA desenvolvida para agilizar e facilitar o processo de faturamento de pedidos no SAP.</h2>'
        '</div>',
        unsafe_allow_html=True
    )
 
    # Divisão
    st.write("---")
 
    # Imagem do lado da explicação
    col1, col2 = st.columns(2)
 
    col1.write(
        "<p style='font-size:1.5rem;'>Este sistema automatiza etapas repetitivas do faturamento dentro do SAP, "
        "permitindo mais eficiência e precisão ao processar pedidos. "
        "Ideal para operações de expedição e logística em ambientes de alto volume.</p>",
        unsafe_allow_html=True
    )
 
    col2.image("img/entrega.gif", use_container_width=True)
 
    # Divisão
    st.write("---")
 
    st.write(
        '<h3 style="text-align:left;">Autor</h3>'
        '<ul style="list-style-type: disc; margin-left: 20px;">'
        '<li>Seu Nome</li>'
        '<li><a href="https://github.com/seu_usuario_github">GitHub</a></li>'
        '</ul>',
        unsafe_allow_html=True
    )
 
if __name__ == "__main__":
    main()