import streamlit as st

# Definir o template
st.set_page_config(page_title='Agente de Previsão de Pedidos',
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
        '<h1 style="font-size:4.5rem;">Agente de Previsão de Pedidos</h1>'
        '</div>',
        unsafe_allow_html=True
    )

    # Subtítulo
    st.write(
        '<div style="display:flex; align-items:center; justify-content:center;">'
        '<h2 style="font-size:2.5rem;">Modelo de RN para previsão de demanda de SimCards e Terminais por dia da semana.</h2>'
        '</div>',
        unsafe_allow_html=True
    )

    # Divisão
    st.write("---")

    # Imagem do lado da explicação
    col1, col2 = st.columns(2)

    col1.write(
        "<p style='font-size:1.5rem;'> Este sistema é um <b>Agente de Previsão de Pedidos</b> desenvolvido para antecipar a demanda de <b>SimCards</b> e <b>Terminais</b> com base no dia da semana. "
        "Utilizando <b>redes neurais locais</b>, ele analisa padrões históricos e realiza previsões precisas sem depender de servidores externos. "
        "Ideal para ambientes logísticos que buscam <b>eficiência, autonomia e segurança</b> na tomada de decisão sobre reposição de estoque.</p>",
        unsafe_allow_html=True
    )

    col2.image("img/pedidos.gif", use_container_width=True)

    # Divisão
    st.write("---")

    st.write(
        '<h3 style="text-align:left;">Autor</h3>'
        '<ul style="list-style-type: disc; margin-left: 20px;">'
        '<li>Caio Douglas Rodrigues De Paula</li>'
        '<li>https://github.com/seu_usuario_githubGitHub</a></li>'
        '</ul>',
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()