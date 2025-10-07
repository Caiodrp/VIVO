import pandas as pd

# Função para carregar e consolidar o arquivo Excel
def load_and_consolidate(file):
    return pd.read_excel(file, sheet_name="PDV'S LESTE")

# Função principal para execução
def main(file):
    # Carregar os dados
    df = load_and_consolidate(file)

    # Garantir que a coluna 'Qtd. PDV\'s' é numérica
    df['Qtd. PDV\'s'] = pd.to_numeric(df['Qtd. PDV\'s'], errors='coerce')  # Converte para numérico, substitui não numéricos por NaN

    # Mapear UF para os nomes dos estados
    estado_map = {
        'MG': 'Minas Gerais',
        'RJ': 'Rio de Janeiro',
        'ES': 'Espírito Santo',
        # Adicione outras UFs conforme necessário
    }

    # Preencher a coluna 'Estado' com base na coluna 'UF'
    df['Estado'] = df['UF'].map(estado_map)

    # Agrupar os dados por CANAL, UF e Estado, somando a quantidade de PDVs
    df_grouped = df.groupby(['CANAL', 'UF', 'Estado'], as_index=False)['Qtd. PDV\'s'].sum()

    # Criar a tabela pivotada para tabela
    df_pivot_table = df_grouped.pivot(index='UF', columns='CANAL', values='Qtd. PDV\'s')

    # Converter todas as colunas numéricas para int
    df_pivot_table = df_pivot_table.fillna(0).astype(int)

    # Calcular o total de PDVs por UF (somando por linhas)
    df_pivot_table['Total PDVs'] = df_pivot_table.iloc[:, :].sum(axis=1)

    # Adicionar a coluna 'Estado' ao DataFrame da tabela
    df_table = df_pivot_table.merge(df_grouped[['UF', 'Estado']], on='UF', how='left')

    # Criar DataFrame para gráfico de barras empilhadas
    df_barra_empilhada = df_grouped[['UF', 'CANAL', 'Qtd. PDV\'s']]

    # Converter a coluna 'Qtd. PDV\'s' para int no DataFrame de barras empilhadas
    df_barra_empilhada['Qtd. PDV\'s'] = df_barra_empilhada['Qtd. PDV\'s'].fillna(0).astype(int)

    # Remover duplicatas no DataFrame de barras empilhadas
    df_barra_empilhada = df_barra_empilhada.drop_duplicates()

    # Criar DataFrame para o gráfico de mapa com bolhas
    df_mapa_bolhas = df_grouped[['Estado', 'CANAL', 'Qtd. PDV\'s']]

    # Exibir os DataFrames finais
    print("DataFrame para Tabela (pdv_table):")
    print(df_table)
    
    print("\nDataFrame para Gráfico de Barras Empilhadas (pdv_barra_empilhada):")
    print(df_barra_empilhada)
    
    print("\nDataFrame para Mapa com Bolhas (pdv_mapa_bolhas):")
    print(df_mapa_bolhas)

    # Renomear os DataFrames para os nomes desejados
    pdv_table = df_table
    pdv_barra_empilhada = df_barra_empilhada
    pdv_mapa_bolhas = df_mapa_bolhas

    return pdv_table, pdv_barra_empilhada, pdv_mapa_bolhas

# Caminho do arquivo
file_path = r"C:\Users\40418197\PDVs-LESTE\PDV'S LESTE.xlsx"

# Executa a função principal
pdv_table, pdv_barra_empilhada, pdv_mapa_bolhas = main(file_path)
