import pandas as pd

# Caminho para o arquivo de entrada
input_file_path = r'Estoque_ETL.xlsx'

# Lendo o arquivo Excel
df = pd.read_excel(input_file_path)

# Criar um DataFrame para o total por UF
total_por_uf = df.groupby('UF').agg(
    Total_Valor_NF=('Valor Total NF', 'sum'),
    Quantidade_Notas=('Valor Total NF', 'count')
).reset_index()

# Criar um DataFrame para o ranking de fabricantes
ranking_fabricantes = df.groupby('Fabricante').agg(
    Total_Valor_NF=('Valor Total NF', 'sum')
).sort_values(by='Total_Valor_NF', ascending=False).reset_index()

# Criar um DataFrame para os SKUs somando a quantidade
somatorio_skus = df.groupby('SKU').agg(
    Quantidade=('Quant.', 'sum')
).reset_index()

# Criar um DataFrame que mantém o SKU e a quantidade por UF
sku_por_uf = df.groupby(['UF', 'SKU']).agg(
    Quantidade=('Quant.', 'sum')
).reset_index()

# Caminho para o arquivo de saída com várias abas
output_file_path = r'Estoque_PBI.xlsx'

# Exportar todos os DataFrames em um único arquivo, cada um em uma aba
with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    total_por_uf.to_excel(writer, index=False, sheet_name='est_Total_por_UF')
    ranking_fabricantes.to_excel(writer, index=False, sheet_name='est_Ranking_Fabricantes')
    somatorio_skus.to_excel(writer, index=False, sheet_name='est_Somatório_SKUs')
    sku_por_uf.to_excel(writer, index=False, sheet_name='est_SKU_por_UF')

print(f"ETL executado e dados salvos em '{output_file_path}'.")