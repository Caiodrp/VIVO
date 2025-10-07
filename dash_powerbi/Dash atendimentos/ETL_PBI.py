import dask.dataframe as dd
import pandas as pd
import time
import os

# Caminhos para os arquivos
input_file_path = r'Pedidos_ETL.xlsx'  
output_file_path = r'Pedidos_PBI.xlsx'

# Função para medir o tempo de execução
def log_time(start_time, process_name):
    end_time = time.time()
    print(f"{process_name} levou {end_time - start_time:.2f} segundos.")

# Início do processo
print("Iniciando o script ETL...")
start_time = time.time()

# Verificar se o arquivo existe
if not os.path.exists(input_file_path):
    print(f"Erro: O arquivo '{input_file_path}' não foi encontrado.")
    exit(1)

# Carregando os dados
print("Lendo o arquivo Excel...")
load_start_time = time.time()

colunas_selecionadas = ['CEN', 'QTD', 'FORN', 'DT NF', 'VLR', 'TIPO MAT', 'Canal ']
dtypes = {'CEN': 'object', 'QTD': 'int32', 'FORN': 'object', 'DT NF': 'datetime64[ns]', 'VLR': 'float64'}

# Ler o arquivo Excel e converter para Dask DataFrame
df = pd.read_excel(input_file_path, usecols=colunas_selecionadas, dtype=dtypes)
df = dd.from_pandas(df, npartitions=4)
log_time(load_start_time, "Leitura do arquivo Excel")

# Criar colunas 'MES', 'UF' e 'Mês_Ordem'
print("Criando colunas 'MES', 'UF' e 'Mês_Ordem'...")
process_start_time = time.time()

df['MES'] = df['DT NF'].dt.month.map({
    1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5: 'MAI', 6: 'JUN',
    7: 'JUL', 8: 'AGO', 9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'
}, meta=('MES', 'object'))

df['UF'] = df['CEN'].map({'MNW5': 'MG', 'RJW5': 'RJ', 'ESW5': 'ES'}, meta=('UF', 'object'))
df['Mês_Ordem'] = df['DT NF'].dt.month

log_time(process_start_time, "Criação de colunas")

# Consolidar dados
print("Consolidando dados em uma única tabela...")
consolidation_start_time = time.time()

final_table = df[['UF', 'MES', 'QTD', 'VLR', 'TIPO MAT', 'Canal ', 'Mês_Ordem']].copy()
final_table['Total_Pedidos'] = 1

# Agrupar e computar com Dask
final_table = final_table.groupby(['UF', 'MES', 'TIPO MAT', 'Canal ', 'Mês_Ordem']).agg({
    'QTD': 'sum',
    'VLR': 'sum',
    'Total_Pedidos': 'count'
}).compute()

# Resetar índice
final_table = final_table.reset_index()

# Tabela de Total por Tipo de Material
total_por_tipo_material = final_table.groupby('TIPO MAT').agg({
    'Total_Pedidos': 'sum',
    'QTD': 'sum',
    'VLR': 'sum'
}).reset_index()

total_por_tipo_material.rename(columns={
    'Total_Pedidos': 'Total_Pedidos_por_Tipo_Material', 
    'QTD': 'QTD_por_Tipo_Material', 
    'VLR': 'VLR_por_Tipo_Material'
}, inplace=True)

# Tabela de Total por Tipo de Canal
total_por_tipo_canal = final_table.groupby('Canal ').agg({
    'Total_Pedidos': 'sum',
    'QTD': 'sum',
    'VLR': 'sum'
}).reset_index()

total_por_tipo_canal.rename(columns={
    'Total_Pedidos': 'Total_Pedidos_por_Tipo_Canal', 
    'QTD': 'QTD_por_Tipo_Canal', 
    'VLR': 'VLR_por_Tipo_Canal'
}, inplace=True)

log_time(consolidation_start_time, "Consolidação de dados")

# Exportar para Excel
print("Exportando dados consolidados para o Excel...")
export_start_time = time.time()

with pd.ExcelWriter(output_file_path, engine='openpyxl') as writer:
    final_table.to_excel(writer, index=False, sheet_name='ped_Dados Consolidados')
    total_por_tipo_material.to_excel(writer, index=False, sheet_name='ped_Total por Tipo de Canal')
    total_por_tipo_canal.to_excel(writer, index=False, sheet_name='ped_Total por Tipo de Material')

log_time(export_start_time, "Exportação para Excel")

# Tempo total
log_time(start_time, "Tempo total de execução")
print("Processo concluído. Arquivo salvo como Pedidos_PBI.xlsx.")