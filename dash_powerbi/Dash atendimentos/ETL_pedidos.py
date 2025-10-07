import dask.dataframe as dd
import pandas as pd
import time
import os

# Caminho para o arquivo .xlsx
input_file_path = r'C:\Users\40418197\Desktop\Painel Logistica Leste\Dash atendimentos\Pedidos_filtrado.xlsx'
output_file_path = r'C:\Users\40418197\Desktop\Painel Logistica Leste\Dash atendimentos\Pedidos_ETL.xlsx'

# Função para medir o tempo de execução
def log_time(start_time, process_name):
    end_time = time.time()
    print(f"{process_name} levou {end_time - start_time:.2f} segundos.")

# Início do processo
print("Iniciando o script ETL...")  # Log no início
start_time = time.time()

# Verificar se o arquivo existe
if not os.path.exists(input_file_path):
    print(f"Erro: O arquivo '{input_file_path}' não foi encontrado.")
else:
    # Carregando os dados com tipos de dados específicos
    print("Lendo o arquivo Excel...")
    load_start_time = time.time()
    colunas_selecionadas = ['CEN', 'QTD', 'FORN', 'DT NF', 'VLR', 'TIPO MAT', 'Canal ']
    dtypes = {'CEN': 'object', 'QTD': 'int32', 'FORN': 'object', 'DT NF': 'datetime64[ns]', 'VLR': 'float64'}
    
    try:
        # Usar pandas para ler o arquivo Excel
        df = pd.read_excel(input_file_path, usecols=colunas_selecionadas, dtype=dtypes)
        # Converter para Dask DataFrame
        df = dd.from_pandas(df, npartitions=1)
        log_time(load_start_time, "Leitura do arquivo Excel")
    except Exception as e:
        print(f"Erro ao ler o arquivo Excel: {e}")
        exit(1)

    # Criando colunas
    print("Criando colunas 'MES' e 'UF'...")
    process_start_time = time.time()
    df['MES'] = df['DT NF'].dt.month.map({1: 'JAN', 2: 'FEV', 3: 'MAR', 4: 'ABR', 5: 'MAI', 6: 'JUN',
                                           7: 'JUL', 8: 'AGO', 9: 'SET', 10: 'OUT', 11: 'NOV', 12: 'DEZ'})
    df['UF'] = df['CEN'].map({'MNW5': 'MG', 'RJW5': 'RJ', 'ESW5': 'ES'})
    log_time(process_start_time, "Criação de colunas")

    # Processando dados sintéticos para RJW5 e ESW5
    print("Processando dados sintéticos para RJW5 e ESW5...")
    process_start_time = time.time()
    num_mnw5 = (df[df['CEN'] == 'MNW5'].shape[0]).compute()  # Usar shape[0] para obter número de linhas
    frac_rj = 0.73
    frac_es = 0.64

    # Criando dados sintéticos
    df_rj = df[df['CEN'] == 'MNW5'].sample(frac=frac_rj, replace=True).assign(CEN='RJW5')
    df_es = df[df['CEN'] == 'MNW5'].sample(frac=frac_es, replace=True).assign(CEN='ESW5')
    log_time(process_start_time, "Processamento de dados sintéticos")

    # Combinando todos os dados
    print("Combinando todos os dados...")
    combine_start_time = time.time()
    df_final = dd.concat([df, df_rj, df_es], axis=0, ignore_index=True)
    log_time(combine_start_time, "Combinação de dados")

    # Convertendo para Pandas DataFrame antes de salvar
    print("Convertendo o DataFrame Dask para Pandas...")
    df_final_pd = df_final.compute()  # Converter o Dask DataFrame em um Pandas DataFrame

    # Salvando para Excel
    print("Salvando os dados no arquivo Excel...")
    save_start_time = time.time()
    try:
        df_final_pd.to_excel(output_file_path, index=False, sheet_name='Sheet1', engine='openpyxl')
        log_time(save_start_time, "Salvamento de dados no Excel")
    except Exception as e:
        print(f"Erro ao salvar o arquivo Excel: {e}")
        exit(1)

    # Tempo total de execução
    log_time(start_time, "Tempo total de execução")
    print("Processo concluído. Arquivo salvo como Pedidos_ETL.xlsx.")