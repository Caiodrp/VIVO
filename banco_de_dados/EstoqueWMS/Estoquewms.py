import os
import pandas as pd
from sqlalchemy import create_engine

# Configurações do banco de dados
db_config = {
    'dbname': 'OperacaoVIVO',
    'user': 'postgres',
    'password': os.getenv('DB_PASSWORD'),  # Var ENV
    'host': 'localhost',
    'port': 5432
}

# Criar a string de conexão com o PostgreSQL
engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}")

# Caminho para a planilha
file_path = r'C:\Users\40418197\Desktop\Banco de Dados\EstoqueWMS\Estoque.xlsx'

# Nome da tabela onde os dados serão inseridos
table_name = 'EstoqueWMS'

# Ler a planilha
df = pd.read_excel(file_path)

# Carregar os dados para o PostgreSQL (os dados serão inseridos na tabela especificada)
df.to_sql(table_name, engine, if_exists='replace', index=False)

print(f"Tabela {table_name} criada com sucesso com os dados de {file_path}")