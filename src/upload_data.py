import os
import pandas as pd
import sqlalchemy

# Endereços do projeto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')

# Adquirindo nomes dos arquivos com os dados
file_names = [i for i in os.listdir(DATA_DIR) if i.endswith('.csv')]

# Abrindo conexão com o banco
connection = sqlalchemy.create_engine('sqlite:///' + os.path.join(DATA_DIR, 'olist.db'))

# Para cada arquivo de dados, cria uma tabela no banco de dados

print('Criando DB, aguarde...')

for i in file_names:    
    df_tmp = pd.read_csv(os.path.join(DATA_DIR, i))
    table_name = 'tb_' + i.replace('olist_', '').replace('_dataset', '').replace('.csv', '')    
    print(f'Tabela: {table_name}')
    df_tmp.to_sql(table_name, connection, if_exists='replace', index=False)

print('Concluído!')
