from sqlalchemy import create_engine, MetaData, Table
import json

def compare_tables():
    dados_tabulares = {}

    # Carregar credenciais
    with open('dags/credentials/credentials.json', 'r') as file_vault:
        credentials = json.load(file_vault)

    user = credentials['user_name']
    password = credentials['password']
    ip_connection = credentials['ip_connection']
    port = credentials['port']
    db = credentials['db']

    # Conexão com o banco de dados
    engine = create_engine(f'postgresql://{user}:{password}@{ip_connection}:{port}/{db}')
    metadata = MetaData()

    with open('data_out/list_tables.json', 'r') as tables_names:
        tables_list = json.load(tables_names)

    for db, schemas in tables_list.items():
        for schema, tabelas in schemas.items():
            for tabela in tabelas:
                
                # Nome do banco de dados principal
                main_table = "usuario"
                schema1 = "new_db_1"
                schema2 = schema

                # Refletir as tabelas dos dois schemas
                tabela_schema1 = Table(main_table, metadata, autoload_with=engine, schema=schema1)
                tabela_schema2 = Table(main_table, metadata, autoload_with=engine, schema=schema2)

                # Função para comparar tabelas
                def comparar_tabelas(t1, t2):
                    diferencas = []

                    # Comparar colunas existentes em t1 mas não em t2
                    colunas_t1 = set(c.name for c in t1.columns)
                    colunas_t2 = set(c.name for c in t2.columns)

                    diferencas.extend(f'Coluna apenas em {t1.fullname}: {col}' for col in colunas_t1 - colunas_t2)

                    return diferencas

    # Comparar as tabelas
    diferencas = comparar_tabelas(tabela_schema1, tabela_schema2)
    for diferenca in diferencas:
        print(diferenca)
