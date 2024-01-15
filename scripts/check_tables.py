from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy.schema import CreateTable
import json

# Carregar credenciais
with open('dags/credentials/credentials.json', 'r') as file:
    credentials = json.load(file)

user = credentials['user_name']
password = credentials['password']
ip_connection = credentials['ip_connection']
port = credentials['port']
db_name = credentials['db']

# Carregar informações do esquema principal
with open('data_out/main_db_schema.json', 'r') as main_info_file:
    main_info = json.load(main_info_file)

main_db = main_info['main_db']
main_schema = main_info['main_schema']

# Configuração da conexão com o banco de dados
database_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{main_db}'
master_engine = create_engine(database_uri)

# Obter a lista de tabelas do esquema principal
inspector = inspect(master_engine)
main_tables = inspector.get_table_names(schema=main_schema)

# Criando metadados
metadata = MetaData()
# metadata.reflect(engine, schema=main_schema)

# Carregar lista de tabelas e esquemas
with open('data_out/list_tables.json', 'r') as tables_file:
    tables_list = json.load(tables_file)


for db, schemas in tables_list.items():
    set_tables = set()
    engine = create_engine(f'postgresql://{user}:{password}@{ip_connection}:{port}/{db}')
    inspector = inspect(engine)
    for schema, tabelas in schemas.items():
        existing_tables = inspector.get_table_names(schema=schema)

        for table_cheker in main_tables:
            if table_cheker not in existing_tables:
                set_tables.add(table_cheker)

        for table_absent in set_tables:
            metadados_table = Table(table_absent,metadata,autoload_with=master_engine,schema=main_schema)
            for column in metadados_table.columns:
                print("")
                # print(f"Tipo de coluna {column.name} = {column.type}")
                # print(f"{column.nullable}")
                # print(f"{column.primary_key}")
                # print(f"{column.foreign_keys}")
                # print(f"{column.key}")
                
                
                # primary_keys = [key.name for key in table_absent.primary_key]
                # forigner_keys = [key.name for key in table_absent.primary_key]
                # indices = [index.name for index in table_absent.indexes]
                # unique_constraints = [constraint for constraint in table_absent.constraints if isinstance(constraint, UniqueConstraint)]

                # print(f"Primary keys desta table {primary_keys}")
                # print(f"Foreign keys desta table {forigner_keys}")
                # print(f"indices  desta table {indices}")
                # for uc in unique_constraints:
                #     print(f"unique constrains: {uc}")


                
            
        

    #     for table_name in main_tables:
    #         if table_name not in existing_tables:
    #             try:
    #                 main_table = metadata.tables[f'{main_schema}.{table_name}']
    #                 clone_metadata = MetaData(schema=main_schema)
    #                 clone_table = main_table.tometadata(clone_metadata)
    #                 clone_table.create(engine)
    #                 print(f"Criada tabela {table_name} em schema {schema}")
    #             except Exception as e:
    #                 print("")
    # engine.dispose()
