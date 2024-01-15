from sqlalchemy import create_engine, MetaData, inspect, Table
from sqlalchemy import Column, DDL, text
from sqlalchemy.schema import ForeignKeyConstraint, CreateTable, ForeignKey
import json
from credentials.credential_file import read_json_file

def create_absent_column():
    # Carregar credenciais
    file_path = 'dags/credentials/credentials.json'

    credentials = read_json_file(file_path)

    user = credentials['user_name']
    password = credentials['password']
    ip_connection = credentials['ip_connection']
    port = credentials['port']
    

    # Carregar informações do esquema principal
    main_info_path = 'data_out/main_db_schema.json'

    main_info = read_json_file(main_info_path)
    main_db = main_info['main_db']
    main_schema = main_info['main_schema']

    # Configuração da conexão com o banco de dados
    database_uri = f'postgresql://{user}:{password}@{ip_connection}:{port}/{main_db}'
    master_engine = create_engine(database_uri)

    # Obter a lista de tabelas do esquema principal
    inspector_master = inspect(master_engine)
    main_tables = inspector_master.get_table_names(schema=main_schema)

    # Criando metadados
    metadata = MetaData()
    # metadata.reflect(engine, schema=main_schema)

    # Carregar lista de tabelas e esquemas

    list_tables_path = 'data_out/list_tables.json'
    tables_list = read_json_file(list_tables_path)
    
    del tables_list[main_db][main_schema]


    dict_default_value = {"DATE":'01-01-2000'}
    for db, schemas in tables_list.items():
        engine = create_engine(f'postgresql://{user}:{password}@{ip_connection}:{port}/{db}')
        for schema, tabelas in schemas.items():
            inspector = inspect(engine)
            existing_tables = inspector.get_table_names(schema=schema)
            for table_name in existing_tables:
                metadados_table = Table(table_name, metadata, autoload_with=engine, schema=schema)
                columns_to_check = [column.name for column in metadados_table.columns]
                metadados_checker = Table(table_name, metadata, autoload_with=master_engine, schema=main_schema)
                columns_checker = [column.name for column in metadados_checker.columns]
                for cl_name in columns_checker:
                    if cl_name not in columns_to_check:
                        # Criação da instrução SQL para adicionar a coluna

                        is_not_null = not metadados_checker.c[cl_name].nullable
                        if is_not_null is True:
                            default_value = dict_default_value.get(str(metadados_checker.columns[cl_name].type))
                            alter_table_command = text(f"""ALTER TABLE {schema}.{table_name} ADD COLUMN {cl_name} {metadados_checker.columns[cl_name].type} NOT NULL
                                                    DEFAULT '{default_value}' """)
                            with engine.begin() as connection:
                                connection.execute(alter_table_command)
                                
                        else:
                            alter_table_command = text(f'ALTER TABLE {schema}.{table_name} ADD COLUMN {cl_name} {metadados_checker.columns[cl_name].type}')
                            with engine.begin() as connection:
                                connection.execute(alter_table_command)

                        


                        print(f"Coluna {cl_name} adicionada em {db}.{schema}.{table_name}")
                        print("")
                    else:
                        print(f"Coluna {cl_name} Existente em {db}.{schema}.{table_name}")
                        print("")


        