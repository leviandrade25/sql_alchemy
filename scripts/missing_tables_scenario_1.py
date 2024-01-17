import pyodbc
from sqlalchemy import create_engine, MetaData, Table, Column, text, DDL

def get_tables_names(metadata):
    print("get_table_names")
    #print("engine.tables.keys()")
    #print(engine.tables.keys())
    print(metadata.tables.keys())
    return metadata.tables.keys()

def get_table_definition(metadata, tabla, connection):
    print("get_table_definitions")
    tabla_split = tabla.split(".")[1]
    table = Table(tabla_split, metadata, autoload_with=connection)
    #print("table")
    print(table)
    return table

def generate_table_creation_script(tabla):
    print("generate_table_creation_scripts")
    script = f"CREATE TABLE {tabla.schema}.{tabla.name} ("
    for col in tabla.columns:
        script += f"    \"{col.name}\" {str(col.type).split(' ')[0]},"
    script = script.rstrip(',\n') + ")"
    #print("script")
    print(script)
    return script

def create_missing_tables_in_database_cloned(metadata_standard, metadata_cloned, conn_standard, conn_cloned, database_cloned):
    print("create_missing_tables_in_database_cloned")
    # Obtener nombres de tablas
    tables_database_standard = get_tables_names(metadata_standard)
    #print("tables_database_standard")
    #print(tables_database_standard)
    
    tables_database_cloned = get_tables_names(metadata_cloned)
    #print("tables_database_cloned")
    #print(tables_database_cloned)

    # Obtener tablas que existen en metadata_standard pero no en metadata_cloned
    tables_to_create = set(tables_database_standard) - set(tables_database_cloned)

    print("tables_to_create")
    print(tables_to_create)

    creation_scripts = []

    
    for tabla in tables_to_create:
        # Obtener definición de tabla desde metadata_standard
        table_definition = get_table_definition(metadata_standard, tabla, conn_standard)
        
        
        # Generar script de creación de tabla
        table_creation_script = generate_table_creation_script(table_definition)

         
        creation_scripts.append(table_creation_script)
        # Ejecutar el script en metadata_cloned
        conn_cloned.execute(DDL(table_creation_script))
        conn_cloned.commit()
        


        print(f"Tabla {tabla} created in {tables_database_cloned}")
    

    return creation_scripts

def main():
    # Configuración de las conexiones a las bases de datos en Azure
    server_standard = '601a-server-poc-master-checker.database.windows.net'
    database_standard = '601A-Standard'
    username_standard = 'admins-sa'
    password_standard = 'db_admin1'
    
    server_cloned = '601a-server-poc-master-checker.database.windows.net'
    database_cloned = '601A-Cloned'
    username_cloned = 'admins-sa'
    password_cloned = 'db_admin1'

    # Crear conexiones
    connection_standard = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_standard};DATABASE={database_standard};UID={username_standard};PWD={password_standard}')
    connection_cloned = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_cloned};DATABASE={database_cloned};UID={username_cloned};PWD={password_cloned}')

    # Crear motores SQLAlchemy
    engine_standard = create_engine(f"mssql+pyodbc://{username_standard}:{password_standard}@{server_standard}/{database_standard}?driver=ODBC+Driver+17+for+SQL+Server")
    engine_cloned = create_engine(f"mssql+pyodbc://{username_cloned}:{password_cloned}@{server_cloned}/{database_cloned}?driver=ODBC+Driver+17+for+SQL+Server")

    # Conecta a las bases de datos standard y cloned
    #conn_standard = metadata_standard.connect()
    conn_standard = engine_standard.connect()
    conn_clonned = engine_cloned.connect()

    # Obtener la lista de esquemas en ambas bases de datos
    #query_esquemas = "SELECT schema_name FROM information_schema.schemata"
    query_schemas = "SELECT schema_name FROM information_schema.schemata"

    #esquemas1 = [esquema[0] for esquema in conn_standard.execute(DDL(query_esquemas))]
    #esquemas2 = [esquema[0] for esquema in conn_clonned.execute(DDL(query_esquemas))]

    schemas_standard = [esquema[0] for esquema in conn_standard.execute(DDL(query_schemas))]
    schemas_cloned = [esquema[0] for esquema in conn_clonned.execute(DDL(query_schemas))]

    print("schemas_standard")
    print(schemas_standard)

    print("schemas_cloned")
    print(schemas_cloned)

    #print("list esquemas unidos")
    #print(list(esquemas1) & list(esquemas2))

    print("set schemas together")
    print(set(schemas_standard) & set(schemas_cloned))

    # Recorrer todos los esquemas
    
    
    for schema in set(schemas_standard) & set(schemas_cloned):

        print(" ")
        print("schema")
        print(schema)

        # Lee la estructura de la tabla desde la base de datos standard
        metadata_standard = MetaData(schema=schema)
        # Refleja las tablas desde la base de datos standard
        metadata_standard.reflect(bind=conn_standard)

        # Lee la estructura de la tabla desde la base de datos clonada
        metadata_cloned = MetaData(schema=schema)
        # Refleja las tablas desde la base de datos clonada
        metadata_cloned.reflect(bind=conn_clonned)

        # Comparar y crear tablas
        creation_scripts = create_missing_tables_in_database_cloned(metadata_standard, metadata_cloned, conn_standard, conn_clonned, database_cloned)
    

    # Cerrar conexiones
    connection_standard.close()
    connection_cloned.close()

if __name__ == "__main__":
    main()