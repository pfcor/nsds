import pkg_resources
import json
import re


# # # # # # # # # #
#                 #
#   DB FUNCTIONS  #
#                 #
# # # # # # # # # #


import cx_Oracle, sqlite3, sqlalchemy

#
# AUX
#

def get_connection_type(connection_type):
    """
    recebe connection_type [str ou list/tuple] e retorna uma lista de tipos de conexão
    pronta para ser utilizada pelas funções de conexão (connect_oracle e connect_sqlite)
    """

    if isinstance(connection_type, str): # caso de ser string
        connection_type = connection_type.lower()
        if connection_type == 'all':
            connection_type = ['cc', 'engine']
        elif not connection_type in ['connection', 'cursor', 'engine']:
            print(f'invalid connection_type input: {connection_type}')
            raise ValueError
        else:
            connection_type = [connection_type]
    elif isinstance(connection_type, (list, tuple)): # se já for inserido list/tuple
        connection_type = list(set(connection_type)) # excluindo duplicatas
        for i, ct in enumerate(connection_type.copy()):
            try:
                ct = ct.lower()
                connection_type[i] = ct
            except AttributeError:
                print(f'invalid connection_type input: {ct}')
                raise ValueError
            if ct not in ['connection', 'cursor', 'engine']:
                print(f'invalid connection_type input: {ct}')
                raise ValueError
    connection_type.sort() 
    if connection_type[:2] == ['connection', 'cursor']:
        connection_type = ['cc'] + connection_type[2:] # cc (connection/cursor) é unificado para garantir que o cursor retornado foi criado a partir da conexão também retornada 
    return connection_type

def get_sas_bigdata_connection_info():
    resource_package = __name__
    resource_path = '/'.join(('oracle_connection_config.json',))
    return json.loads(pkg_resources.resource_string(resource_package, resource_path).decode('utf-8'))  

#
# ORACLE
#

def connect_oracle(**kwargs):
    """
    Conexão com banco de dados Oracle

    inputs:
    :: connection_info [dict] -> dicionario com campos "user", "password", "host" e "service"
    :: user, password, host, service [str] -> infos de conexão inseridas individualmente
    :: connection_type [str ou list] -> define o tipo de conexão retornado | ["connection" (default), "cursor", "engine", "all"]
    :: encoding [str] -> encoding a ser utilizado na conexão | default: "utf-8"

    output:
    :: objeto conector ou lista de objetos conectores ao Oracle definido por connection_type,
       no caso de uma lista de tipos de conexão, independente da ordem entrada, o retorno será 
       na ordem: connection > cursor > engine. se connection e cursor forem inseridos, o cursor
       será obtido da connection e não de um objeto criado em separado. 
    """

    # criando a lista de conexões a serem retornadas
    connection_type = get_connection_type(kwargs.get('connection_type', ['connection', 'cursor']))

    # obtendo enconding
    encoding = kwargs.get('encoding', 'utf-8')

    # obtendo dados de conexão
    if not 'connection_info' in kwargs: # se o dict for colocado será utilizado, ou seja, deve estar completo
        connection_info = kwargs
    else:
        connection_info = kwargs.get('connection_info')
        
    user = connection_info.get('user')
    password = connection_info.get('password')
    host = connection_info.get('host')
    service = connection_info.get('service')

    # verificando se temos todos os dados para conexão
    assert user, "User missing"
    assert password, "Password missing"
    assert host, "Host missing"
    assert service, "Service missing"

    # construindo strings de conexão
    connection_string = f'{user}/{password}@{host}:1521/{service}'
    engine_string = 'oracle://' + connection_string.replace('/', ':', 1)

    # criando os objetos de conexão
    cnxn_objects = []
    for ct in connection_type:
        if ct == 'cc':
            connection = cx_Oracle.connect(connection_string, encoding=encoding)
            cursor = connection.cursor()
            cnxn_objects = [connection, cursor] # podemos fazer isso por a construção da lista de conexões garante 'cc' no primeiro índice
        elif ct == 'connection':
            cnxn_objects.append(cx_Oracle.connect(connection_string, encoding=encoding))
        elif ct == 'cursor':
            cnxn_objects.append(cx_Oracle.connect(connection_string, encoding=encoding).cursor())
        elif ct == 'engine':
            cnxn_objects.append(sqlalchemy.create_engine(engine_string, encoding=encoding))
    
    # retornando objetos de conexão
    if len(cnxn_objects) > 1:
        return tuple(cnxn for cnxn in cnxn_objects)
    else:
        return cnxn_objects[0]

def connect_sas_bigdata(connection_type=['connection', 'cursor'], encoding='utf-8'):
    """
    Atalho para conectar ao 'schema' do time de ciência de dados no Oracle sem a necessidade
    de inserir dados da conta, utilizando o arquivo 'oracle_connection_config.json'
    """
    connection_info = get_sas_bigdata_connection_info()
    return connect_oracle(connection_info=connection_info, connection_type=connection_type, encoding=encoding)

def find_table_oracle(partial_name, sbd_only=False, sbd_owned=False):
    oracle_connection, oracle_cursor = connect_sas_bigdata()

    if sbd_only or sbd_owned:
        assert bool(sbd_only) != bool(sbd_owned), "sdb_only and sdb_owned can't be True at the same time"
        if sbd_only:
            table = 'all_tables'
        elif sbd_owned:
            table = 'user_tables'
    else:
        table = 'dba_tables'
    
    q = f'SELECT owner, table_name FROM {table}'
    oracle_cursor.execute(q) #.fetchall()
    return [(x[0], x[1]) for x in oracle_cursor if partial_name.lower() in x[1].lower()]

# def find_column_oracle(partial_name, sbd_only=False, sbd_owned=False):
#     oracle_connection, oracle_cursor = connect_sas_bigdata(['connection', 'cursor'])

#     if sbd_only or sbd_owned:
#         assert bool(sbd_only) != bool(sbd_owned), "sdb_only and sdb_owned can't be True at the same time"
#         if sbd_only:
#             table = 'all_tables'
#         elif sbd_owned:
#             table = 'user_tables'
#     else:
#         table = 'dba_tables'

#     q = f'select owner, table_name from {table}'
#     oracle_cursor.execute(q)
#     table_names = [f'{schema}.{table}' for schema, table in oracle_cursor]
#     return table_names
#     q = "SELECT column_name FROM user_tab_cols"
#     r = oracle_cursor.execute(q).fetchall()
#     for c in r:
#         print(c)
#     return

#     for table in table_names:
#         # cols = [x[0] for x in res2]
#         for col, _ in r:
#             if partial_name in col:
#                 print("{0}: {1}".format(table, e))



#
# SQLITE
#

def connect_sqlite(dbpath, **kwargs):

    # criando a lista de conexões a serem retornadas
    connection_type = get_connection_type(kwargs.get('connection_type', ['connection', 'cursor']))

    # construindo strings de conexão
    connection_string = dbpath
    engine_string = f'sqlite:///{dbpath}'

    # criando os objetos de conexão
    cnxn_objects = []
    for ct in connection_type:
        if ct == 'cc':
            connection = sqlite3.connect(connection_string)
            cursor = connection.cursor()
            cnxn_objects = [connection, cursor] # podemos fazer isso por a construção da lista de conexões garante 'cc' no primeiro índice
        elif ct == 'connection':
            cnxn_objects.append(sqlite3.connect(connection_string))
        elif ct == 'cursor':
            cnxn_objects.append(sqlite3.connect(connection_string).cursor())
        elif ct == 'engine':
            cnxn_objects.append(sqlalchemy.create_engine(engine_string))

    # retornando objetos de conexão
    if len(cnxn_objects) > 1:
        return tuple(cnxn for cnxn in cnxn_objects)
    else:
        return cnxn_objects[0]

#
# OPERATIONS
#

def create_table(table_name, cursor, **kwargs):
    """Cria tabela com nome table_name por meio do cursor"""

    # definindo cols_types, lista de str 'column_name column_type'
    cols_types = kwargs.get('cols_types')
    if not cols_types:
        cols = kwargs.get('cols', kwargs.get('columns'))
        types = kwargs.get('types')
        assert cols and types, 'Nomes de colunas (cols) e seus tipos (types) devem ser inseridos'
        assert len(cols) == len(types), 'cols e types devem ter o mesmo tamanho'
        cols_types = [' '.join(col_typ) for col_typ in zip(cols, types)]
    elif isinstance(cols_types, (list, tuple)):
        assert len(cols_types) > 0, 'Lista de colunas e tipos (cols_types) vazia'
        if isinstance(cols_types[0], (list, tuple)):
            assert len(cols_types[0]) == 2, f'Valor de cols_types[0] inválido: {cols_types[0]}'
            cols_types = [f'{col_typ[0]} {col_typ[1]}' for col_typ in cols_types]
        elif isinstance(cols_types[0], str):
            pass
        else:
            raise ValueError
    elif isinstance(cols_types, dict):
        cols_types = [f'{col} {typ}' for col, typ in cols_types.items()]

    # montando a query
    columns = ', '.join([ct.upper() for ct in cols_types])
    if_not_exists = kwargs.get('if_not_exists', False) # por padrão apenas cria sem checar se já existe ou não
    q = f'CREATE TABLE {"IF NOT EXISTS " if if_not_exists else ""}{table_name} ({columns})'
    
    # construíndo a tabela
    cursor.execute(q)

def drop_table(table_name, cursor):
    q = f'DROP TABLE {table_name}'
    cursor.execute(q)

def insert_rows(table_name, sql_connection, cols, rows, db='oracle'):
    try:
        db, connection_type = tuple(re.search(".*'(.+?)'", str(sql_connection)).group(1).split('.'))
    except AttributeError:
        try:
            db, connection_type = tuple( re.search("<(.+?) ", str(sql_connection)).group(1).split('.'))
        except:
            print(f'sql_connection inválido: {sql_connection}')
            raise

    if connection_type.lower() == 'connection':
        cursor = sql_connection.cursor()
    elif connection_type.lower() == 'cursor':
        cursor = sql_connection
    else:
        print(f'sql_connection inválido: {sql_connection}')
        raise TypeError

    columns = [c.upper() for c in cols]
    if db == 'oracle':
        q = f"""
        insert into {table_name} ({', '.join(columns)}) 
        values ({', '.join(f':{i}' for i in range(1, len(columns)+1))})
        """
        
    elif db in ('sqlite', 'sqlite3'):
        q = f"""
        insert into {table_name}  
        values ({', '.join('?' for i in range(1, len(columns)+1))})
        """
    
    else:
        print(f'db {db} não implementada')
        raise ValueError

    cursor.executemany(q, rows)
    try:
        sql_connection.commit()
    except:
        pass

