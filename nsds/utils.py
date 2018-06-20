import pkg_resources
import json
import re
import os


# # # # # # # # # #
#                 #
#   DB FUNCTIONS  #
#                 #
# # # # # # # # # #


import cx_Oracle, sqlite3, sqlalchemy

#
# AUX
#

def format_connection_type(connection_type):
    """
    Formatar entrada de tipo de conexão para uso nas funções de conexão a bancos de dados.
    
    inputs:
    :: connection_type [str ou list/tuple] -> tipo(s) de conexão a ser(em) retornado(s) | ["connection" (default), "cursor", "engine", "all"]

    output:
    :: [list] -> conexões formatadas para funções de conexão
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

def get_db_module_connectortype(sql_connector):
    """
    A partir do objeto conector a banco de dados inserido, retorna-se o banco e o tipo de conexão

    inputs:
    :: connector [connector object] -> conector a um banco de dados

    output:
    :: [tuple de str] com (db, tipo de conexão) 
    """

    connector_class = re.search(r"'(.+?)'", str(type(sql_connector))).group(1).split('.')
    module, connector_type = connector_class[0].lower(), connector_class[-1].lower()
    if 'oracle' in module:
        db = 'oracle'
    elif 'sqlite' in module:
        db = 'sqlite'
    else:
        try:
            db = re.search(r"(.+?)://", str(sql_connector)).group(1).split('(')[1]
        except AttributeError:
            print(f'Conector inválido: {sql_connector}')
            raise
    return db, module, connector_type

    try:
        a = re.search(r"^<(.+?) ", str(sql_connector)).group(1).split('.')[0]
    except AttributeError:
        try:
            a = re.search(r"(.+?)://", str(sql_connector)).group(1).split('(')[1]
        except AttributeError:
            print(f'Conector inválido: {sql_connector}')
            raise

def get_cursor(sql_connector):

    module, connector_type = get_db_connectiontype(sql_connector)
    module, connector_type = module.lower(), connector_type.lower() 
    
    implemented = ('cx_oracle', 'sqlite3', 'sqlalchemy')
    if module not in implemented:
        raise NotImplementedError
    if module == 'sqlalchemy':
        if connector_type == 'engine':
            cursor = sql_connector.connect()
        elif connector_type == 'connection':
            cursor = sql_connector
    else:
        if connector_type == 'connection':
            cursor = sql_connector.cursor()
        elif connector_type == 'cursor':
            cursor = sql_connector
    return cursor

def format_columns(**kwargs):

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
    else:
        raise ValueError

    return ', '.join([ct.upper() for ct in cols_types])


#
# CONFIG
#

def save_oracle_connection_info(**kwargs):
    
    if not 'connection_info' in kwargs: # se o dict for colocado será utilizado, ou seja, deve estar completo
        user = kwargs.get('schema')
        user = kwargs.get('user')
        password = kwargs.get('password')
        host = kwargs.get('host')
        service = kwargs.get('service')

        assert user, "User missing"
        assert password, "Password missing"
        assert host, "Host missing"
        assert service, "Service missing"

        new_connection_info = {'user': user, 'password': password, 'host': host, 'service': service}
    
    else:
        new_connection_info = kwargs.get('connection_info')

    try:
        connection_info = get_oracle_connection_info(v=False)
    except:
        connection_info = {}

    connection_info.update({user.upper(): new_connection_info})

    filename = kwargs.get('filename', 'oracle_connection_config.json')
    with open(filename, 'w') as fp:
        json.dump(connection_info, fp, indent=4)

def del_oracle_connection_info(schema, filename='oracle_connection_config.json'):
    connection_info = get_oracle_connection_info()
    del connection_info[schema.upper()]
    with open(filename, 'w') as fp:
        json.dump(connection_info, fp, indent=4)

def get_oracle_connection_info(schema=None, filename='oracle_connection_config.json', v=True):
    """
    Obtém dados de conexão para o schema no Oracle.

    output:
    :: [dict] com dados de conexão "user", "password", "host" e "service"
    """
    try:
        with open(filename) as config_file:
            oracle_connection_info = json.loads(config_file.read())
    except FileNotFoundError:
        if v:
            print(f'Arquivo {filename} não encontrado. Verificar se está na mesma pasta do script')
        raise
    
    # resource_package = __name__
    # resource_path = '/'.join(('oracle_connection_config.json',))
    # print(pkg_resources.resource_string(resource_package, resource_path))
    # oracle_connection_info = json.loads(pkg_resources.resource_string(resource_package, resource_path).decode('utf-8'))
    
    if schema:
        return oracle_connection_info[schema.upper()]
    else:
        return oracle_connection_info


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
    connection_type = format_connection_type(kwargs.get('connection_type', ['connection', 'cursor']))

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
    Conexão com o 'schema' SAS_BIGDATA do time de DS no Oracle. Utiliza o arquivo 'oracle_connection_config.json',
    que deve estar na mesma pasta deste script.

    inputs:
    :: connection_type [str ou list] -> define o tipo de conexão retornado | ["connection" (default), "cursor", "engine", "all"]
    :: encoding [str] -> encoding a ser utilizado na conexão | default: "utf-8"

    output:
    :: objeto conector ou lista de objetos conectores ao SAS_BIGADATA no Oracle definido por connection_type,
       no caso de uma lista de tipos de conexão, independente da ordem entrada, o retorno será 
       na ordem: connection > cursor > engine. se connection e cursor forem inseridos, o cursor
       será obtido da connection e não de um objeto criado em separado. 
    """
    connection_info = get_oracle_connection_info('sas_bigdata')
    return connect_oracle(connection_info=connection_info, connection_type=connection_type, encoding=encoding)

def find_table_oracle(partial_table_name=None, sbd_only=False, sbd_owned=False):
    oracle_connection, oracle_cursor = connect_sas_bigdata(['connection', 'cursor'])

    if sbd_only or sbd_owned:
        assert bool(sbd_only) != bool(sbd_owned), "sdb_only and sdb_owned can't be True at the same time"
        if sbd_only:
            t = 'all'
        elif sbd_owned:
            t = 'user'
    else:
        t = 'dba'

    if not partial_table_name:
        partial_table_name = ''
    q = f"""SELECT {"owner, " if not sbd_owned else "'SAS_BIGDATA', "} table_name FROM {t}_tables where table_name like \'%{partial_table_name.upper()}%\'"""
    
    matches = oracle_cursor.execute(q).fetchall()
    # matches = [(x[0], x[1]) for x in oracle_cursor]
    oracle_connection.close()
    return matches

def find_column_oracle(partial_column_name, partial_table_name=None, sbd_only=False, sbd_owned=False):
    oracle_connection, oracle_cursor = connect_sas_bigdata(['connection', 'cursor'])

    if sbd_only or sbd_owned:
        assert bool(sbd_only) != bool(sbd_owned), "sdb_only and sdb_owned can't be True at the same time"
        if sbd_only:
            t = 'all'
        elif sbd_owned:
            t = 'user'
    else:
        t = 'dba'

    q = f"""
    select 
        {"tabs.owner, " if not sbd_owned else "'SAS_BIGDATA', "}tabs.table_name, cols.column_name
    from 
        {t}_tables tabs
    INNER JOIN
        {t}_tab_cols cols
        ON tabs.table_name = cols.table_name
    WHERE 
        tabs.table_name LIKE '%{partial_table_name.upper() if partial_table_name else ''}%'
        AND cols.column_name LIKE '%{partial_column_name.upper()}%'
    """

    oracle_cursor.execute(q)
    matches = oracle_cursor.execute(q).fetchall()
    oracle_connection.close()
    return matches



#
# SQLITE
#

def connect_sqlite(dbpath, **kwargs):

    # criando a lista de conexões a serem retornadas
    connection_type = format_connection_type(kwargs.get('connection_type', ['connection', 'cursor']))

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

def create_table(table_name, sql_connector, **kwargs):
    """Cria tabela com nome table_name por meio do cursor"""

    # a função aceita qualquer tipo de conexão. aqui extrai-se o cursor necessário para criar-se a tabela
    cursor = get_cursor(sql_connector)
    assert cursor

    # montando a query
    columns = format_columns(**kwargs)
    if_not_exists = kwargs.get('if_not_exists', False) # por padrão apenas cria sem checar se já existe ou não
    q = f'CREATE TABLE {"IF NOT EXISTS " if if_not_exists else ""}{table_name} ({columns})'
    
    # construíndo a tabela
    cursor.execute(q)

def drop_table(table_name, sql_connector):

    # a função aceita qualquer tipo de conexão. aqui extrai-se o cursor necessário para dropar-se a tabela
    cursor = get_cursor(sql_connector)
    assert cursor

    q = f'DROP TABLE {table_name}'
    cursor.execute(q)

def insert_rows(table_name, sql_connector, cols, rows, db='oracle'):
    """
    sql_connector: cx_Oracle/sqlite3.Connection, cx_Oracle/sqlite3.Cursor, Engine
    """

    try:
        db, connection_type = tuple(re.search(".*'(.+?)'", str(sql_connection)).group(1).split('.'))
    except AttributeError:
        try:
            db, connection_type = tuple(re.search("<(.+?) ", str(sql_connection)).group(1).split('.'))
        except:
            try:
                connection_type, db = tuple(re.search("(.+?)://", str(sql_connection)).group(1).split('('))
            except:
                print(f'sql_connection inválido: {sql_connection}')
                raise

    print(db, connection_type)
    return
    if connection_type.lower() == 'connection':
        cursor = sql_connection.cursor()
    elif connection_type.lower() == 'cursor':
        cursor = sql_connection
    else:
        print(f'sql_connection inválido: {sql_connection}')
        raise TypeError

    columns = [c.upper() for c in cols]
    if db.lower() in ('cx_oracle'):
        q = f"""
        insert into {table_name} ({', '.join(columns)}) 
        values ({', '.join(f':{i}' for i in range(1, len(columns)+1))})
        """
    elif db.lower() in ('sqlite', 'sqlite3'):
        q = f"""
        insert into {table_name}  
        values ({', '.join('?' for i in range(1, len(columns)+1))})
        """
    else:
        print(f'db {db} não implementada')
        raise ValueError

    cursor.executemany(q, rows)
    # try:
    #     sql_connection.commit()
    # except:
    #     pass

