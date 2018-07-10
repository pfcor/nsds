from . import helpers

import pkg_resources
import json
import re
import os
import cx_Oracle, sqlite3, sqlalchemy


# # # # # # # # # #
#                 #
#   DB FUNCTIONS  #
#                 #
# # # # # # # # # #



#
# CONFIG
#

def get_connection_info(connection_name=None, config_filename='connections.json', v=True):
    """
    Obtém dados de conexão dada por connection name guardada no arquivo config_filename.

    inputs:
    :: connection_name [str] -> nome da conexão
    :: config_filename [str] -> nome do arquivo com os dados de conexão

    output:
    :: [dict] com dados de conexão salvos no arquivo (e.g. "user", "password", "host" e "service")
    """
    try:
        with open(config_filename) as config_file:
            connection_info = json.loads(config_file.read())

        if connection_name:
            return connection_info[connection_name.upper()]
        else:
            return connection_info

    except FileNotFoundError:
        if v:
            print(f'Arquivo {config_filename} não encontrado. Verificar se está na mesma pasta do script')
        raise
    except KeyError:
        if v:
            print(f'Conexão {connection_name} não encontrada. Verificar se já foi salva utilizando')
        raise
    
    # resource_package = __name__
    # resource_path = '/'.join(('oracle_connection_config.json',))
    # print(pkg_resources.resource_string(resource_package, resource_path))
    # oracle_connection_info = json.loads(pkg_resources.resource_string(resource_package, resource_path).decode('utf-8'))


def save_connection_info(connection_name, **kwargs):
    """
    Salva informações de conexão com bancos de dados em um arquivo json

    inputs:
    :: user, password, host, service, schema, etc... [str] -> infos de conexão inseridas individualmente
    :: connection_string [str] -> se houver, será utilizada prioritariamente
    :: connection_name [str] -> nome da conexão 
    :: flavor [str] -> banco utilizado ('oracle', 'sqlite') 
    :: config_filename -> arquivo onde serão salvos os dados de conexão (default: connections.json)
    """
    config_filename = kwargs.pop('config_filename', 'connections.json')

    try:
        connection_info = get_connection_info(config_filename=config_filename, v=False)
    except:
        connection_info = {}
    finally:
        connection_info.update({connection_name.upper(): {k.lower(): v for k, v in kwargs.items()}})

    with open(config_filename, 'w') as fp:
        json.dump(connection_info, fp, indent=4)


def del_connection_info(connection_name, config_filename='connections.json'):
    """
    Remove informações de conexão do connection_name no arquivo config_filename

    inputs:
    :: connection_name [str] -> nome da conexão 
    :: config_filename -> arquivo onde serão salvos os dados de conexão (default: "connections.json") 
    """

    connection_info = get_connection_info(config_filename='connections.json', v=True)
    del connection_info[connection_name.upper()]
    with open(config_filename, 'w') as fp:
        json.dump(connection_info, fp, indent=4)


#
# CONEXÃO
#

def connect_oracle(connection_name=None, *connection_type, **kwargs):
    """
    Conexão com banco de dados Oracle

    inputs:
    :: connection_name [str] -> nome da conexão 
    :: *connection_type [str] -> define o tipo de conexão retornado | "connection" (default), "cursor", "engine", "all"
    :: config_filename -> arquivo onde serão salvos os dados de conexão (default: "connections.json")
    :: connection_info [dict] -> dicionario com campos "user", "password", "host" e "service"
    :: user, password, host, service [str] -> infos de conexão inseridas individualmente
    :: encoding [str] -> encoding a ser utilizado na conexão | default: "utf-8"

    output:
    :: objeto conector ou lista de objetos conectores definido por connection_type,
       no caso de uma lista de tipos de conexão, independente da ordem entrada, o retorno será 
       na ordem: connection > cursor > engine.
    """  

    # obtendo lista de conexões a serem retornadas
    connection_type = helpers.get_connection_type(connection_type, kwargs)

    # obtendo enconding
    encoding = kwargs.pop('encoding', 'utf-8')

    # obtendo dados de conexão
    connection_string = kwargs.pop('connection_string', None)
    engine_string = connection_string
    if not connection_string:
        if connection_name:
            connection_info = get_connection_info(connection_name, config_filename=kwargs.get('config_filename', 'connections.json'))
        elif not 'connection_info' in kwargs: # se o dict for colocado será utilizado, ou seja, deve estar completo
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
        return tuple(cnxn_objects)
    else:
        return cnxn_objects[0]


def connect_sas_bigdata(*connection_type, encoding='utf-8', config_filename='connections.json'):
    """
    Conexão com o 'schema' SAS_BIGDATA do time de DS no Oracle. Utiliza o arquivo 'connections.json',
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
    return connect_oracle('sas_bigdata', connection_type, encoding=encoding, config_filename='connections.json')


def connect_sqlite(connection_name=None, *connection_type, **kwargs):
    """
    Conexão com banco de dados SQLite

    inputs:
    :: connection_name [str] -> nome da conexão 
    :: *connection_type [str] -> define o tipo de conexão retornado | "connection" (default), "cursor", "engine", "all"
    :: config_filename -> arquivo onde serão salvos os dados de conexão (default: "connections.json")
    :: dbpath -> path para arquivo .db

    output:
    :: objeto conector ou lista de objetos conectores definido por connection_type.
       no caso de uma lista de tipos de conexão, independente da ordem entrada, o retorno será 
       na ordem: connection > cursor > engine.
    """  

    # obtendo lista de conexões a serem retornadas
    connection_type = helpers.get_connection_type(connection_type, kwargs)

    # construindo strings de conexão
    if connection_name:
        connection_info = get_connection_info(connection_name, config_filename=kwargs.get('config_filename', 'connections.json'))
        connection_string = connection_info['dbpath']
    else:
        connection_string = kwargs.pop('dbpath', None)
        assert connection_string, 'dbpath deve ser fornecido'

    if not connection_string[-3:] == '.db':
        connection_string += '.db'    
    engine_string = f'sqlite:///{connection_string}'

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
# CONSULTA
#

def find_table(sql_connector, partial_table_name=None, fetch='all', tables='dba'):

    cursor = helpers.get_cursor(sql_connector)
    db, _, _ = helpers.get_db_module_connectortype(sql_connector)
    db = db.lower()

    if not partial_table_name:
        partial_table_name = ''

    if db == 'oracle':
        q = f"""
        SELECT 
            {"owner, " if not tables=='user' else ""}table_name 
        FROM 
            {tables}_tables where table_name like \'%{partial_table_name.upper()}%\'
        """

    elif db == 'sqlite':
        q = f"""
        SELECT 
            name 
        FROM 
            sqlite_master 
        WHERE 
            type='table' AND name like '%{partial_table_name}%'"""

    else:
        raise NotImplementedError

    if fetch == 'all':
        matches = cursor.execute(q).fetchall()
    elif not fetch:
        matches = cursor.execute(q)
    elif isinstance(fetch, int):
        matches = cursor.execute(q).fetchmany(fetch)
    return matches


def table_exists(sql_connector, table_name, fetch='all', tables='dba', owner=None):
    
    cursor = helpers.get_cursor(sql_connector)
    db, _, _ = helpers.get_db_module_connectortype(sql_connector)
    db = db.lower()

    if '.' in table_name:
            owner, table_name = table_name.split('.')

    if db == 'oracle':
        q = f"""
        select 
            count(*)
        from 
            {tables}_objects
        where 
            object_type in ('TABLE', 'VIEW')
        and 
            object_name = '{table_name.upper()}'
            {f"and owner = '{owner.upper()}'" if owner else ''}
        """
    
    elif db == 'sqlite':
        q = f"""
        SELECT 
            count(*) 
        FROM 
            sqlite_master 
        WHERE 
            type='table' AND name = '{table_name}'
        """
    
    else:
        raise NotImplementedError

    return bool(cursor.execute(q).fetchone()[0])


def find_column(sql_connector, partial_column_name, partial_table_name=None, tables='dba', fetch='all'):

    cursor = helpers.get_cursor(sql_connector)
    db, _, _ = helpers.get_db_module_connectortype(sql_connector)
    db = db.lower()

    if db == 'oracle':
        q = f"""
        select 
            {"tabs.owner, " if not tables=='user' else ""}tabs.table_name, cols.column_name
        from 
            {tables}_tables tabs
        INNER JOIN
            {tables}_tab_cols cols
            ON tabs.table_name = cols.table_name
        WHERE 
            tabs.table_name LIKE '%{partial_table_name.upper() if partial_table_name else ''}%'
            AND cols.column_name LIKE '%{partial_column_name.upper()}%'
        """
    else:
        raise NotImplementedError

    if fetch == 'all':
        matches = cursor.execute(q).fetchall()
    elif not fetch:
        matches = cursor.execute(q)
    elif isinstance(fetch, int):
        matches = cursor.execute(q).fetchmany(fetch)
    return matches


#
# OPERATIONS
#

def create_table(table_name, sql_connector, **kwargs):
    """Cria tabela com nome table_name por meio do cursor"""

    # a função aceita qualquer tipo de conexão. aqui extrai-se o cursor necessário para criar-se a tabela
    cursor = helpers.get_cursor(sql_connector)
    assert cursor

    # montando a query
    columns = helpers.format_columns(**kwargs)
    if_not_exists = kwargs.get('if_not_exists', False) # por padrão apenas cria sem checar se já existe ou não
    q = f'CREATE TABLE {"IF NOT EXISTS " if if_not_exists else ""}{table_name} ({columns})'
    
    # construíndo a tabela
    cursor.execute(q)


def drop_table(table_name, sql_connector):

    # a função aceita qualquer tipo de conexão. aqui extrai-se o cursor necessário para dropar-se a tabela
    cursor = helpers.get_cursor(sql_connector)

    q = f'DROP TABLE {table_name}'
    cursor.execute(q)


def insert_rows(rows, cols, table_name, sql_connector, db='oracle'):
    """
    sql_connector: cx_Oracle/sqlite3.Connection, cx_Oracle/sqlite3.Cursor, Engine
    """

    try:
        db, module, connection = helpers.get_db_module_connectortype(sql_connector)
        cursor = helpers.get_cursor(sql_connector)
    except:
        print(f'sql_connector inválido: {sql_connector}')
        raise

    columns = [c.upper() for c in cols]
    if db == 'oracle':
        q = f"""
        insert into {table_name} ({', '.join(columns)}) 
        values ({', '.join(f':{i}' for i in range(1, len(columns)+1))})
        """
    elif db == 'sqlite':
        q = f"""
        insert into {table_name}  
        values ({', '.join('?' for i in range(1, len(columns)+1))})
        """
    else:
        print(f'db {db} não implementada')
        raise NotImplementedError

    cursor.executemany(q, rows)


def insert_df(df, table_name, sql_connector, if_exists='fail'):

    columns = [c.upper() for c in df.columns]
    types = [t.upper() for t in get_types_pd2oracle(df)]
    rows = df.values.tolist()

    if table_exists(table_name, sbd_owned=True):
        if if_exists == 'replace':
            drop_table(table_name, sql_connector)
            create_table(table_name, sql_connector, cols=columns, types=types)
        elif if_exists == 'fail':
            print('Tabela já existe')
            raise Exception
    else:
        create_table(table_name, sql_connector, cols=columns, types=types)
    
    insert_rows(table_name=table_name, sql_connector=sql_connector, cols=columns, rows=rows)
    sql_connector.commit()


def get_types_pd2oracle(df):

    types = []
    for t in df.dtypes:
        if 'int' in str(t):
            types.append('integer')
        elif 'float' in str(t):
            types.append('float')
        elif 'object' in str(t):
            types.append('varchar(40)')
        elif 'date' in str(t):
            types.append('date')
        else:
            types.append('varchar(40)')
    return types


if __name__ == '__main__':
    import pandas as pd
    o_con, o_cur, o_eng = connect_sas_bigdata('all')
    table_name = 'p_teste_nsds'

    d = pd.DataFrame({
        'nome': ['aaa', 'bbb', 'ccc'],
        'idade': [10, 20, 30]
    })

    insert_df(d, table_name, o_con, if_exists='append')
