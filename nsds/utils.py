import pkg_resources
import json


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
    connection_type = kwargs.get('connection_type', 'connection')
    connection_type = get_connection_type(connection_type)

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


def connect_sas_bigdata(connection_type='connection', encoding='utf-8'):
    """
    atalho para conectar ao 'schema' do time de ciência de dados no Oracle sem a necessidade
    de inserir dados da conta, utilizando o arquivo 'oracle_connection_config.json'
    """
    resource_package = __name__
    resource_path = '/'.join(('oracle_connection_config.json',))
    connection_info = json.loads(pkg_resources.resource_string(resource_package, resource_path).decode('utf-8'))
    return connect_oracle(connection_info=connection_info, connection_type=connection_type, encoding=encoding)


#
# SQLITE
#

def connect_sqlite(dbpath, **kwargs):

    # criando a lista de conexões a serem retornadas
    connection_type = kwargs.get('connection_type', 'connection')
    connection_type = get_connection_type(connection_type)

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