import re
import cx_Oracle, sqlite3, sqlalchemy


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


def get_connection_type(connection_type, kwargs):

    if 'connection_type' in kwargs:
        connection_type = kwargs.pop('connection_type')
    elif not connection_type:
        connection_type = 'connection'
    elif len(connection_type) == 1:
        connection_type = connection_type[0]
    connection_type = format_connection_type(connection_type)
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

    db, module, connector_type = get_db_module_connectortype(sql_connector)
    db, module, connector_type = db.lower(), module.lower(), connector_type.lower() 

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
