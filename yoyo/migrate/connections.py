
_schemes = {}

class BadConnectionURI(Exception):
    """
    An invalid connection URI
    """

def connection_for(scheme):
    """
    Register a connection function with a scheme. Each connection function must
    take standard connection arguments and return a dbapi connection object and
    the module used to connect.
    """
    def decorate(func):
        _schemes[scheme] = func
        return func
    return decorate

@connection_for('mysql')
def connect_mysql(username, password, host, port, database):
    import MySQLdb

    kwargs = {}
    if username is not None:
        kwargs['user'] = user
    if password is not None:
        kwargs['passwd'] = password
    if host is not None:
        kwargs['host'] = host
    if port is not None:
        kwargs['port'] = host
    kwargs['db'] = database

    return MySQLdb.connect(**kwargs)


@connection_for('postgres')
@connection_for('postgresql')
@connection_for('psql')
def connect_postgres(username, password, host, port, database):
    import psycopg2

    connargs = []
    if username is not None:
        connargs.append('user=%s' % username)
    if password is not None:
        connargs.append('password=%s' % password)
    if port is not None:
        connargs.append('port=%d' % port)
    if host is not None:
        connargs.append('host=%s' % host)
    connargs.append('dbname=%s' % database)
    return psycopg2.connect(' '.join(connargs)), psycopg2

def connect(uri):
    """
    Return ``(<dbapi connection>, <dbapi module>)`` for the given URI
    """
    scheme, username, password, host, port, database = parse_uri(uri)
    try:
        connection_func = _schemes[scheme.lower()]
    except KeyError:
        raise BadConnectionURI('Unrecognised database connection scheme %r' % scheme)
    return connection_func(username, password, host, port, database)

def parse_uri(uri):
    """
    Examples::

        >>> parse_uri('postgres://fred:bassett@dbserver:5432/fredsdatabase')
        ('postgres', 'fred', 'bassett', 'dbserver', 5432, 'fredsdatabase')
        >>> parse_uri('mysql:///jimsdatabase')
        ('mysql', None, None, None, None, 'jimsdatabase')
    """
    scheme = username = password = host = port = database = None

    try:
        scheme, uri = uri.split('://', 1)
    except ValueError:
        raise BadConnectionURI("No scheme specified in connection URI %r" % uri)

    try:
        netloc, uri = uri.split('/', 1)
        try:
            auth, netloc = netloc.split('@', 1)
            try:
                username, password = auth.split(':', 1)
            except ValueError:
                username = auth
        except ValueError:
            auth = ''

        if netloc:
            try:
                host, port = netloc.split(':')
                try:
                    port = int(port)
                except ValueError:
                    raise BadConnectionURI('Port %r is not numeric' % port)
            except ValueError:
                host = netloc

    except ValueError:
        pass

    database = uri
    if not database:
        return BadConnectionURI("No database name given in connection URI %r" % uri)

    return scheme, username, password, host, port, database
