# Copyright 2015 Oliver Cope
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import absolute_import

from functools import wraps

from . import exceptions

_schemes = {}

drivers = {
    'odbc': 'pyodbc',
    'postgresql': 'psycopg2',
    'postgres': 'psycopg2',
    'psql': 'psycopg2',
    'mysql': 'MySQLdb',
    'sqlite': 'sqlite3',
}


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

        @wraps(func)
        def with_driver(*args, **kwargs):
            driver = __import__(drivers[scheme], globals(), locals())
            exceptions.register(driver.DatabaseError)
            return func(driver, *args, **kwargs)
        _schemes[scheme] = with_driver

        return func

    return decorate


@connection_for('odbc')
def connect_odbc(driver, username, password, host, port, database, db_params):

    kwargs = db_params
    if username is not None:
        kwargs['UID'] = username
    if password is not None:
        kwargs['PWD'] = password
    if host is not None:
        kwargs['ServerName'] = host
    if port is not None:
        kwargs['Port'] = port
    if database is not None:
        kwargs['Database'] = database
    connection_string = ''
    for k, v in kwargs:
        connection_string += k + '=' + v + ';'
    return driver.connect(connection_string), driver.paramstyle


@connection_for('mysql')
def connect_mysql(driver, username, password, host, port, database, db_params):

    kwargs = db_params
    if username is not None:
        kwargs['user'] = username
    if password is not None:
        kwargs['passwd'] = password
    if host is not None:
        kwargs['host'] = host
    if port is not None:
        kwargs['port'] = port
    kwargs['db'] = database

    return driver.connect(**kwargs), driver.paramstyle


@connection_for('sqlite')
def connect_sqlite(
        driver, username, password, host, port, database, db_params):
    return driver.connect(database), driver.paramstyle


@connection_for('postgres')
@connection_for('postgresql')
@connection_for('psql')
def connect_postgres(
        driver, username, password, host, port, database, db_params):
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
    return driver.connect(' '.join(connargs)), driver.paramstyle


def connect(uri):
    """
    Connect to the given DB uri in the format
    ``driver://user:pass@host:port/database_name?param=value``,
    returning a DB-API connection
    object and the paramstyle used by the DB-API module.
    """

    scheme, username, password, host, port, database, params = parse_uri(uri)
    try:
        connection_func = _schemes[scheme.lower()]
    except KeyError:
        raise BadConnectionURI('Unrecognised database connection scheme %r' %
                               scheme)
    return connection_func(username, password, host, port, database, params)


def parse_uri(uri):
    """
    Examples::

        >>> parse_uri('postgres://fred:bassett@server:5432/fredsdatabase')
        ('postgres', 'fred', 'bassett', 'server', 5432, 'fredsdatabase', None)
        >>> parse_uri('mysql:///jimsdatabase')
        ('mysql', None, None, None, None, 'jimsdatabase', None, None)
        >>> parse_uri('odbc://user:password@server/database?DSN=dsn')
        ('odbc', 'user', 'password', 'server', None, 'database', {'DSN':'dsn'})
    """
    scheme = username = password = host = port = database = None

    try:
        scheme, uri = uri.split('://', 1)
    except ValueError:
        raise BadConnectionURI("No scheme specified in connection URI %r" %
                               uri)

    try:
        netloc, uri = uri.split('/', 1)
    except ValueError:
        netloc = ''

    try:
        auth, netloc = netloc.split('@', 1)
    except ValueError:
        auth = ''

    if auth:
        try:
            username, password = auth.split(':', 1)
        except ValueError:
            username = auth

    if netloc:
        try:
            host, port = netloc.split(':')
            try:
                port = int(port)
            except ValueError:
                raise BadConnectionURI('Port %r is not numeric' % port)
        except ValueError:
            host = netloc

    try:
        database, db_params = uri.split('?', 1)
        db_params_str = db_params.split('&')
        db_params = {}
        for arg in db_params_str:
            arg_name, arg_value = arg.split('=', 1)
            db_params[arg_name] = arg_value

    except ValueError:
        database = uri
        db_params = None

    return scheme, username, password, host, port, database, db_params


def unparse_uri(uri_tuple):
    """
    Examples::

        >>> unparse_uri(('postgres', 'fred', 'bassett', 'dbserver', 5432,
        ...              'fredsdatabase'))
        'postgres://fred:bassett@dbserver:5432/fredsdatabase'

        >>> unparse_uri(('postgres', 'pgsql', None, None, None, 'template1'))
        'postgres://pgsql@/template1'

        >>> unparse_uri(('mysql', 'jim', None, 'localhost', None,
        ...              'jimsdatabase'))
        'mysql://jim@localhost/jimsdatabase'
    """

    scheme, username, password, host, port, database, db_params = uri_tuple
    uri = scheme + "://"
    if username:
        uri += username
        if password:
            uri += ':' + str(password)
        uri += '@'
    if host:
        uri += host
    if port:
        uri += ':%s' % (port,)
    uri += '/'
    uri += database
    if db_params:
        uri += '?'
        db_params_str = []
        for k, v in db_params.items():
            db_params_str.append(k + '=' + v)
        uri += '&'.join(db_params_str)
    return uri
