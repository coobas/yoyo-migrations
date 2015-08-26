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

from collections import namedtuple
from functools import wraps
from importlib import import_module

from . import exceptions
from .compat import urlsplit, urlunsplit, parse_qsl, urlencode, quote, unquote

_schemes = {}

drivers = {
    'odbc': 'pyodbc',
    'postgresql': 'psycopg2',
    'postgres': 'psycopg2',
    'psql': 'psycopg2',
    'mysql': 'MySQLdb',
    'sqlite': 'sqlite3',
}


_DatabaseURI = namedtuple('_DatabaseURI',
                          'scheme username password hostname port database '
                          'args')


class DatabaseURI(_DatabaseURI):

    @property
    def netloc(self):
        hostname = self.hostname or ''
        if self.port:
            hostpart = '{}:{}'.format(hostname, self.port)
        else:
            hostpart = hostname

        if self.username:
            return '{}:{}@{}'.format(quote(self.username),
                                     quote(self.password),
                                     hostpart)
        else:
            return hostpart

    @property
    def uri(self):
        return urlunsplit((self.scheme,
                           self.netloc,
                           self.database,
                           urlencode(self.args),
                           ''))


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
            driver = import_module(drivers[scheme])
            exceptions.register(driver.DatabaseError)
            return func(driver, *args, **kwargs)
        _schemes[scheme] = with_driver

        return func

    return decorate


@connection_for('odbc')
def connect_odbc(driver, username, password, host, port, database, db_params):

    args = [('UID', username),
            ('PWD', password),
            ('ServerName', host),
            ('Port', port),
            ('Database', database)]
    args.extend(db_params.items())
    s = ';'.join('{}={}'.format(k, v) for k, v in args if v is not None)
    return driver.connect(s), driver.paramstyle


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


def parse_uri(s):
    """
    Examples::

        >>> parse_uri('postgres://fred:bassett@server:5432/fredsdatabase')
        ('postgres', 'fred', 'bassett', 'server', 5432, 'fredsdatabase', None)
        >>> parse_uri('mysql:///jimsdatabase')
        ('mysql', None, None, None, None, 'jimsdatabase', None, None)
        >>> parse_uri('odbc://user:password@server/database?DSN=dsn')
        ('odbc', 'user', 'password', 'server', None, 'database', {'DSN':'dsn'})
    """
    result = urlsplit(s)

    if not result.scheme:
        raise BadConnectionURI("No scheme specified in connection URI %r" % s)

    return DatabaseURI(scheme=result.scheme,
                       username=(unquote(result.username)
                                 if result.username is not None
                                 else None),
                       password=(unquote(result.password)
                                 if result.password is not None
                                 else None),
                       hostname=result.hostname,
                       port=result.port,
                       database=result.path.lstrip('/'),
                       args=dict(parse_qsl(result.query)))
