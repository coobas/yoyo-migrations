Yoyo database migrations
========================

Yoyo is a database schema migration tool using plain SQL and python's builtin
DB-API.

.. image:: https://drone.io/bitbucket.org/ollyc/yoyo/status.png


What does yoyo-migrations do?
-----------------------------

As database applications evolve, changes to the database schema are often
required. These can usually be written as one-off SQL scripts containing
CREATE/ALTER table statements (although any SQL or python script may be used
with yoyo).

Yoyo provides a command line tool for reading a directory of such
scripts and applying them to your database as required.

Installation
------------

Install from the PyPI with the command::

  pip install yoyo-migrations

Database support
----------------

PostgreSQL, MySQL and SQLite databases are supported.
An ODBC backend is also available, but is unsupported (patches welcome!)


Usage
-----

Yoyo is usually invoked as a command line script.

Start a new migration::

  yoyo new ./migrations -m "Add column to foo"


Apply migrations from directory ``migrations`` to a PostgreSQL database::

   yoyo apply ./migrations postgresql://scott:password@localhost/db

Rollback migrations previously applied to a MySQL database::

   yoyo rollback ./migrations mysql://user:password@localhost/database

Reapply (ie rollback then apply again) migrations to a SQLite database at
location ``/home/sheila/important-data.db``::

    yoyo reapply ./migrations sqlite:////home/sheila/important-data.db

By default, yoyo-migrations starts in an interactive mode, prompting you for
each migration file before applying it, making it easy to preview which
migrations to apply and rollback.

The migrations directory should contain a series of migration scripts. Each
migration script is a python file (``.py``) containing a series of steps. Each
step should comprise a migration query and (optionally) a rollback query. For
example::

    #
    # file: migrations/0001.create-foo.py
    #
    from yoyo import step
    step(
        "CREATE TABLE foo (id INT, bar VARCHAR(20), PRIMARY KEY (id))",
        "DROP TABLE foo",
    )

Migrations may also declare dependencies on previous migrations via the
``__depends__`` attribute::

    #
    # file: migrations/0002.modify-foo.py
    #
    __depends__ = ['0001.create-foo']

    step(
        "CREATE TABLE foo (id INT, bar VARCHAR(20), PRIMARY KEY (id))",
        "DROP TABLE foo",
    )


The filename of each file (without the .py extension) is used as the identifier
for each migration. In the absence of a ``__depends__`` attribute, migrations
are applied in filename order, so it's useful to
name your files using a date (eg '20090115-xyz.py') or some other incrementing
number.

yoyo creates a table in your target database, ``_yoyo_migration``, to
track which migrations have been applied.

Steps may also take an optional argument ``ignore_errors``, which must be one
of ``apply``, ``rollback``, or ``all``. If in the previous example the table
foo might have already been created by another means, we could add
``ignore_errors='apply'`` to the step to allow the migrations to continue
regardless::

    #
    # file: migrations/0001.create-foo.py
    #
    from yoyo import step
    step(
        "CREATE TABLE foo (id INT, bar VARCHAR(20), PRIMARY KEY (id))",
        "DROP TABLE foo",
        ignore_errors='apply',
    )

Steps can also be python callable objects that take a database connection as
their single argument. For example::

    #
    # file: migrations/0002.update-keys.py
    #
    from yoyo import step
    def do_step(conn):
        cursor = conn.cursor()
        cursor.execute(
            "INSERT INTO sysinfo "
            " (osname, hostname, release, version, arch)"
            " VALUES (%s, %s, %s, %s, %s %s)",
            os.uname()
        )

    step(do_step)

Configuration file
------------------

Yoyo looks for a configuration file named ``yoyo.ini`` in the current working
directory or any ancestor directory. This can contain the following
options::

  [DEFAULT]

  # List of migration source directories. "%(here)s" is expanded to the
  # full path of the directory containing this ini file.
  sources = %(here)s/migrations %(here)s/lib/module/migrations

  # Target database
  database = postgresql://scott:tiger@localhost/mydb

  # Verbosity level. Goes from 0 (least verbose) to 3 (most verbose)
  verbosity = 3

  # Disable interactive features
  batch_mode = on

  # Editor to use when starting new migrations
  # "{}" is expanded to the filename of the new migration
  editor = /usr/local/bin/vim -f {}

  # An arbitrary command to run after a migration has been created
  # "{}" is expanded to the filename of the new migration
  post_create_command = hg add {}


Config file inheritance may be used to customize configuration per site::

  #
  # file: yoyo-defaults.ini
  #
  [DEFAULT]
  sources = %(here)s/migrations


  #
  # file: yoyo.ini
  #
  [DEFAULT]
  %inherit %(here)s/yoyo-defaults.ini
  database = sqlite:///%(here)s/mydb.sqlite



Transactions
------------

Each migration is run in a separate transaction and savepoints are used
to isolate steps within each migration.

If an error occurs during a step and the step has ``ignore_errors`` set,
then that individual step will be rolled back and
execution will pick up from the next step.
If ``ignore_errors`` is not set then the entire migration will be rolled back
and execution stopped.

Note that some databases (eg MySQL) do not support rollback on DDL statements
(eg ``CREATE ...`` and ``ALTER ...`` statements). For these databases
you may need to manually intervene to reset the database state
should errors occur during your migration.

Using ``group`` allows you to nest steps, giving you control of where
rollbacks happen. For example::

    group([
      step("ALTER TABLE employees ADD tax_code TEXT"),
      step("CREATE INDEX tax_code_idx ON employees (tax_code)")
    ], ignore_errors='all')
    step("UPDATE employees SET tax_code='C' WHERE pay_grade < 4")
    step("UPDATE employees SET tax_code='B' WHERE pay_grade >= 6")
    step("UPDATE employees SET tax_code='A' WHERE pay_grade >= 8")


Post-apply hook
---------------

It can be useful to have a script that's run after successful migrations. For
example you could use this to update database permissions or re-create views.
To do this, create a migration file called ``post-apply.py``. This file should
have the same format as any other migration file.

Password security
-----------------

You normally specify your database username and password as part of the
database connection string on the command line. On a multi-user machine, other
users could view your database password in the process list.

The ``-p`` or ``--prompt-password`` flag causes yoyo to prompt
for a password, ignoring any password specified in the connection string. This
password will not be available to other users via the system's process list.

Configuration file
------------------

Yoyo looks for a configuration file called ``yoyo.ini``, in
the current working directory or any ancestor directory.

If no configuration file is found ``yoyo`` will prompt you to
create one, popuplated with the current command line args.

Using a configuration file saves typing,
avoids your database username and password showing in
process listings and lessens the risk of accidentally running ``yoyo``
on the wrong database (ie by re-running an earlier ``yoyo`` entry in
your command history when you have moved to a different directory).

If you do not want this config file to be used, add the ``--no-config``
parameter to the command line options.

Connections
-----------

Database connections are specified using a URI. Examples:

SQLite
~~~~~~

::

  # Use 4 slashes for an absolute database path on unix like platforms
  database = sqlite:////home/user/mydb.sqlite

  # Absolute path on Windows.
  database = sqlite:///c:\home\user\mydb.sqlite

  # Use 3 slashes for a relative path
  database = sqlite:///mydb.sqlite


MySQL
~~~~~

::

  # Network database connection
  database = mysql://scott:tiger@localhost/mydatabase

  # Connect via a unix socket
  database = mysql://scott:tiger@/mydatabase?unix_socket=/tmp/mysql.sock


PostgreSQL
~~~~~~~~~~

::

  # Network database connection
  database = postgresql://scott:tiger@localhost/mydatabase

  # Omit the host to use a socket connection
  database = postgresql://scott:tiger@/mydatabase


Using yoyo from python code
---------------------------

The following example shows how to apply migrations from inside python code::

    from yoyo import read_migrations, get_backend

    backend = get_backend('postgres://myuser@localhost/mydatabase')
    migrations = read_migrations('path/to/migrations')
    backend.apply_migrations(migrations)

.. :vim:sw=4:et
