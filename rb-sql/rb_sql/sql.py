import datetime
import io
import logging
import random
import threading
import traceback
from contextlib import contextmanager
from warnings import filterwarnings

import dateutil.parser
import pytz
import sqlalchemy
import sqlalchemy.exc
from sqlalchemy import MetaData, Table, text

from .structures import dictobj
from .types import DICT_TYPE, LIST_TYPE, STRING_TYPE

logger = logging.getLogger(__name__)
filterwarnings("ignore", message="Invalid utf8mb4 character string")
filterwarnings("ignore", message="Duplicate entry")


_thread_locals = None
_sql_objects = {}


def chunked(iterator, chunksize):
    """
    Yields items from 'iterator' in chunks of size 'chunksize'.

    >>> list(chunked([1, 2, 3, 4, 5], chunksize=2))
    [(1, 2), (3, 4), (5,)]
    """
    chunk = []
    for idx, item in enumerate(iterator, 1):
        chunk.append(item)
        if idx % chunksize == 0:
            yield chunk
            chunk = []
    if chunk:
        yield chunk


def thread_id():
    t = threading.current_thread()
    return "{}/{}".format(t.name, t.ident % 100000)


class SQLBase(object):
    def __init__(
        self,
        write_url=None,
        read_urls=None,
        echo=False,
        echo_pool=False,
        isolation_level=None,
        pool_size=5,
        max_overflow=10,
        poolclass=None,
        pool_recycle=30 * 60,
        pool_pre_ping=False,
        flask_storage=False,
        writer_is_reader=True,
    ):
        global _thread_locals
        kwargs = {"logging_name": __name__, "echo": echo, "echo_pool": echo_pool}

        for (arg, val) in (
            ("pool_recycle", pool_recycle),
            ("pool_size", pool_size),
            ("max_overflow", max_overflow),
            ("isolation_level", isolation_level),
            ("poolclass", poolclass),
            ("pool_pre_ping", pool_pre_ping),
        ):
            if val is not None:
                kwargs[arg] = val

        read_urls = read_urls or []
        if not isinstance(read_urls, (tuple, list)):
            read_urls = [read_urls]

        self.writer_is_reader = writer_is_reader
        self.write_engine = None
        self.read_engines = None
        self.write_url = write_url
        self.read_urls = read_urls or []
        self.connection_kwargs = kwargs
        self.metadata = MetaData()

        if write_url:
            self.main_url = write_url
        else:
            self.main_url = read_urls[0]

        self.mysql = True
        self.flask_storage = flask_storage

        try:
            self.mysql = "mysql" in self.main_url
            self.postgres = "postgres" in self.main_url
            self.sqlite = "sqlite" in self.main_url
        except:
            logger.error("Exception determining database flavor, defaulting to mysql")

        logger.info("mysql=%r postgres=%r sqlite=%r", self.mysql, self.postgres, self.sqlite)

        if flask_storage:
            import flask

            _thread_locals = flask.g
        else:
            _thread_locals = threading.local()

    def get_engine(self, readonly=False):
        if readonly:
            if self.read_engines is None:
                read_urls = set(self.read_urls)
                if self.writer_is_reader and self.write_url:
                    read_urls.add(self.write_url)

                engines = []

                for c in read_urls:
                    engines.append(
                        sqlalchemy.create_engine(c, **self.connection_kwargs)
                    )
                self.read_engines = engines
                logger.warning(
                    "[sql] Created %r SQL read engines", len(self.read_engines)
                )

            return random.choice(self.read_engines)
        else:
            if self.write_engine is None:
                self.write_engine = sqlalchemy.create_engine(
                    self.write_url, **self.connection_kwargs
                )

            return self.write_engine

    def dispose(self):
        if self.read_engines is None and self.write_engine is None:
            logger.error("SQL dispose called, but no engine present")
            return False

        list(map(lambda e: e.dispose, self.read_engines or []))

        if self.write_engine:
            self.write_engine.dispose()

        return True

    def conn(self):
        ro = self.get_readonly()
        key = "conn-{}-{}".format(self.main_url, 1 if ro else 0)
        if not hasattr(_thread_locals, key):
            c = self.get_engine(readonly=ro).connect()
            setattr(_thread_locals, key, SQLConn(self, c))

        c = getattr(_thread_locals, key)
        return c

    # FIXME a little crude to copy/paste but let's get it working first
    def cleanup_conn(self, dump_log=False):
        logs = None

        for r in [0, 1]:
            key = "conn-{}-{}".format(self.main_url, r)
            if hasattr(_thread_locals, key):
                c = getattr(_thread_locals, key)
                c.cleanup()
                if dump_log:
                    ios = io.StringIO()
                    c.dump_log(ios)
                    logs = ios.getvalue()

                delattr(_thread_locals, key)

        return logs

    # transaction decorator
    def is_transaction(self, orig_fn):
        def new_fn(*args, **kwargs):
            with self.transaction():
                return orig_fn(*args, **kwargs)

        return new_fn

    def table(self, table_name):
        key = "{}-tables"
        if not hasattr(_thread_locals, key):
            setattr(_thread_locals, key, {})

        tables = getattr(_thread_locals, key)
        if table_name not in tables:
            try:
                tables[table_name] = Table(
                    table_name,
                    self.metadata,
                    autoload=True,
                    autoload_with=self.get_engine(),
                )
            except sqlalchemy.exc.NoSuchTableError:
                return None

        return tables[table_name]

    def table_exists(self, table_name):
        insp = sqlalchemy.inspect(self.get_engine())
        return insp.has_table(table_name)

    def ensure_table(self, table_name, query, drop_first=False, dry_run=False):
        do_create = False
        if self.table_exists(table_name):
            if drop_first:
                do_create = True
                self.execute("drop table {}".format(table_name))
        else:
            do_create = True

        if do_create:
            logger.warning("Creating table %r", table_name)
            self.execute(
                "create table {} ({})".format(table_name, query), dry_run=dry_run
            )
        else:
            logger.warning("Not creating table %r", table_name)

    @classmethod
    def in_clause(cls, in_list):
        return ",".join(["%s"] * len(in_list))

    @classmethod
    def dict_in_clause(cls, bindvars, in_list, prefix):
        vars = []
        for i, v in enumerate(in_list):
            var = f"{prefix}_{i}"
            vars.append(var)
            bindvars[var] = v

        return ",".join([f"%({x})s" for x in vars])

    def auto_where(self, asdict=False, **kwargs):
        asdict = asdict or not self.mysql
        where = []
        bindvars = {} if asdict or not self.mysql else []
        for k, v in kwargs.items():
            if v is not None:
                if asdict:
                    bindvars[k] = v
                    if self.mysql:
                        where.append("{}=%({})s".format(k, k))
                    else:
                        where.append("{}=:{}".format(k, k))
                else:
                    where.append("{}=%s".format(k))
                    bindvars.append(v)

        return where, bindvars

    @classmethod
    def where_clause(cls, where_list, join_with="and", prefix="where"):
        if where_list is None:
            where_list = []
        elif isinstance(where_list, STRING_TYPE):
            where_list = [where_list]

        join_with = " {} ".format(join_with.strip())
        return "{where_prefix}{where_clause}".format(
            where_prefix="{} ".format(prefix) if prefix and len(where_list) else "",
            where_clause=join_with.join(where_list) if where_list else "",
        )

    @classmethod
    def construct_where(cls, where_data, prefix="where"):
        def _sub(dd):
            out = []
            for k, v in dd.items():
                bindvars.append(v)
                out.append("{}=%s".format(k))

            return "({})".format(cls.where_clause(out, join_with="and", prefix=""))

        bindvars = []

        if not isinstance(where_data, LIST_TYPE):
            where_data = [where_data]

        return (
            bindvars,
            cls.where_clause(
                [_sub(x) for x in where_data], join_with="or", prefix=prefix
            ),
        )

    @classmethod
    def limit(cls, start=None, limit=None, page=None):
        if page is not None and limit is not None:
            start = (page - 1) * limit

        if start or limit:
            if start:
                return "limit {},{}".format(
                    max(0, start), max(1, limit or 1)
                )  # Is 1 a good default?
            else:
                return "limit {}".format(max(1, limit))
        else:
            return ""

    @classmethod
    def orderby(cls, sort_key, default=None):
        sort_key = sort_key or default
        if sort_key is None:
            return ""
        elif isinstance(sort_key, LIST_TYPE):
            return "order by {} {}".format(*sort_key) if sort_key else ""
        elif isinstance(sort_key, STRING_TYPE):
            keys = []
            for key in sort_key.split(","):
                order = "asc"
                if key[0] == "-":
                    order = "desc"
                    key = key[1:]
                keys.append("{} {}".format(key, order))

            return "order by {}".format(",".join(keys))
        else:
            logger.error("Invalid sort key specified: %r", sort_key)
            return ""

    @classmethod
    def process_date(cls, dtstr, default=None, strip_timezone=True):
        if not dtstr:
            dtstr = default

        if not dtstr:
            return dtstr

        if isinstance(dtstr, datetime.datetime):
            dt = dtstr
        else:
            dt = dateutil.parser.parse(dtstr) if dtstr else None

        if dt.tzinfo is None and not strip_timezone:
            dt = pytz.utc.localize(dt)
        elif dt.tzinfo is not None and strip_timezone:
            dt = dt.astimezone(pytz.utc).replace(tzinfo=None)

        return dt

    def result_count(self, with_count, results, count_query, bindvars=None):
        if with_count == "count_only":
            return self.select_one(count_query, bindvars).count

        if with_count:
            bindvars = bindvars or {}
            return {
                "results": results,
                "count": self.select_one(count_query, bindvars).count,
            }
        else:
            return results

    @contextmanager
    def as_readonly(self):
        prev_ro = self.get_readonly()
        try:
            self.set_readonly(True)
            yield 1
        finally:
            self.set_readonly(prev_ro)

    def get_readonly(self, default=False):
        if not self.write_url:
            return True

        key = "readonly-{}".format(self.main_url)
        return getattr(_thread_locals, key, default)

    def set_readonly(self, readonly=True):
        key = "readonly-{}".format(self.main_url)
        setattr(_thread_locals, key, readonly)

    # This is intended to proxy any unknown function automatically to the connection
    def __getattr__(self, item):
        conn = self.conn()

        def proxy(*args, **kwargs):
            return getattr(conn, item)(*args, **kwargs)

        return proxy


class SQLConn(object):
    def __init__(self, sql, conn):
        self.sql = sql
        self._transaction = []
        self._connection = conn
        self._log = []

    def __str__(self):
        return "SQLConn({})".format(hex(id(self._connection)))

    def log(self, msg, *args, **kwargs):
        logmsg = "[%s %s] %s" % (
            thread_id(),
            datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            msg.format(*args, **kwargs),
        )
        self._log.append(logmsg)

    def dump_log(self, fh):
        for l in self._log:
            fh.write(l)
            fh.write("\n")

    def panic(self, error):
        logger.error(
            "An error occurred during a SQL operation.  Error: %r -- Dumping logs: %r",
            error,
            self._log,
        )

    def cleanup(self):
        if self._transaction:
            self.log("Rolling back transaction")
            self.rollback_transaction()
            self.panic("SQL object has leftover transaction.  ROLLING BACK.")

        if self._connection:
            self.log("Closing connection")
            self._connection.close()
            self._connection = None

    def close(self):
        self.cleanup()

    def maybe_text(self, q):
        if self.sql.mysql:
            return q
        else:
            return text(q)

    @contextmanager
    def transaction(self):
        self.begin_transaction()
        try:
            yield 1

            self.commit_transaction()
        except Exception as e:
            self.rollback_transaction()
            raise e

    @contextmanager
    def cursor(self, options=None):
        if options:
            yield self._connection.execution_options(**options)
        else:
            yield self._connection

    def begin_transaction(self):
        if not self._connection:
            self.panic("Connection is none.  WHY??")
            raise Exception("Bailing")

        if self.sql.mysql and self._transaction:
            self._transaction.append(None)
            return

        self._transaction.append(self._connection.begin())

    def commit_transaction(self):
        trans = self._transaction.pop()
        if trans is not None:
            trans.commit()

    def rollback_transaction(self):
        while True:
            if not self._transaction:
                break
            trans = self._transaction.pop()
            if trans is not None:
                trans.rollback()

        self._transaction = []

    def select_column(self, statement, data=None):
        with self.cursor() as c:
            rs = c.execute(self.maybe_text(statement), data or {})

            for row in rs:
                yield row[0]

    def select_columns(self, statement, data=None):
        cols = {}
        with self.cursor() as c:
            rs = c.execute(self.maybe_text(statement), data or {})

            cols = {k: [] for k in rs.keys()}

            for row in rs:
                list(map(lambda x: cols[x[0]].append(x[1]), row.items()))

        return cols

    def select_foreach(
        self, statement, data=None, stream=False, log=False, as_dictobj=True
    ):
        options = {}
        if stream:
            options["stream_results"] = True

        if log:
            logger.warning(
                "Executing (ro=%r) query=%r, data=%r",
                self.sql.get_readonly(),
                statement,
                data,
            )

        with self.cursor() as c:
            rs = c.execute(self.maybe_text(statement), data or {})

            for row in rs:
                yield dictobj(row.items()) if as_dictobj else row

    def select_lists(
        self,
        statement,
        data=None,
        stream=False,
        log=False,
        columns=None,
    ):
        options = {}
        if stream:
            options["stream_results"] = True

        if log:
            logger.warning(
                "Executing (ro=%r) query=%r, data=%r",
                self.sql.get_readonly(),
                statement,
                data,
            )

        with self.cursor() as c:
            rs = c.execute(self.maybe_text(statement), data or {})

            for row in rs:
                yield [row[c] for c in columns]

    def select_one(self, statement, data=None, as_dictobj=True):
        with self.cursor() as c:
            rs = c.execute(self.maybe_text(statement), data or {})
            print("------", rs.rowcount)
            row = rs.fetchone()
            rs.close()
            if rs.rowcount != 1:
                # FIXME make custom exception
                raise Exception(
                    "Query was expected to return 1 row, returned %r (query=%r, data=%r)"
                    % (rs.rowcount, statement, data)
                )
            return dictobj(row.items()) if as_dictobj else row

    def select_0or1(self, statement, data=None):
        with self.cursor() as c:
            rs = c.execute(self.maybe_text(statement), data or {})
            if rs.rowcount <= 0:
                return None
            elif rs.rowcount == 1:
                return dictobj(rs.fetchone().items())
            else:
                # FIXME make custom exception
                raise Exception(
                    "Query was expected to return 0 or 1 rows, returned %r (query=%r, data=%r)"
                    % (rs.rowcount, statement, data)
                )

    def insert(
        self,
        table_name,
        data,
        ignore_duplicates=False,
        batch_size=200,
        on_duplicate=None,
        returning=True,
    ):
        def _ignore_pre():
            if self.sql.mysql:
                return " ignore"
            elif self.sql.sqlite:
                return "or ignore"

            return ""

        def _ignore_post():
            if self.sql.postgres:
                return "on conflict do nothing"

            return ""

        if not data:
            return 0

        sample = data if isinstance(data, (dict, dictobj)) else data[0]
        columns = sorted(sample.keys())
        if self.sql.mysql:
            values = ["%({})s".format(c) for c in columns]
        else:
            values = [":{}".format(c) for c in columns]

        query = "insert{ignore_pre} into {table}({columns}) values ({values}) {on_duplicate} {ignore_post} {returning}".format(
            ignore_pre=_ignore_pre() if ignore_duplicates else "",
            ignore_post=_ignore_post() if ignore_duplicates else "",
            table=table_name,
            columns=", ".join(columns),
            values=", ".join(values),
            returning="returning *" if self.sql.postgres and returning else "",
            on_duplicate=on_duplicate or "",
        )

        with self.cursor() as c:
            if isinstance(data, DICT_TYPE):
                rs = c.execute(self.maybe_text(query), data)
                if self.sql.postgres:
                    return dictobj(rs.fetchone().items())
                else:
                    return rs.lastrowid  # mysql only??
            else:
                count = 0
                for chunk in chunked(data, batch_size):
                    count += c.execute(self.maybe_text(query), chunk).rowcount

                return count

    def update(self, table_name, where, data=None, where_data=None):
        if self.sql.mysql:
            bindvars = [data[k] for k in sorted(data.keys())] + (where_data or [])
            set_vals = ', '.join([u'{}=%s'.format(k) for k in sorted(data.keys())])
        else:
            bindvars = {}
            bindvars.update(data)
            bindvars.update(where_data or {})
            set_vals = ', '.join([u'{}=:{}'.format(k, k) for k in sorted(data.keys())])

        query = "update {table} set {sets} {where}".format(
            table=table_name,
            sets=set_vals,
            where=self.sql.where_clause(where),
        )

        with self.cursor() as c:
            rs = c.execute(self.maybe_text(query), bindvars)
            return rs.rowcount

    def update_multiple(
        self, table_name, where, data=None, where_columns=None, batch_size=200
    ):
        sample = data if isinstance(data, DICT_TYPE) else data[0]
        columns = sorted([k for k in sample.keys() if k not in (where_columns or [])])

        query = "update {table} set {sets} {where}".format(
            table=table_name,
            sets=", ".join(["{}=%({})s".format(k, k) for k in columns]),
            where=self.sql.where_clause(where),
        )

        with self.cursor() as c:
            count = 0
            for chunk in chunked(data, batch_size):
                count += c.execute(self.maybe_text(query), chunk).rowcount

            return count

    def delete(self, table_name, where=None, data=None):
        query = "delete from {table} {where}".format(
            table=table_name, where=self.sql.where_clause(where)
        )

        with self.cursor() as c:
            rs = c.execute(self.maybe_text(query), data or [])
            return rs.rowcount

    def execute(self, query, data=None, dry_run=False, log=False):
        if dry_run or log:
            logger.warning("SQL Run: {}".format(query))

        if dry_run:
            return

        with self.cursor() as c:
            rs = c.execute(self.maybe_text(query), data or [])

        return rs.rowcount


class MigrationStatement(object):
    def __init__(self, statement=None, message=None, ignore_error=False):
        self.statement = statement
        self.message = message
        self.ignore_error = ignore_error

    @classmethod
    def log(self, logs, msg, *args):
        formatted = msg % args
        if logs:
            logs.append(formatted)
        logger.warning(formatted)

    def execute(self, SQL, dry_run=False, logs=None):
        if self.message:
            self.log(logs, "%s", self.message)

        try:
            self.log(logs, "SQL Execute: %s", self.statement)
            SQL.execute(self.statement, dry_run=dry_run, log=False)
        except Exception as e:
            self.log(logs, "Error while running statment: %r", traceback.format_exc())
            if not self.ignore_error:
                raise e


class Migration(object):
    registry = {}

    def __init__(self, version, message):
        self.registry[version] = self
        self.statements = []
        self.message = message
        self.logs = []

    @classmethod
    def log(cls, logs, msg, *args):
        formatted = msg % args
        logs.append(formatted)
        logger.warning(formatted)

    @classmethod
    def migrate(cls, SQL, dry_run=False, initial=False, apply_versions=None):
        logs = []
        apply_versions = apply_versions or []

        if SQL.sqlite:
            SQL.ensure_table(
                "migrations",
                """
                    migration_id integer not null primary key,
                    migration_datetime datetime,
                    version_pre int,
                    version_post int
                """,
            )
        elif SQL.postgres:
            SQL.ensure_table(
                "migrations",
                """
                    migration_id serial primary key,
                    migration_datetime timestamp,
                    version_pre int,
                    version_post int
                """,
            )
        elif SQL.mysql:
            SQL.ensure_table(
                "migrations",
                """
                    migration_id integer not null primary key auto_increment,
                    migration_datetime datetime,
                    version_pre int,
                    version_post int
                """,
            )
        else:
            raise Exception("Unknown database")

        res = SQL.select_0or1("select max(version_post) as version from migrations")
        version = res.version if (res and res.version is not None and not initial) else -1
        todo = sorted([x for x in cls.registry.keys() if x > version] + apply_versions)
        cls.log(logs, "Version = %d, todo = %r, initial=%r", version, todo, initial)

        version_pre = version_post = version
        for v in todo:
            cls.log(logs, "Running migration %d: %s", v, cls.registry[v].message)

            for statement in cls.registry[v].statements:
                statement.execute(SQL=SQL, dry_run=dry_run, logs=logs)

            if v > version_pre:
                version_post = v

        if len(todo) and not dry_run:
            SQL.insert(
                "migrations",
                {
                    "migration_datetime": datetime.datetime.utcnow(),
                    "version_pre": version,
                    "version_post": version_post,
                },
            )

        return logs

    def add_statement(self, statement, ignore_error=False, message=None):
        self.statements.append(
            MigrationStatement(statement, ignore_error=ignore_error, message=message)
        )


def sql_factory(
    sql_key=None,
    password=None,
    database=None,
    write_url=None,
    read_urls=None,
    isolation_level="READ COMMITTED",
    pool_recycle=60 * 60 * 2,
    flask_storage=True,
    writer_is_reader=False,
    pool_size=5,
    max_overflow=10,
):
    sql_key = sql_key or database
    if sql_key not in _sql_objects:
        write_url = write_url
        read_urls = read_urls
        logger.warning(
            "Creating SQL object, database=%r, sql_key=%r, write_url=%r, read_urls=%r",
            database,
            sql_key,
            write_url,
            read_urls,
        )
        _sql_objects[sql_key] = SQLBase(
            write_url=write_url.format(password=password, database=database)
            if write_url
            else None,
            read_urls=[
                x.format(password=password, database=database) for x in read_urls
            ],
            isolation_level=isolation_level,
            echo_pool=True,
            pool_recycle=pool_recycle,
            flask_storage=flask_storage,
            writer_is_reader=writer_is_reader,
            pool_size=pool_size,
            max_overflow=max_overflow,
        )

    return _sql_objects[sql_key]
