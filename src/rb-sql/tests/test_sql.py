import copy
import datetime
import io
import logging
import unittest

import pytz
from rb_sql import Migration, SQLBase, chunked, dictobj, sql_factory

SQL = sql_factory(
    write_url="sqlite+pysqlite:///./test.sql",
    read_urls=[
       "sqlite+pysqlite:///./test.sql",
    ],
    password="1wombat2",
    database="test",
    flask_storage=False,
    isolation_level="SERIALIZABLE",
)
# SQL.mysql = mysql
logger = logging.getLogger(__name__)


class TestException(Exception):
    pass


class TestSQLBase(unittest.TestCase):
    def setUp(self) -> None:
        SQL.write_engine = None
        SQL.read_engines = None

    def test_get_engine(self):
        a = SQL.get_engine(readonly=False)
        b = SQL.get_engine(readonly=True)
        self.assertEqual(a.__class__.__name__, "Engine")
        self.assertEqual(b.__class__.__name__, "Engine")
        self.assertNotEqual(a, b)
        self.assertEqual(a, SQL.write_engine)
        self.assertEqual(b, SQL.read_engines[0])

        self.assertTrue(SQL.dispose())

    def test_dispose_null(self):
        self.assertFalse(SQL.dispose())

    def test_log(self):
        c = SQL.conn()
        c.log("a")
        c.log("b")
        self.assertEqual(len(c._log), 2)

        c.dump_log(io.StringIO())  # smoke test


class TestTransactions(unittest.TestCase):
    def setUp(self):
        super(TestTransactions, self).setUp()
        SQL.execute("drop table if exists foo")
        SQL.execute("create table foo(bar integer, baz varchar(20))")

    def tearDown(self):
        SQL.execute("drop table if exists foo")
        super(TestTransactions, self).tearDown()

        # use dump_log=True if you would like to do debugging
        logs = SQL.cleanup_conn(dump_log=False)
        # print logs

    def _test_table(self):
        self.assertIsNone(SQL.table("badtable"))
        self.assertIsNotNone(SQL.table("foo"))

    def _test_as_readonly(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})

        with SQL.as_readonly():
            self.assertEqual(
                [1, 2], list(SQL.select_column("select bar from foo order by bar"))
            )

    def test_implied_readonly(self):
        SQLW = SQLBase(
            write_url="mysql+pymysql://wombat:{password}@unit_test-mysql/{database}?charset=utf8mb4",
            read_urls=[
                "mysql+pymysql://wombat:{password}@unit_test-mysql/{database}?charset=utf8mb4"
            ],
        )
        self.assertEqual(SQLW.get_readonly(), False)

        SQLW2 = SQLBase(
            write_url="mysql+pymysql://wombat:{password}@unit_test-mysql/{database}?charset=utf8mb4",
        )
        self.assertEqual(SQLW2.get_readonly(), False)

        SQLRO = SQLBase(
            write_url=None,
            read_urls=[
                "mysql+pymysql://wombat:{password}@unit_test-mysql/{database}?charset=utf8mb4"
            ],
        )
        self.assertEqual(SQLRO.get_readonly(), True)

    def _test_select_column(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        self.assertEqual(
            [1, 2], list(SQL.select_column("select bar from foo order by bar"))
        )
        self.assertEqual(
            ["aaa", "bbb"], list(SQL.select_column("select baz from foo order by bar"))
        )

    def _test_select_columns(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        self.assertEqual(
            {"bar": [1, 2], "baz": ["aaa", "bbb"]},
            SQL.select_columns("select bar, baz from foo order by bar"),
        )

    def _test_select_foreach(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        fe = SQL.select_foreach("select * from foo order by bar")
        self.assertEqual(next(fe), {"bar": 1, "baz": "aaa"})
        self.assertEqual(next(fe), {"bar": 2, "baz": "bbb"})
        with self.assertRaises(StopIteration):
            next(fe)

    def _test_select_lists(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        fe = SQL.select_lists("select * from foo order by bar", columns=["bar", "baz"])
        self.assertEqual(next(fe), [1, "aaa"])
        self.assertEqual(next(fe), [2, "bbb"])
        with self.assertRaises(StopIteration):
            next(fe)

    def test_select_one(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        SQL.insert("foo", {"bar": 2, "baz": "ccc"})
        self.assertEqual(
            SQL.select_one("select * from foo where bar=1"), {"bar": 1, "baz": "aaa"}
        )

        with self.assertRaises(Exception):
            SQL.select_one("select * from foo where bar=3")

        with self.assertRaises(Exception):
            SQL.select_one("select * from foo where bar=2")

    def _test_select_0or1(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        SQL.insert("foo", {"bar": 2, "baz": "ccc"})
        self.assertEqual(
            SQL.select_0or1("select * from foo where bar=1"), {"bar": 1, "baz": "aaa"}
        )

        self.assertEqual(SQL.select_0or1("select * from foo where bar=3"), None)

        with self.assertRaises(Exception):
            SQL.select_0or1("select * from foo where bar=2")

    def _test_result_count(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        r = list(SQL.select_foreach("select bar from foo"))
        count_query = "select count(*) as count from foo"
        self.assertEqual(
            SQL.result_count(False, r, count_query), [{"bar": 1}, {"bar": 2}]
        )
        self.assertEqual(
            SQL.result_count(True, r, count_query),
            {"results": [{"bar": 1}, {"bar": 2}], "count": 2},
        )

    def _test_update_delete(self):
        SQL.insert("foo", {"bar": 1, "baz": "aaa"})
        SQL.insert("foo", {"bar": 2, "baz": "bbb"})
        SQL.insert("foo", {"bar": 2, "baz": "ccc"})

        self.assertEqual(
            list(SQL.select_foreach("select * from foo order by baz")),
            [
                {"bar": 1, "baz": "aaa"},
                {"bar": 2, "baz": "bbb"},
                {"bar": 2, "baz": "ccc"},
            ],
        )

        SQL.update("foo", where="bar=%s", where_data=[2], data={"baz": "xxx"})

        self.assertEqual(
            list(SQL.select_foreach("select * from foo order by baz")),
            [
                {"bar": 1, "baz": "aaa"},
                {"bar": 2, "baz": "xxx"},
                {"bar": 2, "baz": "xxx"},
            ],
        )

        SQL.delete("foo", where="bar=%s", data=[1])

        self.assertEqual(
            list(SQL.select_foreach("select * from foo order by baz")),
            [{"bar": 2, "baz": "xxx"}, {"bar": 2, "baz": "xxx"}],
        )

    @SQL.is_transaction
    def _myfn(self, data, data2=None, err=False, err2=False):
        for el in data:
            SQL.insert("foo", {"bar": el})

        if data2:
            self._myfn(data=data2, err=err2)

        if err:
            raise TestException("sup")

    def _test_transaction_decorator_commit(self):
        self._myfn([1, 2])
        self.assertEqual([1, 2], list(SQL.select_column("select bar from foo")))

    def _test_transaction_decorator_rollback(self):
        with self.assertRaises(TestException):
            self._myfn([1, 2], err=True)
        self.assertEqual([], list(SQL.select_column("select bar from foo")))

    def _test_transaction_decorator_nested_commit(self):
        self._myfn([1, 2], data2=[3, 4])
        self.assertEqual([1, 2, 3, 4], list(SQL.select_column("select bar from foo")))

    def _test_transaction_decorator_nested_rollback1(self):
        with self.assertRaises(TestException):
            self._myfn([1, 2], data2=[3, 4], err=True)
        self.assertEqual([], list(SQL.select_column("select bar from foo")))

    def _test_transaction_decorator_nested_rollback2(self):
        with self.assertRaises(TestException):
            self._myfn([1, 2], data2=[3, 4], err2=True)
        self.assertEqual([], list(SQL.select_column("select bar from foo")))

    def _test_basic_commit(self):
        # basic
        with SQL.transaction():
            SQL.insert("foo", {"bar": 1})
            SQL.insert("foo", {"bar": 2})

            # within the transaction we should see what we inserted
            self.assertEqual([1, 2], list(SQL.select_column("select bar from foo")))

        # after/outside the transaction we should still see them, because they are committed
        self.assertEqual([1, 2], list(SQL.select_column("select bar from foo")))

    def _test_basic_rollback(self):
        # basic
        with self.assertRaises(TestException):
            with SQL.transaction():
                SQL.insert("foo", {"bar": 1})
                SQL.insert("foo", {"bar": 2})

                # within the transaction we should see what we inserted
                self.assertEqual(
                    [1, 2], list(SQL.select_column("select bar from foo"))
                )
                raise TestException("a mystery")

        # after/outside the transaction we should not because it gets r ollwed back
        self.assertEqual([], list(SQL.select_column("select bar from foo")))

    def _test_nested_rollback(self):
        with self.assertRaises(TestException):
            with SQL.transaction():
                SQL.insert("foo", {"bar": 1})
                SQL.insert("foo", {"bar": 2})

                # within the transaction we should see what we inserted
                self.assertEqual(
                    [1, 2], list(SQL.select_column("select bar from foo"))
                )

                with SQL.transaction():
                    SQL.insert("foo", {"bar": 3})
                    self.assertEqual(
                        [1, 2, 3], list(SQL.select_column("select bar from foo"))
                    )

                raise TestException("a mystery")

        # after/outside the transaction we should not because it gets r ollwed back
        self.assertEqual([], list(SQL.select_column("select bar from foo")))

    def _test_nested_rollback2(self):
        with self.assertRaises(TestException):
            with SQL.transaction():
                SQL.insert("foo", {"bar": 1})
                SQL.insert("foo", {"bar": 2})

                # within the transaction we should see what we inserted
                self.assertEqual(
                    [1, 2], list(SQL.select_column("select bar from foo"))
                )

                with SQL.transaction():
                    SQL.insert("foo", {"bar": 3})
                    self.assertEqual(
                        [1, 2, 3], list(SQL.select_column("select bar from foo"))
                    )
                    raise TestException("a mystery")

        # after/outside the transaction we should not because it gets r ollwed back
        self.assertEqual([], list(SQL.select_column("select bar from foo")))

    def _test_nested_commit(self):
        with SQL.transaction():
            SQL.insert("foo", {"bar": 1})
            SQL.insert("foo", {"bar": 2})

            # within the transaction we should see what we inserted
            self.assertEqual([1, 2], list(SQL.select_column("select bar from foo")))

            with SQL.transaction():
                SQL.insert("foo", {"bar": 3})
                self.assertEqual(
                    [1, 2, 3], list(SQL.select_column("select bar from foo"))
                )

        # after/outside the transaction we should not because it gets r ollwed back
        self.assertEqual([1, 2, 3], list(SQL.select_column("select bar from foo")))

    def test_bulk_insert(self):
        num = 10
        data = [{"bar": c} for c in range(num)]
        SQL.insert("foo", data)
        vals = list(SQL.select_column("select bar from foo order by bar"))
        self.assertEqual(vals, list(range(num)))

    def test_ensure_table(self):
        SQL.execute("drop table if exists xxx")
        SQL.ensure_table("xxx", "foo integer", dry_run=True)
        SQL.ensure_table("xxx", "foo integer", dry_run=False, drop_first=False)
        SQL.ensure_table("xxx", "foo integer", dry_run=True, drop_first=True)

    def test_execute_fail(self):
        with self.assertRaises(Exception):
            SQL.execute("gibberish", message="more gibberish")


class TestMigration(unittest.TestCase):
    def setUp(self):
        SQL.execute("drop table if exists migrations")
        SQL.execute("drop table if exists test1")
        SQL.execute("drop table if exists test2")

    def _test_migrate(self):
        logging.basicConfig(level=logging.ERROR)

        initial = Migration(1, "initial version")
        initial.add_statement("drop table if exists test1")
        initial.add_statement("drop table if exists test2")
        initial.add_statement("create table test1(bar integer, baz varchar(20))")

        new = Migration(2, "next version")
        new.add_statement("drop table if exists test2")
        new.add_statement("create table test2(bar integer, baz varchar(20))")

        # ok these are really just smoke tests
        # we could look at the logs and comapre to expected values if needed...
        # logs = Migration.migrate(SQL, dry_run=True)
        # logs = Migration.migrate(SQL, dry_run=True, initial=False)
        # logs = Migration.migrate(SQL, dry_run=True, initial=True)
        # logs = Migration.migrate(SQL, dry_run=True, initial=False, apply_versions=[2])

        # let's try a full migration
        print("exists", SQL.table_exists("test1"))
        self.assertFalse(SQL.table_exists("test1"))
        self.assertFalse(SQL.table_exists("test2"))
        logs = Migration.migrate(SQL, dry_run=False)
        self.assertTrue(SQL.table_exists("test1"))
        self.assertTrue(SQL.table_exists("test2"))

        # let's run an initial=True which should start from scratch again
        self.assertTrue(SQL.table_exists("test1"))
        self.assertTrue(SQL.table_exists("test2"))
        logs = Migration.migrate(SQL, dry_run=False, initial=True)
        self.assertTrue(SQL.table_exists("test1"))
        self.assertTrue(SQL.table_exists("test2"))

        # let's delete one table and run just a specific version
        SQL.execute("drop table test2")
        self.assertTrue(SQL.table_exists("test1"))
        self.assertFalse(SQL.table_exists("test2"))
        logs = Migration.migrate(SQL, dry_run=False, initial=False, apply_versions=[2])
        self.assertTrue(SQL.table_exists("test1"))
        self.assertTrue(SQL.table_exists("test2"))


class TestHelpers(unittest.TestCase):
    def test_in_clause(self):
        mylist = [1, 2, 3, 4, 5]
        expected = "%s,%s,%s,%s,%s"
        self.assertEqual(expected, SQL.in_clause(mylist))

    def test_dict_in_clause(self):
        bindvars = {}
        mylist = [1, 2, 3, 4, 5]
        self.assertEqual(
            "%(x_0)s,%(x_1)s,%(x_2)s,%(x_3)s,%(x_4)s",
            SQL.dict_in_clause(bindvars, mylist, "x"),
        )

        # this function has the side effect of adding entries to bindvars
        self.assertEqual(bindvars, {"x_0": 1, "x_1": 2, "x_2": 3, "x_3": 4, "x_4": 5})

    def test_construct_where(self):
        b, w = SQL.construct_where({"a": 1, "b": 2})
        self.assertEqual(w, "where (a=%s and b=%s)")
        self.assertEqual(b, [1, 2])

        b, w = SQL.construct_where([{"a": 1, "b": 2}, {"c": 3}])
        self.assertEqual(w, "where (a=%s and b=%s) or (c=%s)")
        self.assertEqual(b, [1, 2, 3])

    def test_where_clause(self):
        self.assertEqual("", SQL.where_clause([]))
        self.assertEqual("where a=b", SQL.where_clause(["a=b"]))
        self.assertEqual("where a=b", SQL.where_clause("a=b"))
        self.assertEqual("and a=b", SQL.where_clause(["a=b"], prefix="and"))
        self.assertEqual("a=b", SQL.where_clause(["a=b"], prefix=""))
        self.assertEqual("where a=b and b=c", SQL.where_clause(["a=b", "b=c"]))
        self.assertEqual(
            "where a=b or b=c", SQL.where_clause(["a=b", "b=c"], join_with="or")
        )

    def test_orderby(self):
        self.assertEqual("", SQL.orderby(None))
        self.assertEqual("order by foo asc", SQL.orderby(None, "foo"))
        self.assertEqual("order by foo desc", SQL.orderby(None, "-foo"))
        self.assertEqual("order by bar asc", SQL.orderby("bar", "foo"))
        self.assertEqual("order by bar desc", SQL.orderby("-bar", "foo"))

        self.assertEqual("order by foo asc", SQL.orderby(None, ("foo", "asc")))
        self.assertEqual("order by foo desc", SQL.orderby(None, ("foo", "desc")))
        self.assertEqual("order by bar asc", SQL.orderby("bar", ("foo", "asc")))
        self.assertEqual("order by bar desc", SQL.orderby("-bar", ("foo", "asc")))

        self.assertEqual("order by bar asc", SQL.orderby(("bar", "asc"), None))
        self.assertEqual("order by bar desc", SQL.orderby(("bar", "desc"), None))

        self.assertEqual("", SQL.orderby(1))

    def test_limit(self):
        self.assertEqual("", SQL.limit(start=0, limit=0))
        self.assertEqual("limit 10,1", SQL.limit(start=10, limit=0))
        self.assertEqual("limit 10,10", SQL.limit(start=10, limit=10))
        self.assertEqual("limit 10", SQL.limit(start=0, limit=10))
        self.assertEqual("limit 10", SQL.limit(page=1, limit=10))
        self.assertEqual("limit 10,10", SQL.limit(page=2, limit=10))

    def test_chunked(self):
        input = list(range(8))
        self.assertEqual(
            [list(x) for x in chunked(input, 3)], [[0, 1, 2], [3, 4, 5], [6, 7]]
        )

    def test_auto_where_mysql(self):
        SQL.mysql = True
        SQL.postgres = False

        w, b = SQL.auto_where(a=1, b=2, c=3)
        self.assertEqual(w, ["a=%s", "b=%s", "c=%s"])
        self.assertEqual(b, [1, 2, 3])

        w, b = SQL.auto_where(a=1, b=2, c=3, asdict=True)
        self.assertEqual(w, ["a=%(a)s", "b=%(b)s", "c=%(c)s"])
        self.assertEqual(b, {"a": 1, "b": 2, "c": 3})

    def test_process_date(self):
        self.assertEqual(SQL.process_date(""), None)
        self.assertEqual(SQL.process_date(None), None)
        self.assertEqual(
            SQL.process_date(None, "1900-01-01"), datetime.datetime(1900, 1, 1)
        )
        self.assertEqual(
            SQL.process_date("2020-01-01", strip_timezone=False),
            datetime.datetime(2020, 1, 1, tzinfo=pytz.utc),
        )
        self.assertEqual(
            SQL.process_date("2020-01-01", strip_timezone=True),
            datetime.datetime(2020, 1, 1),
        )
        self.assertEqual(
            SQL.process_date("2020-01-01T10:10:10", strip_timezone=True),
            datetime.datetime(2020, 1, 1, 10, 10, 10),
        )


class TestStructures(unittest.TestCase):
    def test_dictobj(self):
        def _testit(d):
            self.assertEqual(d.to_json(), {"x": 1, "y": 2})
            self.assertEqual(d.asdict(), {"x": 1, "y": 2})
            self.assertEqual(d.x, 1)
            self.assertEqual(d.y, 2)
            self.assertEqual(d["x"], 1)
            self.assertEqual(d["y"], 2)

            self.assertEqual(repr(d), repr({"x": 1, "y": 2}))
            self.assertEqual(len(d), 2)
            self.assertEqual("x" in d, True)
            self.assertEqual("z" in d, False)

            self.assertEqual(list(sorted(d.items())), [("x", 1), ("y", 2)])
            self.assertEqual(list(sorted(d.keys())), ["x", "y"])
            self.assertEqual(list(sorted(d.values())), [1, 2])

        d1 = dictobj({"x": 1, "y": 2})
        d2 = copy.deepcopy(d1)
        d3 = dictobj(d1)
        d4 = d1.copy()
        _testit(d1)
        _testit(d2)
        _testit(d3)
        _testit(d4)

        d1.x = -1
        self.assertEqual(d1.x, -1)
        d1["x"] = -2
        self.assertEqual(d1.x, -2)
        d1.pop("x")
        self.assertEqual(d1.to_json(), {"y": 2})

        d1.update({"x": -2, "z": 5})
        self.assertEqual(d1.to_json(), {"x": -2, "y": 2, "z": 5})

        self.assertEqual(d1.setdefault("x", -3), -2)
        self.assertEqual(d1.setdefault("a", -3), -3)

        d1.popitem()  # should get rid of last item, 'a'
        self.assertEqual(d1.to_json(), {"x": -2, "y": 2, "z": 5})

        d1.clear()
        self.assertEqual(d1.to_json(), {})
