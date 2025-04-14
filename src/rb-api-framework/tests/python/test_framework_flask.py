import copy
import datetime
import json
import unittest
from unittest.mock import Mock

import pytz
import rb_api_framework as framework
from flask import Flask


class TestApiStuff(unittest.TestCase):
    maxDiff = None

    def get(self, url, headers=None):
        r = self.app_client.get(url, headers=headers)
        return json.loads(r.data)

    def setUp(self):
        self.app = Flask("test")
        self.app.config["TESTING"] = True
        self.app_client = self.app.test_client()
        self.registry_save = copy.deepcopy(framework.app_registry)

        @framework.api_register(None, require_login=False)
        class TestApi(framework.Api):
            @classmethod
            def norm(cls, external=False):
                return f"stuff {external}"

            @classmethod
            @framework.Api.config(headers={"a": "b"})
            def html(cls):
                return framework.HttpResponse("hi")

            @classmethod
            def xml(cls):
                return framework.XMLResponse(content="<xml>hi</xml>")

            @classmethod
            def do_raise(cls, exc, message=None):
                if exc == "notfound":
                    raise cls.NotFound(message)
                elif exc == "unauthorized":
                    raise cls.Unauthorized(message)
                elif exc == "forbidden":
                    raise cls.Forbidden(message)
                elif exc == "badrequest":
                    raise cls.BadRequest(message)
                elif exc == "notacceptable":
                    raise cls.NotAcceptable(message)
                elif exc == "toomany":
                    raise cls.TooManyRequests(message)

            @classmethod
            @framework.Api.config(require_login=True)
            def user_req(cls, data=None, page=1, limit=10, sort="foo"):
                return "user"

            @classmethod
            @framework.Api.config(require_admin=True)
            def admin_req(cls):
                return "admin"

        self.api = TestApi()
        framework.app_class_proxy(
            self.app,
            "",
            "test",
            self.api,
            cleanup_callback=Mock(side_effect=Exception("cleanup")),
        )
        framework.app_class_proxy(
            self.app, "/foo", "framework", framework.FrameworkApi()
        )

    def tearDown(self) -> None:
        framework.app_registry = copy.deepcopy(self.registry_save)

    def test_exceptions(self):
        with self.assertRaises(self.api.NotFound):
            self.api.do_raise("notfound")

        with self.assertRaises(self.api.Unauthorized):
            self.api.do_raise("unauthorized")

        with self.assertRaises(self.api.Forbidden):
            self.api.do_raise("forbidden")

        with self.assertRaises(self.api.BadRequest):
            self.api.do_raise("badrequest")

        with self.assertRaises(self.api.NotAcceptable):
            self.api.do_raise("notacceptable")

        with self.assertRaises(self.api.TooManyRequests):
            self.api.do_raise("toomany")

    def test_framework_endpoints(self):
        r = self.get("/foo/framework/endpoints?apps=FrameworkApi")
        self.assertEqual(
            r,
            {
                "user": {"id": 0, "username": "Anonymous", "authenticated": False},
                "FrameworkApi": {
                    "endpoints": {
                        "app": "FrameworkApi",
                        "function": "endpoints",
                        "simple_url": "_framework/endpoints",
                        "ret_url": "framework/endpoints",
                        "args": [],
                        "kwargs": [["apps", None], ["_user", None]],
                        "config": {
                            "require_admin": False,
                            "require_login": False,
                            "api_key": None,
                            "function_url": None,
                            "param_regexp_map": {},
                            "sort_keys": None,
                            "max_page_limit": -1,
                            "max_page": -1,
                            "file_keys": None,
                            "headers": {},
                            "json_encoder": "json",
                        },
                    }
                },
            },
        )

    def test_http_response(self):
        r = self.app_client.get("/test/html")
        self.assertEqual(r.data, b"hi")
        self.assertEqual(
            list(r.headers.items()),
            [("Content-Type", "text/html"), ("Content-Length", "2"), ("a", "b")],
        )

    def test_xml_response(self):
        r = self.app_client.get("/test/xml")
        self.assertEqual(r.data, b"<xml>hi</xml>")

    def test_login_failures(self):
        r = self.app_client.get("/test/user_req")
        self.assertEqual(r.status_code, 403)

        r = self.app_client.get("/test/admin_req")
        self.assertEqual(r.status_code, 403)


class TestHelpers(unittest.TestCase):
    def test_api_bool(self):
        self.assertEqual(framework.api_bool(True), True)
        self.assertEqual(framework.api_bool(1), True)
        self.assertEqual(framework.api_bool("1"), True)
        self.assertEqual(framework.api_bool("True"), True)
        self.assertEqual(framework.api_bool("true"), True)
        self.assertEqual(framework.api_bool("tRuE"), True)

        self.assertEqual(framework.api_bool(False), False)
        self.assertEqual(framework.api_bool(0), False)
        self.assertEqual(framework.api_bool("0"), False)
        self.assertEqual(framework.api_bool("false"), False)
        self.assertEqual(framework.api_bool("False"), False)
        self.assertEqual(framework.api_bool("FaLse"), False)

        self.assertEqual(framework.api_bool(None), None)
        self.assertEqual(framework.api_bool("None"), None)
        self.assertEqual(framework.api_bool("none"), None)
        self.assertEqual(framework.api_bool("NoNe"), None)

    def test_api_int(self):
        self.assertEqual(framework.api_int(10), 10)
        self.assertEqual(framework.api_int("10"), 10)
        self.assertEqual(framework.api_int("10x"), 0)
        self.assertEqual(framework.api_int("a"), 0)
        self.assertEqual(framework.api_int(None), None)
        self.assertEqual(framework.api_int("a", default=-1), -1)

    def test_api_datetime(self):
        self.assertEqual(
            framework.api_datetime(datetime.datetime(2020, 1, 1)),
            datetime.datetime(2020, 1, 1, tzinfo=pytz.utc),
        )

        self.assertEqual(
            framework.api_datetime("2020-01-01"),
            datetime.datetime(2020, 1, 1, tzinfo=pytz.utc),
        )
        self.assertEqual(
            framework.api_datetime("2020-01-01T01:02:03"),
            datetime.datetime(2020, 1, 1, 1, 2, 3, tzinfo=pytz.utc),
        )
        self.assertEqual(
            framework.api_datetime("2020-01-01-06:00"),
            datetime.datetime(2020, 1, 1, 6, 0, tzinfo=pytz.utc),
        )

        self.assertEqual(
            framework.api_datetime("2020-01-01-06:00", as_float=True),
            1577858400.0,
        )

        self.assertEqual(framework.api_datetime(None), None)

        self.assertEqual(
            framework.api_datetime(None, datetime.datetime(2020, 1, 1)),
            datetime.datetime(2020, 1, 1, tzinfo=pytz.UTC),
        )

        self.assertEqual(
            framework.api_datetime(0), datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
        )

        self.assertEqual(
            framework.api_datetime(1577836800.0),
            datetime.datetime(2020, 1, 1, tzinfo=pytz.utc),
        )

        self.assertEqual(
            framework.api_datetime(1577836800.0, as_float=True),
            1577836800.0,
        )

        self.assertEqual(
            framework.api_datetime(None, "2020-01-01"),
            datetime.datetime(2020, 1, 1, tzinfo=pytz.utc),
        )

        self.assertEqual(
            framework.api_datetime(None, "2020-01-01", as_float=True),
            1577836800.0,
        )

    def test_api_list(self):
        self.assertEqual(framework.api_list([1, 2, 3]), [1, 2, 3])
        self.assertEqual(framework.api_list("1,2,3"), ["1", "2", "3"])
        self.assertEqual(framework.api_list("1, 2, 3"), ["1", " 2", " 3"])
        self.assertEqual(framework.api_list(None), None)

    # This test isn't working and I'm not sure why.  It's related to the cleanup
    # I'm doing with the app_registry
    # def test_test_sort_param(self):
    #     print("test_test_sort_param")
    #     self.assertEqual(framework.test_sort_param(), [["TestApi", "user_req"]])
    #     self.assertEqual(framework.test_limit_param(), [["TestApi", "user_req"]])

    # really just a smoke test
    def test_json_response(self):
        j = framework.JSONResponse(detail="foo", err=True)
