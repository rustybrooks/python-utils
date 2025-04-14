import copy
import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

import rb_api_framework as framework
from flask import Flask


class FakeSessionResponse:
    def __init__(self, r=None, status_code=None, content=None):
        if r:
            self.content = r.data
            self.status_code = r.status_code
        else:
            self.content = content
            self.status_code = status_code

    def json(self):
        return json.loads(self.content)


class FakeSession:
    def __init__(self, app_client):
        self.app_client = app_client

    def get(self, url, headers=None):
        r = self.app_client.get(url, headers=headers)
        return FakeSessionResponse(r)

    def post(self, url, headers=None, data=None):
        r = self.app_client.post(url, headers=headers, data=data)
        return FakeSessionResponse(r)


class FlaskApiTestCase(unittest.TestCase):
    maxDiff = None

    def setUp(self) -> None:
        self.app = Flask("test")
        self.app.config["TESTING"] = True
        self.app_client = self.app.test_client()
        self.registry_save = copy.deepcopy(framework.app_registry)

    def tearDown(self) -> None:
        framework.app_registry = copy.deepcopy(self.registry_save)

    def get(self, url, headers=None):
        r = self.app_client.get(url, headers=headers)
        return json.loads(r.data)

    def post(self, url, headers=None, data=None):
        r = self.app_client.post(url, headers=headers, data=data)
        return json.loads(r.data)


class ClientTest(FlaskApiTestCase):
    def setUp(self):
        super().setUp()

        @framework.api_register(None, require_login=False)
        class TestApi(framework.Api):
            @classmethod
            def foo(cls):
                return "stuff"

            @classmethod
            @framework.Api.config(max_page_limit=10)
            def paged(cls, page=1, limit=5):
                return {
                    "results": [
                        y
                        for y in [(page - 1) * limit + x for x in range(limit)]
                        if y < 12
                    ],
                    "count": 12,
                }

        self.api = TestApi()
        framework.app_class_proxy(self.app, "", "test", self.api)
        framework.app_class_proxy(self.app, "", "framework", framework.FrameworkApi())

    def test_client(self):
        with patch("rb_api_framework.client.session", FakeSession(self.app_client)):
            c = framework.client.Frameworks(
                base_url="/",
                framework_endpoint="framework/endpoints",
                framework_key="test",
            )
            self.assertEqual(c.list_apis(), ["FrameworkApi", "TestApi"])
            self.assertEqual(
                c.list_endpoints(),
                {"FrameworkApi": ["endpoints"], "TestApi": ["foo", "paged"]},
            )

            r = c.TestApi.foo()
            self.assertEqual(r, "stuff")

            self.assertEqual(
                c.TestApi.paged(page=1, limit=5),
                {
                    "results": [0, 1, 2, 3, 4],
                    "count": 12,
                    "previous": None,
                    "next": "/_test/paged?page=2",
                },
            )

            r = list(c.TestApi.paged.walk())
            self.assertEqual(r, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11])

    def test_errors(self):
        session_mock = Mock()
        session_mock.post = Mock(
            return_value=FakeSessionResponse(status_code=500, content="")
        )
        session_mock.get = FakeSession(self.app_client).get

        c = framework.client.Frameworks(
            base_url="/", framework_endpoint="framework/endpoints", framework_key="test"
        )

        session_mock.post = Mock(
            return_value=FakeSessionResponse(status_code=400, content="")
        )
        with self.assertRaises(self.api.BadRequest):
            with patch("rb_api_framework.client.session", session_mock):
                c.TestApi.foo()

        session_mock.post = Mock(
            return_value=FakeSessionResponse(status_code=403, content="")
        )
        with self.assertRaises(self.api.Forbidden):
            with patch("rb_api_framework.client.session", session_mock):
                c.TestApi.foo()

        session_mock.post = Mock(
            return_value=FakeSessionResponse(status_code=500, content="")
        )
        with self.assertRaises(self.api.APIException):
            with patch("rb_api_framework.client.session", session_mock):
                c.TestApi.foo()

    def test_materialize(self):
        tmp = tempfile.mkdtemp()
        c = framework.client.Frameworks(
            base_url="/",
            framework_endpoint="framework/endpoints",
            framework_key="test",
            save_path=tmp,
        )
        with patch("rb_api_framework.client.session", FakeSession(self.app_client)):
            c.materialize()

        json_file = os.path.join(tmp, "test.json")
        with open(json_file) as f:
            data = json.load(f)
            self.assertEqual(data, c._endpoints)

        os.unlink(json_file)

    def test_factory(self):
        session_mock = Mock()
        session_mock.post = Mock(
            return_value=FakeSessionResponse(status_code=200, content='{"key": "foo"}')
        )
        session_mock.get = FakeSession(self.app_client).get
        with patch(
            "rb_api_framework.client.api_frameworks",
            {
                "test": ["/", "framework/endpoints"],
                "ci-foo": ["/ci-foo", "framework/endpoints"],
                "prod-foo": ["/prod-foo", "framework/endpoints"],
            },
        ):
            with patch("rb_api_framework.client.session", session_mock):
                c = framework.client.factory("test")
                self.assertEqual(c._base_url, "/")
                self.assertEqual(c._framework_endpoint, "framework/endpoints")

                self.assertEqual(c.list_apis(), ["FrameworkApi", "TestApi"])
                self.assertEqual(
                    c.list_endpoints(),
                    {"FrameworkApi": ["endpoints"], "TestApi": ["foo", "paged"]},
                )

                c = framework.client.factory("ci-foo")
                self.assertEqual(c._base_url, "/ci-foo")

                c = framework.client.factory(environment="ci", application="foo")
                self.assertEqual(c._base_url, "/ci-foo")
