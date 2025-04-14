import json
import logging
import os

import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

from rb_api_framework import Api, OurJSONEncoder

api_frameworks = {}

logger = logging.getLogger(__name__)


session = requests.session()
retry = Retry(
    total=5,
    read=5,
    connect=5,
    backoff_factor=0.25,
    status_forcelist=(500, 429, 502, 503, 504),
)
adapter = HTTPAdapter(max_retries=retry)
session.mount("http://", adapter)
session.mount("https://", adapter)


class FrameworkEndpoint(object):
    def __init__(self, framework, command):
        self.framework = framework
        self.command = command

    def __call__(self, *args, **kwargs):
        headers = self.framework._headers.copy()

        cmd = self.framework.api_data[self.command]
        url = cmd["simple_url"]
        if "data" in kwargs:
            data = kwargs["data"]  # FIXME
        else:
            data = {}
            for k, v in zip(args, cmd["args"]):
                data[k] = v
            data.update(kwargs)

        whole_url = os.path.join(self.framework._base_url, url)
        file_keys = cmd["config"].get("file_keys")
        files = {}
        if file_keys:
            for fk in file_keys:
                file_param = data.pop(fk, None)
                if not file_param:
                    continue

                if hasattr(fk, "read"):
                    files[fk] = (
                        data.pop("{}_name".format(fk), "unknown"),
                        file_param,
                        "application/octet-stream",
                    )
                else:
                    files[fk] = (
                        os.path.split(file_param)[-1],
                        open(file_param, "rb"),
                        "application/octet-stream",
                    )

                files["__data"] = json.dumps(data, cls=OurJSONEncoder)

            # logger.warn("framework client upload, url=%r, headers=%r, files=%r", whole_url, headers, files)
            r = session.post(whole_url, files=files, headers=headers)
        else:
            headers["Content-Type"] = "application/json"
            r = session.post(
                whole_url, data=json.dumps(data, cls=OurJSONEncoder), headers=headers
            )

        if r.status_code >= 300:
            status_code_map = {
                400: Api.BadRequest,
                401: Api.Unauthorized,
                403: Api.Forbidden,
                404: Api.NotFound,
                406: Api.NotAcceptable,
                500: Api.APIException,
            }

            error_class = status_code_map.get(r.status_code, Api.APIException)
            try:
                e = r.json()
            except:
                if isinstance(r.content, bytes):
                    e = {"content": r.content.decode("utf-8")}
                else:
                    e = {"content": r.content}

            ec = error_class(e)
            if r.status_code not in status_code_map:
                ec.status_code = r.status_code
            raise ec

        try:
            return r.json()
        except:
            if isinstance(r.content, bytes):
                return r.content.decode("utf-8")
            else:
                return r.content

    def walk(self, *args, **kwargs):
        stop_limit = kwargs.pop("stop_limit", None)
        kwargs["page"] = kwargs.get("page", 1)

        count = 0
        while True:
            r = self(*args, **kwargs)
            for res in r["results"]:
                yield res
                count += 1

                if stop_limit and count == stop_limit:
                    return

            if not r["next"]:
                break

            kwargs["page"] += 1

    def info(self):
        return self.framework.api_data[self.command]


class Framework(object):
    def __init__(self, base_url, headers, api_data):
        self._base_url = base_url
        self.api_data = api_data
        self._headers = headers

    def __getattr__(self, command):
        return FrameworkEndpoint(self, command)

    def list_functions(self):
        return [x for x in self.api_data.keys() if not x.startswith("_")]


class Frameworks(object):
    def __init__(
        self,
        base_url,
        framework_endpoint,
        framework_key=None,
        environment=None,
        application=None,
        headers=None,
        privileged_key=None,
        save_path=None,
        load_cache=False,
    ):
        self._headers = headers or {}
        self._base_url = base_url
        self._framework_endpoint = framework_endpoint
        self._endpoints = None
        self._framework_key = framework_key
        self._privileged_key = privileged_key
        self._environment = environment
        self._application = application
        self._save_path = (
            os.path.join(save_path, self._framework_key) + ".json"
            if save_path
            else None
        )
        self._load_cache = load_cache

    def lazy_load(self, load_cache=False):
        if self._endpoints:
            return

        load_cache = self._load_cache or load_cache
        if load_cache and self._save_path and os.path.exists(self._save_path):
            logger.warn("Loading framework data from %r", self._save_path)
            # FIXME we need a try/catch but I don't know what to look for
            with open(self._save_path) as f:
                self._endpoints = json.load(f)
            return

        if self._base_url is None:
            raise Exception(
                "Did not find framework matching the name '{}'".format(
                    self._framework_key
                )
            )

        these_headers = {}
        these_headers.update(self._headers)

        url = os.path.join(self._base_url, self._framework_endpoint)
        r = session.get(url, headers=these_headers)
        if r.status_code >= 300:
            raise Exception(
                "Status code %d encountered while trying to get framework endpoint: %r"
                % (r.status_code, url)
            )
        try:
            self._endpoints = r.json()
        except Exception as e:
            raise Exception(
                "Framework endpoint did not return data: url=%r, error=%r" % (url, e)
            )

    def __getattr__(self, apiname):
        if apiname.startswith("_"):
            return self.__dict__.get("apiname")

        self.lazy_load()
        return Framework(self._base_url, self._headers, self._endpoints[apiname])

    def list_apis(self):
        self.lazy_load()
        return [x for x in sorted(self._endpoints.keys()) if x != "user"]

    def list_endpoints(self):
        self.lazy_load()
        return {
            x: [foo for foo in sorted(y.keys()) if not foo.startswith("_")]
            for x, y in self._endpoints.items()
            if x != "user"
        }

    def materialize(self):
        logger.warn("Saving framework data to %r", self._save_path)
        self.lazy_load(load_cache=False)
        with open(self._save_path, "w") as f:
            json.dump(self._endpoints, f)


def factory(framework=None, application=None, environment=None, **kwargs):
    headers = {}

    if framework:
        environment = framework.split("-")[0]
    else:
        framework = "{}-{}".format(environment, application)

    base_url, framework_endpoint = api_frameworks.get(framework, [None, None])
    return Frameworks(
        base_url=base_url,
        framework_endpoint=framework_endpoint,
        framework_key=framework,
        environment=environment,
        application=application,
        headers=headers,
        **kwargs
    )
