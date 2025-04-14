import copy
import datetime
import decimal
import functools
import inspect
import json
import logging
import os
import traceback
from collections import defaultdict

import dateutil.parser
import newrelic.agent
import pytz
import werkzeug.exceptions
from flask import Response, request

try:
    from urllib.parse import urlencode
except:
    from urllib import urlencode


logger = logging.getLogger(__name__)
app_registry = {}


def build_absolute_url(path, params):
    return path + "?" + urlencode(params)


class FlaskUser(object):
    def __init__(self, username, user_id):
        self.username = username
        self.user_id = user_id
        self.id = user_id
        self.is_staff = False

    def is_authenticated(self):
        return self.id != 0


# for compatibility with Django
class HttpResponse(Response):
    def __init__(self, content, status=200, content_type="text/html"):
        super().__init__(
            response=content, content_type=content_type, status=status
        )


class FileResponse(Response):
    def __init__(self, response_object=None, content=None, content_type=None):
        if response_object:
            super().__init__(
                response=response_object.response,
                content_type=response_object.content_type,
                status=response_object.status,
            )
        else:
            super().__init__(
                response=content, content_type=content_type
            )



class JSONResponse(Response):
    @newrelic.agent.function_trace()
    def __init__(
        self,
        data=None,
        detail=None,
        err=False,
        status=None,
        indent=None,
        json_encoder="json",
    ):
        if not status:
            status = 400 if err else 200

        self._data = data if data is not None else {}
        if detail:
            self._data["detail"] = detail

        super().__init__(
            response=json_encode(self._data, encoder=json_encoder, indent=indent),
            status=status,
            mimetype="application/json",
        )


class XMLResponse(Response):
    def __init__(self, data=None, detail=None, err=False, status=None, content=None):
        status = status or 200
        self.content = content if content is not None else ""

        super().__init__(
            response=self.content, status=status, mimetype="application/xml"
        )


class RequestFile(object):
    def __init__(self, fobj):
        self.fobj = fobj

    @property
    def name(self):
        return self.fobj.filename

    @property
    def size(self):
        return -1

    def chunks(self):
        while True:
            data = self.fobj.read(1024 * 32)
            if data:
                yield data


def get_file(_request, key):
    if key not in _request.files:
        return None

    return RequestFile(_request.files[key])


def default_login_method(request=None, **kwargs):
    # auth_key = request.headers.get("authorization-key")
    # if auth_key and auth_key in [... secrets here from secure source...]:
    #     if auth_key == rolled_auth_secret():
    #         logger.error("Using rolled auth key: %r", request.path)
    #
    #     return FlaskUser("Token", 1)
    #
    return FlaskUser("Anonymous", 0)

DEFAULT_LOGIN_METHOD = default_login_method


def app_proxy(sa, fn, fnname, config, urlroot, urlprefix, cleanup_callback=None):
    _require_login = config.get("require_login", False)
    _require_admin = config.get("require_admin", False)
    _headers = config.get("headers", {})
    _headers_is_fn = callable(_headers)

    app_blob = {
        "_newrelic_group": os.path.join("/", urlprefix, urlroot).rstrip("/"),
        "_fnname": fnname,
        "_config": config,
    }
    app_blob["_args"], app_blob["_kwargs"], app_blob["_va"], app_blob["_kw"] = config[
        "fn_args"
    ]
    app_blob["combined_args"] = app_blob["_kwargs"] + [
        (x, None) for x in app_blob["_args"]
    ]

    @functools.wraps(fn)
    def appfn(*fn_args, **fn_kwargs):
        doer = sa()

        try:
            if request.data:
                api_data = request.get_json()
            else:
                api_data = request.form
        except werkzeug.exceptions.BadRequest as e:
            logger.warning(
                "werkzeug.exceptions.BadRequest: url=%r, data=%r, headers=%r",
                request.path,
                request.data,
                [],
            )
            raise e

        url_data = request.args
        login_fn = _require_login if callable(_require_login) else default_login_method
        blob = {
            "fn_args": fn_args,
            "fn_kwargs": fn_kwargs,
            "path": request.path,
            "request": request,
            "api_data": api_data,
            "url_data": url_data,
            "user": login_fn(request=request, api_data=api_data, url_data=url_data),
        }

        # ensure user is logged in if required
        if _require_login and not blob["user"].is_authenticated():
            return JSONResponse(detail="Authentication required", status=403)

        # ensure user is admin if required
        if _require_admin:
            return JSONResponse(detail="Admin accounts not enabled", status=403)

        # This does not handle file uploads, fix later
        val = process_api(fn, doer, app_blob, blob)
        if cleanup_callback:
            try:
                cleanup_callback()
            except Exception:
                logger.error(
                    "Error performing cleanup callback: %r", traceback.format_exc()
                )

        if _headers:
            these_headers = _headers() if _headers_is_fn else _headers
            for k, v in these_headers.items():
                val.headers[k] = v

        return val

    return appfn


def app_class_proxy(base_app, urlprefix, urlroot, app, cleanup_callback=None):
    counts = defaultdict(int)
    app_registry[urlroot] = app
    for fnname in app._api_functions():
        fn = app._fn(fnname)
        config = app._get_config(fn)

        for u in urls_from_config(urlroot, fnname, fn, config, canonical=True):
            u = os.path.join("/", urlprefix, u.lstrip("/^").rstrip("/?$"))
            view_name = "{}-{}-{}-{}".format(
                urlprefix.replace("/", "_"),
                urlroot.replace("/", "_"),
                fnname,
                counts[fnname],
            )
            logger.info("Adding url %r to %r", u, view_name)
            base_app.add_url_rule(
                u,
                endpoint=view_name,
                view_func=app_proxy(
                    app.__class__,
                    fn,
                    fnname,
                    config,
                    urlroot,
                    urlprefix,
                    cleanup_callback=cleanup_callback,
                ),
                methods=["GET", "POST", "PATCH", "PUT", "DELETE", "HEAD"],
            )
            counts[fnname] += 1


def json_encode(data, encoder, indent=None):
    if encoder == "json":
        return json.dumps(data, cls=OurJSONEncoder, indent=indent)
    elif encoder == "ujson":
        import ujson

        return ujson.dumps(data)
    else:
        raise Exception("Unknown JSON encoder: {}".format(encoder))


class OurJSONEncoder(json.JSONEncoder):
    @newrelic.agent.function_trace()
    def default(self, obj):
        if hasattr(obj, "to_json"):
            return obj.to_json()
        elif isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        elif isinstance(obj, bytes):
            return obj.decode("utf-8")
        elif isinstance(obj, set):
            return list(obj)
        elif isinstance(obj, decimal.Decimal):
            return float(obj)

        return json.JSONEncoder.default(self, obj)


class Api(object):
    fn_config = defaultdict(dict)
    fn_lookup = defaultdict(dict)

    class APIException(Exception):
        status_code = 500
        default_detail = "A server error occured"

        def __init__(self, detail=None, status_code=None):
            self.detail = detail or self.default_detail
            if status_code is not None:
                self.status_code = status_code

        def __str__(self):
            return repr(self.detail)

    class NotFound(APIException):
        default_detail = "Not Found"
        status_code = 404

    class Unauthorized(APIException):
        default_detail = "Unauthorized"
        status_code = 401

    class Forbidden(APIException):
        default_detail = "Forbidden"
        status_code = 403

    class BadRequest(APIException):
        default_detail = "Bad Request"
        status_code = 400

    class NotAcceptable(APIException):
        default_detail = "Not Acceptable"
        status_code = 406

    class TooManyRequests(APIException):
        default_detail = "Too Many Requests"
        status_code = 429

    def __init__(self):
        pass

    @classmethod
    def config(cls, *args, **kwargs):
        if "<data>" in args:
            raise Exception("<data> is a reserved keyword in API functions")

        def makewrap(f):
            for a in args:
                cls.fn_config[f].update(a)
            cls.fn_config[f].update(kwargs)
            return f

        return makewrap

    @classmethod
    def _get_args(cls, fn):
        argspec = inspect.getfullargspec(fn)
        border = len(argspec.args or []) - len(argspec.defaults or [])
        list_args = argspec.args[:border]
        keyword_args = list(zip(argspec.args[border:], argspec.defaults or []))
        return (
            [x for x in list_args if x not in ["self", "cls"]],
            keyword_args,
            argspec.varargs,
            argspec.kwonlyargs,
        )

    @classmethod
    def _get_config(cls, fn):
        return cls.fn_config[fn]

    def _api_functions(self):
        return self.registry.keys()

    def _fn(self, function_name):
        return self.fn_lookup.get(function_name)


def api_register(_version=None, **register_kwargs):
    ignore = ["config"]

    def class_rebuilder(cls):
        cls.registry = {}

        cls.fn_config = copy.deepcopy(
            cls.fn_config
        )  # probably don't need to be copies, nothing in there usually
        cls.fn_lookup = copy.deepcopy(cls.fn_lookup)
        for element in inspect.getmembers(cls, predicate=inspect.ismethod):
            k, v = element
            if k.startswith("_"):
                continue

            if k in ignore:
                continue

            unwrapped = v.__func__
            individual_config = cls.fn_config[unwrapped]

            # All config params should be set to defaults here
            cls.fn_config[unwrapped] = {
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
            }

            cls.fn_config[unwrapped].update(register_kwargs)
            cls.fn_config[unwrapped].update(individual_config)
            cls.fn_config[unwrapped].update({"fn_args": cls._get_args(v.__func__)})
            cls.fn_lookup[k] = v.__func__
            cls.registry[k] = v

        return cls

    return class_rebuilder


def route_pieces(fnname, fn, config, canonical=False):
    def _get_regexp(key):
        return config.get("param_regexp_map", {}).get(
            key, r"(?P<{}>[^\/]+)".format(key)
        )

    pieces = []
    route = config.get("route", [])

    list_args, key_args, va, kw = config["fn_args"]

    api_key = config.get("api_key")

    if isinstance(api_key, str):
        api_key = [api_key]

    api_key_in_list = False
    non_url_args = [
        "q",
        "sort",
        "limit",
        "page",
        "data",
    ]  # special params that should only be query args
    if api_key:
        non_url_args.extend(api_key)
        api_key_in_list = all([x in list_args for x in api_key])

    if route:
        pieces.append(os.path.join(*route[1]))
    else:
        # First url (rest friendly)
        foo = []

        # any "api keys" will go before the function name, like
        # /users/rustybrooks/add vs /users/add/rustybrooks
        if api_key_in_list:
            if canonical:
                foo.extend(["<{}>".format(a) for a in api_key])
            else:
                foo.extend([_get_regexp(a) for a in api_key])

        # any functions not starting with "index" will be in the url between the api keys and
        # the other list args
        if not fnname.startswith("index"):
            foo.append(config["function_url"] or fnname)

        # The rest of the list arguments go after the function name
        if canonical:
            foo += ["<{}>".format(x) for x in list_args if x not in non_url_args]
        else:
            foo += [_get_regexp(x) for x in list_args if x not in non_url_args]

        route_piece = os.path.join(*foo) if len(foo) else ""
        # if route_piece:
        pieces.append(route_piece)

        # Second url (simple)
        foo = []
        if not fnname.startswith("index"):
            foo.append(config["function_url"] or fnname)

        pieces.append(os.path.join(*foo) if len(foo) else "")

    return pieces


def urls_from_config(urlroot, fnname, fn, config, canonical=False):
    urls = []

    urlbase = urlroot
    pieces = route_pieces(fnname, fn, config, canonical=canonical)

    urls.append(r"^" + os.path.join(urlbase, pieces[0]).rstrip("/") + r"/?$")
    if len(pieces) > 1:
        urls.append(
            r"^"
            + os.path.join("_" + urlbase.lstrip("/"), pieces[1]).rstrip("/")
            + r"/?$"
        )

    return urls


sentinel = object()


@newrelic.agent.function_trace()
def process_api(fn, api_object, app_blob, blob):
    api_data_orig = blob["api_data"]
    blob_api_data = api_data_orig if isinstance(api_data_orig, dict) else {}

    fn_kwargs = blob["fn_kwargs"]
    file_keys = (
        app_blob["_config"].get("file_keys") or []
    )  # not all places have file_keys in config yet

    try:
        stored = {k: v for k, v in app_blob["_kwargs"]}
        for arg, default in app_blob["combined_args"]:
            if arg in fn_kwargs:
                continue

            if arg == "data":
                fn_kwargs["data"] = api_data_orig
            elif arg in file_keys:
                fn_kwargs[arg] = get_file(blob["request"], arg)
            elif arg in ["_user", "_request"]:
                fn_kwargs[arg] = blob[arg[1:]]
            else:
                val = blob_api_data.get(arg, blob["url_data"].get(arg, sentinel))
                if arg in ["limit", "page"]:
                    try:
                        val = int(val)
                    except (ValueError, TypeError):
                        val = default

                if val is not sentinel:
                    if arg == "limit" and app_blob["_config"]["max_page_limit"] > 0:
                        val = min(val, app_blob["_config"]["max_page_limit"])
                        if val == 0:  # why do people do this?  but they do
                            raise Api.BadRequest(
                                "limit=0 not valid value - must pass positive integer"
                            )
                    elif (
                        arg == "page"
                        and val
                        and val > app_blob["_config"].get("max_page", -1) > 0
                    ):
                        raise Api.BadRequest(
                            "Maximum allowed value for page parameter for this endpoint is {}".format(
                                app_blob["_config"]["max_page"]
                            )
                        )
                    elif (
                        arg == "sort"
                        and val
                        and app_blob["_config"]["sort_keys"] is not None
                    ):
                        sortkey = val
                        if sortkey[0] == "-":
                            sortkey = sortkey[1:]

                        if sortkey not in app_blob["_config"]["sort_keys"]:
                            raise Api.BadRequest(
                                "Invalid 'sort' parameter '{}'.  Allowed values: {}".format(
                                    val, ", ".join(app_blob["_config"]["sort_keys"])
                                )
                            )

                    stored[arg] = fn_kwargs[arg] = val

                if arg == "sort" and not val:
                    fn_kwargs[arg] = default

        # logger.info("transaction name=%r, group=%r", app_blob["_fnname"], app_blob["_newrelic_group"])
        newrelic.agent.set_transaction_name(
            app_blob["_config"]["function_url"] or app_blob["_fnname"],
            group=app_blob["_newrelic_group"],
        )

        # run API function and massage result if required
        api_object._status_code = 200

        retval = fn(api_object, *blob["fn_args"], **fn_kwargs)
    except Api.APIException as e:
        if isinstance(e.detail, str):
            data = {"detail": e.detail}
        else:
            data = e.detail

        if e.status_code not in [200, 201, 403, 429]:
            logger.warning(
                "APIException thrown: code=%r, url=%r, urlparams=%r, post body=%r, response=%r",
                e.status_code,
                blob["path"],
                blob["url_data"],
                blob["api_data"],
                data,
            )

        retval = JSONResponse(
            data=data,
            status=e.status_code,
            json_encoder=app_blob["_config"]["json_encoder"],
        )

    # return results
    if isinstance(retval, (HttpResponse, FileResponse, JSONResponse, XMLResponse)):
        return retval

    if (
        hasattr(retval, "items")
        and "results" in retval
        and "count" in retval
        and stored.get("page") is not None
        and stored.get("limit") is not None
    ):
        prev_url = retval.get("previous")
        next_url = retval.get("next")

        if (
            not prev_url
            and stored["page"] > 1
            and (stored["page"] - 1) * stored["limit"] < retval["count"]
        ):
            params = blob["url_data"].copy()
            params["page"] = stored["page"] - 1
            if params.get("limit"):
                params["limit"] = stored["limit"]
            prev_url = build_absolute_url(blob["path"], params)

        if not next_url and stored["page"] * stored["limit"] < retval["count"]:
            params = blob["url_data"].copy()
            params["page"] = stored["page"] + 1
            if params.get("limit"):
                params["limit"] = stored["limit"]
            next_url = build_absolute_url(blob["path"], params)

        retval["previous"] = prev_url
        retval["next"] = next_url

    return JSONResponse(
        data=retval,
        status=api_object._status_code,
        json_encoder=app_blob["_config"].get("json_encoder", "json"),
    )


@api_register()
class FrameworkApi(Api):
    @classmethod
    def endpoints(cls, apps=None, _user=None):
        def _clean_url(url):
            for c in "?^$":
                url = url.replace(c, "")

            url = url.rstrip("/")
            return url

        apps = apps.split(",") if apps else []
        data = {}
        for urlroot in sorted(app_registry.keys()):
            app = app_registry[urlroot]
            if apps and app.__class__.__name__ not in apps:
                continue

            these_endpoints = {}
            for fnname in sorted(app._api_functions()):
                fn = app._fn(fnname)
                config = app._get_config(fn)

                # ensure user is admin if required
                if config.get("require_admin", False) and (
                    not _user.is_authenticated() or not _user.is_staff
                ):
                    continue

                urls = urls_from_config(urlroot, fnname, fn, config, canonical=True)
                list_args, key_args, va, kw = config["fn_args"]

                config["require_login"] = bool(
                    config.get("require_login", False)
                )  # require_login can be a function in some cases
                this = {
                    "app": app.__class__.__name__,
                    "function": fnname,
                    "simple_url": _clean_url(urls[1] if len(urls) > 1 else urls[0]),
                    "ret_url": _clean_url(urls[0]),
                    "args": list_args,
                    "kwargs": key_args,
                    "config": {k: v for k, v in config.items() if k not in ["fn_args"]},
                }
                these_endpoints[fnname] = this
            data[app.__class__.__name__] = these_endpoints
            data["user"] = {
                "id": _user.id,
                "username": _user.username,
                "authenticated": _user.is_authenticated(),
            }

        return data


def api_bool(val):
    if val is None:
        return None

    if isinstance(val, bool):
        return val
    elif isinstance(val, int):
        return bool(val)

    if val.lower() == "true":
        return True
    elif val.lower() == "none":
        return None

    try:
        val = int(val)
        if val:
            return True
    except ValueError:
        pass

    return False


def api_int(intstr, default=0):
    if intstr is None:
        return None

    try:
        return int(intstr)
    except ValueError:
        return default


def api_float(fstr, default=0):
    if fstr is None:
        return None

    try:
        return float(fstr)
    except ValueError:
        return default


def api_datetime(dtstr, default=None, as_float=False):
    def _normalize(_d):
        if isinstance(_d, datetime.datetime):
            return _d
        elif isinstance(_d, (int, float)):
            return datetime.datetime.utcfromtimestamp(_d)
        else:
            return dateutil.parser.parse(_d, fuzzy=True) if _d else None

    dt = _normalize(dtstr)

    if not dt and default:
        dt = _normalize(default)

    if dt and dt.tzinfo is None:
        dt = pytz.utc.localize(dt)

    if as_float and isinstance(dt, (datetime.datetime, datetime.date)):
        epoch = datetime.datetime(1970, 1, 1, tzinfo=pytz.utc)
        return (dt - epoch).total_seconds()
    else:
        return dt


def api_list(liststr):
    if liststr is None:
        return None

    if isinstance(liststr, list):
        return liststr

    return liststr.split(",")


def test_sort_param():
    missing_sort = []
    for urlroot, app in app_registry.items():
        for fnname in sorted(app._api_functions()):
            fn = app._fn(fnname)
            config = app._get_config(fn)

            list_args, key_args, va, kw = config["fn_args"]

            if "sort" in dict(key_args) and not config["sort_keys"]:
                missing_sort += [[app.__class__.__name__, fnname]]

    return missing_sort


def test_limit_param():
    missing_limit = []
    for urlroot, app in app_registry.items():
        for fnname in sorted(app._api_functions()):
            fn = app._fn(fnname)
            config = app._get_config(fn)

            list_args, key_args, va, kw = config["fn_args"]

            if "limit" in dict(key_args) and config["max_page_limit"] <= 0:
                missing_limit += [[app.__class__.__name__, fnname]]

    return missing_limit
