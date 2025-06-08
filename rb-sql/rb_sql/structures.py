from .types import DICT_TYPE, LIST_TYPE


# very crude.  Dig up my old dictobj from somewhere
class dictobj(object):
    def __init__(self, d=None):
        if isinstance(d, dictobj):
            self.__dict__["__dict"] = d.__dict__["__dict"]
        elif isinstance(d, LIST_TYPE):
            self.__dict__["__dict"] = dict(d)
        elif isinstance(d, DICT_TYPE):
            self.__dict__["__dict"] = d
        else:
            self.__dict__["__dict"] = {}

    def to_json(self):
        return self.__dict__["__dict"]

    def __getitem__(self, key):
        return self.__dict__["__dict"][key]

    def __setitem__(self, key, value):
        self.__dict__["__dict"][key] = value

    def __getattr__(self, key):
        if key not in self.__dict__.get("__dict", {}):
            raise AttributeError("key '{}' not found".format(key))
        return self.__dict__["__dict"][key]

    def __setattr__(self, key, value):
        self.__dict__["__dict"][key] = value

    def __repr__(self):
        return repr(self.__dict__["__dict"])

    def __len__(self):
        return len(self.__dict__["__dict"])

    def __eq__(self, other):
        if isinstance(other, dictobj):
            return self.__dict__["__dict"] == other.__dict__["__dict"]
        else:
            return self.__dict__["__dict"] == other

    def __contains__(self, key):
        return key in self.__dict__["__dict"]

    def __copy__(self):
        cls = self.__class__
        result = cls.__new__(cls)
        result.__dict__["__dict"].update(self.__dict__["__dict"])
        return result

    def clear(self):
        self.__dict__["__dict"].clear()

    def copy(self):
        return dictobj(self.__dict__["__dict"])

    def items(self):
        return self.__dict__["__dict"].items()

    def keys(self):
        return self.__dict__["__dict"].keys()

    def values(self):
        return self.__dict__["__dict"].values()

    def update(self, *args, **kwargs):
        return self.__dict__["__dict"].update(*args, **kwargs)

    def get(self, *args, **kwargs):
        return self.__dict__["__dict"].get(*args, **kwargs)

    def pop(self, *args, **kwargs):
        return self.__dict__["__dict"].pop(*args, **kwargs)

    def popitem(self, *args, **kwargs):
        return self.__dict__["__dict"].popitem(*args, **kwargs)

    def setdefault(self, *args, **kwargs):
        return self.__dict__["__dict"].setdefault(*args, **kwargs)

    def asdict(self):
        return self.__dict__["__dict"].copy()
