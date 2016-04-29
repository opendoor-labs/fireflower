import json

import importlib
from luigi import Parameter

__all__ = [
    'Parameter',
    'JSONParameter',
    'SignatureParameter',
    'ClassParameter',
]


class JSONParameter(Parameter):
    def serialize(self, x):
        return json.dumps(x)

    def parse(self, x):
        return json.loads(x)


class SignatureParameter(JSONParameter):
    """
    Marker subclass for JSONParameter that are detected by SignatureTask as
    parameters to generate signatures for. This parameter type should be used
    whenever the parameter value has a long serialized representation.
    (e.g. experiment configs, feature lists, etc.)
    """
    pass


class ClassParameter(Parameter):
    """ Luigi parameter that holds a class or other type.
        Serialized as the fully qualified name of the type. """
    def parse(self, s):
        components = s.rsplit('.', maxsplit=1)
        if len(components) != 2:
            raise RuntimeError("expected fully qualified classname parameter, got %s" % s)
        modulename, classname = components
        module = importlib.import_module(modulename)
        return getattr(module, classname)

    def serialize(self, cls):
        return cls.__module__+"."+cls.__qualname__
