import json

from luigi import Parameter

__all__ = [
    'Parameter',
    'JSONParameter',
    'SignatureParameter',
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
