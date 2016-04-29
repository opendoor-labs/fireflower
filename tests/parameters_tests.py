import luigi
from luigi.task_register import load_task
from unittest import TestCase
import collections

from fireflower.parameters import ClassParameter, JSONParameter


class TestJsonTask(luigi.Task):
    param = JSONParameter()


class TestClassTask(luigi.Task):
    param = ClassParameter()


class ParametersTests(TestCase):
    def test_json_param(self):
        task = TestJsonTask(param={'pi': 3.14})

        serialized = task.to_str_params()
        self.assertEqual(serialized['param'], '{"pi": 3.14}')

        deserialized = load_task(None, 'TestJsonTask', {"param": '{"pi": 3.14}'})
        self.assertEqual(deserialized.param, {'pi': 3.14})

    def test_class_param(self):
        task = TestClassTask(param=collections.OrderedDict)

        serialized = task.to_str_params()
        self.assertEqual(serialized['param'], "collections.OrderedDict")

        deserialized = load_task(None, 'TestClassTask', {"param": "collections.OrderedDict"})
        self.assertEqual(deserialized.param, collections.OrderedDict)
