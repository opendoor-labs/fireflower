from collections import namedtuple
from unittest import TestCase

from raven.base import DummyClient
from raven.contrib.flask import Sentry
from sqlalchemy.exc import DBAPIError

from fireflower import FireflowerTask
from fireflower.core import FireflowerStateManager, luigi_run_wrapper
import sys
import traceback

FakeContext = namedtuple('FakeContext', ['clear'])


class CoreTests(TestCase):
    def test_sqlalchemy_exc_handling(self):
        sentry = Sentry(client=DummyClient(), client_cls=DummyClient)
        FireflowerStateManager.register_sentry(sentry)
        @luigi_run_wrapper
        def error_raiser(self):
            inner_func()

        def inner_func():
            raise DBAPIError('test error', {}, None)

        class FakeTask(FireflowerTask):
            def __init__(self):
                self.param_args = []
                self.param_kwargs = {}
        try:
            error_raiser(FakeTask())
        except:
            type_, _, tb = sys.exc_info()
            self.assertEqual(type_, DBAPIError)
            self.assertEqual(traceback.extract_tb(tb)[3].name, 'inner_func')
