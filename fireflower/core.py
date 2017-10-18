from functools import wraps
from shutil import copyfileobj
import sys
import structlog
import contextlib
import uuid

from luigi.interface import _WorkerSchedulerFactory
from luigi.rpc import RemoteScheduler
from luigi.scheduler import Scheduler, SimpleTaskState
from luigi.s3 import S3Target

__all__ = [
    'FireflowerStateManager',
    'FireflowerWorkerSchedulerFactory',
    'FireflowerCentralPlannerScheduler',
]

logger = structlog.get_logger()


class FireflowerStateManager(object):
    """
    Fireflower expects a SqlAlchemy session and a sentry object to be
    registered when the luigi process is booted up. See signals.pipeline.luigid
    for example.
    """
    session = None
    structlog_threadlocal = False
    sentry = None

    @classmethod
    def register_sqlalchemy_session(cls, session):
        cls.session = session

    @classmethod
    def register_sentry(cls, sentry):
        cls.sentry = sentry

    @classmethod
    def register_structlog_threadlocal(cls):
        """ call if you've configured structlog for threadlocal storage;
            fireflower will stick task info in structlog """
        cls.structlog_threadlocal = True

    @classmethod
    @contextlib.contextmanager
    def bind_structlog(cls, **kwargs):
        if cls.structlog_threadlocal:
            with structlog.threadlocal.tmp_bind(logger, **kwargs):
                yield
        else:
            yield


def luigi_run_wrapper(func):
    if (FireflowerStateManager.sentry is None
            and not FireflowerStateManager.structlog_threadlocal):
        # no wrapping necessary
        return func

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        task_uuid = str(uuid.uuid4())
        try:
            logger.new()  # Clear everything from previous .bind() calls
            logger.bind(task_uuid=task_uuid)
            with FireflowerStateManager.bind_structlog(
                    uuid=task_uuid,
                    task_family=self.task_family):
                return func(self, *args, **kwargs)
        except Exception:
            if (FireflowerStateManager.sentry and
                    FireflowerStateManager.sentry.client):
                extra = {
                    'task_uuid': task_uuid,
                    'task_family': self.task_family,
                    'task_args': self.param_args,
                    'task_kwargs': self.param_kwargs,
                }
                FireflowerStateManager.sentry.captureException(extra=extra)
            raise
        finally:
            if (FireflowerStateManager.sentry and
                    FireflowerStateManager.sentry.client):
                FireflowerStateManager.sentry.client.context.clear()
    return wrapper


class FireflowerWorkerSchedulerFactory(_WorkerSchedulerFactory):
    def __init__(self, remote_host='', remote_port='', s3_state_path=''):
        self._remote_host = remote_host
        self._remote_port = remote_port
        self._s3_state_path = s3_state_path

    def create_local_scheduler(self):
        from fireflower.models import FireflowerTaskHistory
        task_history = FireflowerTaskHistory()
        if self._s3_state_path:
            state = S3TaskState('state.pkl', self._s3_state_path)
        else:
            state = None
        return FireflowerCentralPlannerScheduler(prune_on_get_work=True,
                                                 task_history_impl=task_history,
                                                 state=state)

    def create_remote_scheduler(self, url=None):
        url = url or 'http://{}:{}'.format(self._remote_host, self._remote_port)
        return RemoteScheduler(url)


class FireflowerCentralPlannerScheduler(Scheduler):
    def __init__(self, *args, **kwargs):
        state = kwargs.pop('state') if 'state' in kwargs else None
        super(FireflowerCentralPlannerScheduler, self).__init__(*args, **kwargs)
        if state is not None:
            self._state = state


class S3TaskState(SimpleTaskState):
    def __init__(self, local_path, s3_path):
        super(S3TaskState, self).__init__(local_path)
        self._s3_file = S3Target(s3_path)

    def dump(self):
        super(S3TaskState, self).dump()
        with open(self._state_path, 'r') as f_src, \
                self._s3_file.open('w') as f_dst:
            copyfileobj(f_src, f_dst)

    def load(self):
        if self._s3_file.exists():
            with self._s3_file.open('r') as f_src, \
                    open(self._state_path, 'w') as f_dst:
                copyfileobj(f_src, f_dst)
        super(S3TaskState, self).load()
