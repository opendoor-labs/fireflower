from datetime import datetime

from luigi.db_task_history import DbTaskHistory, Base
from sqlalchemy import Column, Integer, String, Text, ForeignKey, TIMESTAMP, \
    desc, DateTime, orm
from sqlalchemy.ext.declarative import declarative_base

from fireflower.core import FireflowerStateManager
from fireflower.utils import JSONEncoded

__all__ = [
    'FireflowerTaskHistory',
    'TaskRecord',
    'TaskEvent',
    'TaskParameter',
]

FireflowerDeclBase = declarative_base()


class FireflowerTaskHistory(DbTaskHistory):
    """
    Overrides Luigi's internal DbTaskHistory class so we can use Signal's
    DB session and avoid exposing our DB connection url in a .cfg file.
    """
    def __init__(self):
        self.engine = FireflowerStateManager.session.bind
        self.session_factory = orm.sessionmaker(bind=self.engine,
                                                expire_on_commit=False)
        Base.metadata.create_all(self.engine)
        self.tasks = {}  # task_id -> TaskRecord


class TaskParameter(FireflowerDeclBase):
    """
    Table to track luigi.Parameter()s of a Task.
    """
    __tablename__ = 'task_parameters'

    task_id = Column(Integer, ForeignKey('tasks.id'), primary_key=True)
    name = Column(String(128), primary_key=True)
    value = Column(Text())

    def __repr__(self):
        return "TaskParameter(task_id=%d, name=%s, value=%s)" % (self.task_id,
                                                                 self.name,
                                                                 self.value)


class TaskEvent(FireflowerDeclBase):
    """
    Table to track when a task is scheduled, starts, finishes, and fails.
    """
    __tablename__ = 'task_events'

    id = Column(Integer, primary_key=True)
    task_id = Column(Integer, ForeignKey('tasks.id'))
    event_name = Column(String(20))
    ts = Column(TIMESTAMP, index=True, nullable=False)

    def __repr__(self):
        return "TaskEvent(task_id=%s, event_name=%s, ts=%s" % (self.task_id,
                                                               self.event_name,
                                                               self.ts)


class TaskRecord(FireflowerDeclBase):
    """
    Base table to track information about a luigi.Task.

    References to other tables are available through task.events,
    task.parameters, etc.
    """
    __tablename__ = 'tasks'

    id = Column(Integer, primary_key=True)
    name = Column(String(128), index=True)
    host = Column(String(128))
    parameters = orm.relationship(
        'TaskParameter',
        collection_class=orm.collections.attribute_mapped_collection('name'),
        cascade="all, delete-orphan")
    events = orm.relationship(
        'TaskEvent',
        order_by=(desc(TaskEvent.ts), desc(TaskEvent.id)),
        backref='task')

    def __repr__(self):
        return "TaskRecord(name=%s, host=%s)" % (self.name, self.host)


class TaskOutput(FireflowerDeclBase):
    __tablename__ = 'task_outputs'

    id = Column(Integer, primary_key=True)
    task_id = Column(String(255), unique=True)
    value = Column(JSONEncoded(), nullable=True)
    task_family = Column(String(255))  # from luigi.Task.task_family
    params = Column(JSONEncoded(), nullable=True)  # from luigi.Task.str_params
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    def __init__(self, task_id, value, task_family, params):
        self.task_id = task_id
        self.value = value
        self.task_family = task_family
        self.params = params
