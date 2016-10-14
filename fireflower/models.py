from datetime import datetime

import luigi
from luigi.db_task_history import DbTaskHistory, Base
from luigi.task_register import load_task
from sqlalchemy import Column, Integer, String, Text, ForeignKey, TIMESTAMP, \
    desc, DateTime, orm
from sqlalchemy.dialects.postgresql import JSONB
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
    task_id = Column(Integer, ForeignKey('tasks.id'), index=True)
    event_name = Column(String(20), index=True)
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
    task_id = Column(String, index=True)
    host = Column(String(128))
    parameters = orm.relationship(
        'TaskParameter',
        collection_class=orm.collections.attribute_mapped_collection('name'),
        cascade="all, delete-orphan")
    events = orm.relationship(
        'TaskEvent',
        order_by=(desc(TaskEvent.ts), desc(TaskEvent.id)),
        backref='task')

    def make_task(self, task_module : str) -> luigi.Task:
        """ Reifies the luigi.Task object from its name and saved parameters """
        return load_task(
            task_module,
            self.name,
            {name: param.value for name, param in self.parameters.items()})

    def __repr__(self):
        return "TaskRecord(name=%s, host=%s)" % (self.name, self.host)


class TaskOutput(FireflowerDeclBase):
    __tablename__ = 'task_outputs'

    id = Column(Integer, primary_key=True)
    task_id = Column(Text(), unique=True)
    value = Column(JSONEncoded(), nullable=True)
    task_family = Column(String(255))  # from luigi.Task.task_family
    params = Column(JSONEncoded(), nullable=True)  # from luigi.Task.str_params
    param_dict = Column(JSONB, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow, index=True)

    def __init__(self, task_id, value, task_family, params, param_dict):

        self.task_id = task_id
        self.value = value
        self.task_family = task_family
        self.params = params
        self.param_dict = param_dict

    def make_task(self, task_module : str) -> luigi.Task:
        """ Reifies the luigi.Task object from its name and saved parameters """
        return load_task(
            task_module,
            self.task_family,
            self.params)

# This query joins tasks, task_events and task_parameters together, and
# is meant to be used as a view for easy querying. Returns the luigi 
# task ID string, and a JSON array of events.
TaskHistoryView = """
select task_id, array_to_json(array_agg(row_to_json((select q from (select hostname, event_name, event_timestamp) q)))) as events
    from (
        select t.id,
               t.name||'('||coalesce(params.params,'')||')' as task_id,
               t.host as hostname,
               task_events.event_name as event_name,
               task_events.ts at time zone 'UTC' as event_timestamp 
        from tasks t
        left join (
            -- the same task signature will appear in multiple rows as task,
            -- so select out the task+params as a string to join by that
            select task_id,
                   string_agg(tp.name||'='||tp.value,', ') as params
            from task_parameters tp
            group by task_id) as params on params.task_id=t.id
        left join task_events on task_events.task_id=t.id
                         order by task_events.ts) as task_w_str_id
    group by task_id
    order by max(event_timestamp)
"""
