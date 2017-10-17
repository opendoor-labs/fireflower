from unittest import mock

from fireflower import utils


def test_get_pending_task_count():
    with mock.patch('fireflower.utils.requests.get') as m:
        m.side_effect = [mock.Mock(json=lambda: {'response': {
            'pending-task-1': {},
            'pending-task-2': {},
            'pending-upstream-disabled-task-1': {},
            'pending-upstream-failed-task-1': {},
            'pending-upstream-failed-task-2': {},
        }}, status_code=200), mock.Mock(json=lambda: {'response': {
            'pending-upstream-disabled-task-1': {},
        }}, status_code=200), mock.Mock(json=lambda: {'response': {
            'pending-upstream-failed-task-1': {},
            'pending-upstream-failed-task-2': {},
        }}, status_code=200)]

        assert utils.get_pending_task_count('scheduler', 8082) == 2


def test_get_running_task_count():
    with mock.patch('fireflower.utils.requests.get') as m:
        m.return_value = mock.Mock(json=lambda: {'response': {
            'active-task-1': {},
            'active-task-2': {},
        }}, status_code=200)

        assert utils.get_running_task_count('scheduler', 8082) == 2
