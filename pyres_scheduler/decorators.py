from datetime import timedelta
from functools import update_wrapper

# Not unused, here for convenience of importing this module
from apscheduler.triggers import IntervalTrigger, CronTrigger as crontab

import pyres_scheduler

def periodic_task(priority, run_every):
    """
    Decorator to register a function with no arguments, f,
    as a task to be scheduled periodically.
    """

    def _periodic_task(f):
        per_task = PeriodicTask(f, priority, run_every)
        pyres_scheduler.periodic_tasks.append(per_task)
        return per_task
    return _periodic_task

class PeriodicTask(object):
    """
    Class that wraps a function to perform periodically
    """
    def __init__(self, func, priority, run_every):
        self.func = func
        self.priority = priority
        if isinstance(run_every, timedelta):
            run_every = IntervalTrigger(run_every)
        self.run_every = run_every

        # Allow this class to be called by pyres
        self.queue = str(priority)
        self.perform = func

        # Wrap func
        update_wrapper(self, func)

    def __call__(self):
        return self.func()

    def __repr__(self):
        return 'PeriodicTask(func=%s, priority=%s, run_every=%s)' % (self.func, self.priority, self.run_every)
