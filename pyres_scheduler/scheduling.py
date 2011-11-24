from logging import getLogger

from apscheduler.triggers import IntervalTrigger, CronTrigger

logger = getLogger(__name__)

def schedule_periodic_tasks(sched, periodic_tasks):
    jobs_to_unschedule, tasks_to_add = process_periodic_tasks(
            sched.get_jobs(), periodic_tasks)
    for job in jobs_to_unschedule:
        try:
            sched.unschedule_job(job)
        except KeyError:
            logger.exception('No such job to unschedule')
        else:
            logger.info('Unscheduled job %s' % (job,))
    for task in tasks_to_add:
        try:
            job = sched.add_job(trigger=task.run_every, func=task,
                          args=None, kwargs=None)
        except:
            logger.exception('Error adding task')
        else:
            logger.info('Scheduled job %s' % (job,))

def process_periodic_tasks(sched_jobs, periodic_tasks):
    """
    Returns jobs to unschedule and tasks to add
    """
    sched_func_to_job = {job.func: job for job in sched_jobs}
    # Each job should call a unique function
    assert len(sched_func_to_job) == len(sched_jobs)

    jobs_to_unschedule, tasks_to_add = [], []

    for periodic_task in set(periodic_tasks):
        if periodic_task in sched_func_to_job:
            sched_job = sched_func_to_job.pop(periodic_task)
            if triggers_are_identical(sched_job.trigger, periodic_task.run_every):
                continue
            else:
                jobs_to_unschedule.append(sched_job)
                tasks_to_add.append(periodic_task)
        else:
            tasks_to_add.append(periodic_task)

    for job in sched_func_to_job.itervalues():
        jobs_to_unschedule.append(job)

    return jobs_to_unschedule, tasks_to_add

def triggers_are_identical(trigger_a, trigger_b):
    if isinstance(trigger_a, IntervalTrigger):
        # with regards to interval triggers we don't care about the start_date
        # (start_date is omitted in stringify)
        return str(trigger_a) == str(trigger_b)
    elif isinstance(trigger_a, CronTrigger):
        # we care about start_date for crontabs if specified (defaults to blank)
        return repr(trigger_a) == repr(trigger_b)
    raise Exception('triggers_are_identical fell through without returning '
                    'True or False, args: %s, %s' % (trigger_a, trigger_b))
