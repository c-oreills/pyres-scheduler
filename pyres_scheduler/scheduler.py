from datetime import datetime, timedelta
from logging import getLogger
import sys

from apscheduler.events import JobEvent, EVENT_JOB_MISSED, EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from apscheduler.job import MaxInstancesReachedError
from apscheduler.scheduler import Scheduler

logger = getLogger(__name__)

class PyresScheduler(Scheduler):
    """
    Scheduler that submits jobs to Pyres
    """
    def __init__(self, *args, **kwargs):
        super(PyresScheduler, self).__init__(*args, **kwargs)
        self._resque = None

    def _run_job(self, job, run_times):
        """
        Acts as a harness that enqueues the actual job code in Pyres in a thread.
        """
        for run_time in run_times:
            # See if the job missed its run time window, and handle possible
            # misfires accordingly
            difference = datetime.now() - run_time
            grace_time = timedelta(seconds=job.misfire_grace_time)
            if difference > grace_time:
                # Notify listeners about a missed run
                event = JobEvent(EVENT_JOB_MISSED, job, run_time)
                self._notify_listeners(event)
                logger.warning('Run time of job "%s" was missed by %s',
                               job, difference)
            else:
                try:
                    job.add_instance()
                except MaxInstancesReachedError:
                    event = JobEvent(EVENT_JOB_MISSED, job, run_time)
                    self._notify_listeners(event)
                    logger.warning('Execution of job "%s" skipped: '
                                   'maximum number of running instances '
                                   'reached (%d)', job, job.max_instances)
                    break

                logger.info('Queueing job "%s" (scheduled at %s)', job,
                            run_time)

                try:
                    assert not job.kwargs, 'Cannot enqueue resque jobs with kwargs'
                    self._resque.enqueue(job.func, *job.args)
                except:
                    # Notify listeners about the exception
                    exc, tb = sys.exc_info()[1:]
                    event = JobEvent(EVENT_JOB_ERROR, job, run_time,
                                     exception=exc, traceback=tb)
                    self._notify_listeners(event)

                    logger.exception('Job "%s" raised an exception', job)
                else:
                    # Notify listeners about successful execution
                    event = JobEvent(EVENT_JOB_EXECUTED, job, run_time)
                    self._notify_listeners(event)

                    logger.info('Job "%s" queued successfully', job)

                job.remove_instance()

                # If coalescing is enabled, don't attempt any further runs
                if job.coalesce:
                    break

    def start(self):
        if self._resque is None:
            raise Exception('Cannot start without resque instance, use add_resque method')
        super(PyresScheduler, self).start()

    def add_resque(self, resque):
        self._resque = resque
