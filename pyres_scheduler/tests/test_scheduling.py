from apscheduler.job import Job
from apscheduler.triggers import IntervalTrigger
from utils.testing import TestCase, Mock

from pyres_scheduler.scheduling import triggers_are_identical, process_periodic_tasks
from pyres_scheduler.decorators import periodic_task, crontab


from datetime import datetime, timedelta

class TestTriggerIdenticalChecks(TestCase):
    def test_interval_similarity(self):
        interval_a = IntervalTrigger(timedelta(hours=1), datetime.now())
        interval_b = IntervalTrigger(timedelta(minutes=60), datetime.now())
        interval_c = IntervalTrigger(timedelta(seconds=60*60), datetime.now() + timedelta(hours=1))
        interval_d = IntervalTrigger(timedelta(seconds=1), datetime.now())

        self.assertTrue(triggers_are_identical(interval_a, interval_a))
        self.assertTrue(triggers_are_identical(interval_a, interval_b))
        self.assertTrue(triggers_are_identical(interval_a, interval_c))
        self.assertFalse(triggers_are_identical(interval_a, interval_d))

    def test_crontab_similarity(self):
        crontab_a = crontab(hour=1)
        crontab_b = crontab(minute=10)
        crontab_c = crontab(hour=2)
        crontab_d = crontab(hour=2)
        crontab_e = crontab(hour=2, start_date=datetime.now() + timedelta(days=2))

        self.assertTrue(triggers_are_identical(crontab_a, crontab_a))
        self.assertFalse(triggers_are_identical(crontab_a, crontab_b))
        self.assertFalse(triggers_are_identical(crontab_a, crontab_c))
        self.assertTrue(triggers_are_identical(crontab_c, crontab_d))
        self.assertFalse(triggers_are_identical(crontab_c, crontab_e))

    def test_crontab_bad_kwargs(self):
        # By default, the apscheduler doesn't error when we give cron bad kwargs
        # This can lead to silently dropping args (e.g. when putting 'hours'
        # instead of 'hour'). It needs patching!
        try:
            crontab(seeecond=10, third=20)
        except:
            pass
        else:
            pass
            # TODO: reenable this once APScheduler releases new version, until then be careful
            #self.warn('Successfully called crontab with bad arguments')

class TestProcessPeriodicTasks(TestCase):
    def test_same_task(self):
        trigger = IntervalTrigger(timedelta(hours=1))

        @periodic_task(priority=9, run_every=trigger)
        def test_fn():
            pass

        job = Mock(trigger=trigger, func=test_fn)

        jobs_to_unschedule, tasks_to_add = process_periodic_tasks(
               [job], [test_fn])

        self.assertEqual(jobs_to_unschedule, [])
        self.assertEqual(tasks_to_add, [])

    def test_same_func_diff_priorities(self):
        # This test isn't strictly necessary, since APScheduler doesn't store
        # priority information to db, it loads it from the module each time.
        # Were that to change, this should fail!
        trigger = IntervalTrigger(timedelta(hours=1))

        def test_fn():
            pass

        test_fn_a = periodic_task(priority=9, run_every=trigger)(test_fn)
        test_fn_b = periodic_task(priority=8, run_every=trigger)(test_fn)

        job = Mock(trigger=trigger, func=test_fn_a)

        jobs_to_unschedule, tasks_to_add = process_periodic_tasks(
               [job], [test_fn_b])

        self.assertEqual(jobs_to_unschedule, [job])
        self.assertEqual(tasks_to_add, [test_fn_b])

    def test_same_func_diff_triggers(self):
        trigger_a = IntervalTrigger(timedelta(hours=1))
        trigger_b = IntervalTrigger(timedelta(hours=2))

        def test_fn():
            pass

        test_fn_a = periodic_task(priority=9, run_every=trigger_a)(test_fn)
        test_fn_b = periodic_task(priority=9, run_every=trigger_b)(test_fn)

        job = Mock(trigger=trigger_a, func=test_fn_a)

        jobs_to_unschedule, tasks_to_add = process_periodic_tasks(
               [job], [test_fn_b])

        self.assertEqual(jobs_to_unschedule, [job])
        self.assertEqual(tasks_to_add, [test_fn_b])

    def test_duplicate_tasks(self):
        trigger = IntervalTrigger(timedelta(hours=1))

        @periodic_task(priority=9, run_every=trigger)
        def test_fn():
            pass

        jobs_to_unschedule, tasks_to_add = process_periodic_tasks(
               [], [test_fn, test_fn])

        self.assertEqual(jobs_to_unschedule, [])
        self.assertEqual(tasks_to_add, [test_fn])
