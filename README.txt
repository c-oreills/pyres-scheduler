Wraps pyres and APScheduler so tasks can be scheduled asynchronously


Example usage:
    from pyres import ResQ
    from pyres_scheduler import PyresScheduler
    from pyres_scheduler.decorators import periodic_task, timedelta

    pyres_sched = PyresScheduler()
    resque = ResQ()
    pyres_sched.add_resque(resque)

    @periodic_task(priority='high', run_every=timedelta(seconds=60))
    def my_task():
        print 'Executing task'

    pyres_sched.start()
    pyres_sched.add_job(my_task)


Due to the nature of pyres, functions must be available for import in order to
enqueue them (i.e. you cannot enqueue functions defined in an interactive
Python prompt, they must be saved and accessible to your pyres_worker)
