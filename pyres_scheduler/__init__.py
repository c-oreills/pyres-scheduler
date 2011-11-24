# Here for convenience
from pyres_scheduler.scheduler import PyresScheduler

# Populated by periodic_task decorators
periodic_tasks = []

def import_tasks(module_list):
    """
    Imports modules from module_list, thereby triggering any periodic_task
    decorators in them and populating the above periodic_tasks list
    """
    for module in module_list:
        __import__(module)
