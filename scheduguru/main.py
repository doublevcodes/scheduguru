from _queue import Empty
import threading
import loguru
import typing as t
from queue import Full, Queue
from uuid import uuid4

def construct_func_rich(name: str, args: tuple[t.Any], kwargs: dict[str, t.Any]):
    return f"{name}({', '.join([repr(arg) for arg in args] + [f'{it[0]}={it[1]}' for it in kwargs.items()])}"


class Thread(threading.Thread):

    def run(self) -> None:
        try:
            self.ret = self._target(*self._args, **self._kwargs)
        except BaseException as e:
            self.exc = e

    def join(self, timeout=None):
        super().join(timeout)
        if hasattr(self, 'exc'):
            raise self.exc
        return self.ret

class Scheduler:

    _task_id = uuid4().int

    def __init__(self, name: str, wait_time: int = 10) -> None:

        self.name = name
        self._log = loguru.logger
        self._log.debug(f"Attempted to initialise scheduler with name {self.name}")

        # The threading lock is used to ensure that multiple things don't access the task list at once
        self._thread_lock = threading.Lock()

        # The list of tasks queued for execution
        self._task_list: Queue[tuple[t.Hashable, t.Callable, tuple[t.Any], dict[t.Any, t.Any]]] = Queue()

        # This is the maximum time the scheduler can go without any tasks in the queue
        self._wait_time = wait_time

        # Initialise the scheduling thread
        self._init_scheduler()

    def __contains__(self, task_id: t.Hashable) -> bool:
        # Using a lock ensure that nothing else can access the task queue while checking
        with self._thread_lock:
            return task_id in self._task_list

    def _run_funcs_scheduled(self):

        try:
            self._log.debug(f"Attempting to find task in task queue (will wait {self._wait_time} seconds at most)")

            # Setting the timeout kwarg will raise an exception if no task is found within that time
            task = self._task_list.get(timeout=self._wait_time)

            self._log.debug(f"Found task in queue with ID {task[0]}")

            # The thread name is set to help with logging
            task_thread = Thread(target=task[1], args=task[2], kwargs=task[3], name=f"task-{task[1].__name__}-thread")


            self._log.debug(f"Created thread to run task with ID {task[0]}: {repr(task_thread)} and now attempting to execute task")
            task_thread.start()

            try:
                # Calling Thread.join will raise an exception if the thread raised an exception
                task_thread.join()
                self._log.success(f"Successfully executed task with ID {task[0]}: {construct_func_rich(task[1].__name__, task[2], task[3])}")

            except Exception as thread_error:
                self._log.warning(f"{thread_error.__class__.__name__} occurred executing the task with ID {task[0]} in thread {repr(task_thread)}")
            
            # Recursively call the function to make sure
            self._run_funcs_scheduled()

        except Exception as exc:

            # Check that the exception is because the queue was empty, which should be the only type of error raised
            if isinstance(exc, Empty):
                self._log.warning(f"A task was not found in the queue within the specified time of {self._wait_time}")
                self._log.warning(f"The scheduler {self.name} will now terminate")

            # In the unlikely event that some unknown error occurred
            else:
                self._log.critical(f"{exc.__class__.__name__} occurred during the main loop of the scheduler")

    def _init_scheduler(self):

        # Define the main loop the the scheduler will run
        sched_thread = Thread(target=self._run_funcs_scheduled, name="master-thread")
        self._log.success(f"Master scheduling thread successfully created ({repr(sched_thread)})")
        self._log.debug(f"Master scheduler thread attempting to start ({repr(sched_thread)})")

        # Start the main loop
        sched_thread.start()

    def schedule(self, task: t.Callable, args: tuple[t.Any] = (), kwargs: dict[str, t.Any] = {}):
        
        # If a specified task_id is not passed in to the function, grab the next unique task_id
        task_id = Scheduler._task_id

        self._log.debug(f"Attempting to schedule new task with ID {task_id}: {construct_func_rich(task.__name__, args, kwargs)})")

        if task_id in self._task_list.queue:
            self._log.warning(f"Task with ID {task_id}: {construct_func_rich(task.__name__, args, kwargs)} could not be scheduled for execution because a task with that ID already exists")
            return

        with self._thread_lock:
            try:
                self._task_list.put((task_id, task, args, kwargs))
            except Full:
                self._log.critical("The queue was full after the specified wait time")
        self._log.success(f"Successfully scheduled new task with ID {task_id}: {construct_func_rich(task.__name__, args, kwargs)}) for execution ASAP")

        Scheduler._task_id = uuid4().int
