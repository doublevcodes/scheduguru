import queue, _queue
import threading
import loguru
import typing as t
from queue import Queue


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

    _task_count = 0

    def __init__(self, name: str, wait_time: int = 10) -> None:
        
        self.name = name
        self._log = loguru.logger

        # The threading lock is used to ensure that multiple things don't access the task list at once
        self._thread_lock = threading.Lock()

        # The list of tasks queued for execution
        self._task_list: Queue[tuple[t.Hashable, t.Callable, tuple[t.Any], dict[t.Any, t.Any]]] = Queue()
        self._wait_time = wait_time

        # Initialise the scheduling thread
        self.init_scheduler()
        self._log.success(f"Scheduler successfully initialised with name {name}")

    def __contains__(self, task_id: t.Hashable):
        with self._thread_lock:
            return task_id in self._task_list

    def run_funcs_scheduled(self):
        self._log.debug(f"Attempting to find task in task queue")
        try:
            task = self._task_list.get(timeout=self._wait_time)
            self._log.debug(f"Found task in queue with ID {task[0]}")
            task_thread = Thread(target=task[1], args=task[2], kwargs=task[3], name=f"taskid-{task[0]}-thread")
            self._log.debug(f"Created thread to run task with ID {task[0]}: {repr(task_thread)} and now attempting to execute task")
            task_thread.start()
            try:
                task_thread.join()
                self._log.success(f"Successsfully executed task with ID {task[0]}")
            except Exception:
                self._log.exception(f"An error occurred executing the task with ID {task[0]} in thread {repr(task)}")
            self.run_funcs_scheduled()
        except Exception as exc:
            if isinstance(exc, _queue.Empty):
                self._log.critical(f"A task was not found in the queue within the specified time of {self._wait_time}")
                self._log.warning(f"The scheduler {self.name} will now terminate")



    def init_scheduler(self):
        sched_thread = Thread(target=self.run_funcs_scheduled, name="master-thread")
        self._log.success(f"Master scheduling thread successfully created ({repr(sched_thread)})")
        self._log.debug(f"Master scheduler thread attempting to start ({repr(sched_thread)})")
        sched_thread.start()


    def schedule(self, task: t.Callable, args: tuple[t.Any] = (), kwargs: dict[str, t.Any] = {}, wait_time: int = 0, task_id: t.Hashable = None):
        
        if task_id is None: task_id = Scheduler._task_count

        self._log.debug(f"Attempting to schedule new task with ID {task_id}: {task.__name__}({', '.join([repr(arg) for arg in args] + [f'{it[0]}={it[1]}' for it in kwargs.items()])})")

        if task_id in self._task_list.queue:
            self._log.warning(f"Task with ID {task_id}: {task.__name__}({', '.join([repr(arg) for arg in args] + [f'{it[0]}={it[1]}' for it in kwargs.items()])}) could not be scheduled for execution because a task with that ID already exists")
            return

        with self._thread_lock:
            try:
                self._task_list.put((task_id, task, args, kwargs), timeout=wait_time)
            except queue.Full as exc:
                self._log.exception("The queue was full after the specified wait time")
        self._log.success(f"Successfully scheduled new task with ID {task_id}: {task.__name__}({', '.join([repr(arg) for arg in args] + [f'{it[0]}={it[1]}' for it in kwargs.items()])}) for execution ASAP")

        Scheduler._task_count += 1

def sched(test, more, this, param):
    if test == 1:
        raise RecursionError
    else:
        return more

sch = Scheduler(__name__)
sch.schedule(sched, (1, 2, 3, 4))
sch.schedule(sched, (2, 3, 4, 5))