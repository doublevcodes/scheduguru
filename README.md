# Greetings from Scheduguru

Scheduguru was a project I created very recently to enrichen my learning of threading and asynchronous programming in Python.

## How to use

Currently there's not much to go by so this is all the documentation you get:

### The `Scheduler` class

This is the main class that Scheduguru revolves around. When initialised the following parameters can be passed in:

- `name: str`: This is a string that defines the Scheduler's name. Has no functional significance. If in doubt, set this to the value of `__name__`
- `wait_time: int`: An integer (in seconds) that defines how long the scheduler should wait for new tasks without terminating

There is only one publicly available method which is the `Scheduler.schedule` method.

### The `Scheduler.schedule` method

This method schedules an event for execution as soon as possible. It adds it to the task queue which the scheduler will listen for changes to and execute any new tasks.

The specification of the `Scheduler.schedule` method is as follows:

- `task: Callable` - a **reference** to the function which is to be executed
- `args: tuple[Any]` - any positional arguments to be passed into the function
- `kwargs: dict[str, Any]` - any keyword arguments to be passed into the function

### Go and explore the rest! _(not that there is anything else)_
