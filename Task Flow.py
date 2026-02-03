import time
import threading
import uuid
from enum import Enum
from typing import Callable, Dict, List, Optional


# -------------------- Enums --------------------
class TaskStatus(Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    SKIPPED = "SKIPPED"


class WorkflowStatus(Enum):
    IDLE = "IDLE"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


# -------------------- Task --------------------
class Task:
    def __init__(
        self,
        name: str,
        func: Callable,
        retries: int = 0,
        timeout: Optional[int] = None,
        depends_on: Optional[List[str]] = None
    ):
        self.id = str(uuid.uuid4())
        self.name = name
        self.func = func
        self.retries = retries
        self.timeout = timeout
        self.depends_on = depends_on or []
        self.status = TaskStatus.PENDING
        self.error: Optional[str] = None
        self.result = None

    def can_run(self, completed_tasks: Dict[str, "Task"]) -> bool:
        return all(
            dep in completed_tasks and
            completed_tasks[dep].status == TaskStatus.SUCCESS
            for dep in self.depends_on
        )


# -------------------- Workflow --------------------
class Workflow:
    def __init__(self, name: str):
        self.id = str(uuid.uuid4())
        self.name = name
        self.tasks: Dict[str, Task] = {}
        self.status = WorkflowStatus.IDLE
        self.created_at = time.time()

    def add_task(self, task: Task):
        if task.name in self.tasks:
            raise ValueError("Task name must be unique")
        self.tasks[task.name] = task

    def get_ready_tasks(self) -> List[Task]:
        completed = self.tasks
        return [
            task for task in self.tasks.values()
            if task.status == TaskStatus.PENDING and task.can_run(completed)
        ]

    def is_completed(self) -> bool:
        return all(
            task.status in (TaskStatus.SUCCESS, TaskStatus.SKIPPED)
            for task in self.tasks.values()
        )


# -------------------- Executor --------------------
class TaskExecutor:
    def __init__(self):
        self.lock = threading.Lock()

    def execute(self, task: Task):
        attempt = 0
        task.status = TaskStatus.RUNNING

        while attempt <= task.retries:
            try:
                if task.timeout:
                    result = self._run_with_timeout(task)
                else:
                    result = task.func()

                task.result = result
                task.status = TaskStatus.SUCCESS
                return

            except Exception as e:
                attempt += 1
                task.error = str(e)
                if attempt > task.retries:
                    task.status = TaskStatus.FAILED

    def _run_with_timeout(self, task: Task):
        result_container = {}
        exception_container = {}

        def target():
            try:
                result_container["result"] = task.func()
            except Exception as e:
                exception_container["error"] = e

        thread = threading.Thread(target=target)
        thread.start()
        thread.join(task.timeout)

        if thread.is_alive():
            raise TimeoutError("Task execution timed out")

        if "error" in exception_container:
            raise exception_container["error"]

        return result_container.get("result")


# -------------------- Engine --------------------
class TaskFlowEngine:
    def __init__(self):
        self.workflows: Dict[str, Workflow] = {}
        self.executor = TaskExecutor()

    def create_workflow(self, name: str) -> Workflow:
        wf = Workflow(name)
        self.workflows[wf.id] = wf
        return wf

    def run_workflow(self, workflow_id: str):
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            raise ValueError("Workflow not found")

        workflow.status = WorkflowStatus.RUNNING

        while True:
            ready_tasks = workflow.get_ready_tasks()

            if not ready_tasks:
                if workflow.is_completed():
                    workflow.status = WorkflowStatus.COMPLETED
                else:
                    workflow.status = WorkflowStatus.FAILED
                break

            threads = []
            for task in ready_tasks:
                t = threading.Thread(
                    target=self.executor.execute,
                    args=(task,)
                )
                threads.append(t)
                t.start()

            for t in threads:
                t.join()

    def workflow_status(self, workflow_id: str):
        workflow = self.workflows.get(workflow_id)
        if not workflow:
            return None

        return {
            "workflow": workflow.name,
            "status": workflow.status.value,
            "tasks": {
                name: task.status.value
                for name, task in workflow.tasks.items()
            }
        }


# -------------------- Example Usage --------------------
if __name__ == "__main__":
    engine = TaskFlowEngine()

    wf = engine.create_workflow("Sample Workflow")

    def task_a():
        time.sleep(1)
        return "A done"

    def task_b():
        time.sleep(2)
        return "B done"

    def task_c():
        time.sleep(1)
        return "C done"

    wf.add_task(Task("task_a", task_a))
    wf.add_task(Task("task_b", task_b, depends_on=["task_a"]))
    wf.add_task(Task("task_c", task_c, depends_on=["task_b"]))

    engine.run_workflow(wf.id)

    print(engine.workflow_status(wf.id))