"""Summary.

_extended_summary_
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar

from Pegasus.api import Workflow

if TYPE_CHECKING:
    from a4x.orchestration import Workflow as A4XWorkflow

T = TypeVar("T")


class PegasusWMS:
    """_summary_.

    _extended_summary_
    """

    def __init__(self, workflow: A4XWorkflow) -> None:
        """__init__ _summary_.

        _extended_summary_

        :param workflow: _description_
        :type workflow: A4XWorkflow
        """
        self._a4x_workflow: A4XWorkflow = workflow
        self._pegasus_workflow: Workflow | None = None
        self._log = logging.getLogger(__name__)

    @staticmethod
    def check(func: Callable[..., T]) -> Callable[..., T]:
        """Check _summary_.

        _extended_summary_

        :param func: _description_
        :type func: Callable[..., T]
        :return: _description_
        :rtype: Callable[..., T]
        """

        @wraps(func)
        def wrapper(self: PegasusWMS, *args: tuple, **kwargs: dict) -> T:
            if self._pegasus_workflow is None:
                raise ValueError("Pegasus workflow not initialized")

            return func(self, *args, **kwargs)

        return wrapper

    @check
    def plan(self) -> None:
        """Plan _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.plan()  # type: ignore[union-attr]

    def run(self) -> None:
        """Run _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.run()  # type: ignore[union-attr]

    def status(self) -> None:
        """Status _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.status()  # type: ignore[union-attr]

    def wait(self) -> None:
        """Wait _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.wait()  # type: ignore[union-attr]

    def remove(self) -> None:
        """Remove _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.remove()  # type: ignore[union-attr]

    def analyze(self) -> None:
        """Analyze _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.analyze()  # type: ignore[union-attr]

    def statistics(self) -> None:
        """Statistics _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.statistics()  # type: ignore[union-attr]

    def transform(self) -> None:
        """Transform _summary_.

        _extended_summary_
        """
        a4wf = self._a4x_workflow
        wf = self._pegasus_workflow = Workflow(name=self._a4x_workflow.name)

        for task in a4wf.tasks:
            _pegasus_task = wf.add_job(
                name=task.task_name,
                exe=task.exe_path,
                args=task.args,
                input_files=task.get_inputs(),
                output_files=task.outputs,
                **task.jobspec_settings.__dict__,
            )
            self._log.debug(f"Added task {task.task_name} to Pegasus workflow")
