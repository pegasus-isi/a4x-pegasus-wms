"""Summary.

_extended_summary_
"""

from __future__ import annotations

import logging
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar

from Pegasus.api import (
    Job,
    ReplicaCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)

if TYPE_CHECKING:
    from a4x.orchestration import Task
    from a4x.orchestration import Workflow as A4XWorkflow

T = TypeVar("T")


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

    @check
    def plan(self) -> None:
        """Plan _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.plan()  # type: ignore[union-attr]

    @check
    def run(self) -> None:
        """Run _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.run()  # type: ignore[union-attr]

    @check
    def status(self) -> None:
        """Status _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.status()  # type: ignore[union-attr]

    @check
    def wait(self) -> None:
        """Wait _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.wait()  # type: ignore[union-attr]

    @check
    def remove(self) -> None:
        """Remove _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.remove()  # type: ignore[union-attr]

    @check
    def analyze(self) -> None:
        """Analyze _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.analyze()  # type: ignore[union-attr]

    @check
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
        wf = self._pegasus_workflow = Workflow(name=a4wf.name)
        tfs = set()

        for task in a4wf.tasks:
            self._log.debug(f"Adding task {task.task_name} to Pegasus workflow")
            job = self._transform_task(task)
            tf = Transformation(
                task.task_name,
                site="local",
                pfn=task.exe_path.resolve(),
                is_stageable=True,
            )
            tfs.add(tf)
            wf.add_job(job)

        self._log.debug(f"Adding {len(tfs)} transformations to Pegasus workflow")
        tc = TransformationCatalog()
        wf.add_transformation_catalog(tc)
        for tf in tfs:
            tc.add_transformations(tf)

        self._log.debug("Adding replicas to Pegasus workflow")
        rc = ReplicaCatalog()
        wf.add_replica_catalog(rc)
        for wf_input in a4wf.task_inputs_from_graph():
            wf.add_replicas("local", wf_input, str(wf_input.resolve()))

    def _transform_task(self, task: Task) -> Job:
        job = Job(task.task_name)
        if task.args:
            job.add_args(task.args)
        if task.inputs:
            job.add_inputs(task.inputs, task.add_input_extra_kwargs)
        if task.outputs:
            job.add_outputs(task.outputs, task.add_output_extra_kwargs)
        if task.stdin:
            job.add_stdin(task.stdin)
        if task.stdout:
            job.add_stdout(task.stdout)
        if task.stderr:
            job.add_stderr(task.stderr)

        if task.environment:
            for name, value in task.environment.items():
                job.add_env(name, value)

        resources = task.jobspec_settings.resources
        self._transform_resources(job, resources)

        return job

    def _transform_resources(self, job: Job, resources: dict) -> None:
        if not resources:
            return

        if "num_cores" in resources:
            job.add_pegasus_profile(cores=resources["num_cores"])

        if "num_nodes" in resources:
            job.add_pegasus_profile(cores=resources["num_nodes"])

        if "num_task" in resources:
            job.add_pegasus_profile(cores=resources["num_tasks"])

        if "per_resource_type" in resources:
            job.add_pegasus_profile(cores=resources["per_resource_type"])

        if "per_resource_task_count" in resources:
            job.add_pegasus_profile(cores=resources["per_resource_task_count"])

        if "gpus_per_node" in resources:
            job.add_pegasus_profile(cores=resources["gpus_per_node"])

        if "exclusive" in resources:
            job.add_pegasus_profile(cores=resources["exclusive"])
