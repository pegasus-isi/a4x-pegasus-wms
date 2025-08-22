"""Summary.

_extended_summary_
"""

from __future__ import annotations

import logging
import os
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar

from a4x.orchestration import Path as A4XPath
from Pegasus.api import (
    # Arch,
    Job,
    Properties,
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
        self._props = Properties()
        self._props["pegasus.mode"] = "development"
        if "JAVA_HOME" in os.environ:
            self._props["env.JAVA_HOME"] = os.environ["JAVA_HOME"]

    @check
    def plan(self) -> None:
        """Plan _summary_.

        _extended_summary_
        """
        self._props.write()
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

        for _task in a4wf.graph:
            task = a4wf.graph.nodes[_task]["task"]
            self._log.debug(f"Adding task {task.task_name} to Pegasus workflow")
            job = self._transform_task(task)
            tf = Transformation(
                task.task_name,
                site="local",
                pfn=task.exe_path.resolve(),
                is_stageable=True,
                # arch=Arch.AARCH64,
            )
            tfs.add(tf)
            wf.add_jobs(job)

        self._log.debug(f"Adding {len(tfs)} transformations to Pegasus workflow")
        tc = TransformationCatalog()
        wf.add_transformation_catalog(tc)
        for tf in tfs:
            tc.add_transformations(tf)

        self._log.debug("Adding replicas to Pegasus workflow")
        rc = ReplicaCatalog()
        wf.add_replica_catalog(rc)
        for _, wf_inputs in a4wf.task_inputs_from_graph.items():
            if wf_inputs:
                for wf_input in wf_inputs:
                    self._log.debug(
                        f"Adding replica {wf_input.path.name} to Pegasus workflow"
                    )
                    if wf_input.is_logical:
                        self._log.debug(f"Skipping logical input {wf_input.path.name}")
                        continue

                    rc.add_replica("local", wf_input.path.name, wf_input.path.resolve())

    def _transform_task(self, task: Task) -> Job:
        job = Job(task.task_name)

        for arg in task.args or []:
            if isinstance(arg, (A4XPath, os.PathLike)):
                arg = get_path(arg)
            job.add_args(arg)

        if task.inputs:
            job.add_inputs(
                *[get_path(input) for input in task.inputs],
                **task.add_input_extra_kwargs,
            )

        if task.outputs:
            job.add_outputs(
                *[get_path(output) for output in task.outputs],
                **task.add_output_extra_kwargs,
            )

        if task.stdin:
            job.add_stdin(get_path(task.stdin))

        if task.stdout:
            job.add_stdout(get_path(task.stdout))

        if task.stderr:
            job.add_stderr(get_path(task.stderr))

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
            job.add_pegasus_profile(nodes=resources["num_nodes"])

        if "num_task" in resources:
            job.add_pegasus_profile(cores=resources["num_task"])

        if "gpus_per_node" in resources:
            job.add_pegasus_profile(gpus=resources["gpus_per_node"])

        if "per_resource_type" in resources:
            job.add_pegasus_profile(cores=resources["per_resource_type"])

        if "per_resource_task_count" in resources:
            job.add_pegasus_profile(cores=resources["per_resource_task_count"])

        if "exclusive" in resources:
            job.add_pegasus_profile(cores=resources["exclusive"])


def get_path(path: A4XPath | os.PathLike | str) -> str:
    """Transform a path to a string for use in Pegasus workflow objects."""
    if isinstance(path, A4XPath):
        return str(path.path) if path.is_logical else path.path.name

    if isinstance(path, os.PathLike):
        return str(path)

    return path
