"""Summary.

_extended_summary_
"""

from __future__ import annotations

import inspect
import logging
import os
from functools import wraps
from typing import TYPE_CHECKING, Callable, TypeVar

from a4x.orchestration import Path as A4XPath
from a4x.orchestration import PersistencyType as A4XPersistency
from a4x.orchestration import Scheduler as A4XScheduler
from a4x.orchestration import Site as A4XSite
from a4x.orchestration import StorageType as A4XStorageType
from a4x.orchestration.plugin import Plugin as A4XPlugin
from Pegasus.api import (
    OS,
    Arch,
    Directory,
    File,
    FileServer,
    # Arch,
    Job,
    Operation,
    Properties,
    ReplicaCatalog,
    Site,
    SiteCatalog,
    Transformation,
    TransformationCatalog,
    Workflow,
)

if TYPE_CHECKING:
    from io import TextIO

    from a4x.orchestration import Directory as A4XDirectory
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


def validate_keyword_args(reference_func: Callable[..., T]) -> Callable[..., T]:
    """Validates keyword arguments based on the signature of a reference function."""

    def decorator(func: callable) -> T:
        signature = inspect.signature(reference_func)
        valid_kwargs = set()
        for i, (name, _) in enumerate(signature.parameters.items()):
            # Skip self or cls if 'reference_func' is a method or classmethod
            if i == 0 and name in ("self", "cls"):
                continue
            valid_kwargs.add(name)

        @wraps(func)
        def wrapper(*args: tuple, **kwargs: dict) -> T:  # noqa: ANN401
            filtered_kwargs = {k: v for k, v in kwargs.items() if k in valid_kwargs}
            return func(*args, **filtered_kwargs)

        return wrapper

    return decorator


class PegasusWMS(A4XPlugin):
    """_summary_.

    _extended_summary_
    """

    def __init__(self, workflow: A4XWorkflow) -> None:
        """__init__ _summary_.

        _extended_summary_

        :param workflow: _description_
        :type workflow: A4XWorkflow
        """
        super().__init__("pegasus", workflow)
        self._a4x_workflow_annotation_key = "pegasus"
        self._pegasus_workflow: Workflow | None = None
        self._log = logging.getLogger(__name__)
        self._props = Properties()
        self._props["pegasus.mode"] = "development"
        if "JAVA_HOME" in os.environ:
            self._props["env.JAVA_HOME"] = os.environ["JAVA_HOME"]
        self.file_path = None
        if "config_file_path" in self.plugin_settings:
            self.file_path = self.plugin_settings["config_file_path"]

    @classmethod
    def get_default_site(cls) -> A4XSite | None:
        """Returns the default execution site for Pegasus workflows."""
        return None

    @validate_keyword_args(Workflow.plan)
    @check
    def plan(self, file_path: str | TextIO | None = None, **plan_kwargs: dict) -> None:
        """Plan _summary_.

        _extended_summary_
        """
        self._props.write(file=file_path)
        self._pegasus_workflow.plan(**plan_kwargs)  # type: ignore[union-attr]

    @validate_keyword_args(Workflow.run)
    @check
    def run(self, **kwargs: dict) -> None:
        """Run _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.run(**kwargs)  # type: ignore[union-attr]

    @validate_keyword_args(Workflow.status)
    @check
    def status(self, **kwargs: dict) -> dict | None:
        """Status _summary_.

        _extended_summary_
        """
        return self._pegasus_workflow.status(**kwargs)  # type: ignore[union-attr]

    @validate_keyword_args(Workflow.wait)
    @check
    def wait(self, **kwargs: dict) -> None:
        """Wait _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.wait(**kwargs)  # type: ignore[union-attr]

    @validate_keyword_args(Workflow.remove)
    @check
    def remove(self, **kwargs: dict) -> None:
        """Remove _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.remove(**kwargs)  # type: ignore[union-attr]

    @validate_keyword_args(Workflow.analyze)
    @check
    def analyze(self, **kwargs: dict) -> None:
        """Analyze _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.analyze(**kwargs)  # type: ignore[union-attr]

    @validate_keyword_args(Workflow.statistics)
    @check
    def statistics(self, **kwargs: dict) -> None:
        """Statistics _summary_.

        _extended_summary_
        """
        self._pegasus_workflow.statistics(**kwargs)  # type: ignore[union-attr]

    def transform(self) -> None:
        """Transform _summary_.

        _extended_summary_
        """
        a4wf = self.a4x_wflow
        wf = self._pegasus_workflow = Workflow(name=a4wf.name)

        self._log.debug("Adding sites to Pegasus workflow")
        site_catalog = self._transform_sites(a4wf)
        wf.add_site_catalog(site_catalog)

        self._log.debug("Adding replicas to Pegasus workflow")
        rc = ReplicaCatalog()
        wf.add_replica_catalog(rc)
        file_mapping = {}
        for wf_file in a4wf.tasks_inputs_and_outputs:
            self._log.debug(f"Adding replica {wf_file.path_attr} to Pegasus workflow")
            pegasus_file = File(str(wf_file.path_attr))
            file_mapping[wf_file] = pegasus_file
            if (
                not wf_file.is_virtual
                and wf_file.is_resolved
                and wf_file.path is not None
            ):
                rc.add_replica(
                    wf_file.directory.site.name,
                    pegasus_file,
                    str(wf_file.path),
                )
            else:
                self._log.debug(f"Skipping logical input {wf_file.path_attr}")

        # TODO tweak as needed. Mainly, add support for Sites
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

    def _transform_sites(self, a4wf: A4XWorkflow) -> SiteCatalog:
        site_catalog = SiteCatalog()
        for a4x_site in a4wf.sites:
            site_info = self._transform_optional_site_info(a4x_site)
            site = Site(a4x_site.name, **site_info)
            self._transform_grid_info(a4x_site, site)
            for directory in a4x_site.values():
                self._transform_directory(directory, site)
            site_catalog.add_sites(site)
        return site_catalog

    def _transform_directory(self, directory: A4XDirectory, site: Site) -> None:
        # TODO confirm that this is a good default
        dir_type = Directory.LOCAL_STORAGE
        shared_file_system = False
        if directory.storage_type == A4XStorageType.LOCAL:
            if directory.persistency == A4XPersistency.PERSISTENT:
                dir_type = Directory.LOCAL_STORAGE
            elif directory.persistency == A4XPersistency.SCRATCH:
                dir_type = Directory.LOCAL_SCRATCH
        elif directory.storage_type == A4XStorageType.SHARED:
            shared_file_system = True
            if directory.persistency == A4XPersistency.PERSISTENT:
                dir_type = Directory.SHARED_STORAGE
            elif directory.persistency == A4XPersistency.SCRATCH:
                dir_type = Directory.SHARED_SCRATCH
        pegasus_directory = Directory(
            dir_type, directory.path, shared_file_system=shared_file_system
        ).add_file_servers(FileServer("file://" + str(directory.path), Operation.ALL))
        site.add_directories(pegasus_directory)

    def _transform_grid_info(self, a4x_site: A4XSite, pegasus_site: Site) -> None:
        if a4x_site.scheduler == A4XScheduler.FLUX:
            pegasus_site.add_pegasus_profile(style="glite")
            pegasus_site.add_condor_profile(resource="batch flux")
        elif a4x_site.scheduler == A4XScheduler.SLURM:
            pegasus_site.add_pegasus_profile(style="glite")
            pegasus_site.add_condor_profile(resource="batch slurm")
        elif a4x_site.scheduler == A4XScheduler.SGE:
            pegasus_site.add_pegasus_profile(style="glite")
            pegasus_site.add_condor_profile(resource="batch sge")
        elif a4x_site.scheduler == A4XScheduler.PBS:
            pegasus_site.add_pegasus_profile(style="glite")
            pegasus_site.add_condor_profile(resource="batch pbs")
        elif a4x_site.scheduler == A4XScheduler.LSF:
            pegasus_site.add_pegasus_profile(style="glite")
            pegasus_site.add_condor_profile(resource="batch lsf")
        elif a4x_site.scheduler == A4XScheduler.CONDOR:
            pegasus_site.add_pegasus_profile(style="condor")
            pegasus_site.add_condor_profile(universe="vanilla")
        elif a4x_site.scheduler == A4XScheduler.UNKNOWN:
            pass
        else:
            raise ValueError(
                f"A4X Site {a4x_site.name} has an invalid scheduler {a4x_site.scheduler}"  # noqa: E501
            )

    def _transform_optional_site_info(self, a4x_site: A4XSite) -> dict:
        site_info = {}

        if self._a4x_workflow_annotation_key in a4x_site.annotations:
            if "arch" in a4x_site.annotations[self._a4x_workflow_annotation_key]:
                site_info["arch"] = Arch(
                    a4x_site.annotations[self._a4x_workflow_annotation_key]["arch"]
                )
            if "os_type" in a4x_site.annotations[self._a4x_workflow_annotation_key]:
                site_info["arch"] = OS(
                    a4x_site.annotations[self._a4x_workflow_annotation_key]["os_type"]
                )
            if "os_release" in a4x_site.annotations[self._a4x_workflow_annotation_key]:
                site_info["os_release"] = a4x_site.annotations[
                    self._a4x_workflow_annotation_key
                ]["os_release"]
            if "os_version" in a4x_site.annotations[self._a4x_workflow_annotation_key]:
                site_info["os_version"] = a4x_site.annotations[
                    self._a4x_workflow_annotation_key
                ]["os_version"]

        return site_info

    def _transform_task(self, task: Task, file_mapping: dict) -> Job:
        job = Job(task.task_name)

        # TODO rework to create a shell script out of the task's commands
        for arg in task.args or []:
            if isinstance(arg, (A4XPath, os.PathLike)):
                arg = get_path(arg)
            job.add_args(arg)

        # TODO rework inputs/outputs to use a File -> replica ID dict
        #      in place of the current logic with 'get_path'
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

        # TODO Add logic here to handle File objects when needed
        #      Alternatively, add the logic to 'get_path'
        if task.stdin:
            job.add_stdin(get_path(task.stdin))

        if task.stdout:
            job.add_stdout(get_path(task.stdout))

        if task.stderr:
            job.add_stderr(get_path(task.stderr))

        if task.environment:
            for name, value in task.environment.items():
                job.add_env(name, value)

        # TODO adjust as needed to handle changes to _transform_resources
        resources = task.jobspec_settings.resources
        self._transform_resources(job, resources)

        return job

    def _transform_resources(self, job: Job, resources: dict) -> None:
        # TODO rework to leverage the SchedulableWork base class from A4X
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

    def execute(self, **kwargs: dict) -> None:
        """Run the Pegasus workflow."""
        # TODO figure out if it's possible to build a
        #      pegasus Workflow from a config file
        if self._pegasus_workflow is None:
            self.transform()
        self.run(**kwargs)

    def create_plugin_settings_for_a4x_config(self) -> dict:
        """Get the plugin settings dict to be added to the A4X-Orchestration YAML config."""  # noqa: E501
        return {"config_file_path": self.file_path}

    def configure_plugin(
        self, file_path: str | None = None, **plan_kwargs: dict
    ) -> None:
        """Execute Pegasus-specific code needed for the :code:`a4x.orchestration.plugin.Plugin` to configure the workflow."""  # noqa: E501
        # TODO add extra args as needed for self.transform
        self.file_path = file_path
        if self._pegasus_workflow is None:
            self.transform()
        self.plan(self.file_path, **plan_kwargs)


def get_path(path: A4XPath | os.PathLike | str) -> str:
    """Transform a path to a string for use in Pegasus workflow objects."""
    if isinstance(path, A4XPath):
        return str(path.path) if path.is_logical else path.path.name

    if isinstance(path, os.PathLike):
        return str(path)

    return path
