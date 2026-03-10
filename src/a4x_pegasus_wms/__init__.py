"""Summary.

_extended_summary_
"""

from __future__ import annotations

import inspect
import logging
import os
from functools import wraps
from pathlib import Path
from string import Template
from typing import TYPE_CHECKING, Callable, TypeVar

from a4x.orchestration import File as A4XFile
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
from Pegasus.client._client import from_env

if TYPE_CHECKING:
    from io import TextIO

    from a4x.orchestration import Directory as A4XDirectory
    from a4x.orchestration import SchedulableWork as A4XSchedulable
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
        wrapped_func_signature = inspect.signature(func)

        @wraps(func)
        def wrapper(*args: tuple, **kwargs: dict) -> T:  # noqa: ANN401
            # Get the names of keyword arguments explicitly defined by
            # the wrapped plugin method
            wrapped_func_params = set(wrapped_func_signature.parameters.keys())
            # Get the key-value pairs for kwargs defined explicitly by the
            # wrapped plugin method
            wrapped_func_kwargs = {
                k: v for k, v in kwargs.items() if k in wrapped_func_params
            }
            # Assume all kwargs not explicitly defined by the wrapped plugin
            # method to be kwargs from the Pegasus API
            pegasus_kwargs = {
                k: v for k, v in kwargs.items() if k not in wrapped_func_params
            }
            # Filter Pegasus API kwargs based on the inspected Pegasus API signature
            filtered_kwargs = {
                k: v for k, v in pegasus_kwargs.items() if k in valid_kwargs
            }
            # Invoke the function with positional args and non-Pegasus kwargs
            # passed as-is and with the filtered kwargs
            return func(*args, **wrapped_func_kwargs, **filtered_kwargs)

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
        self.workflow_file = None
        self.properties_file = None
        self.script_output_dir = None
        self.pegasus_submit_dir = None
        self.output_sites = None
        if (
            "workflow_file" in self.plugin_settings
            and self.plugin_settings["workflow_file"] is not None
        ):
            self.workflow_file = Path(self.plugin_settings["workflow_file"])
        if (
            "properties_file" in self.plugin_settings
            and self.plugin_settings["workflow_file"] is not None
        ):
            self.properties_file = Path(self.plugin_settings["properties_file"])
        if (
            "script_output_dir" in self.plugin_settings
            and self.plugin_settings["script_output_dir"] is not None
        ):
            self.script_output_dir = Path(self.plugin_settings["script_output_dir"])
        if (
            "pegasus_submit_dir" in self.plugin_settings
            and self.plugin_settings["pegasus_submit_dir"] is not None
        ):
            self.pegasus_submit_dir = Path(self.plugin_settings["pegasus_submit_dir"])
        if (
            "output_sites" in self.plugin_settings
            and self.plugin_settings["output_sites"] is not None
        ):
            self.output_sites = set(self.plugin_settings["output_sites"])

    @classmethod
    def get_default_site(cls) -> A4XSite | None:
        """Returns the default execution site for Pegasus workflows."""
        return None

    @check
    def write(
        self,
        workflow_file: str | os.PathLike | TextIO | None = None,
        properties_file: str | os.PathLike | TextIO | None = None,
    ) -> None:
        """Write Pegasus information to file."""
        self._props.write(
            file=str(properties_file)
            if isinstance(properties_file, os.PathLike)
            else properties_file
        )
        self._pegasus_workflow.write(
            file=str(workflow_file)
            if isinstance(workflow_file, os.PathLike)
            else workflow_file
        )

    @validate_keyword_args(Workflow.plan)
    @check
    def plan(
        self,
        workflow_file: str | os.PathLike | TextIO | None = None,
        properties_file: str | os.PathLike | TextIO | None = None,
        **plan_kwargs: dict,
    ) -> None:
        """Plan _summary_.

        _extended_summary_
        """
        self.write(workflow_file=workflow_file, properties_file=properties_file)
        if self.output_sites is not None:
            plan_kwargs["output_sites"] = list(self.output_sites)
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

    def transform(
        self,
        script_out_dir: os.PathLike | str | None = None,
        exist_ok: bool = True,
        use_pegasus_shared_filesystem: bool = False,
        set_auxillary_local_if_only_one_site: bool = True,
    ) -> None:
        """Transform _summary_.

        _extended_summary_
        """
        if script_out_dir is None:
            script_out_dir = Path.cwd() / "pegasus_a4x_scripts"
        a4wf = self.a4x_wflow
        # Create the Pegasus Workflow
        wf = self._pegasus_workflow = Workflow(name=a4wf.name)

        # Call self._transform_sites to create a Pegasus SiteCatalog
        self._log.debug("Adding sites to Pegasus workflow")
        site_catalog = self._transform_sites(
            a4wf, use_pegasus_shared_filesystem, set_auxillary_local_if_only_one_site
        )
        # Add the SiteCatalog to the Pegasus Workflow
        wf.add_site_catalog(site_catalog)

        self._log.debug("Adding replicas to Pegasus workflow")
        # Create a ReplicaCatalog
        rc = ReplicaCatalog()
        # Add the ReplicaCatalog to the Workflow
        wf.add_replica_catalog(rc)
        # The file_mapping dict maps A4X File objects to Pegasus File objects
        file_mapping = {}
        # Loop over A4X File objects found in the 'inputs' property
        # of every A4X Task in the workflow
        for wf_file in a4wf.all_task_inputs:
            self._log.debug(f"Adding replica {wf_file.path_attr} to Pegasus workflow")
            # Create the Pegasus File
            pegasus_file = File(str(wf_file.path_attr))
            # Add an A4X File -> Pegasus File mapping into file_mapping
            file_mapping[wf_file] = pegasus_file
            # If the A4X File is "resolved" (i.e., File.resolve has previously been
            # called) and not virtual (i.e., File is associated with a directory on
            # a Site), add a replica for the Pegasus File
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

        # Loop over A4X File objects in the `outputs` property of every A4X Task
        # in the workflow
        for wf_file in a4wf.all_task_outputs:
            self._log.debug(
                f"Adding non-replica output file {wf_file.path_attr} to Pegasus workflow"  # noqa: E501
            )
            # Create the Pegasus File and add it to 'file_mapping' if needed
            if wf_file not in file_mapping:
                pegasus_file = File(str(wf_file.path_attr))
                file_mapping[wf_file] = pegasus_file

        # Create Pegasus Job and Transformation objects from A4X Task objects
        tfs = set()
        job_map = {}
        job_children = {}

        for _task in a4wf.graph:
            # Get the actual A4X Task from the NetworkX graph
            task = a4wf.graph.nodes[_task]["task"]
            self._log.debug(f"Adding task {task.task_name} to Pegasus workflow")
            # Call self._transform_task to create the Pegasus Job and the
            # Bash script for the task
            job, job_script_path = self._transform_task(
                task,
                script_out_dir=script_out_dir,
                exist_ok=exist_ok,
                file_mapping=file_mapping,
            )
            # Create the Pegasus Transformation for the Task
            # Note that we set the site to "local" because we assume the
            # scripts are mainly available at the submission site (i.e., the
            # site where the HTCondor daemons for workflow submission are running)
            tf = Transformation(
                task.task_name,
                site="local",
                pfn=job_script_path.resolve(),
                is_stageable=True,
            )
            tfs.add(tf)
            # Add the Pegasus Job to the Workflow
            wf.add_jobs(job)
            job_map[task.task_name] = job
            job_children[task.task_name] = list(a4wf.graph.successors(task.task_name))

        self._log.debug(f"Adding {len(tfs)} transformations to Pegasus workflow")
        # Add the Pegasus Transformations to a TransformationCatalog and Workflow
        tc = TransformationCatalog()
        for tf in tfs:
            tc.add_transformations(tf)
        wf.add_transformation_catalog(tc)

        # Add job dependencies to the Workflow
        for parent_task_name, children_task_names in job_children.items():
            wf.add_dependency(
                job=job_map[parent_task_name],
                children=[job_map[child] for child in children_task_names],
            )

    def _transform_sites(
        self,
        a4wf: A4XWorkflow,
        use_pegasus_shared_filesystem: bool,
        set_auxillary_local_if_only_one_site: bool,
    ) -> SiteCatalog:
        """Create a Pegasus SiteCatalog from an A4X Workflow's 'sites' property."""
        # Create the SiteCatalog
        site_catalog = SiteCatalog()
        self.output_sites = set()
        # For each A4X Site, ...
        for a4x_site in a4wf.sites:
            # Get optional Pegasus Site constructor info
            site_info = self._transform_optional_site_info(a4x_site)
            data_configuration = None
            auxillary_local = None
            if "data_configuration" in site_info:
                data_configuration = site_info["data_configuration"]
                del site_info["data_configuration"]
            if "auxillary_local" in site_info:
                auxillary_local = site_info["auxillary_local"]
                del site_info["auxillary_local"]
            # Create the Pegasus Site object
            site = Site(a4x_site.name, **site_info)
            # Populate profiles for the Pegasus Site using the A4X Site
            self.output_sites.add(
                self._transform_grid_info(
                    a4x_site, site, data_configuration, auxillary_local
                )
            )
            has_shared_scratch = False
            # Add all directories associated with the A4X Site
            for directory in a4x_site.values():
                dir_type = self._transform_directory(
                    directory, site, use_pegasus_shared_filesystem
                )
                if dir_type == Directory.SHARED_SCRATCH:
                    has_shared_scratch = True
            if not has_shared_scratch:
                raise ValueError("Pegasus requires a shared scratch directory")
            if set_auxillary_local_if_only_one_site and len(a4wf.sites) == 1:
                site.add_pegasus_profile(auxillary_local=True)
            # Add the Pegasus Site to the SiteCatalog
            site_catalog.add_sites(site)
        if len(self.output_sites) == 0:
            self.output_sites = None
        return site_catalog

    def _transform_directory(
        self, directory: A4XDirectory, site: Site, use_pegasus_shared_filesystem: bool
    ) -> str:
        """Add a Pegasus Directory to a Pegasus Site based on an A4X Directory."""
        # TODO confirm that this is a good default
        dir_type = Directory.LOCAL_STORAGE
        shared_file_system = False
        # The Pegasus directory type is based on a combination of storage type
        # (LOCAL vs SHARED) and persistency (PERSISTENT vs SCRATCH)
        if directory.storage_type == A4XStorageType.LOCAL:
            # If the directory represents persistent storage, set the
            # Pegasus directory type to LOCAL_STORAGE
            if directory.persistency == A4XPersistency.PERSISTENT:
                dir_type = Directory.LOCAL_STORAGE
            # If the directory represents scratch/temporary storage, set the
            # Pegasus directory type to LOCAL_SCRATCH
            elif directory.persistency == A4XPersistency.SCRATCH:
                dir_type = Directory.LOCAL_SCRATCH
        elif directory.storage_type == A4XStorageType.SHARED:
            # Regardless of persistency, set shared_file_system to True
            shared_file_system = True
            # If the directory represents persistent storage, set the
            # Pegasus directory type to SHARED_STORAGE
            if directory.persistency == A4XPersistency.PERSISTENT:
                dir_type = Directory.SHARED_STORAGE
            # If the directory represents scratch/temporary storage, set the
            # Pegasus directory type to SHARED_SCRATCH
            elif directory.persistency == A4XPersistency.SCRATCH:
                dir_type = Directory.SHARED_SCRATCH
        # Create the Pegasus Directory object based on the A4X Directory object
        # and add a file server based on the A4X Directory path
        pegasus_directory = Directory(
            dir_type,
            directory.path,
            shared_file_system=shared_file_system and use_pegasus_shared_filesystem,
        ).add_file_servers(FileServer("file://" + str(directory.path), Operation.ALL))
        # If the directory's annotations include a "file_server_prefix" key under
        # "pegasus", we will add an extra file server using that prefix
        if (
            self._a4x_workflow_annotation_key in directory.annotations
            and "file_server_prefix"
            in directory.annotations[self._a4x_workflow_annotation_key]
        ):
            pegasus_directory.add_file_servers(
                FileServer(
                    directory.annotations[self._a4x_workflow_annotation_key][
                        "file_server_prefix"
                    ]
                    + str(directory.path),
                    Operation.ALL,
                )
            )
        # Add the Pegasus Directory to the Pegasus Site
        site.add_directories(pegasus_directory)
        return dir_type

    def _transform_grid_info(
        self,
        a4x_site: A4XSite,
        pegasus_site: Site,
        data_configuration: str | None,
        auxillary_local: bool | None,
    ) -> str:
        """Update the Pegasus Site with scheduler-related info from the A4X Site."""
        if auxillary_local is not None:
            pegasus_site.add_pegasus_profile(auxillary_local=auxillary_local)
        # If the scheduler is Flux, set the Pegasus profile to 'glite' and
        # set the Condor profile to 'batch flux'
        if a4x_site.scheduler == A4XScheduler.FLUX:
            pegasus_site.add_pegasus_profile(
                style="glite",
                data_configuration="sharedfs"
                if data_configuration is None
                else data_configuration,
                cores=1,
            )
            pegasus_site.add_condor_profile(grid_resource="batch flux")
            return a4x_site.name
        # If the scheduler is Slurm, set the Pegasus profile to 'glite' and
        # set the Condor profile to 'batch slurm'
        if a4x_site.scheduler == A4XScheduler.SLURM:
            pegasus_site.add_pegasus_profile(
                style="glite",
                data_configuration="sharedfs"
                if data_configuration is None
                else data_configuration,
                cores=1,
            )
            pegasus_site.add_condor_profile(grid_resource="batch slurm")
            return a4x_site.name
        # If the scheduler is SGE, set the Pegasus profile to 'glite' and
        # set the Condor profile to 'batch sge'
        if a4x_site.scheduler == A4XScheduler.SGE:
            pegasus_site.add_pegasus_profile(
                style="glite",
                data_configuration="sharedfs"
                if data_configuration is None
                else data_configuration,
                cores=1,
            )
            pegasus_site.add_condor_profile(grid_resource="batch sge")
            return a4x_site.name
        # If the scheduler is PBS, set the Pegasus profile to 'glite' and
        # set the Condor profile to 'batch pbs'
        if a4x_site.scheduler == A4XScheduler.PBS:
            pegasus_site.add_pegasus_profile(
                style="glite",
                data_configuration="sharedfs"
                if data_configuration is None
                else data_configuration,
                cores=1,
            )
            pegasus_site.add_condor_profile(grid_resource="batch pbs")
            return a4x_site.name
        # If the scheduler is LSF, set the Pegasus profile to 'glite' and
        # set the Condor profile to 'batch lsf'
        if a4x_site.scheduler == A4XScheduler.LSF:
            pegasus_site.add_pegasus_profile(
                style="glite",
                data_configuration="sharedfs"
                if data_configuration is None
                else data_configuration,
                cores=1,
            )
            pegasus_site.add_condor_profile(grid_resource="batch lsf")
            return a4x_site.name
        # If the scheduler is Condor, set the Pegasus profile to 'condor' and
        # set the Condor profile to 'vanilla'
        if a4x_site.scheduler == A4XScheduler.CONDOR:
            pegasus_site.add_pegasus_profile(
                style="condor",
                data_configuration="condorio"
                if data_configuration is None
                else data_configuration,
            )
            pegasus_site.add_condor_profile(universe="vanilla")
            return "local"
        # If the scheduler is unknown, do nothing
        if a4x_site.scheduler == A4XScheduler.UNKNOWN:
            return "local"
        # If some other value is somehow provided, error out
        raise ValueError(
            f"A4X Site {a4x_site.name} has an invalid scheduler {a4x_site.scheduler}"  # noqa: E501
        )

    def _transform_optional_site_info(self, a4x_site: A4XSite) -> dict:
        """Extract optional info for Pegasus Sites from the annotations in an A4X Site."""  # noqa: E501
        site_info = {}

        # If the plugin-specific key is not in the A4X Site's annotations, skip
        if self._a4x_workflow_annotation_key in a4x_site.annotations:
            # If 'arch' is in the annotations for this plugin, create a Pegasus Arch
            # object from the value in the annotations
            if "arch" in a4x_site.annotations[self._a4x_workflow_annotation_key]:
                site_info["arch"] = Arch(
                    a4x_site.annotations[self._a4x_workflow_annotation_key]["arch"]
                )
            # If 'os_type' is in the annotations for this plugin, create a Pegasus OS
            # object from the value in the annotations
            if "os_type" in a4x_site.annotations[self._a4x_workflow_annotation_key]:
                site_info["arch"] = OS(
                    a4x_site.annotations[self._a4x_workflow_annotation_key]["os_type"]
                )
            # If 'os_release' is in the annotations for this plugin,
            # copy the value as-is
            if "os_release" in a4x_site.annotations[self._a4x_workflow_annotation_key]:
                site_info["os_release"] = a4x_site.annotations[
                    self._a4x_workflow_annotation_key
                ]["os_release"]
            # If 'os_version' is in the annotations for this plugin,
            # copy the value as-is
            if "os_version" in a4x_site.annotations[self._a4x_workflow_annotation_key]:
                site_info["os_version"] = a4x_site.annotations[
                    self._a4x_workflow_annotation_key
                ]["os_version"]
            if (
                "data_configuration"
                in a4x_site.annotations[self._a4x_workflow_annotation_key]
            ):
                site_info["data_configuration"] = a4x_site.annotations[
                    self._a4x_workflow_annotation_key
                ]["data_configuration"]
            if (
                "auxillary_local"
                in a4x_site.annotations[self._a4x_workflow_annotation_key]
            ):
                site_info["auxillary_local"] = a4x_site.annotations[
                    self._a4x_workflow_annotation_key
                ]["auxillary_local"]
                assert isinstance(site_info["auxillary_local"]), (
                    "The 'auxillary_local' annotation must be a boolean"
                )

        return site_info

    def _transform_task(  # noqa: C901
        self,
        task: Task,
        script_out_dir: os.PathLike | str,
        exist_ok: bool,
        file_mapping: dict,
    ) -> tuple:
        # Create the Pegasus Job
        job = Job(task.task_name)

        # If the A4X Task has a site, grab it. Also, set the execution site for the Job
        # to the site name in 'task.site'. Otherwise, create a default site named
        # "condorpool" with the CONDOR scheduler.
        if task.site is not None:
            job.add_selector_profile(execution_site=task.site.name)
            task_site = task.site
        else:
            # TODO should I set the execution site to "condorpool" here?
            task_site = A4XSite("condorpool", A4XScheduler.CONDOR)

        # Create mappings of A4X Files in 'task.inputs' and 'task.outputs'
        # to their indexes in their respective lists.
        # We do this to avoid repeatedly calling 'list.index()' in
        # self._transform_commands.
        input_to_idx = {in_file: i for i, in_file in enumerate(task.inputs)}
        output_to_idx = {out_file: i for i, out_file in enumerate(task.outputs)}

        # Convert 'script_out_dir' to a pathlib.Path and create the directory
        script_out_path = Path(script_out_dir).expanduser().resolve()
        script_out_path.mkdir(parents=True, exist_ok=exist_ok)

        # Convert 'task.commands' into a Bash script for the task
        cmd_script = self._transform_commands(
            task.task_name, task.commands, task_site, input_to_idx, output_to_idx
        )
        script_out_fname = script_out_path / f"{task.task_name}.sh"
        # Write the task's Bash script to file
        with script_out_fname.open("w") as f:
            f.write(cmd_script)

        # Build arguments for the script from 'task.inputs' and 'task.outputs'
        job_args = []

        # Define inputs for the Pegasus Job based on the A4X Task's 'inputs' field
        # Also, add '-i' flags for the script based on 'inputs'
        if task.inputs:
            job_inputs = []
            for input_file in task.inputs:
                job_args.extend(["-i", file_mapping[input_file]])
                job_inputs.append(file_mapping[input_file])
            job.add_inputs(
                *job_inputs,
                **task.add_input_extra_kwargs,
            )

        # Define outputs for the Pegasus Job based on the A4X Task's 'outputs' field
        # Also, add '-o' flags for the script based on 'outputs'
        if task.outputs:
            job_outputs = []
            for output_file in task.outputs:
                job_args.extend(["-o", file_mapping[output_file]])
                job_outputs.append(file_mapping[output_file])
            job.add_outputs(
                *job_outputs,
                **task.add_output_extra_kwargs,
            )

        # Add arguments for the job
        job.add_args(*job_args)

        # Set stdin for the Pegasus Job based on the A4X Task's 'stdin' field
        if task.stdin:
            if task.stdin in task.inputs:
                job.stdin = get_path(task.stdin, file_mapping)
            else:
                job.set_stdin(get_path(task.stdin, file_mapping))

        # Set stdout for the Pegasus Job based on the A4X Task's 'stdout' field
        if task.stdout:
            if task.stdout in task.outputs:
                job.stdout = get_path(task.stdout, file_mapping)
            else:
                job.set_stdout(get_path(task.stdout, file_mapping))

        # Set stderr for the Pegasus Job based on the A4X Task's 'stderr' field
        if task.stderr:
            if task.stderr in task.outputs:
                job.stderr = get_path(task.stderr, file_mapping)
            else:
                job.set_stderr(get_path(task.stderr, file_mapping))

        # Set environment for the Pegasus Job based on the A4X
        # Task's 'environment' field
        if task.environment:
            for name, value in task.environment.items():
                job.add_env(name, value)

        # Update the Pegasus Job with scheduling information from the A4X
        # Tasks's SchedulableWork parent class
        self._transform_schedulable(job, task)

        return job, script_out_fname

    def _transform_commands(
        self,
        task_name: str,
        commands: list,
        site: A4XSite,
        input_to_idx: dict,
        output_to_idx: dict,
    ) -> str:
        """Turn a list of A4X Commands into the contents of a Bash script.

        This function will generate task definitions as Bash scripts, using a list of
        A4X Command objects to define the body of the script. To handle any A4X File
        objects in the Command objects, this function uses two dictionaries that map
        File objects to their index in either the Task.inputs or Task.outputs field.
        """
        # Create a Template for the Bash script
        command_script_template = Template(
            r"""
#!/usr/bin/env bash

function usage {
    echo "Usage: ${task_name}.sh -i <input> ... -o <output> ..."
    echo "Options:"
    echo "========"
    echo "  * -i <input>: provides an input filename. Can provide multiple times."
    echo "                Order should match the inputs provided to the Task/Job."
    echo "  * -o <output>: provides an output filename. Can provide multiple times."
    echo "                Order should match the outputs provided to the Task/Job."
}

inputs=()
outputs=()

while getopts "i:o:h" opt; do
    case $$opt in
        i)
            inputs+=("$$OPTARG")
            ;;
        o)
            outputs+=("$$OPTARG")
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$$OPTARG" >&2
            usage
            exit 1
            ;;
    esac
done

shift $$((OPTIND - 1))

$merged_command_string
""".strip()  # noqa: W605
        )
        # Build a list of strings for each command
        command_strings = []
        for cmd in commands:
            curr_cmd_str = ""
            if cmd.is_block_command:
                # If the command is a "block command",
                # just assign to curr_cmd_str
                curr_cmd_str = cmd.command_or_exe
            else:
                # If the command is not a "block command",
                # then we need to convert cmd.command_or_exe
                # and maybe cmd.args into one big string.
                #
                # We may also need to add a parallel launcher (e.g., srun)
                # invocation. We let A4X create the parallel launcher
                # invocation.
                curr_cmd_list = cmd.generate_parallel_launch(site)
                if isinstance(cmd.command_or_exe, A4XFile):
                    # If command_or_exe is an A4X File object, we assume
                    # that it is resolved. If it is not, raise an error
                    file_path = cmd.command_or_exe.path
                    if file_path is None:
                        raise RuntimeError(
                            f"Got an unresolved A4X File: {cmd.command_or_exe.path_attr}"  # noqa: E501
                        )
                    curr_cmd_list.append(str(file_path))
                else:
                    # If command_or_exe is not an A4X File object, then
                    # it is a string. So, we just add it as-is.
                    curr_cmd_list.append(cmd.command_or_exe)
                # Add the contents of cmd.args to curr_cmd_list
                for arg in cmd.args:
                    if isinstance(arg, A4XFile):
                        # If the argument is an A4X File, there are 3
                        # possible options.
                        #  1. If the File is an input to the task, the generated
                        #     Bash script should pull the value from the 'inputs' array.
                        #     We use 'input_to_idx' to determine the index into 'inputs'
                        #  2. If the File is an output to the task, the generated
                        #     Bash script should pull the value from the
                        #     'outputs' array. We use 'output_to_idx' to determine
                        #     the index into 'outputs'.
                        #  3. Otherwise, we assume the File is already resolved and
                        #     just use the path as-is. If that assumption is wrong,
                        #     raise an error.
                        if arg in input_to_idx:
                            curr_cmd_list.append(f'"${{inputs[{input_to_idx[arg]}]}}"')
                        elif arg in output_to_idx:
                            curr_cmd_list.append(
                                f'"${{outputs[{output_to_idx[arg]}]}}"'
                            )
                        else:
                            file_path = arg.path
                            if file_path is None:
                                raise RuntimeError(
                                    "Got an unresolved A4X File that is neither"
                                    f"input nor output: {arg.path.path_attr}"
                                )
                            curr_cmd_list.append(str(file_path))
                    else:
                        # If the argument is not an A4X File, just cast the value to str
                        # and append to curr_cmd_list
                        curr_cmd_list.append(str(arg))
                # Merge the contents of curr_cmd_list with spaces to create the full
                # stringified command to add to the script
                curr_cmd_str = " ".join([str(cmd_elem) for cmd_elem in curr_cmd_list])
            # Add the stringified version of the command to command_strings
            command_strings.append(curr_cmd_str)
        merged_command_string = "\n".join(command_strings)
        return command_script_template.substitute(
            task_name=task_name, merged_command_string=merged_command_string
        )

    def _transform_schedulable(self, job: Job, schedulable: A4XSchedulable) -> None:
        """Populate a Pegasus Job with information from an A4X SchedulableWork."""
        # Get the A4X Resources object
        resources = schedulable.get_resources()
        # If a duration has been specified, set the Job's 'runtime' profile entry
        if schedulable.duration is not None:
            job.add_pegasus_profile(runtime=convert_time(schedulable.duration))
        # If a queue has been specified, set the Job's 'queue' profile entry
        if schedulable.queue is not None:
            job.add_pegasus_profile(queue=schedulable.queue)
        # If a project has been specified, set the Job's 'project' profile entry
        if schedulable.project is not None:
            job.add_pegasus_profile(project=schedulable.project)
        # Only populate resource information if 'resources' is not None
        if resources is not None:
            # Get the resources per task/slot
            per_task_resources = resources.resources_per_slot
            # Check if nodes are specified in the Slot object
            nodes_not_in_slot = (
                resources.num_nodes is not None and resources.num_nodes > 0
            )
            # If nodes are NOT specified in the Slot object, ...
            if nodes_not_in_slot:
                # Set the Job's 'nodes' property using resources
                job.add_pegasus_profile(nodes=resources.num_nodes)
                # Set the Job's 'cores' property using resources and per_task_resources
                job.add_pegasus_profile(
                    cores=resources.num_procs * per_task_resources.cores
                )
                # If the number of slots per node is specified, set the Job's
                # 'ppn' profile entry
                if resources.num_slots_per_node is not None:
                    job.add_pegasus_profile(ppn=resources.num_slots_per_node)
                # If the number of GPUs is specfied in the Slot, set the Job's
                # 'gpus' profile entry
                if per_task_resources.gpus is not None:
                    job.add_pegasus_profile(
                        gpus=resources.num_procs * per_task_resources.gpus
                    )
            # If nodes are specified in the slot object, we should only set the number
            # of nodes in the Job's profile
            else:
                job.add_pegasus_profile(nodes=per_task_resources.nodes)

    def execute(
        self,
        pegasus_home: str = None,
        replan: bool = False,
        **kwargs: dict,
    ) -> None:
        """Run the Pegasus workflow."""
        if replan:
            if self._pegasus_workflow is None:
                raise RuntimeError(
                    "No Pegasus workflow found. Run 'PegasusWMS.configure' first."  # noqa: E501
                )
            self.plan(
                workflow_file=self.workflow_file,
                properties_file=self.properties_file,
                **kwargs,
            )
            self.run(**kwargs)
        else:
            if self.pegasus_submit_dir is None:
                raise RuntimeError(
                    "Cannot execute an unplanned Pegasus workflow. Run 'PegasusWMS.configure' first."  # noqa: E501
                )
            pegasus_client = from_env(pegasus_home)
            pegasus_client.run(str(self.pegasus_submit_dir))

    def create_plugin_settings_for_a4x_config(self) -> dict:
        """Get the plugin settings dict to be added to the A4X-Orchestration YAML config."""  # noqa: E501
        return {
            "workflow_file": str(self.workflow_file)
            if self.workflow_file is not None
            else None,
            "properties_file": str(self.properties_file)
            if self.properties_file is not None
            else None,
            "script_output_dir": str(self.script_output_dir)
            if self.script_output_dir is not None
            else None,
            "pegasus_submit_dir": str(self.pegasus_submit_dir)
            if self.pegasus_submit_dir is not None
            else None,
            "output_sites": tuple(self.output_sites)
            if self.output_sites is not None
            else None,
        }

    def configure_plugin(
        self,
        workflow_file: str | os.PathLike | None = None,
        properties_file: str | os.PathLike | None = None,
        script_out_dir: os.PathLike | str | None = None,
        exist_ok: bool = True,
        use_pegasus_shared_filesystem: bool = False,
        set_auxillary_local_if_only_one_site: bool = True,
        _skip_plan: bool = False,
        **plan_kwargs: dict,
    ) -> None:
        """Execute Pegasus-specific code needed for the :code:`a4x.orchestration.plugin.Plugin` to configure the workflow."""  # noqa: E501
        self.workflow_file = Path(workflow_file) if workflow_file is not None else None
        self.properties_file = (
            Path(properties_file) if properties_file is not None else None
        )
        self.script_output_dir = (
            Path(script_out_dir) if script_out_dir is not None else None
        )
        if self._pegasus_workflow is None:
            self.transform(
                script_out_dir=script_out_dir,
                exist_ok=exist_ok,
                use_pegasus_shared_filesystem=use_pegasus_shared_filesystem,
                set_auxillary_local_if_only_one_site=set_auxillary_local_if_only_one_site,
            )
        if not _skip_plan:
            self.plan(
                workflow_file=self.workflow_file,
                properties_file=self.properties_file,
                **plan_kwargs,
            )
            if self._pegasus_workflow.braindump is None:
                raise RuntimeError(
                    "Cannot get braindump after planning the Pegasus workflow"
                )
            self.pegasus_submit_dir = self._pegasus_workflow.braindump.submit_dir


def get_path(path: A4XFile | os.PathLike | str, file_mapping: dict) -> str:
    """Transform a path to a string for use in Pegasus workflow objects."""
    if isinstance(path, A4XFile):
        return file_mapping[path]

    if isinstance(path, os.PathLike):
        return str(path)

    return path


def convert_time(time_val: str | int) -> int:
    """Convert a time string to seconds.

    Since Pegasus only accepts runtime as a number of seconds, we need to convert
    string values for time to seconds.
    """
    if isinstance(time_val, int):
        return time_val
    num_seconds = None
    if ":" in time_val:
        time_split = time_val.split(":")
        if len(time_split) == 2:
            num_seconds = int(float(time_split[0]) * 60) + int(time_split[1])
        elif len(time_split) == 3:
            num_seconds = (
                int(float(time_split[0]) * 60 * 60)  # Convert hours to seconds
                + int(float(time_split[1]) * 60)  # Convert minutes to seconds
                + int(time_split[2])
            )
        else:
            raise ValueError(
                "If a runtime string from A4X contains a colon, it should either be"
                "of format 'mm:ss' or 'hh:mm:ss'"
            )
    elif time_val.endswith("h"):
        num_seconds = int(float(time_val[:-1]) * 60 * 60)
    elif time_val.endswith("m"):
        num_seconds = int(float(time_val[:-1]) * 60)
    elif time_val.endswith("s"):
        num_seconds = int(time_val[:-1])
    else:
        num_seconds = int(time_val)
    return num_seconds
