"""Summary.

_extended_summary_
"""

from __future__ import annotations

from pathlib import Path

import pytest
from a4x.orchestration import (
    Command,
    File,
    PersistencyType,
    Scheduler,
    Site,
    StorageType,
    Task,
    Workflow,
)


def build_lulesh_workflow_example(
    sched: Scheduler, storage_type: StorageType, persistency: PersistencyType
) -> Workflow:
    """Create an LULESH example workflow with A4X-Orchestration.

    Note that this function is mostly copied from the A4X-Orchestration
    unit tests. There are minor edits to better test different aspects of this plugin.
    """
    lulesh_clone_url = "https://github.com/LLNL/LULESH.git"
    sizes = (100, 100, 100, 200, 200, 200, 300, 300, 300)
    iterations = (10, 20, 30, 10, 20, 30, 10, 20, 30)
    lulesh_clone_path = "/a4x/orchestration/repo/tests/lulesh"
    cxx_compiler = Path("/usr/bin/g++")
    mpi_home = Path("/usr")
    num_nodes = 1
    num_procs = 1
    duration = "2h"

    wflow = Workflow(
        f"lulesh_workflow_{str(sched)}_{str(storage_type)}_{str(persistency)}"
    )

    site = Site("test_site", sched)
    cwd_dir = site.add_directory(
        "cwd",
        "/a4x/orchestration/repo/tests/",
        storage_type=storage_type,
        persistency=persistency,
    )
    if storage_type != StorageType.SHARED or persistency != PersistencyType.SCRATCH:
        _ = site.add_directory(
            "lulesh_shared_scratch",
            "/tmp/scratch",  # noqa: S108
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.SCRATCH,
        )
    site.annotations["pegasus"] = {
        "arch": "x86_64",
        "os_type": "linux",
        "os_release": "rhel",
        "os_version": 8,
    }

    wflow.add_site(site)

    make_lulesh_task = Task("make_lulesh")
    lulesh_build_command = Command(f"""
git clone {lulesh_clone_url} {str(lulesh_clone_path)}
cd {str(lulesh_clone_path)}
mkdir build
cd build
cmake -DCMAKE_CXX_COMPILER={str(cxx_compiler)} -DWITH_MPI=ON -DWITH_OPENMP=ON -DMPI_HOME={str(mpi_home)} ..
make
""")  # noqa: E501
    lulesh_build_command.description = "This code clones and builds LULESH"
    make_lulesh_task.append(lulesh_build_command)
    make_lulesh_task.cwd = cwd_dir
    make_lulesh_task.queue = "debug"
    make_lulesh_task.project = "test_project"
    make_lulesh_task.set_site(site)

    wflow.add_task(make_lulesh_task)

    for size, iters in zip(sizes, iterations):
        lulesh_out_file = File(f"{size}.{iters}.log", cwd_dir)
        lulesh_out_file.resolve()
        lulesh_task = Task(f"run_lulesh_{size}_{iters}")
        lulesh_run_command = Command(
            f"{lulesh_clone_path}/build/lulesh2.0",
            "-s",
            str(size),
            "-i",
            str(iters),
            "-p",
        )
        lulesh_run_command.set_resources(
            num_procs=num_procs, num_nodes=num_nodes, allocate_nodes_exclusively=True
        )
        lulesh_task.append(lulesh_run_command)
        lulesh_task.stdout = lulesh_out_file
        lulesh_task.duration = duration
        lulesh_task.queue = "batch"
        lulesh_task.project = "test_project"
        lulesh_task.set_resources(
            num_procs=num_procs, num_nodes=num_nodes, allocate_nodes_exclusively=True
        )
        lulesh_task.add_outputs(lulesh_out_file)

        wflow.add_edge(make_lulesh_task, lulesh_task)

    return wflow


@pytest.fixture
def lulesh_flux_shared_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.FLUX, StorageType.SHARED, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_flux_shared_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.FLUX, StorageType.SHARED, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_flux_local_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.FLUX, StorageType.LOCAL, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_flux_local_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.FLUX, StorageType.LOCAL, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_slurm_shared_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.SLURM, StorageType.SHARED, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_slurm_shared_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.SLURM, StorageType.SHARED, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_slurm_local_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.SLURM, StorageType.LOCAL, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_slurm_local_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.SLURM, StorageType.LOCAL, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_sge_shared_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.SGE, StorageType.SHARED, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_sge_shared_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.SGE, StorageType.SHARED, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_sge_local_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.SGE, StorageType.LOCAL, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_sge_local_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.SGE, StorageType.LOCAL, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_pbs_shared_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.PBS, StorageType.SHARED, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_pbs_shared_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.PBS, StorageType.SHARED, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_pbs_local_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.PBS, StorageType.LOCAL, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_pbs_local_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.PBS, StorageType.LOCAL, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_lsf_shared_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.LSF, StorageType.SHARED, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_lsf_shared_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.LSF, StorageType.SHARED, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_lsf_local_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.LSF, StorageType.LOCAL, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_lsf_local_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.LSF, StorageType.LOCAL, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_condor_shared_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.CONDOR, StorageType.SHARED, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_condor_shared_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.CONDOR, StorageType.SHARED, PersistencyType.SCRATCH
    )


@pytest.fixture
def lulesh_condor_local_storage_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.CONDOR, StorageType.LOCAL, PersistencyType.PERSISTENT
    )


@pytest.fixture
def lulesh_condor_local_scratch_workflow_example() -> Workflow:  # noqa: D103
    return build_lulesh_workflow_example(
        Scheduler.CONDOR, StorageType.LOCAL, PersistencyType.SCRATCH
    )
