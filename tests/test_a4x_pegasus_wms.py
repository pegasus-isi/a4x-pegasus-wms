"""Summary.

_extended_summary_
"""

from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest
from a4x.orchestration import PersistencyType, Scheduler, StorageType, Workflow
from Pegasus.client._client import PegasusClientError

# This is a dependency of A4X-Orchestration, so we don't need
# an additional dependency for this
from ruamel.yaml import YAML

from .build_lulesh_yaml_utils import build_lulesh_yml, sort_config_lists

from a4x_pegasus_wms import PegasusWMS

_TEST_YAML_DIRECTORY = Path(__file__).parent / "test_pegasus_yamls"
_TEST_SCRIPT_DIRECTORY = Path(__file__).parent / "test_pegasus_scripts"


def test_check() -> None:
    """Test."""
    assert 1


def _handle_lulesh_workflow_tests(
    a4x_workflow: Workflow, scheduler: str, expected_config_builder: callable
) -> None:
    a4x_workflow.resolve()
    pegasus_plugin = PegasusWMS(a4x_workflow)

    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        pegasus_yaml_config = tmpdir_path / "workflow.yml"
        pegasus_properties_file = tmpdir_path / "pegasus.properties"
        job_script_outdir = tmpdir_path / "script_outdir"

        expected_config = expected_config_builder(job_script_outdir)

        try:
            pegasus_plugin.configure(
                workflow_file=pegasus_yaml_config,
                properties_file=pegasus_properties_file,
                script_out_dir=job_script_outdir,
                exist_ok=True,
                use_pegasus_shared_filesystem=True,
                _write_only=True,
            )
        except PegasusClientError as e:
            pytest.fail(f"pegasus-plan failed:\n{e!s}")

        yaml = YAML()

        with pegasus_yaml_config.open("r") as f:
            generated_config = yaml.load(f)
        generated_config = {
            k: generated_config[k] for k in generated_config if k in expected_config
        }

        assert sort_config_lists(generated_config) == sort_config_lists(expected_config)

        if scheduler not in ("flux", "slurm", "lsf"):
            scheduler = "default"

        for task in a4x_workflow.get_tasks_in_topological_order():
            script_name = f"{task.task_name}.sh"
            generated_script = job_script_outdir / script_name
            expected_script = _TEST_SCRIPT_DIRECTORY / scheduler / script_name

            assert generated_script.is_file()
            assert expected_script.is_file()

            with generated_script.open("r") as f:
                generated_script_lines = [
                    line.strip() for line in f if line.strip() != ""
                ]

            with expected_script.open("r") as f:
                expected_script_lines = [
                    line.strip() for line in f if line.strip() != ""
                ]

            assert generated_script_lines == expected_script_lines


def test_lulesh_flux_shared_storage_workflow(
    lulesh_flux_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_flux_shared_storage_workflow_example,
        "flux",
        partial(
            build_lulesh_yml,
            sched=Scheduler.FLUX,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_flux_shared_scratch_workflow(
    lulesh_flux_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_flux_shared_scratch_workflow_example,
        "flux",
        partial(
            build_lulesh_yml,
            sched=Scheduler.FLUX,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_flux_local_storage_workflow(
    lulesh_flux_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_flux_local_storage_workflow_example,
        "flux",
        partial(
            build_lulesh_yml,
            sched=Scheduler.FLUX,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_flux_local_scratch_workflow(
    lulesh_flux_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_flux_local_scratch_workflow_example,
        "flux",
        partial(
            build_lulesh_yml,
            sched=Scheduler.FLUX,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_slurm_shared_storage_workflow(
    lulesh_slurm_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_slurm_shared_storage_workflow_example,
        "slurm",
        partial(
            build_lulesh_yml,
            sched=Scheduler.SLURM,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_slurm_shared_scratch_workflow(
    lulesh_slurm_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_slurm_shared_scratch_workflow_example,
        "slurm",
        partial(
            build_lulesh_yml,
            sched=Scheduler.SLURM,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_slurm_local_storage_workflow(
    lulesh_slurm_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_slurm_local_storage_workflow_example,
        "slurm",
        partial(
            build_lulesh_yml,
            sched=Scheduler.SLURM,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_slurm_local_scratch_workflow(
    lulesh_slurm_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_slurm_local_scratch_workflow_example,
        "slurm",
        partial(
            build_lulesh_yml,
            sched=Scheduler.SLURM,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_sge_shared_storage_workflow(
    lulesh_sge_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_sge_shared_storage_workflow_example,
        "sge",
        partial(
            build_lulesh_yml,
            sched=Scheduler.SGE,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_sge_shared_scratch_workflow(
    lulesh_sge_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_sge_shared_scratch_workflow_example,
        "sge",
        partial(
            build_lulesh_yml,
            sched=Scheduler.SGE,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_sge_local_storage_workflow(
    lulesh_sge_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_sge_local_storage_workflow_example,
        "sge",
        partial(
            build_lulesh_yml,
            sched=Scheduler.SGE,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_sge_local_scratch_workflow(
    lulesh_sge_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_sge_local_scratch_workflow_example,
        "sge",
        partial(
            build_lulesh_yml,
            sched=Scheduler.SGE,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_pbs_shared_storage_workflow(
    lulesh_pbs_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_pbs_shared_storage_workflow_example,
        "pbs",
        partial(
            build_lulesh_yml,
            sched=Scheduler.PBS,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_pbs_shared_scratch_workflow(
    lulesh_pbs_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_pbs_shared_scratch_workflow_example,
        "pbs",
        partial(
            build_lulesh_yml,
            sched=Scheduler.PBS,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_pbs_local_storage_workflow(
    lulesh_pbs_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_pbs_local_storage_workflow_example,
        "pbs",
        partial(
            build_lulesh_yml,
            sched=Scheduler.PBS,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_pbs_local_scratch_workflow(
    lulesh_pbs_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_pbs_local_scratch_workflow_example,
        "pbs",
        partial(
            build_lulesh_yml,
            sched=Scheduler.PBS,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_lsf_shared_storage_workflow(
    lulesh_lsf_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_lsf_shared_storage_workflow_example,
        "lsf",
        partial(
            build_lulesh_yml,
            sched=Scheduler.LSF,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_lsf_shared_scratch_workflow(
    lulesh_lsf_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_lsf_shared_scratch_workflow_example,
        "lsf",
        partial(
            build_lulesh_yml,
            sched=Scheduler.LSF,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_lsf_local_storage_workflow(
    lulesh_lsf_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_lsf_local_storage_workflow_example,
        "lsf",
        partial(
            build_lulesh_yml,
            sched=Scheduler.LSF,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_lsf_local_scratch_workflow(
    lulesh_lsf_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_lsf_local_scratch_workflow_example,
        "lsf",
        partial(
            build_lulesh_yml,
            sched=Scheduler.LSF,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_condor_shared_storage_workflow(
    lulesh_condor_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_condor_shared_storage_workflow_example,
        "condor",
        partial(
            build_lulesh_yml,
            sched=Scheduler.CONDOR,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_condor_shared_scratch_workflow(
    lulesh_condor_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_condor_shared_scratch_workflow_example,
        "condor",
        partial(
            build_lulesh_yml,
            sched=Scheduler.CONDOR,
            storage_type=StorageType.SHARED,
            persistency=PersistencyType.SCRATCH,
        ),
    )


def test_lulesh_condor_local_storage_workflow(
    lulesh_condor_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_condor_local_storage_workflow_example,
        "condor",
        partial(
            build_lulesh_yml,
            sched=Scheduler.CONDOR,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.PERSISTENT,
        ),
    )


def test_lulesh_condor_local_scratch_workflow(
    lulesh_condor_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_condor_local_scratch_workflow_example,
        "condor",
        partial(
            build_lulesh_yml,
            sched=Scheduler.CONDOR,
            storage_type=StorageType.LOCAL,
            persistency=PersistencyType.SCRATCH,
        ),
    )
