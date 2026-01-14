"""Summary.

_extended_summary_
"""

from pathlib import Path
from tempfile import TemporaryDirectory

from a4x.orchestration import Workflow

# This is a dependency of A4X-Orchestration, so we don't need
# an additional dependency for this
from ruamel.yaml import YAML

from a4x_pegasus_wms import PegasusWMS

_TEST_YAML_DIRECTORY = Path(__file__).parent / "test_pegasus_yamls"
_TEST_SCRIPT_DIRECTORY = Path(__file__).parent / "test_pegasus_scripts"


def test_check() -> None:
    """Test."""
    assert 1


def _handle_lulesh_workflow_tests(a4x_workflow: Workflow, expected_yaml: Path) -> None:
    a4x_workflow.resolve()
    pegasus_plugin = PegasusWMS(a4x_workflow)
    with TemporaryDirectory() as tmpdir:
        tmpdir_path = Path(tmpdir)
        pegasus_yaml_config = tmpdir_path / expected_yaml.name
        job_script_outdir = tmpdir_path / "script_outdir"

        pegasus_plugin.configure(
            file_path=pegasus_yaml_config,
            script_out_dir=job_script_outdir,
            exist_ok=True,
        )

        yaml = YAML()

        preserve_keys = (
            "name",
            "jobs",
            "jobDependencies",
            "replicas",
            "transformations",
            "sites",
        )

        with pegasus_yaml_config.open("r") as f:
            generated_config = yaml.load(f)
        generated_config = {
            k: generated_config[k] for k in generated_config if k in preserve_keys
        }

        with expected_yaml.open("r") as f:
            expected_config = yaml.load(f)
        expected_config = {
            k: expected_config[k] for k in expected_config if k in preserve_keys
        }

        assert generated_config == expected_config

        for task in a4x_workflow.get_tasks_in_topological_order():
            script_name = f"{task.task_name}.sh"
            generated_script = job_script_outdir / script_name
            expected_script = _TEST_SCRIPT_DIRECTORY / script_name

            assert generated_script.is_file()
            assert expected_script.is_file()

            with generated_script.open("r") as f:
                generated_script_lines = [line for line in f if line.strip() != ""]

            with expected_script.open("r") as f:
                expected_script_lines = [line for line in f if line.strip() != ""]

            assert generated_script_lines == expected_script_lines


def test_lulesh_flux_shared_storage_workflow(
    lulesh_flux_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_flux_shared_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "flux_shared_storage.yaml",
    )


def test_lulesh_flux_shared_scratch_workflow(
    lulesh_flux_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_flux_shared_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "flux_shared_scratch.yaml",
    )


def test_lulesh_flux_local_storage_workflow(
    lulesh_flux_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_flux_local_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "flux_local_storage.yaml",
    )


def test_lulesh_flux_local_scratch_workflow(
    lulesh_flux_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_flux_local_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "flux_local_scratch.yaml",
    )


def test_lulesh_slurm_shared_storage_workflow(
    lulesh_slurm_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_slurm_shared_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "slurm_shared_storage.yaml",
    )


def test_lulesh_slurm_shared_scratch_workflow(
    lulesh_slurm_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_slurm_shared_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "slurm_shared_scratch.yaml",
    )


def test_lulesh_slurm_local_storage_workflow(
    lulesh_slurm_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_slurm_local_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "slurm_local_storage.yaml",
    )


def test_lulesh_slurm_local_scratch_workflow(
    lulesh_slurm_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_slurm_local_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "slurm_local_scratch.yaml",
    )


def test_lulesh_sge_shared_storage_workflow(
    lulesh_sge_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_sge_shared_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "sge_shared_storage.yaml",
    )


def test_lulesh_sge_shared_scratch_workflow(
    lulesh_sge_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_sge_shared_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "sge_shared_scratch.yaml",
    )


def test_lulesh_sge_local_storage_workflow(
    lulesh_sge_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_sge_local_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "sge_local_storage.yaml",
    )


def test_lulesh_sge_local_scratch_workflow(
    lulesh_sge_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_sge_local_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "sge_local_scratch.yaml",
    )


def test_lulesh_pbs_shared_storage_workflow(
    lulesh_pbs_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_pbs_shared_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "pbs_shared_storage.yaml",
    )


def test_lulesh_pbs_shared_scratch_workflow(
    lulesh_pbs_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_pbs_shared_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "pbs_shared_scratch.yaml",
    )


def test_lulesh_pbs_local_storage_workflow(
    lulesh_pbs_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_pbs_local_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "pbs_local_storage.yaml",
    )


def test_lulesh_pbs_local_scratch_workflow(
    lulesh_pbs_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_pbs_local_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "pbs_local_scratch.yaml",
    )


def test_lulesh_lsf_shared_storage_workflow(
    lulesh_lsf_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_lsf_shared_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "lsf_shared_storage.yaml",
    )


def test_lulesh_lsf_shared_scratch_workflow(
    lulesh_lsf_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_lsf_shared_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "lsf_shared_scratch.yaml",
    )


def test_lulesh_lsf_local_storage_workflow(
    lulesh_lsf_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_lsf_local_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "lsf_local_storage.yaml",
    )


def test_lulesh_lsf_local_scratch_workflow(
    lulesh_lsf_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_lsf_local_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "lsf_local_scratch.yaml",
    )


def test_lulesh_condor_shared_storage_workflow(
    lulesh_condor_shared_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_condor_shared_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "condor_shared_storage.yaml",
    )


def test_lulesh_condor_shared_scratch_workflow(
    lulesh_condor_shared_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_condor_shared_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "condor_shared_scratch.yaml",
    )


def test_lulesh_condor_local_storage_workflow(
    lulesh_condor_local_storage_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_condor_local_storage_workflow_example,
        _TEST_YAML_DIRECTORY / "condor_local_storage.yaml",
    )


def test_lulesh_condor_local_scratch_workflow(
    lulesh_condor_local_scratch_workflow_example: Workflow,
) -> None:
    _handle_lulesh_workflow_tests(
        lulesh_condor_local_scratch_workflow_example,
        _TEST_YAML_DIRECTORY / "condor_local_scratch.yaml",
    )
