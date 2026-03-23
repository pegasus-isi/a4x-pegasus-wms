"""Summary.

_extended_summary_
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from a4x.orchestration import PersistencyType, Scheduler, StorageType

if TYPE_CHECKING:
    from pathlib import Path


def _get_pegasus_dir_type(
    sched: Scheduler,
    storage_type: StorageType,
    persistency: PersistencyType,
) -> tuple[bool, str]:
    dir_type = "localStorage"
    shared_file_system = False
    if storage_type == StorageType.LOCAL:
        if persistency == PersistencyType.PERSISTENT:
            dir_type = "localStorage"
        elif persistency == PersistencyType.SCRATCH:
            dir_type = "localScratch"
    elif storage_type == StorageType.SHARED:
        shared_file_system = True
        if persistency == PersistencyType.PERSISTENT:
            dir_type = "sharedStorage"
        elif persistency == PersistencyType.SCRATCH:
            dir_type = "sharedScratch"
    if sched == Scheduler.CONDOR:
        shared_file_system = False
    return shared_file_system, dir_type


def _get_grid_info(sched: Scheduler) -> tuple[dict[str, Any], dict[str, Any]]:
    # TODO consider adding auxillary.local
    if sched == Scheduler.FLUX:
        return {
            "style": "glite",
            "data.configuration": "sharedfs",
            "cores": 1,
            "auxillary.local": "True",
        }, {"grid_resource": "batch flux"}
    if sched == Scheduler.SLURM:
        return {
            "style": "glite",
            "data.configuration": "sharedfs",
            "cores": 1,
            "auxillary.local": "True",
        }, {"grid_resource": "batch slurm"}
    if sched == Scheduler.SGE:
        return {
            "style": "glite",
            "data.configuration": "sharedfs",
            "cores": 1,
            "auxillary.local": "True",
        }, {"grid_resource": "batch sge"}
    if sched == Scheduler.PBS:
        return {
            "style": "glite",
            "data.configuration": "sharedfs",
            "cores": 1,
            "auxillary.local": "True",
        }, {"grid_resource": "batch pbs"}
    if sched == Scheduler.LSF:
        return {
            "style": "glite",
            "data.configuration": "sharedfs",
            "cores": 1,
            "auxillary.local": "True",
        }, {"grid_resource": "batch lsf"}
    if sched == Scheduler.CONDOR:
        return {
            "style": "condor",
            "data.configuration": "condorio",
            "auxillary.local": "True",
        }, {"universe": "vanilla"}
    raise ValueError(f"Unknown scheduler type: {str(sched)}")


def build_lulesh_yml(
    jobscript_dir: Path,
    sched: Scheduler,
    storage_type: StorageType,
    persistency: PersistencyType,
) -> dict:
    dir_shared_fs, dir_type = _get_pegasus_dir_type(sched, storage_type, persistency)
    pegasus_profile, condor_profile = _get_grid_info(sched)
    base_yml: dict[str, Any] = {
        "name": f"lulesh_workflow_{str(sched)}_{str(storage_type)}_{str(persistency)}",
        "siteCatalog": {
            "sites": [
                {
                    "name": "test_site",
                    "arch": "x86_64",
                    "os.type": "linux",
                    "os.release": "rhel",
                    "os.version": 8,
                    "directories": [
                        {
                            "type": dir_type,
                            "path": "/a4x/orchestration/repo/tests",
                            "sharedFileSystem": dir_shared_fs,
                            "fileServers": [
                                {
                                    "url": "file:///a4x/orchestration/repo/tests",
                                    "operation": "all",
                                }
                            ],
                        }
                    ],
                    "profiles": {
                        "condor": condor_profile,
                        "pegasus": pegasus_profile,
                    },
                }
            ]
        },
        "replicaCatalog": {"replicas": []},
        "transformationCatalog": {
            "transformations": [
                {
                    "name": "make_lulesh",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'make_lulesh.sh')!s}",
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_200_20",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_200_20.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_300_10",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_300_10.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_200_10",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_200_10.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_300_20",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_300_20.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_300_30",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_300_30.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_100_30",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_100_30.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_100_10",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_100_10.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_100_20",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_100_20.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
                {
                    "name": "run_lulesh_200_30",
                    "sites": [
                        {
                            "name": "local",
                            "pfn": f"{(jobscript_dir / 'run_lulesh_200_30.sh')!s}",  # noqa: E501
                            "type": "stageable",
                        }
                    ],
                },
            ]
        },
        "jobs": [
            {
                "type": "job",
                "name": "make_lulesh",
                "id": "ID0000001",
                "arguments": [],
                "uses": [],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "debug",
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_100_10",
                "id": "ID0000002",
                "stdout": "100.10.log",
                "arguments": [
                    "-o",
                    "100.10.log",
                ],
                "uses": [
                    {
                        "lfn": "100.10.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_100_20",
                "id": "ID0000003",
                "stdout": "100.20.log",
                "arguments": [
                    "-o",
                    "100.20.log",
                ],
                "uses": [
                    {
                        "lfn": "100.20.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_100_30",
                "id": "ID0000004",
                "stdout": "100.30.log",
                "arguments": [
                    "-o",
                    "100.30.log",
                ],
                "uses": [
                    {
                        "lfn": "100.30.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_200_10",
                "id": "ID0000005",
                "stdout": "200.10.log",
                "arguments": [
                    "-o",
                    "200.10.log",
                ],
                "uses": [
                    {
                        "lfn": "200.10.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_200_20",
                "id": "ID0000006",
                "stdout": "200.20.log",
                "arguments": [
                    "-o",
                    "200.20.log",
                ],
                "uses": [
                    {
                        "lfn": "200.20.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_200_30",
                "id": "ID0000007",
                "stdout": "200.30.log",
                "arguments": [
                    "-o",
                    "200.30.log",
                ],
                "uses": [
                    {
                        "lfn": "200.30.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_300_10",
                "id": "ID0000008",
                "stdout": "300.10.log",
                "arguments": [
                    "-o",
                    "300.10.log",
                ],
                "uses": [
                    {
                        "lfn": "300.10.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_300_20",
                "id": "ID0000009",
                "stdout": "300.20.log",
                "arguments": [
                    "-o",
                    "300.20.log",
                ],
                "uses": [
                    {
                        "lfn": "300.20.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
            {
                "type": "job",
                "name": "run_lulesh_300_30",
                "id": "ID0000010",
                "stdout": "300.30.log",
                "arguments": [
                    "-o",
                    "300.30.log",
                ],
                "uses": [
                    {
                        "lfn": "300.30.log",
                        "type": "output",
                        "stageOut": True,
                        "registerReplica": True,
                    }
                ],
                "profiles": {
                    "pegasus": {
                        "project": "test_project",
                        "queue": "batch",
                        "runtime": 7200,
                        "nodes": 1,
                        "cores": 1,
                        "ppn": 1,
                    },
                    "selector": {"execution.site": "test_site"},
                },
            },
        ],
        "jobDependencies": [
            {
                "id": "ID0000001",
                "children": [
                    "ID0000004",
                    "ID0000008",
                    "ID0000006",
                    "ID0000005",
                    "ID0000002",
                    "ID0000007",
                    "ID0000009",
                    "ID0000003",
                    "ID0000010",
                ],
            }
        ],
    }
    dir_shared_fs, dir_type = _get_pegasus_dir_type(
        sched, StorageType.SHARED, PersistencyType.SCRATCH
    )
    shared_scratch_dir = {
        "type": dir_type,
        "path": "/tmp/scratch",  # noqa: S108
        "sharedFileSystem": dir_shared_fs,
        "fileServers": [{"url": "file:///tmp/scratch", "operation": "all"}],
    }
    if storage_type != StorageType.SHARED or persistency != PersistencyType.SCRATCH:
        base_yml["siteCatalog"]["sites"][0]["directories"].append(shared_scratch_dir)
    for i in range(len(base_yml["siteCatalog"]["sites"][0]["directories"])):
        if (
            base_yml["siteCatalog"]["sites"][0]["directories"][i]["sharedFileSystem"]
            is None
        ):
            del base_yml["siteCatalog"]["sites"][0]["directories"][i][
                "sharedFileSystem"
            ]
    return base_yml


def sort_config_lists(config: dict[str, Any]) -> dict[str, Any]:
    # Sort "children" for each entry in jobDependencies
    for i in range(len(config["jobDependencies"])):
        config["jobDependencies"][i]["children"] = sorted(
            config["jobDependencies"][i]["children"]
        )
    # Sort jobDependencies
    config["jobDependencies"] = sorted(
        config["jobDependencies"], key=lambda entry: entry["id"]
    )
    # Sort jobs
    config["jobs"] = sorted(config["jobs"], key=lambda entry: entry["id"])
    # Sort transofrmationCatalog
    config["transformationCatalog"]["transformations"] = sorted(
        config["transformationCatalog"]["transformations"],
        key=lambda entry: entry["name"],
    )
    # Sort replicaCatalog
    config["replicaCatalog"]["replicas"] = sorted(
        config["replicaCatalog"]["replicas"], key=lambda entry: entry["lfn"]
    )
    # Sort siteCatalog. This is done in 3 steps:
    #   1. For each directory (j) on each site (i), sort the file servers
    #      by the "url" field
    #   2. For each site (i), sort the directories by the "path" field
    #   3. Sort the sites by the "name" field
    for i in range(len(config["siteCatalog"]["sites"])):
        for j in range(len(config["siteCatalog"]["sites"][i]["directories"])):
            config["siteCatalog"]["sites"][i]["directories"][j]["fileServers"] = sorted(
                config["siteCatalog"]["sites"][i]["directories"][j]["fileServers"],
                key=lambda entry: entry["url"],
            )
        config["siteCatalog"]["sites"][i]["directories"] = sorted(
            config["siteCatalog"]["sites"][i]["directories"],
            key=lambda entry: entry["path"],
        )
    config["siteCatalog"]["sites"] = sorted(
        config["siteCatalog"]["sites"], key=lambda entry: entry["name"]
    )
    return config
