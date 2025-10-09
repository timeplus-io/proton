from typing import Iterable

from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_task import EvgTaskRef

from ..etc.utils import Task
from . import earthly


def tasks() -> Iterable[Task]:
    yield Task(
        name="create-silk-asset-group",
        commands=[
            earthly.earthly_exec(
                kind="setup",
                target="create-silk-asset-group",
                args={
                    "branch": r"${branch_name}",
                },
                secrets={
                    "SILK_CLIENT_ID": r"${silk_client_id}",
                    "SILK_CLIENT_SECRET": r"${silk_client_secret}",
                },
            )
        ],
        run_on=earthly.CONTAINER_RUN_DISTROS,
        tags=["misc", "pr-merge-gate"],
    )


def variants() -> Iterable[BuildVariant]:
    yield BuildVariant(
        name="misc",
        tasks=[EvgTaskRef(name=".misc")],
        display_name="Miscellaneous",
    )
