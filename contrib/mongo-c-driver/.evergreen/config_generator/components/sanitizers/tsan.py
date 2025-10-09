from shrub.v3.evg_build_variant import BuildVariant
from shrub.v3.evg_task import EvgTaskRef


TAG = 'sanitizers-matrix-tsan'


def variants():
    expansions = {
        'CFLAGS': '-fno-omit-frame-pointer',
        'CHECK_LOG': 'ON',
        'EXTRA_CONFIGURE_FLAGS': '-DENABLE_EXTRA_ALIGNMENT=OFF -DENABLE_SHM_COUNTERS=OFF',
        'SANITIZE': 'thread',
    }

    return [
        BuildVariant(
            name=TAG,
            display_name=TAG,
            tasks=[EvgTaskRef(name=f'.{TAG}')],
            expansions=expansions,
        ),
    ]
