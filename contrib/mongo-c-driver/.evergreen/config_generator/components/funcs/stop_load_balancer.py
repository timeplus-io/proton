from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class StopLoadBalancer(Function):
    name = 'stop-load-balancer'
    commands = [
        bash_exec(
            script='''\
                # Only run if a load balancer was started.
                if [[ -z "${SINGLE_MONGOS_LB_URI}" ]]; then
                    echo "OK - no load balancer running"
                    exit
                fi
                if [[ -d drivers-evergreen-tools ]]; then
                    cd drivers-evergreen-tools && .evergreen/run-load-balancer.sh stop
                fi
            '''
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return StopLoadBalancer.defn()
