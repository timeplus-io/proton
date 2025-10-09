from shrub.v3.evg_command import EvgCommandType

from config_generator.etc.function import Function
from config_generator.etc.utils import bash_exec


class PrepareKerberos(Function):
    name = 'prepare-kerberos'
    commands = [
        bash_exec(
            command_type=EvgCommandType.SETUP,
            working_dir='mongoc',
            silent=True,
            script='''\
            if test "${keytab|}" && command -v kinit >/dev/null; then
                echo "${keytab}" > /tmp/drivers.keytab.base64
                base64 --decode /tmp/drivers.keytab.base64 > /tmp/drivers.keytab
                if touch /etc/krb5.conf 2>/dev/null; then
                    cat .evergreen/etc/kerberos.realm | tee -a /etc/krb5.conf
                elif command sudo true 2>/dev/null; then
                    cat .evergreen/etc/kerberos.realm | sudo tee -a /etc/krb5.conf
                else
                    echo "Cannot append kerberos.realm to /etc/krb5.conf; skipping." 1>&2
                fi
            fi
            '''
        ),
    ]

    @classmethod
    def call(cls, **kwargs):
        return cls.default_call(**kwargs)


def functions():
    return PrepareKerberos.defn()
