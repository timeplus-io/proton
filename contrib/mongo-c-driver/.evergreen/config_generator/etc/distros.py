from typing import Literal

from pydantic import BaseModel, validator
from packaging.version import Version


class Distro(BaseModel):
    """
    Defines common properties of a given Evergreen distro.

    * name: Name of the distro.
    * os: Name of the operating system.
    * os_type: One of Linux, MacOS, or Windows.
    * os_ver: Version of the operating system.
    * vs_ver: Version of Visual Studio available.
    * size: Size of tasks the distro is designed to handle.
    * arch: Target architecture.
    """

    name: str
    os: str | None = None
    os_type: Literal['linux', 'macos', 'windows'] | None = None
    os_ver: str | None = None
    vs_ver: Literal[
        '2013',
        '2015',
        '2017',
        '2019',
        '2022',
        'vsCurrent',
        'vsCurrent2',
        'vsMulti',
    ] | None = None
    size: Literal['small', 'large'] | None = None
    arch: Literal['arm64', 'power8', 'zseries'] | None = None

    @validator('os_ver')
    @classmethod
    def validate_os_ver(cls, value):
        return Version(value)

# See: https://evergreen.mongodb.com/distros
# pylint: disable=line-too-long
#fmt: off
DEBIAN_DISTROS = [
    Distro(name='debian10-large', os='debian', os_type='linux', os_ver='10', size='large'),
    Distro(name='debian10-small', os='debian', os_type='linux', os_ver='10', size='small'),
    Distro(name='debian11-large', os='debian', os_type='linux', os_ver='11', size='large'),
    Distro(name='debian11-small', os='debian', os_type='linux', os_ver='11', size='small'),
    Distro(name='debian92-large', os='debian', os_type='linux', os_ver='9.2', size='large'),
    Distro(name='debian92-small', os='debian', os_type='linux', os_ver='9.2', size='small'),
]

MACOS_DISTROS = [
    Distro(name='macos-1100', os='macos', os_type='macos', os_ver='11.00'),
]

MACOS_ARM64_DISTROS = [
    Distro(name='macos-1100-arm64', os='macos', os_type='macos', os_ver='11.00', arch='arm64'),
]

RHEL_DISTROS = [
    Distro(name='rhel70-large', os='rhel', os_type='linux', os_ver='7.0', size='large'),
    Distro(name='rhel70-small', os='rhel', os_type='linux', os_ver='7.0', size='small'),
    Distro(name='rhel76-large', os='rhel', os_type='linux', os_ver='7.6', size='large'),
    Distro(name='rhel76-small', os='rhel', os_type='linux', os_ver='7.6', size='small'),
    Distro(name='rhel80-large', os='rhel', os_type='linux', os_ver='8.0', size='large'),
    Distro(name='rhel80-small', os='rhel', os_type='linux', os_ver='8.0', size='small'),
    Distro(name='rhel84-large', os='rhel', os_type='linux', os_ver='8.4', size='large'),
    Distro(name='rhel84-small', os='rhel', os_type='linux', os_ver='8.4', size='small'),
    Distro(name='rhel87-large', os='rhel', os_type='linux', os_ver='8.7', size='large'),
    Distro(name='rhel87-small', os='rhel', os_type='linux', os_ver='8.7', size='small'),
    Distro(name='rhel90-large', os='rhel', os_type='linux', os_ver='9.0', size='large'),
    Distro(name='rhel90-small', os='rhel', os_type='linux', os_ver='9.0', size='small'),
]

RHEL_ARM64_DISTROS = [
    Distro(name='rhel82-arm64-large', os='rhel', os_type='linux', os_ver='8.2', size='large', arch='arm64'),
    Distro(name='rhel82-arm64-small', os='rhel', os_type='linux', os_ver='8.2', size='small', arch='arm64'),
    Distro(name='rhel90-arm64-large', os='rhel', os_type='linux', os_ver='9.0', size='large', arch='arm64'),
    Distro(name='rhel90-arm64-small', os='rhel', os_type='linux', os_ver='9.0', size='small', arch='arm64'),
]

RHEL_POWER8_DISTROS = [
    Distro(name='rhel71-power8-large', os='rhel', os_type='linux', os_ver='7.1', size='large', arch='power8'),
    Distro(name='rhel71-power8-small', os='rhel', os_type='linux', os_ver='7.1', size='small', arch='power8'),
    Distro(name='rhel81-power8-large', os='rhel', os_type='linux', os_ver='8.1', size='large', arch='power8'),
    Distro(name='rhel81-power8-small', os='rhel', os_type='linux', os_ver='8.1', size='small', arch='power8'),
]

RHEL_ZSERIES_DISTROS = [
    Distro(name='rhel72-zseries-large', os='rhel', os_type='linux', os_ver='7.2', size='large', arch='zseries'),
    Distro(name='rhel72-zseries-small', os='rhel', os_type='linux', os_ver='7.2', size='small', arch='zseries'),
    Distro(name='rhel83-zseries-large', os='rhel', os_type='linux', os_ver='8.3', size='large', arch='zseries'),
    Distro(name='rhel83-zseries-small', os='rhel', os_type='linux', os_ver='8.3', size='small', arch='zseries'),
]

UBUNTU_DISTROS = [
    Distro(name='ubuntu1604-large', os='ubuntu', os_type='linux', os_ver='16.04', size='large'),
    Distro(name='ubuntu1604-small', os='ubuntu', os_type='linux', os_ver='16.04', size='small'),
    Distro(name='ubuntu1804-large', os='ubuntu', os_type='linux', os_ver='18.04', size='large'),
    Distro(name='ubuntu1804-small', os='ubuntu', os_type='linux', os_ver='18.04', size='small'),
    Distro(name='ubuntu2004-large', os='ubuntu', os_type='linux', os_ver='20.04', size='large'),
    Distro(name='ubuntu2004-small', os='ubuntu', os_type='linux', os_ver='20.04', size='small'),
    Distro(name='ubuntu2204-large', os='ubuntu', os_type='linux', os_ver='22.04', size='large'),
    Distro(name='ubuntu2204-small', os='ubuntu', os_type='linux', os_ver='22.04', size='small'),
]

UBUNTU_ARM64_DISTROS = [
    Distro(name='ubuntu1604-arm64-large', os='ubuntu', os_type='linux', os_ver='16.04', size='large', arch='arm64'),
    Distro(name='ubuntu1604-arm64-small', os='ubuntu', os_type='linux', os_ver='16.04', size='small', arch='arm64'),
    Distro(name='ubuntu1804-arm64-large', os='ubuntu', os_type='linux', os_ver='18.04', size='large', arch='arm64'),
    Distro(name='ubuntu1804-arm64-small', os='ubuntu', os_type='linux', os_ver='18.04', size='small', arch='arm64'),
    Distro(name='ubuntu2004-arm64-large', os='ubuntu', os_type='linux', os_ver='20.04', size='large', arch='arm64'),
    Distro(name='ubuntu2004-arm64-small', os='ubuntu', os_type='linux', os_ver='20.04', size='small', arch='arm64'),
    Distro(name='ubuntu2204-arm64-large', os='ubuntu', os_type='linux', os_ver='22.04', size='large', arch='arm64'),
    Distro(name='ubuntu2204-arm64-small', os='ubuntu', os_type='linux', os_ver='22.04', size='small', arch='arm64'),
]

WINDOWS_DISTROS = [
    Distro(name='windows-64-vs2013-large', os='windows', os_type='windows', vs_ver='2013', size='large'),
    Distro(name='windows-64-vs2013-small', os='windows', os_type='windows', vs_ver='2013', size='small'),
    Distro(name='windows-64-vs2015-large', os='windows', os_type='windows', vs_ver='2015', size='large'),
    Distro(name='windows-64-vs2015-small', os='windows', os_type='windows', vs_ver='2015', size='small'),
    Distro(name='windows-64-vs2017-large', os='windows', os_type='windows', vs_ver='2017', size='large'),
    Distro(name='windows-64-vs2017-small', os='windows', os_type='windows', vs_ver='2017', size='small'),
    Distro(name='windows-64-vs2019-large', os='windows', os_type='windows', vs_ver='2019', size='large'),
    Distro(name='windows-64-vs2019-small', os='windows', os_type='windows', vs_ver='2019', size='small'),

    Distro(name='windows-2022-large', os='windows', os_type='windows', os_ver='2022'),
    Distro(name='windows-2022-small', os='windows', os_type='windows', os_ver='2022'),

    Distro(name='windows-64-2019', os='windows', os_type='windows', os_ver='2019'),

    Distro(name='windows-64-vsMulti-small', os='windows', os_type='windows', vs_ver='vsMulti', size='small'),

    Distro(name='windows-vsCurrent-2022-large', os='windows', os_type='windows', os_ver='2022', vs_ver='vsCurrent', size='large'),
    Distro(name='windows-vsCurrent-2022-small', os='windows', os_type='windows', os_ver='2022', vs_ver='vsCurrent', size='small'),

    Distro(name='windows-vsCurrent-large', os='windows', os_type='windows', vs_ver='vsCurrent', size='large'), # Windows Server 2019
    Distro(name='windows-vsCurrent-small', os='windows', os_type='windows', vs_ver='vsCurrent', size='small'), # Windows Server 2019

    Distro(name='windows-vsCurrent2-large', os='windows', os_type='windows', vs_ver='vsCurrent2', size='large'),
    Distro(name='windows-vsCurrent2-small', os='windows', os_type='windows', vs_ver='vsCurrent2', size='small'),
]
#fmt: on
# pylint: enable=line-too-long

# Ensure no-arch distros are ordered before arch-specific distros.
ALL_DISTROS = [] + \
    DEBIAN_DISTROS + \
    MACOS_DISTROS + \
    MACOS_ARM64_DISTROS + \
    RHEL_DISTROS + \
    RHEL_ARM64_DISTROS + \
    RHEL_POWER8_DISTROS + \
    RHEL_ZSERIES_DISTROS + \
    UBUNTU_DISTROS + \
    UBUNTU_ARM64_DISTROS + \
    WINDOWS_DISTROS


def find_distro(name) -> Distro:
    candidates = [d for d in ALL_DISTROS if name == d.name]

    if not candidates:
        raise ValueError(f'could not find a distro with the name {name}')

    return candidates[0]


def find_large_distro(name) -> Distro:
    candidates = [d for d in ALL_DISTROS if f'{name}-large' == d.name]

    if candidates:
        return candidates[0]

    return find_distro(name)


def find_small_distro(name) -> Distro:
    candidates = [d for d in ALL_DISTROS if f'{name}-small' == d.name]

    if candidates:
        return candidates[0]

    return find_distro(name)


def make_distro_str(distro_name, compiler, arch) -> str:
    if distro_name.startswith('windows-vsCurrent'):
        # Rename `windows-vsCurrent-*` distros to `windows-<year>` where`<year>`
        # is the Windows Server version used by the distro, e.g.:
        #     ('windows-vsCurrent-2022', 'vs2017x64', None) -> windows-2022-vs2017-x64
        #     ('windows-vsCurrent-2022', 'mingw',     None) -> windows-2022-mingw
        #     ('windows-vsCurrent',      'vs2017x64', None) -> windows-2019-vs2017-x64
        #     ('windows-vsCurrent',      'mingw',     None) -> windows-2019-mingw
        maybe_arch = compiler[len('vs20XY'):]
        if maybe_arch in ('x86', 'x64'):
            compiler_str = compiler[:-len(maybe_arch)] + '-' + maybe_arch
        else:
            compiler_str = compiler
        if distro_name.startswith('windows-vsCurrent-'):
            distro_str = 'windows-' + \
                distro_name[len('windows-vsCurrent-'):] + f'-{compiler_str}'
        else:
            distro_str = 'windows-2019' + f'-{compiler_str}'
    elif distro_name.startswith('windows-64-vs'):
        # Abbreviate 'windows-64-vs<type>' as 'vs<type>' and append '-<arch>' if
        # given in compiler string as 'vs<type><arch>', e.g.:
        #     ('windows-64-vs2017', 'vs2017x64', None) -> vs2017-x64
        #     ('windows-64-vs2017', 'mingw',     None) -> vs2017-mingw
        distro_str = distro_name[len('windows-64-'):] + {
            'vs2013x64': '-x64',
            'vs2013x86': '-x86',
            'vs2015x64': '-x64',
            'vs2015x86': '-x86',
            'vs2017x64': '-x64',
            'vs2017x86': '-x86',
        }.get(compiler, f'-{compiler}')
    else:
        distro_str = distro_name
        if compiler:
            distro_str += f'-{compiler}'

    if arch:
        distro_str += f'-{arch}'

    return distro_str


def to_cc(compiler):
    return {
        'vs2013x64': 'Visual Studio 12 2013 Win64',
        'vs2013x86': 'Visual Studio 12 2013',
        'vs2015x64': 'Visual Studio 14 2015 Win64',
        'vs2015x86': 'Visual Studio 14 2015',
        'vs2017x64': 'Visual Studio 15 2017 Win64',
        'vs2017x86': 'Visual Studio 15 2017',
    }.get(compiler, compiler)
