"""
Download and extract MongoDB components.

Use '--help' for more information.
"""
import argparse
import enum
import hashlib
import json
import os
import platform
import re
import shutil
import sqlite3
import sys
import tarfile
import tempfile
import textwrap
import urllib.error
import urllib.request
import zipfile
from collections import namedtuple
from contextlib import contextmanager
from fnmatch import fnmatch
from pathlib import Path, PurePath, PurePosixPath

DISTRO_ID_MAP = {
    'elementary': 'ubuntu',
    'fedora': 'rhel',
    'centos': 'rhel',
    'mint': 'ubuntu',
    'linuxmint': 'ubuntu',
    'opensuse-leap': 'sles',
    'opensuse': 'sles',
    'redhat': 'rhel',
    'rocky': 'rhel',
    'ol': 'rhel',
}

DISTRO_VERSION_MAP = {
    'elementary': {
        '6': '20.04'
    },
    'fedora': {
        '32': '8',
        '33': '8',
        '34': '8',
        '35': '8',
        '36': '8'
    },
    'linuxmint': {
        '19': '18.04',
        '19.1': '18.04',
        '19.2': '18.04',
        '19.3': '18.04',
        '20': '20.04',
        '20.1': '20.04',
        '20.2': '20.04',
        '20.3': '20.04',
        '21': '22.04'
    },
}

DISTRO_ID_TO_TARGET = {
    'ubuntu': {
        '22.*': 'ubuntu2204',
        '20.*': 'ubuntu2004',
        '18.*': 'ubuntu1804',
        '16.*': 'ubuntu1604',
        '14.*': 'ubuntu1404',
    },
    'debian': {
        '9': 'debian92',
        '10': 'debian10',
        '11': 'debian11',
    },
    'rhel': {
        '6': 'rhel60',
        '6.*': 'rhel60',
        '7': 'rhel70',
        '7.*': 'rhel70',
        '8': 'rhel80',
        '8.*': 'rhel80',
    },
    'sles': {
        '10.*': 'suse10',
        '11.*': 'suse11',
        '12.*': 'suse12',
        '13.*': 'suse13',
        '15.*': 'suse15',
    },
    'amzn': {
        '2018.*': 'amzn64',
        '2': 'amazon2',
    },
}


def infer_target():
    if sys.platform == 'win32':
        return 'windows'
    if sys.platform == 'darwin':
        return 'macos'
    # Now the tricky bit
    cands = map(Path, ['/etc/os-release', '/usr/lib/os-release'])
    for c in cands:
        if c.is_file():
            return _infer_target_os_rel(c)
    raise RuntimeError("We don't know how to find the default '--target'"
                       " option for this system. Please contribute!")


def _infer_target_os_rel(os_rel_path: Path):
    with Path(os_rel_path).open('r', encoding='utf-8') as f:
        content = f.read()
    id_re = re.compile(r'\bID=("?)(.*)\1')
    mat = id_re.search(content)
    assert mat, 'Unable to detect ID from [{}] content:\n{}'.format(
        os_rel_path, content)
    os_id = mat.group(2)
    if os_id == 'arch':
        # There are no Archlinux-specific MongoDB downloads, so we'll just use
        # the build for RHEL8, which is reasonably compatible with other modern
        # distributions (including Arch).
        return 'rhel80'
    ver_id_re = re.compile(r'VERSION_ID=("?)(.*)\1')
    mat = ver_id_re.search(content)
    assert mat, 'Unable to detect VERSION_ID from [{}] content:\n{}'.format(
        os_rel_path, content)
    ver_id = mat.group(2)
    mapped_id = DISTRO_ID_MAP.get(os_id)
    if mapped_id:
        print('Mapping distro "{}" to "{}"'.format(os_id, mapped_id))
        ver_mapper = DISTRO_VERSION_MAP.get(os_id)
        if ver_mapper:
            mapped_version = ver_mapper[ver_id]
            print('Mapping version "{}" to "{}"'.format(
                ver_id, mapped_version))
            ver_id = mapped_version
        os_id = mapped_id
    os_id = os_id.lower()
    if os_id not in DISTRO_ID_TO_TARGET:
        raise RuntimeError("We don't know how to map '{}' to a distribution "
                           "download target. Please contribute!".format(os_id))
    ver_table = DISTRO_ID_TO_TARGET[os_id]
    for pattern, target in ver_table.items():
        if fnmatch(ver_id, pattern):
            return target
    raise RuntimeError(
        "We don't know how to map '{}' version '{}' to a distribution "
        "download target. Please contribute!".format(os_id, ver_id))


def caches_root():
    if sys.platform == 'win32':
        return Path(os.environ['LocalAppData'])
    if sys.platform == 'darwin':
        return Path(os.environ['HOME'] + '/Library/Caches')
    xdg_cache = os.getenv('XDG_CACHE_HOME')
    if xdg_cache:
        return Path(xdg_cache)
    return Path(os.environ['HOME'] + '/.cache')


def cache_dir():
    return caches_root().joinpath('mongodl').absolute()


@contextmanager
def tmp_dir():
    tdir = tempfile.mkdtemp()
    try:
        yield Path(tdir)
    finally:
        shutil.rmtree(tdir)


def _import_json_data(db, json_file):
    db.execute('DELETE FROM meta')
    db.execute('DROP TABLE IF EXISTS components')
    db.execute('DROP TABLE IF EXISTS downloads')
    db.execute('DROP TABLE IF EXISTS versions')
    db.execute(r'''
        CREATE TABLE versions (
            version_id INTEGER PRIMARY KEY,
            date TEXT NOT NULL,
            version TEXT NOT NULL,
            githash TEXT NOT NULL
        )
    ''')
    db.execute(r'''
        CREATE TABLE downloads (
            download_id INTEGER PRIMARY KEY,
            version_id INTEGER NOT NULL REFERENCES versions,
            target TEXT NOT NULL,
            arch TEXT NOT NULL,
            edition TEXT NOT NULL,
            ar_url TEST NOT NULL,
            data TEXT NOT NULL
        )
    ''')
    db.execute(r'''
        CREATE TABLE components (
            component_id INTEGER PRIMARY KEY,
            key TEXT NOT NULL,
            download_id INTEGER NOT NULL REFERENCES downloads,
            data TEXT NOT NULL,
            UNIQUE(key, download_id)
        )
    ''')
    with json_file.open('r') as f:
        data = json.load(f)
    for ver in data['versions']:
        version = ver['version']
        githash = ver['githash']
        date = ver['date']
        db.execute(
            r'''
            INSERT INTO versions (date, version, githash)
            VALUES (?, ?, ?)
            ''',
            (date, version, githash),
        )
        version_id = db.lastrowid
        for dl in ver['downloads']:
            arch = dl.get('arch', 'null')
            target = dl.get('target', 'null')
            edition = dl['edition']
            ar_url = dl['archive']['url']
            db.execute(
                r'''
                INSERT INTO downloads (version_id, target, arch, edition, ar_url, data)
                VALUES (?, ?, ?, ?, ?, ?)
                ''',
                (version_id, target, arch, edition, ar_url, json.dumps(dl)),
            )
            dl_id = db.lastrowid
            for key, data in dl.items():
                if 'url' not in data:
                    continue
                db.execute(
                    r'''
                    INSERT INTO components (key, download_id, data)
                    VALUES (?, ?, ?)
                    ''',
                    (key, dl_id, json.dumps(data)),
                )


def _mkdir(dirpath):
    """
    Ensure a directory at ``dirpath``, and all parent directories thereof.

    Cannot using Path.mkdir(parents, exist_ok) on some Python versions that
    we need to support.
    """
    if dirpath.is_dir():
        return
    par = dirpath.parent
    if par != dirpath:
        _mkdir(par)
    try:
        dirpath.mkdir()
    except FileExistsError:
        pass


def get_dl_db():
    caches = cache_dir()
    _mkdir(caches)
    db = sqlite3.connect(str(caches / 'downloads.db'), isolation_level=None)
    db.executescript(r'''
        CREATE TABLE IF NOT EXISTS meta (
            etag TEXT,
            last_modified TEXT
        )
    ''')
    db.executescript(r'''
        CREATE TABLE IF NOT EXISTS past_downloads (
            url TEXT NOT NULL UNIQUE,
            etag TEXT,
            last_modified TEXT
        )
    ''')
    changed, full_json = _download_file(
        db, 'https://downloads.mongodb.org/full.json')
    if not changed:
        return db
    with db:
        print('Refreshing downloads manifest ...')
        cur = db.cursor()
        cur.execute("begin")
        _import_json_data(cur, full_json)
    return db


def _print_list(db, version, target, arch, edition, component):
    if version or target or arch or edition or component:
        matching = db.execute(
            r'''
            SELECT version, target, arch, edition, key, components.data
              FROM components,
                   downloads USING(download_id),
                   versions USING(version_id)
            WHERE (:component IS NULL OR key=:component)
              AND (:target IS NULL OR target=:target)
              AND (:arch IS NULL OR arch=:arch)
              AND (:edition IS NULL OR edition=:edition)
              AND (:version IS NULL OR version=:version)
            ''',
            dict(version=version,
                 target=target,
                 arch=arch,
                 edition=edition,
                 component=component),
        )
        for version, target, arch, edition, comp_key, comp_data in matching:
            print('Download: {}\n\n'
                  ' Version: {}\n\n'
                  '  Target: {}\n\n'
                  '    Arch: {}\n\n'
                  ' Edition: {}\n\n'
                  '    Info: {}\n\n'.format(comp_key, version, target, arch,
                                            edition, comp_data))
        print('(Omit filter arguments for a list of available filters)')
        return

    arches, targets, editions, versions, components = next(
        iter(
            db.execute(r'''
        VALUES(
            (select group_concat(arch, ', ') from (select distinct arch from downloads)),
            (select group_concat(target, ', ') from (select distinct target from downloads)),
            (select group_concat(edition, ', ') from (select distinct edition from downloads)),
            (select group_concat(version, ', ') from (select distinct version from versions)),
            (select group_concat(key, ', ') from (select distinct key from components))
        )
        ''')))
    versions = '\n'.join(
        textwrap.wrap(versions,
                      width=78,
                      initial_indent='  ',
                      subsequent_indent='  '))
    targets = '\n'.join(
        textwrap.wrap(targets,
                      width=78,
                      initial_indent='  ',
                      subsequent_indent='  '))
    print('Architectures:\n'
          '  {}\n'
          'Targets:\n'
          '{}\n'
          'Editions:\n'
          '  {}\n'
          'Versions:\n'
          '{}\n'
          'Components:\n'
          '  {}\n'.format(arches, targets, editions, versions, components))


def infer_arch():
    a = platform.machine() or platform.processor()
    # Remap platform names to the names used for downloads
    return {
        'AMD64': 'x86_64',
    }.get(a, a)


DLRes = namedtuple('DLRes', ['is_changed', 'path'])


def _download_file(db, url):
    caches = cache_dir()
    info = list(
        db.execute(
            'SELECT etag, last_modified FROM past_downloads WHERE url=?',
            [url]))
    etag = None
    modtime = None
    if info:
        etag, modtime = info[0]
    headers = {}
    if etag:
        headers['If-None-Match'] = etag
    if modtime:
        headers['If-Modified-Since'] = modtime
    req = urllib.request.Request(url, headers=headers)
    digest = hashlib.md5(url.encode("utf-8")).hexdigest()[:4]
    dest = caches / 'files' / digest / PurePosixPath(url).name
    try:
        resp = urllib.request.urlopen(req)
    except urllib.error.HTTPError as e:
        if e.code != 304:
            raise
        return DLRes(False, dest)
    else:
        print('Downloading [{}] ...'.format(url))
        _mkdir(dest.parent)
        got_etag = resp.getheader("ETag")
        got_modtime = resp.getheader('Last-Modified')
        with dest.open('wb') as of:
            buf = resp.read(1024 * 1024 * 4)
            while buf:
                of.write(buf)
                buf = resp.read(1024 * 1024 * 4)
        db.execute(
            'INSERT OR REPLACE INTO past_downloads (url, etag, last_modified) VALUES (?, ?, ?)',
            (url, got_etag, got_modtime))
    return DLRes(True, dest)


def _dl_component(db, out_dir, version, target, arch, edition, component,
                  pattern, strip_components, test):
    print('Download {} v{}-{} for {}-{}'.format(component, version, edition,
                                                target, arch))
    matching = db.execute(
        r'''
        SELECT components.data
        FROM
            components,
            downloads USING(download_id),
            versions USING(version_id)
        WHERE
            target=:target
            AND arch=:arch
            AND edition=:edition
            AND version=:version
            AND key=:component
        ''',
        dict(version=version,
             target=target,
             arch=arch,
             edition=edition,
             component=component),
    )
    found = list(matching)
    if not found:
        raise ValueError(
            'No download for "{}" was found for '
            'the requested version+target+architecture+edition'.format(
                component))
    data = json.loads(found[0][0])
    cached = _download_file(db, data['url']).path
    return _expand_archive(cached,
                           out_dir,
                           pattern,
                           strip_components,
                           test=test)


def pathjoin(items):
    """
    Return a path formed by joining the given path components
    """
    return PurePath('/'.join(items))


def _test_pattern(path, pattern):
    """
    Test whether the given 'path' string matches the globbing pattern 'pattern'.

    Supports the '**' pattern to match any number of intermediate directories.
    """
    if pattern is None:
        return True
    # Convert to path objects
    path = PurePath(path)
    pattern = PurePath(pattern)
    # Split pattern into parts
    pattern_parts = pattern.parts
    if not pattern_parts:
        # An empty pattern always matches
        return True
    path_parts = path.parts
    if not path_parts:
        # Non-empty pattern requires more path components
        return False
    pattern_head = pattern_parts[0]
    pattern_tail = pathjoin(pattern_parts[1:])
    if pattern_head == '**':
        # Special "**" pattern matches and suffix of the path
        # Generate each suffix:
        tails = (path_parts[i:] for i in range(len(path_parts)))
        # Test if any of the suffixes match the remainder of the pattern:
        return any(_test_pattern(pathjoin(t), pattern_tail) for t in tails)
    if not fnmatch(path.parts[0], pattern_head):
        # Leading path component cannot match
        return False
    # The first component matches. Test the remainder:
    return _test_pattern(pathjoin(path_parts[1:]), pattern_tail)


class ExpandResult(enum.Enum):
    Empty = 0
    Okay = 1


def _expand_archive(ar, dest, pattern, strip_components, test):
    '''
    Expand the archive members from 'ar' into 'dest'. If 'pattern' is not-None,
    only extracts members that match the pattern.
    '''
    print('Extract from: [{}]'.format(ar.name))
    print('        into: [{}]'.format(dest))
    if ar.suffix == '.zip':
        n_extracted = _expand_zip(ar,
                                  dest,
                                  pattern,
                                  strip_components,
                                  test=test)
    elif ar.suffix == '.tgz':
        n_extracted = _expand_tgz(ar,
                                  dest,
                                  pattern,
                                  strip_components,
                                  test=test)
    else:
        raise RuntimeError('Unknown archive file extension: ' + ar.suffix)
    verb = 'would be' if test else 'were'
    if n_extracted == 0:
        if pattern and strip_components:
            print('NOTE: No files {verb} extracted. Likely all files {verb} '
                  'excluded by "--only={p}" and/or "--strip-components={s}"'.
                  format(p=pattern, s=strip_components, verb=verb))
        elif pattern:
            print('NOTE: No files {verb} extracted. Likely all files {verb} '
                  'excluded by the "--only={p}" filter'.format(p=pattern,
                                                               verb=verb))
        elif strip_components:
            print('NOTE: No files {verb} extracted. Likely all files {verb} '
                  'excluded by "--strip-components={s}"'.format(
                      s=strip_components, verb=verb))
        else:
            print('NOTE: No files {verb} extracted. Empty archive?'.format(
                verb=verb))
        return ExpandResult.Empty
    elif n_extracted == 1:
        print('One file {v} extracted'.format(v='would be' if test else 'was'))
        return ExpandResult.Okay
    else:
        print('{n} files {verb} extracted'.format(n=n_extracted, verb=verb))
        return ExpandResult.Okay


def _expand_tgz(ar, dest, pattern, strip_components, test):
    'Expand a tar.gz archive'
    n_extracted = 0
    with tarfile.open(str(ar), 'r:*') as tf:
        for mem in tf.getmembers():
            n_extracted += _maybe_extract_member(
                dest,
                mem.name,
                pattern,
                strip_components,
                mem.isdir(),
                lambda: tf.extractfile(mem),
                mem.mode,
                test=test,
            )
    return n_extracted


def _expand_zip(ar, dest, pattern, strip_components, test):
    'Expand a .zip archive.'
    n_extracted = 0
    with zipfile.ZipFile(ar, 'r') as zf:
        for item in zf.infolist():
            n_extracted += _maybe_extract_member(
                dest,
                item.filename,
                pattern,
                strip_components,
                item.is_dir(),
                lambda: zf.open(item, 'r'),
                0o655,
                test=test,
            )
    return n_extracted


def _maybe_extract_member(out, relpath, pattern, strip, is_dir, opener,
                          modebits, test):
    """
    Try to extract an archive member according to the given arguments.

    :return: Zero if the file was excluded by filters, one otherwise.
    """
    relpath = PurePath(relpath)
    print('  | {:-<65} |'.format(str(relpath) + ' '), end='')
    if len(relpath.parts) <= strip:
        # Not enough path components
        print(' (Excluded by --strip-components)')
        return 0
    if not _test_pattern(relpath, pattern):
        # Doesn't match our pattern
        print(' (excluded by pattern)')
        return 0
    stripped = pathjoin(relpath.parts[strip:])
    dest = Path(out) / stripped
    print('\n    -> [{}]'.format(dest))
    if test:
        # We are running in test-only mode: Do not do anything
        return 1
    if is_dir:
        _mkdir(dest)
        return 1
    with opener() as infile:
        _mkdir(dest.parent)
        with dest.open('wb') as outfile:
            shutil.copyfileobj(infile, outfile)
        os.chmod(str(dest), modebits)
    return 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    grp = parser.add_argument_group('List arguments')
    grp.add_argument('--list',
                     action='store_true',
                     help='List available components, targets, editions, and '
                     'architectures. Download arguments will act as filters.')
    dl_grp = parser.add_argument_group(
        'Download arguments',
        description='Select what to download and extract. '
        'Non-required arguments will be inferred '
        'based on the host system.')
    dl_grp.add_argument('--target',
                        '-T',
                        help='The target platform for which to download. '
                        'Use "--list" to list available targets.')
    dl_grp.add_argument('--arch',
                        '-A',
                        help='The architecture for which to download')
    dl_grp.add_argument(
        '--edition',
        '-E',
        help='The edition of the product to download (Default is "enterprise"). '
        'Use "--list" to list available editions.')
    dl_grp.add_argument(
        '--out',
        '-o',
        help='The directory in which to download components. (Required)',
        type=Path)
    dl_grp.add_argument('--version',
                        '-V',
                        help='The product version to download (Required). '
                        'Use "--list" to list available versions.')
    dl_grp.add_argument('--component',
                        '-C',
                        help='The component to download (Required). '
                        'Use "--list" to list available components.')
    dl_grp.add_argument(
        '--only',
        help=
        'Restrict extraction to items that match the given globbing expression. '
        'The full archive member path is matched, so a pattern like "*.exe" '
        'will only match "*.exe" at the top level of the archive. To match '
        'recursively, use the "**" pattern to match any number of '
        'intermediate directories.')
    dl_grp.add_argument(
        '--strip-path-components',
        '-p',
        dest='strip_components',
        metavar='N',
        default=0,
        type=int,
        help=
        'Strip the given number of path components from archive members before '
        'extracting into the destination. The relative path of the archive '
        'member will be used to form the destination path. For example, a '
        'member named [bin/mongod.exe] will be extracted to [<out>/bin/mongod.exe]. '
        'Using --strip-components=1 will remove the first path component, extracting '
        'such an item to [<out>/mongod.exe]. If the path has fewer than N components, '
        'that archive member will be ignored.')
    dl_grp.add_argument(
        '--test',
        action='store_true',
        help='Do not extract or place any files/directories. '
        'Only print what will be extracted without placing any files.')
    dl_grp.add_argument('--empty-is-error',
                        action='store_true',
                        help='If all files are excluded by other filters, '
                        'treat that situation as an error and exit non-zero.')
    args = parser.parse_args()
    db = get_dl_db()

    if args.list:
        _print_list(db, args.version, args.target, args.arch, args.edition,
                    args.component)
        return

    if args.version is None:
        raise argparse.ArgumentError(None, 'A "--version" is required')
    if args.component is None:
        raise argparse.ArgumentError(
            None, 'A "--component" name should be provided')
    if args.out is None:
        raise argparse.ArgumentError(None,
                                     'A "--out" directory should be provided')

    target = args.target or infer_target()
    arch = args.arch or infer_arch()
    edition = args.edition or 'enterprise'
    out = args.out or Path.cwd()
    out = out.absolute()
    result = _dl_component(db,
                           out,
                           version=args.version,
                           target=target,
                           arch=arch,
                           edition=edition,
                           component=args.component,
                           pattern=args.only,
                           strip_components=args.strip_components,
                           test=args.test)
    if result is ExpandResult.Empty:
        return 1
    return 0


if __name__ == '__main__':
    sys.exit(main())
