# -*- coding: utf-8 -*-
# Copyright (C) 2013-2018 by The Linux Foundation and contributors
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys

import time
import json
import fnmatch
import subprocess
import requests
import logging
import hashlib
import pathlib
import uuid

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


VERSION = '2.0-dev'
MANIFEST_LOCKH = None
REPO_LOCKH = {}
GITBIN = '/usr/bin/git'

# default logger. Will probably be overridden.
logger = logging.getLogger(__name__)

_alt_repo_set = None
_alt_repo_map = None

# Used to store our requests session
REQSESSION = None


def get_requests_session():
    global REQSESSION
    if REQSESSION is None:
        REQSESSION = requests.session()
        retry = Retry(connect=3, backoff_factor=0.5)
        adapter = HTTPAdapter(max_retries=retry)
        REQSESSION.mount('http://', adapter)
        REQSESSION.mount('https://', adapter)
        REQSESSION.headers.update({'User-Agent': 'grokmirror/%s' % VERSION})
    return REQSESSION


def get_config_from_git(fullpath, regexp, defaults=None):
    args = ['config', '-z', '--get-regexp', regexp]
    ecode, out, err = run_git_command(fullpath, args)
    gitconfig = defaults
    if not gitconfig:
        gitconfig = dict()
    if not out:
        return gitconfig

    for line in out.split('\x00'):
        if not line:
            continue
        key, value = line.split('\n', 1)
        try:
            chunks = key.split('.')
            cfgkey = chunks[-1]
            gitconfig[cfgkey.lower()] = value
        except ValueError:
            logger.debug('Ignoring git config entry %s', line)

    return gitconfig


def set_git_config(fullpath, param, value, operation='--replace-all'):
    args = ['config', operation, param, value]
    ecode, out, err = run_git_command(fullpath, args)
    return ecode


def git_newer_than(minver):
    from packaging import version
    (retcode, output, error) = run_git_command(None, ['--version'])
    ver = output.split()[-1]
    return version.parse(ver) >= version.parse(minver)


def run_git_command(fullpath, args):
    if 'GITBIN' in os.environ:
        _git = os.environ['GITBIN']
    else:
        _git = GITBIN

    if not os.path.isfile(_git) and os.access(_git, os.X_OK):
        # we hope for the best by using 'git' without full path
        _git = 'git'

    if fullpath is not None:
        cmdargs = [_git, '--no-pager', '--git-dir', fullpath] + args
    else:
        cmdargs = [_git, '--no-pager'] + args

    logger.debug('Running: %s', ' '.join(cmdargs))

    child = subprocess.Popen(cmdargs, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = child.communicate()

    output = output.decode().strip()
    error = error.decode().strip()

    return child.returncode, output, error


def _lockname(fullpath):
    lockpath = os.path.dirname(fullpath)
    lockname = '.%s.lock' % os.path.basename(fullpath)
    if not os.path.exists(lockpath):
        os.makedirs(lockpath)
    repolock = os.path.join(lockpath, lockname)
    return repolock


def lock_repo(fullpath, nonblocking=False):
    repolock = _lockname(fullpath)

    logger.debug('Attempting to exclusive-lock %s', repolock)
    lockfh = open(repolock, 'w')

    if nonblocking:
        flags = LOCK_EX | LOCK_NB
    else:
        flags = LOCK_EX

    lockf(lockfh, flags)
    global REPO_LOCKH
    REPO_LOCKH[fullpath] = lockfh


def unlock_repo(fullpath):
    global REPO_LOCKH
    if fullpath in REPO_LOCKH.keys():
        logger.debug('Unlocking %s', fullpath)
        lockf(REPO_LOCKH[fullpath], LOCK_UN)
        REPO_LOCKH[fullpath].close()
        del REPO_LOCKH[fullpath]


def is_bare_git_repo(path):
    """
    Return True if path (which is already verified to be a directory)
    sufficiently resembles a base git repo (good enough to fool git
    itself).
    """
    logger.debug('Checking if %s is a git repository', path)
    if (os.path.isdir(os.path.join(path, 'objects')) and
            os.path.isdir(os.path.join(path, 'refs')) and
            os.path.isfile(os.path.join(path, 'HEAD'))):
        return True

    logger.debug('Skipping %s: not a git repository', path)
    return False


def get_repo_timestamp(toplevel, gitdir):
    ts = 0

    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    tsfile = os.path.join(fullpath, 'grokmirror.timestamp')
    if os.path.exists(tsfile):
        with open(tsfile, 'rb') as tsfh:
            contents = tsfh.read()
        try:
            ts = int(contents)
            logger.debug('Timestamp for %s: %s', gitdir, ts)
        except ValueError:
            logger.warning('Was not able to parse timestamp in %s', tsfile)
    else:
        logger.debug('No existing timestamp for %s', gitdir)

    return ts


def set_repo_timestamp(toplevel, gitdir, ts):
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    tsfile = os.path.join(fullpath, 'grokmirror.timestamp')

    with open(tsfile, 'wt') as tsfh:
        tsfh.write('%d' % ts)

    logger.debug('Recorded timestamp for %s: %s', gitdir, ts)


def get_repo_obj_info(fullpath):
    args = ['count-objects', '-v']
    retcode, output, error = run_git_command(fullpath, args)
    obj_info = {}

    if output:
        for line in output.split('\n'):
            key, value = line.split(':')
            obj_info[key] = value.strip()

    return obj_info


def get_altrepo(fullpath):
    altfile = os.path.join(fullpath, 'objects', 'info', 'alternates')
    altdir = None
    try:
        with open(altfile, 'r') as fh:
            contents = fh.read().strip()
            if len(contents) > 8 and contents[-8:] == '/objects':
                altdir = os.path.realpath(contents[:-8])
    except IOError:
        pass

    return altdir


def set_altrepo(fullpath, altdir):
    # I assume you already checked if this is a sane operation to perform
    altfile = os.path.join(fullpath, 'objects', 'info', 'alternates')
    objpath = os.path.join(altdir, 'objects')
    if os.path.isdir(objpath):
        with open(altfile, 'w') as fh:
            fh.write(objpath + '\n')
    else:
        logger.critical('objdir %s does not exist, not setting alternates file %s', objpath, altfile)


def get_rootsets(toplevel, obstdir, em=None):
    top_roots = dict()
    obst_roots = dict()
    ignore = ['%s/*' % obstdir.rstrip('/')]
    topdirs = find_all_gitdirs(toplevel, ignore=ignore, normalize=True)
    obstdirs = find_all_gitdirs(obstdir, normalize=True)
    e_rts = None
    if em is not None:
        # noinspection PyTypeChecker
        e_rts = em.counter(total=len(topdirs)+len(obstdirs), desc='Grokking', color='white', unit='repos', leave=False)
    for fullpath in topdirs:
        # Does it have an alternates file pointing to obstdir?
        if e_rts is not None:
            e_rts.update()
        altdir = get_altrepo(fullpath)
        if altdir and altdir.find(obstdir) == 0:
            # return the roots from there instead
            roots = get_repo_roots(altdir)
            if altdir not in obst_roots:
                obst_roots[altdir] = roots
        else:
            roots = get_repo_roots(fullpath)
        if roots:
            top_roots[fullpath] = roots

    for fullpath in obstdirs:
        if e_rts is not None:
            e_rts.update()
        if fullpath in obst_roots:
            continue
        roots = get_repo_roots(fullpath)
        if roots:
            obst_roots[fullpath] = roots

    if e_rts is not None:
        e_rts.close()

    return top_roots, obst_roots


def get_repo_roots(fullpath, force=False):
    if not os.path.exists(fullpath):
        logger.debug('Cannot check roots in %s, as it does not exist', fullpath)
        return None
    rfile = os.path.join(fullpath, 'grokmirror.roots')
    if not force and os.path.exists(rfile):
        with open(rfile, 'rt') as rfh:
            content = rfh.read()
            roots = set(content.split('\n'))
    else:
        logger.debug('Generating roots for %s', fullpath)
        ecode, out, err = run_git_command(fullpath, ['rev-list', '--max-parents=0', '--all'])
        if ecode > 0:
            logger.debug('Error listing roots in %s', fullpath)
            return None

        if not len(out):
            logger.debug('No roots in %s', fullpath)
            return None

        # save it for future use
        with open(rfile, 'w') as rfh:
            rfh.write(out)
            logger.debug('Wrote %s', rfile)
        roots = set(out.split('\n'))

    return roots


def setup_objstore_repo(obstdir, name=None):
    if name is None:
        name = str(uuid.uuid4())
    pathlib.Path(obstdir).mkdir(parents=True, exist_ok=True)
    obstrepo = os.path.join(obstdir, '%s.git' % name)
    logger.debug('Creating objstore repo in %s', obstrepo)
    lock_repo(obstrepo)
    args = ['init', '--bare', obstrepo]
    ecode, out, err = run_git_command(None, args)
    if ecode > 0:
        logger.critical('Error creating objstore repo %s: %s', obstrepo, err)
        sys.exit(1)
    # Remove .sample files from hooks, because they are just dead weight
    hooksdir = os.path.join(obstrepo, 'hooks')
    for child in pathlib.Path(hooksdir).iterdir():
        if child.suffix == '.sample':
            child.unlink()
    # Never auto-gc
    set_git_config(obstrepo, 'gc.auto', '0')
    # All our objects are precious -- we only turn this off when repacking
    set_git_config(obstrepo, 'core.repositoryformatversion', '1')
    set_git_config(obstrepo, 'extensions.preciousObjects', 'true')
    # Set maximum compression, though perhaps we should make this configurable
    set_git_config(obstrepo, 'pack.compression', '9')
    # Set island configs
    set_git_config(obstrepo, 'repack.useDeltaIslands', 'true')
    set_git_config(obstrepo, 'repack.writeBitmaps', 'true')
    set_git_config(obstrepo, 'pack.island', 'refs/virtual/([0-9a-f]+)/', operation='--add')
    unlock_repo(obstrepo)
    return obstrepo


def objstore_virtref(fullpath):
    fullpath = os.path.realpath(fullpath)
    vh = hashlib.sha1()
    vh.update(fullpath.encode())
    return vh.hexdigest()[:12]


def list_repo_remotes(fullpath, withurl=False):
    args = ['remote']
    if withurl:
        args.append('-v')

    ecode, out, err = run_git_command(fullpath, args)
    if not len(out):
        logger.debug('Could not list remotes in %s', fullpath)
        return list()

    if not withurl:
        return out.split('\n')

    remotes = list()
    for line in out.split('\n'):
        remotes.append(tuple(line.split()[:2]))
    return remotes


def add_repo_to_objstore(obstrepo, fullpath):
    virtref = objstore_virtref(fullpath)
    remotes = list_repo_remotes(obstrepo)
    if virtref in remotes:
        logger.debug('%s is already set up for objstore in %s', fullpath, obstrepo)
        return False

    args = ['remote', 'add', virtref, fullpath, '--no-tags']
    ecode, out, err = run_git_command(obstrepo, args)
    if ecode > 0:
        logger.critical('Could not add remote to %s', obstrepo)
        sys.exit(1)
    set_git_config(obstrepo, 'remote.%s.fetch' % virtref, '+refs/*:refs/virtual/%s/*' % virtref)
    return True


def fetch_objstore_repo(obstrepo, fullpath=None):
    my_remotes = list_repo_remotes(obstrepo)
    if fullpath:
        virtref = objstore_virtref(fullpath)
        if virtref in my_remotes:
            remotes = {virtref}
        else:
            logger.debug('%s is not in remotes for %s', fullpath, obstrepo)
            return False
    else:
        remotes = my_remotes

    success = True
    for remote in remotes:
        ecode, out, err = run_git_command(obstrepo, ['fetch', remote, '--prune'])
        if ecode > 0:
            logger.critical('Could not fetch objects from %s to %s', remote, obstrepo)
            success = False

    return success


def is_private_repo(config, fullpath):
    privmasks = config['core'].get('private', '')
    if not len(privmasks):
        return False
    for privmask in privmasks.split('\n'):
        # Does this repo match privrepo
        if fnmatch.fnmatch(fullpath, privmask.strip()):
            return True

    return False


def find_siblings(fullpath, my_roots, known_roots):
    siblings = set()
    for gitpath, gitroots in known_roots.items():
        # Of course we're going to match ourselves
        if fullpath == gitpath or not my_roots:
            continue
        if gitroots and len(gitroots.intersection(my_roots)):
            # These are sibling repositories
            siblings.add(gitpath)

    return siblings


def find_best_obstrepo(mypath, obst_roots):
    # We want to find an intersect with most matching roots
    myroots = get_repo_roots(mypath)
    if not myroots:
        return None
    obstrepo = None
    bestcount = 0
    for path, roots in obst_roots.items():
        if path == mypath or not roots:
            continue
        icount = len(roots.intersection(myroots))
        if icount > bestcount:
            obstrepo = path
            bestcount = icount

    return obstrepo


def get_obstrepo_mapping(obstdir):
    mapping = dict()
    if not os.path.isdir(obstdir):
        return mapping
    for child in pathlib.Path(obstdir).iterdir():
        if child.is_dir() and child.suffix == '.git':
            obstrepo = child.as_posix()
            ecode, out, err = run_git_command(obstrepo, ['remote', '-v'])
            if ecode > 0:
                # weird
                continue
            lines = out.split('\n')
            for line in lines:
                chunks = line.split()
                if len(chunks) < 2:
                    continue
                name, url = chunks[:2]
                if url in mapping:
                    continue
                # Does it still exist?
                if not os.path.isdir(url):
                    continue
                mapping[url] = obstrepo
    return mapping


def find_objstore_repo_for(obstdir, fullpath):
    if not os.path.isdir(obstdir):
        return None

    logger.debug('Finding an objstore repo matching %s', fullpath)
    virtref = objstore_virtref(fullpath)
    for child in pathlib.Path(obstdir).iterdir():
        if child.is_dir() and child.suffix == '.git':
            obstrepo = child.as_posix()
            remotes = list_repo_remotes(obstrepo)
            if virtref in remotes:
                logger.debug('Found %s', child.name)
                return obstrepo

    logger.debug('No matching objstore repos for %s', fullpath)
    return None


def get_forkgroups(obstdir, toplevel):
    forkgroups = dict()
    if not os.path.exists(obstdir):
        return forkgroups
    for child in pathlib.Path(obstdir).iterdir():
        if child.is_dir() and child.suffix == '.git':
            forkgroup = child.stem
            forkgroups[forkgroup] = set()
            obstrepo = child.as_posix()
            remotes = list_repo_remotes(obstrepo, withurl=True)
            for virtref, url in remotes:
                if url.find(toplevel) != 0:
                    continue
                forkgroups[forkgroup].add(url)
    return forkgroups


def get_repo_fingerprint(toplevel, gitdir, force=False):
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    if not os.path.exists(fullpath):
        logger.debug('Cannot fingerprint %s, as it does not exist', fullpath)
        return None

    fpfile = os.path.join(fullpath, 'grokmirror.fingerprint')
    if not force and os.path.exists(fpfile):
        with open(fpfile, 'r') as fpfh:
            fingerprint = fpfh.read()
        logger.debug('Fingerprint for %s: %s', gitdir, fingerprint)
    else:
        logger.debug('Generating fingerprint for %s', gitdir)
        ecode, out, err = run_git_command(fullpath, ['show-ref'])
        if ecode > 0 or not len(out):
            logger.debug('No heads in %s, nothing to fingerprint.', fullpath)
            return None

        # We add the final "\n" to be compatible with cmdline output
        # of git-show-ref
        fingerprint = hashlib.sha1(out.encode() + b"\n").hexdigest()

        # Save it for future use
        if not force:
            set_repo_fingerprint(toplevel, gitdir, fingerprint)

    return fingerprint


def set_repo_fingerprint(toplevel, gitdir, fingerprint=None):
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    fpfile = os.path.join(fullpath, 'grokmirror.fingerprint')

    if fingerprint is None:
        fingerprint = get_repo_fingerprint(toplevel, gitdir, force=True)

    with open(fpfile, 'wt') as fpfh:
        fpfh.write('%s' % fingerprint)

    logger.debug('Recorded fingerprint for %s: %s', gitdir, fingerprint)
    return fingerprint


def get_altrepo_map(toplevel, refresh=False):
    global _alt_repo_map
    if _alt_repo_map is None or refresh:
        logger.info('Finding all repositories using alternates')
        _alt_repo_map = dict()
        tp = pathlib.Path(toplevel)
        for subp in tp.glob('**/*.git'):
            if subp.is_symlink():
                # Don't care about symlinks for altrepo mapping
                continue
            fullpath = subp.resolve().as_posix()
            altrepo = get_altrepo(fullpath)
            if not altrepo:
                continue
            if altrepo not in _alt_repo_map:
                _alt_repo_map[altrepo] = set()
            _alt_repo_map[altrepo].add(fullpath)
    return _alt_repo_map


def is_alt_repo(toplevel, refrepo):
    amap = get_altrepo_map(toplevel)

    looking_for = os.path.realpath(os.path.join(toplevel, refrepo.strip('/')))
    if looking_for in amap:
        return True
    return False


def is_obstrepo(fullpath, obstdir):
    # At this point, both should be normalized
    return fullpath.find(obstdir) == 0


def find_all_gitdirs(toplevel, ignore=None, normalize=False):
    if ignore is None:
        ignore = set()

    logger.info('Finding bare git repos in %s', toplevel)
    logger.debug('Ignore list: %s', ' '.join(ignore))
    gitdirs = set()
    tp = pathlib.Path(toplevel)
    for subp in tp.glob('**/*.git'):
        # Should we ignore this dir?
        ignored = False
        for ignoreglob in ignore:
            if subp.match(ignoreglob):
                ignored = True
                break
        if ignored:
            continue
        fullpath = subp.resolve().as_posix()
        if not is_bare_git_repo(fullpath):
            continue

        if normalize:
            fullpath = os.path.realpath(fullpath)

        logger.debug('Found %s', fullpath)
        if fullpath not in gitdirs:
            gitdirs.add(fullpath)

    return gitdirs


def manifest_lock(manifile):
    global MANIFEST_LOCKH
    if MANIFEST_LOCKH is not None:
        logger.debug('Manifest %s already locked', manifile)

    manilock = _lockname(manifile)
    MANIFEST_LOCKH = open(manilock, 'w')
    logger.debug('Attempting to lock %s', manilock)
    lockf(MANIFEST_LOCKH, LOCK_EX)
    logger.debug('Manifest lock obtained')


def manifest_unlock(manifile):
    global MANIFEST_LOCKH
    if MANIFEST_LOCKH is not None:
        logger.debug('Unlocking manifest %s', manifile)
        lockf(MANIFEST_LOCKH, LOCK_UN)
        # noinspection PyUnresolvedReferences
        MANIFEST_LOCKH.close()
        MANIFEST_LOCKH = None


def read_manifest(manifile, wait=False):
    while True:
        if not wait or os.path.exists(manifile):
            break
        logger.info('Manifest file not yet found, waiting...')
        # Unlock the manifest so other processes aren't waiting for us
        was_locked = False
        if MANIFEST_LOCKH is not None:
            was_locked = True
            manifest_unlock(manifile)
        time.sleep(1)
        if was_locked:
            manifest_lock(manifile)

    if not os.path.exists(manifile):
        logger.info('%s not found, assuming initial run', manifile)
        return {}

    if manifile.find('.gz') > 0:
        import gzip
        fh = gzip.open(manifile, 'rb')
    else:
        fh = open(manifile, 'rb')

    logger.debug('Reading %s', manifile)
    jdata = fh.read().decode('utf-8')
    fh.close()

    # noinspection PyBroadException
    try:
        manifest = json.loads(jdata)
    except:
        # We'll regenerate the file entirely on failure to parse
        logger.critical('Unable to parse %s, will regenerate', manifile)
        manifest = dict()

    logger.debug('Manifest contains %s entries', len(manifest.keys()))

    return manifest


def write_manifest(manifile, manifest, mtime=None, pretty=False):
    import tempfile
    import shutil
    import gzip

    logger.debug('Writing new %s', manifile)

    (dirname, basename) = os.path.split(manifile)
    (fd, tmpfile) = tempfile.mkstemp(prefix=basename, dir=dirname)
    fh = os.fdopen(fd, 'wb', 0)
    logger.debug('Created a temporary file in %s', tmpfile)
    logger.debug('Writing to %s', tmpfile)
    try:
        if pretty:
            jdata = json.dumps(manifest, indent=2, sort_keys=True)
        else:
            jdata = json.dumps(manifest)

        jdata = jdata.encode('utf-8')
        if manifile.endswith('.gz'):
            gfh = gzip.GzipFile(fileobj=fh, mode='wb')
            gfh.write(jdata)
            gfh.close()
        else:
            fh.write(jdata)

        os.fsync(fd)
        fh.close()
        # set mode to current umask
        curmask = os.umask(0)
        os.chmod(tmpfile, 0o0666 ^ curmask)
        os.umask(curmask)
        if mtime is not None:
            logger.debug('Setting mtime to %s', mtime)
            os.utime(tmpfile, (mtime, mtime))
        logger.debug('Moving %s to %s', tmpfile, manifile)
        shutil.move(tmpfile, manifile)

    finally:
        # If something failed, don't leave these trailing around
        if os.path.exists(tmpfile):
            logger.debug('Removing %s', tmpfile)
            os.unlink(tmpfile)


def load_config_file(cfgfile):
    from configparser import ConfigParser, ExtendedInterpolation
    if not os.path.exists(cfgfile):
        sys.stderr.write('ERORR: File does not exist: %s\n' % cfgfile)
        sys.exit(1)
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(cfgfile)

    if 'core' not in config:
        sys.stderr.write('ERROR: Section [core] must exist in: %s\n' % cfgfile)
        sys.stderr.write('       Perhaps this is a grokmirror-1.x config file?\n')
        sys.exit(1)

    return config
