# -*- coding: utf-8 -*-
# Copyright (C) 2013-2020 by The Linux Foundation and contributors
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
import logging.handlers
import hashlib
import pathlib
import uuid
import tempfile
import shutil
import gzip
import datetime

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry


VERSION = '2.0.9'
MANIFEST_LOCKH = None
REPO_LOCKH = dict()
GITBIN = '/usr/bin/git'

# default logger. Will be overridden.
logger = logging.getLogger(__name__)

_alt_repo_map = None

# Used to store our requests session
REQSESSION = None

OBST_PREAMBULE = ('# WARNING: This is a grokmirror object storage repository.\n'
                  '# Deleting or moving it will cause corruption in the following repositories\n'
                  '# (caution, this list may be incomplete):\n')


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


def run_shell_command(cmdargs, stdin=None, decode=True):
    logger.debug('Running: %s', ' '.join(cmdargs))

    child = subprocess.Popen(cmdargs, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, error = child.communicate(input=stdin)

    if decode:
        output = output.decode().strip()
        error = error.decode().strip()

    return child.returncode, output, error


def run_git_command(fullpath, args, stdin=None, decode=True):
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

    return run_shell_command(cmdargs, stdin, decode=decode)


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
    obj_info = dict()

    if output:
        for line in output.split('\n'):
            key, value = line.split(':')
            obj_info[key] = value.strip()

    return obj_info


def get_repo_defs(toplevel, gitdir, usenow=False, ignorerefs=None):
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    description = None
    try:
        descfile = os.path.join(fullpath, 'description')
        with open(descfile, 'rb') as fh:
            contents = fh.read().strip()
            if len(contents) and contents.find(b'edit this file') < 0:
                # We don't need to tell mirrors to edit this file
                description = contents.decode(errors='replace')
    except IOError:
        pass

    entries = get_config_from_git(fullpath, r'gitweb\..*')
    owner = entries.get('owner', None)

    modified = 0

    if not usenow:
        args = ['for-each-ref', '--sort=-committerdate', '--format=%(committerdate:iso-strict)', '--count=1']
        ecode, out, err = run_git_command(fullpath, args)
        if len(out):
            try:
                modified = datetime.datetime.fromisoformat(out)
            except AttributeError:
                # Python 3.6 doesn't have fromisoformat
                # remove : from the TZ info
                out = out[:-3] + out[-2:]
                modified = datetime.datetime.strptime(out, '%Y-%m-%dT%H:%M:%S%z')

    if not modified:
        modified = datetime.datetime.now()

    head = None
    try:
        with open(os.path.join(fullpath, 'HEAD')) as fh:
            head = fh.read().strip()
    except IOError:
        pass

    forkgroup = None
    altrepo = get_altrepo(fullpath)
    if altrepo and os.path.exists(os.path.join(altrepo, 'grokmirror.objstore')):
        forkgroup = os.path.basename(altrepo)[:-4]

    # we need a way to quickly compare whether mirrored repositories match
    # what is in the master manifest. To this end, we calculate a so-called
    # "state fingerprint" -- basically the output of "git show-ref | sha1sum".
    # git show-ref output is deterministic and should accurately list all refs
    # and their relation to heads/tags/etc.
    fingerprint = get_repo_fingerprint(toplevel, gitdir, force=True, ignorerefs=ignorerefs)
    # Record it in the repo for other use
    set_repo_fingerprint(toplevel, gitdir, fingerprint)
    repoinfo = {
        'modified': int(modified.timestamp()),
        'fingerprint': fingerprint,
        'head': head,
    }

    # Don't add empty things to manifest
    if owner:
        repoinfo['owner'] = owner
    if description:
        repoinfo['description'] = description
    if forkgroup:
        repoinfo['forkgroup'] = forkgroup

    return repoinfo


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


def get_rootsets(toplevel, obstdir):
    top_roots = dict()
    obst_roots = dict()
    topdirs = find_all_gitdirs(toplevel, normalize=True, exclude_objstore=True)
    obstdirs = find_all_gitdirs(obstdir, normalize=True, exclude_objstore=False)
    for fullpath in topdirs:
        roots = get_repo_roots(fullpath)
        if roots:
            top_roots[fullpath] = roots

    for fullpath in obstdirs:
        if fullpath in obst_roots:
            continue
        roots = get_repo_roots(fullpath)
        if roots:
            obst_roots[fullpath] = roots

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


def setup_bare_repo(fullpath):
    args = ['init', '--bare', fullpath]
    ecode, out, err = run_git_command(None, args)
    if ecode > 0:
        logger.critical('Unable to bare-init %s', fullpath)
        return False

    # Remove .sample files from hooks, because they are just dead weight
    hooksdir = os.path.join(fullpath, 'hooks')
    for child in pathlib.Path(hooksdir).iterdir():
        if child.suffix == '.sample':
            child.unlink()
    # We never want auto-gc anywhere
    set_git_config(fullpath, 'gc.auto', '0')
    # We don't care about FETCH_HEAD information and writing to it just
    # wastes IO cycles
    os.symlink('/dev/null', os.path.join(fullpath, 'FETCH_HEAD'))
    return True


def setup_objstore_repo(obstdir, name=None):
    if name is None:
        name = str(uuid.uuid4())
    pathlib.Path(obstdir).mkdir(parents=True, exist_ok=True)
    obstrepo = os.path.join(obstdir, '%s.git' % name)
    logger.debug('Creating objstore repo in %s', obstrepo)
    lock_repo(obstrepo)
    if not setup_bare_repo(obstrepo):
        sys.exit(1)
    # All our objects are precious -- we only turn this off when repacking
    set_git_config(obstrepo, 'core.repositoryformatversion', '1')
    set_git_config(obstrepo, 'extensions.preciousObjects', 'true')
    # Set maximum compression, though perhaps we should make this configurable
    set_git_config(obstrepo, 'pack.compression', '9')
    # Set island configs
    set_git_config(obstrepo, 'repack.useDeltaIslands', 'true')
    set_git_config(obstrepo, 'repack.writeBitmaps', 'true')
    set_git_config(obstrepo, 'pack.island', 'refs/virtual/([0-9a-f]+)/', operation='--add')
    telltale = os.path.join(obstrepo, 'grokmirror.objstore')
    with open(telltale, 'w') as fh:
        fh.write(OBST_PREAMBULE)
    unlock_repo(obstrepo)
    return obstrepo


def objstore_virtref(fullpath):
    fullpath = os.path.realpath(fullpath)
    vh = hashlib.sha1()
    vh.update(fullpath.encode())
    return vh.hexdigest()[:12]


def objstore_trim_virtref(obstrepo, virtref):
    args = ['for-each-ref', '--format', 'delete %(refname)', f'refs/virtual/{virtref}']
    ecode, out, err = run_git_command(obstrepo, args)
    if ecode == 0 and len(out):
        out += '\n'
        args = ['update-ref', '--stdin']
        run_git_command(obstrepo, args, stdin=out.encode())


def remove_from_objstore(obstrepo, fullpath):
    # is fullpath still using us?
    altrepo = get_altrepo(fullpath)
    if altrepo and os.path.realpath(obstrepo) == os.path.realpath(altrepo):
        # Repack the child first, using minimal flags
        args = ['repack', '-abq']
        ecode, out, err = run_git_command(fullpath, args)
        if ecode > 0:
            logger.debug('Could not repack child repo %s for removal from %s', fullpath, obstrepo)
            return False
        os.unlink(os.path.join(fullpath, 'objects', 'info', 'alternates'))

    virtref = objstore_virtref(fullpath)
    objstore_trim_virtref(obstrepo, virtref)

    args = ['remote', 'remove', virtref]
    run_git_command(obstrepo, args)
    try:
        os.unlink(os.path.join(obstrepo, 'grokmirror.%s.fingerprint' % virtref))
    except (IOError, FileNotFoundError):
        pass
    return True


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
        entry = tuple(line.split()[:2])
        if entry not in remotes:
            remotes.append(entry)
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
    telltale = os.path.join(obstrepo, 'grokmirror.objstore')
    knownsiblings = set()
    if os.path.exists(telltale):
        with open(telltale) as fh:
            for line in fh.readlines():
                line = line.strip()
                if not len(line) or line[0] == '#':
                    continue
                if os.path.isdir(line):
                    knownsiblings.add(line)
    knownsiblings.add(fullpath)
    with open(telltale, 'w') as fh:
        fh.write(OBST_PREAMBULE)
        fh.write('\n'.join(sorted(list(knownsiblings))) + '\n')

    return True


def _fetch_objstore_repo_using_plumbing(srcrepo, obstrepo, virtref):
    # Copies objects to objstore repos using direct git plumbing
    # as opposed to using "fetch". See discussion here:
    # http://lore.kernel.org/git/20200720173220.GB2045458@coredump.intra.peff.net
    # First, hardlink all objects and packs
    srcobj = os.path.join(srcrepo, 'objects')
    dstobj = os.path.join(obstrepo, 'objects')
    torm = set()
    for root, dirs, files in os.walk(srcobj, topdown=True):
        if 'info' in dirs:
            dirs.remove('info')
        subpath = root.replace(srcobj, '').lstrip('/')
        for file in files:
            srcpath = os.path.join(root, file)
            if file.endswith('.bitmap'):
                torm.add(srcpath)
                continue
            dstpath = os.path.join(dstobj, subpath, file)
            if not os.path.exists(dstpath):
                pathlib.Path(os.path.dirname(dstpath)).mkdir(parents=True, exist_ok=True)
                os.link(srcpath, dstpath)
                torm.add(srcpath)

    # Now we generate a list of refs on both sides
    srcargs = ['for-each-ref', f'--format=%(objectname) refs/virtual/{virtref}/%(refname:lstrip=1)']
    ecode, out, err = run_git_command(srcrepo, srcargs)
    if ecode > 0:
        logger.debug('Could not for-each-ref %s: %s', srcrepo, err)
        return False
    srcset = set(out.strip().split('\n'))

    dstargs = ['for-each-ref', f'--format=%(objectname) %(refname)', f'refs/virtual/{virtref}']
    ecode, out, err = run_git_command(obstrepo, dstargs)
    if ecode > 0:
        logger.debug('Could not for-each-ref %s: %s', obstrepo, err)
        return False
    dstset = set(out.strip().split('\n'))

    # Now we create a stdin list of commands for update-ref
    mapping = dict()
    newset = srcset.difference(dstset)
    if newset:
        for refline in newset:
            obj, ref = refline.split(' ', 1)
            mapping[ref] = obj

    commands = ''
    oldset = dstset.difference(srcset)
    if oldset:
        for refline in oldset:
            if not len(refline):
                continue
            obj, ref = refline.split(' ', 1)
            if ref in mapping:
                commands += f'update {ref} {mapping[ref]} {obj}\n'
                mapping.pop(ref)
            else:
                commands += f'delete {ref} {obj}\n'

    for ref, obj in mapping.items():
        commands += f'create {ref} {obj}\n'

    logger.debug('stdin=%s', commands)
    args = ['update-ref', '--stdin']
    ecode, out, err = run_git_command(obstrepo, args, stdin=commands.encode())
    if ecode > 0:
        logger.debug('Could not update-ref %s: %s', obstrepo, err)
        return False

    for file in torm:
        os.unlink(file)

    return True


def fetch_objstore_repo(obstrepo, fullpath=None, pack_refs=False, use_plumbing=False):
    my_remotes = list_repo_remotes(obstrepo, withurl=True)
    if fullpath:
        virtref = objstore_virtref(fullpath)
        if (virtref, fullpath) in my_remotes:
            remotes = {(virtref, fullpath)}
        else:
            logger.debug('%s is not in remotes for %s', fullpath, obstrepo)
            return False
    else:
        remotes = my_remotes

    success = True
    for (virtref, url) in remotes:
        if use_plumbing:
            success = _fetch_objstore_repo_using_plumbing(url, obstrepo, virtref)
        else:
            ecode, out, err = run_git_command(obstrepo, ['fetch', virtref, '--prune'])
            if ecode > 0:
                success = False

        if success:
            r_fp = os.path.join(url, 'grokmirror.fingerprint')
            if os.path.exists(r_fp):
                l_fp = os.path.join(obstrepo, 'grokmirror.%s.fingerprint' % virtref)
                shutil.copy(r_fp, l_fp)
            if pack_refs:
                try:
                    lock_repo(obstrepo, nonblocking=True)
                    run_git_command(obstrepo, ['pack-refs'])
                    unlock_repo(obstrepo)
                except IOError:
                    # Next run will take care of it
                    pass

        else:
            logger.info('Could not fetch objects from %s to %s', url, obstrepo)

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


def find_siblings(fullpath, my_roots, known_roots, exact=False):
    siblings = set()
    for gitpath, gitroots in known_roots.items():
        # Of course we're going to match ourselves
        if fullpath == gitpath or not my_roots or not gitroots or not len(gitroots.intersection(my_roots)):
            continue
        if gitroots == my_roots:
            siblings.add(gitpath)
            continue
        if exact:
            continue
        if gitroots.issubset(my_roots) or my_roots.issubset(gitroots):
            siblings.add(gitpath)
            continue
        sumdiff = len(gitroots.difference(my_roots)) + len(my_roots.difference(gitroots))
        # If we only differ by a single root, consider us siblings
        if sumdiff <= 2:
            siblings.add(gitpath)
            continue

    return siblings


def find_best_obstrepo(mypath, obst_roots, toplevel, baselines, minratio=0.2):
    # We want to find a repo with best intersect len to total roots len ratio,
    # but we'll ignore any repos where the ratio is too low, in order not to lump
    # together repositories that have very weak common histories.
    myroots = get_repo_roots(mypath)
    if not myroots:
        return None
    obstrepo = None
    bestratio = 0
    for path, roots in obst_roots.items():
        if path == mypath or not roots:
            continue
        icount = len(roots.intersection(myroots))
        if icount == 0:
            # No match at all
            continue
        # Baseline repos win over the ratio logic
        if len(baselines):
            # Any of its member siblings match baselines?
            s_remotes = list_repo_remotes(path, withurl=True)
            for virtref, childpath in s_remotes:
                gitdir = '/' + os.path.relpath(childpath, toplevel)
                for baseline in baselines:
                    # Does this repo match a baseline
                    if fnmatch.fnmatch(gitdir, baseline):
                        # Use this one
                        return path

        ratio = icount / len(roots)
        if ratio < minratio:
            continue
        if ratio > bestratio:
            obstrepo = path
            bestratio = ratio

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


def get_repo_fingerprint(toplevel, gitdir, force=False, ignorerefs=None):
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

        if ignorerefs:
            hasher = hashlib.sha1()
            for line in out.split('\n'):
                rhash, rname = line.split(maxsplit=1)
                ignored = False
                for ignoreref in ignorerefs:
                    if fnmatch.fnmatch(rname, ignoreref):
                        ignored = True
                        break
                if ignored:
                    continue
                hasher.update(line.encode() + b'\n')

            fingerprint = hasher.hexdigest()
        else:
            # We add the final "\n" to be compatible with cmdline output
            # of git-show-ref
            fingerprint = hashlib.sha1(out.encode() + b'\n').hexdigest()

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
        logger.info('   search: finding all repos using alternates')
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


def is_obstrepo(fullpath, obstdir=None):
    if obstdir:
        # At this point, both should be normalized
        return fullpath.find(obstdir) == 0
    # Just check if it has a grokmirror.objstore file in the repo
    return os.path.exists(os.path.join(fullpath, 'grokmirror.objstore'))


def find_all_gitdirs(toplevel, ignore=None, normalize=False, exclude_objstore=True):
    global _alt_repo_map
    if _alt_repo_map is None:
        _alt_repo_map = dict()
        build_amap = True
    else:
        build_amap = False

    if ignore is None:
        ignore = set()

    logger.info('   search: finding all repos in %s', toplevel)
    logger.debug('Ignore list: %s', ' '.join(ignore))
    gitdirs = set()
    for root, dirs, files in os.walk(toplevel, topdown=True):
        if not len(dirs):
            continue

        torm = set()
        for name in dirs:
            fullpath = os.path.join(root, name)
            # Should we ignore this dir?
            ignored = False
            for ignoredir in ignore:
                if fnmatch.fnmatch(fullpath, ignoredir):
                    torm.add(name)
                    ignored = True
                    break
            if ignored:
                continue
            if not is_bare_git_repo(fullpath):
                continue
            if exclude_objstore and os.path.exists(os.path.join(fullpath, 'grokmirror.objstore')):
                continue
            if normalize:
                fullpath = os.path.realpath(fullpath)

            logger.debug('Found %s', os.path.join(root, name))
            gitdirs.add(fullpath)
            torm.add(name)

            if build_amap:
                altrepo = get_altrepo(fullpath)
                if not altrepo:
                    continue
                if altrepo not in _alt_repo_map:
                    _alt_repo_map[altrepo] = set()
                _alt_repo_map[altrepo].add(fullpath)

        for name in torm:
            # don't recurse into the found *.git dirs
            dirs.remove(name)

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
        # noinspection PyTypeChecker
        lockf(MANIFEST_LOCKH, LOCK_UN)
        # noinspection PyUnresolvedReferences
        MANIFEST_LOCKH.close()
        MANIFEST_LOCKH = None


def read_manifest(manifile, wait=False):
    while True:
        if not wait or os.path.exists(manifile):
            break
        logger.info(' manifest: manifest does not exist yet, waiting ...')
        # Unlock the manifest so other processes aren't waiting for us
        was_locked = False
        if MANIFEST_LOCKH is not None:
            was_locked = True
            manifest_unlock(manifile)
        time.sleep(1)
        if was_locked:
            manifest_lock(manifile)

    if not os.path.exists(manifile):
        logger.info(' manifest: no local manifest, assuming initial run')
        return dict()

    if manifile.find('.gz') > 0:
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

    toplevel = os.path.realpath(os.path.expanduser(config['core'].get('toplevel')))
    if not os.access(toplevel, os.W_OK):
        logger.critical('Toplevel %s does not exist or is not writable', toplevel)
        sys.exit(1)
    # Just in case we did expanduser
    config['core']['toplevel'] = toplevel

    obstdir = config['core'].get('objstore', None)
    if obstdir is None:
        obstdir = os.path.join(toplevel, 'objstore')
        config['core']['objstore'] = obstdir

    # Handle some other defaults
    manifile = config['core'].get('manifest')
    if not manifile:
        config['core']['manifest'] = os.path.join(toplevel, 'manifest.js.gz')

    fstat = os.stat(cfgfile)
    # stick last config file modification date into the config object,
    # so we can catch config file updates
    config.last_modified = fstat[8]

    return config


def is_precious(fullpath):
    args = ['config', '--get', 'extensions.preciousObjects']
    retcode, output, error = run_git_command(fullpath, args)
    if output.strip().lower() in ('yes', 'true', '1'):
        return True
    return False


def get_repack_level(obj_info, max_loose_objects=1200, max_packs=20, pc_loose_objects=10, pc_loose_size=10):
    # for now, hardcode the maximum loose objects and packs
    # XXX: we can probably set this in git config values?
    #      I don't think this makes sense as a global setting, because
    #      optimal values will depend on the size of the repo as a whole
    packs = int(obj_info['packs'])
    count_loose = int(obj_info['count'])

    needs_repack = 0

    # first, compare against max values:
    if packs >= max_packs:
        logger.debug('Triggering full repack because packs > %s', max_packs)
        needs_repack = 2
    elif count_loose >= max_loose_objects:
        logger.debug('Triggering quick repack because loose objects > %s', max_loose_objects)
        needs_repack = 1
    else:
        # is the number of loose objects or their size more than 10% of
        # the overall total?
        in_pack = int(obj_info['in-pack'])
        size_loose = int(obj_info['size'])
        size_pack = int(obj_info['size-pack'])
        total_obj = count_loose + in_pack
        total_size = size_loose + size_pack
        # If we have an alternate, then add those numbers in
        alternate = obj_info.get('alternate')
        if alternate and len(alternate) > 8 and alternate[-8:] == '/objects':
            alt_obj_info = get_repo_obj_info(alternate[:-8])
            total_obj += int(alt_obj_info['in-pack'])
            total_size += int(alt_obj_info['size-pack'])

        # set some arbitrary "worth bothering" limits so we don't
        # continuously repack tiny repos.
        if total_obj > 500 and count_loose / total_obj * 100 >= pc_loose_objects:
            logger.debug('Triggering repack because loose objects > %s%% of total', pc_loose_objects)
            needs_repack = 1
        elif total_size > 1024 and size_loose / total_size * 100 >= pc_loose_size:
            logger.debug('Triggering repack because loose size > %s%% of total', pc_loose_size)
            needs_repack = 1

    return needs_repack


def init_logger(subcommand, logfile, loglevel, verbose):
    global logger

    logger = logging.getLogger('grokmirror')
    logger.setLevel(logging.DEBUG)

    if logfile:
        ch = logging.handlers.WatchedFileHandler(os.path.expanduser(logfile))
        formatter = logging.Formatter(subcommand + '[%(process)d] %(asctime)s - %(levelname)s - %(message)s')
        ch.setFormatter(formatter)
        ch.setLevel(loglevel)
        logger.addHandler(ch)

    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)

    if verbose:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.CRITICAL)

    logger.addHandler(ch)
    return logger
