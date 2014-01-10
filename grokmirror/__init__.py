# Copyright (C) 2013 by The Linux Foundation and contributors
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

import logging

import hashlib

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB
from StringIO import StringIO

from git import Repo

VERSION = '0.4.0-pre'
MANIFEST_LOCKH = None
REPO_LOCKH = {}

# default logger. Will probably be overridden.
logger = logging.getLogger(__name__)

def _lockname(fullpath):
    lockpath = os.path.dirname(fullpath)
    lockname = '.%s.lock' % os.path.basename(fullpath)
    if not os.path.exists(lockpath):
        os.makedirs(lockpath)
    repolock = os.path.join(lockpath, lockname)
    return repolock


def lock_repo(fullpath, nonblocking=False):
    repolock = _lockname(fullpath)

    logger.debug('Attempting to exclusive-lock %s' % repolock)
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
        logger.debug('Unlocking %s' % fullpath)
        repolock = _lockname(fullpath)
        lockf(REPO_LOCKH[fullpath], LOCK_UN)
        REPO_LOCKH[fullpath].close()
        del REPO_LOCKH[fullpath]

def is_bare_git_repo(path):
    """
    Return True if path (which is already verified to be a directory)
    sufficiently resembles a base git repo (good enough to fool git
    itself).
    """
    logger.debug('Checking if %s is a git repository' % path)
    if (os.path.isdir(os.path.join(path, 'objects')) and
            os.path.isdir(os.path.join(path, 'refs')) and
            os.path.isfile(os.path.join(path, 'HEAD'))):
        return True

    logger.debug('Skipping %s: not a git repository' % path)
    return False

def get_repo_timestamp(toplevel, gitdir):
    ts = 0

    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    tsfile   = os.path.join(fullpath, 'grokmirror.timestamp')
    if os.path.exists(tsfile):
        tsfh = open(tsfile, 'r')
        contents = tsfh.read()
        tsfh.close()
        try:
            ts = int(contents)
            logger.debug('Timestamp for %s: %s' % (gitdir, ts))
        except ValueError, ex:
            logger.warning('Was not able to parse timestamp in %s' % tsfile)
    else:
        logger.debug('No existing timestamp for %s' % gitdir)

    return ts

def set_repo_timestamp(toplevel, gitdir, ts):
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    tsfile   = os.path.join(fullpath, 'grokmirror.timestamp')

    tsfh = open(tsfile, 'w')
    tsfh.write('%d' % ts)
    tsfh.close()

    logger.debug('Recorded timestamp for %s: %s' % (gitdir, ts))

def get_repo_fingerprint(toplevel, gitdir, force=False):
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    fpfile   = os.path.join(fullpath, 'grokmirror.fingerprint')
    if not force and os.path.exists(fpfile):
        fpfh = open(fpfile, 'r')
        fingerprint = fpfh.read()
        fpfh.close()
        logger.debug('Fingerprint for %s: %s' % (gitdir, fingerprint))
    else:
        logger.debug('Generating fingerprint for %s' % gitdir)
        repo = Repo(fullpath)

        # We add the final "\n" to be compatible with cmdline output
        # of git-show-ref
        try:
            fingerprint = hashlib.sha1(repo.git.show_ref()+"\n").hexdigest()
        except:
            logger.critical('Could not fingerprint %s. Bad repo?' % gitdir)
            return None

        # Save it for future use
        if not force:
            set_repo_fingerprint(toplevel, gitdir, fingerprint)

    return fingerprint

def set_repo_fingerprint(toplevel, gitdir, fingerprint=None):
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    fpfile   = os.path.join(fullpath, 'grokmirror.fingerprint')

    if fingerprint is None:
        fingerprint = get_repo_fingerprint(toplevel, gitdir, force=True)

    fpfh = open(fpfile, 'w')
    fpfh.write('%s' % fingerprint)
    fpfh.close()

    logger.debug('Recorded fingerprint for %s: %s' % (gitdir, fingerprint))
    return fingerprint

def find_all_gitdirs(toplevel, ignore=[]):
    logger.info('Finding bare git repos in %s' % toplevel)
    logger.debug('Ignore list: %s' % ' '.join(ignore))
    gitdirs = []
    for root, dirs, files in os.walk(toplevel, topdown=True):
        if not len(dirs):
            continue

        torm = []
        for name in dirs:
            # Should we ignore this dir?
            ignored = False
            for ignoredir in ignore:
                if fnmatch.fnmatch(os.path.join(root, name), ignoredir):
                    torm.append(name)
                    ignored = True
                    break
            if not ignored and is_bare_git_repo(os.path.join(root, name)):
                logger.debug('Found %s' % os.path.join(root, name))
                gitdirs.append(os.path.join(root, name))
                torm.append(name)

        for name in torm:
            # don't recurse into the found *.git dirs
            dirs.remove(name)

    return gitdirs

def manifest_lock(manifile):
    global MANIFEST_LOCKH
    if MANIFEST_LOCKH is not None:
        logger.debug('Manifest already locked')

    manilock = _lockname(manifile)
    MANIFEST_LOCKH = open(manilock, 'w')
    logger.debug('Attempting to lock %s' % manilock)
    lockf(MANIFEST_LOCKH, LOCK_EX)
    logger.debug('Manifest lock obtained')

def manifest_unlock(manifile):
    global MANIFEST_LOCKH
    if MANIFEST_LOCKH is not None:
        logger.debug('Unlocking manifest')
        manilock = _lockname(manifile)
        lockf(MANIFEST_LOCKH, LOCK_UN)
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
        logger.info('%s not found, assuming initial run' % manifile)
        return {}

    if manifile.find('.gz') > 0:
        import gzip
        fh = gzip.open(manifile, 'rb')
    else:
        fh = open(manifile, 'r')

    logger.info('Reading %s' % manifile)
    try:
        manifest = json.load(fh)
    except:
        # We'll regenerate the file entirely on failure to parse
        logger.critical('Unable to parse %s, will regenerate' % manifile)
        manifest = {}

    fh.close()
    logger.debug('Manifest contains %s entries' % len(manifest.keys()))

    return manifest

def write_manifest(manifile, manifest, manifest_permissions, mtime=None, pretty=False):
    import tempfile
    import shutil
    import gzip

    logger.info('Writing new %s' % manifile)

    (dirname, basename) = os.path.split(manifile)
    (fd, tmpfile) = tempfile.mkstemp(prefix=basename, dir=dirname)
    fh = os.fdopen(fd, 'w', 0)
    logger.debug('Created a temporary file in %s' % tmpfile)
    logger.debug('Writing to %s' % tmpfile)
    try:
        if manifile.find('.gz') > 0:
            gfh = gzip.GzipFile(fileobj=fh, mode='wb')
            if pretty:
                json.dump(manifest, gfh, indent=2, sort_keys=True)
            else:
                json.dump(manifest, gfh)
            gfh.close()
        else:
            if pretty:
                json.dump(manifest, fh, indent=2, sort_keys=True)
            else:
                json.dump(manifest, fh)

        os.fsync(fd)
        fh.close()
        os.chmod(tmpfile, int(manifest_permissions,8))
        if mtime is not None:
            logger.debug('Setting mtime to %s' % mtime)
            os.utime(tmpfile, (mtime, mtime))
        logger.debug('Moving %s to %s' % (tmpfile, manifile))
        shutil.move(tmpfile, manifile)

    finally:
        # If something failed, don't leave these trailing around
        if os.path.exists(tmpfile):
            logger.debug('Removing %s' % tmpfile)
            os.unlink(tmpfile)

