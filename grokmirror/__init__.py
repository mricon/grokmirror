#!/usr/bin/python -tt
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

import json
import fnmatch

import logging

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

VERSION = '0.3.1'
MANIFEST_LOCKH = None
REPO_LOCKH = {}

# default logger. Will probably be overridden.
logger = logging.getLogger(__name__)

def lock_repo(fullpath, nonblocking=False):
    repolock = os.path.join(fullpath, 'grokmirror.lock')
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
        lockf(REPO_LOCKH[fullpath], LOCK_UN)
        REPO_LOCKH[fullpath].close()
        del REPO_LOCKH[fullpath]

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
            if not ignored and name.find('.git') > 0:
                logger.debug('Found %s' % os.path.join(root, name))
                gitdirs.append(os.path.join(root, name))
                torm.append(name)

        for name in torm:
            # don't recurse into the found *.git dirs
            dirs.remove(name)

    return gitdirs

def manifest_lock(manifile):
    (dirname, basename) = os.path.split(manifile)
    global MANIFEST_LOCKH
    MANIFEST_LOCKH = open(os.path.join(dirname, '.%s.lock' % basename), 'w')
    logger.debug('Attempting to lock the manifest')
    lockf(MANIFEST_LOCKH, LOCK_EX)
    logger.debug('Manifest lock obtained')

def manifest_unlock(manifile):
    global MANIFEST_LOCKH
    if MANIFEST_LOCKH is not None:
        logger.debug('Unlocking manifest')
        lockf(MANIFEST_LOCKH, LOCK_UN)
        MANIFEST_LOCKH.close()

def read_manifest(manifile):
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

def write_manifest(manifile, manifest, mtime=None):
    import tempfile
    import shutil
    import gzip

    (dirname, basename) = os.path.split(manifile)
    (fd, tmpfile) = tempfile.mkstemp(prefix=basename, dir=dirname)
    logger.info('Writing new %s' % manifile)
    try:
        if manifile.find('.gz') > 0:
            fh = gzip.open(tmpfile, 'wb')
        else:
            fh = open(tmpfile, 'w')
        # Probably should make indent configurable, but extra whitespaces
        # don't change the size of manifest.js.gz by any appreciable amount
        json.dump(manifest, fh, indent=2)
        fh.close()
        os.chmod(tmpfile, 0644)
        if mtime is not None:
            os.utime(tmpfile, (mtime, mtime))
        shutil.move(tmpfile, manifile)

    finally:
        # If something failed, don't leave these trailing around
        if os.path.exists(tmpfile):
            os.unlink(tmpfile)

