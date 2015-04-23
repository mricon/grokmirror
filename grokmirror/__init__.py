# -*- coding: utf-8 -*-
# Copyright (C) 2015 by The Linux Foundation and contributors
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

from __future__ import (absolute_import,
                        division,
                        print_function)

__author__ = 'Konstantin Ryabitsev <konstantin@linuxfoundation.org>'

import os
import sys

import time
import anyjson
import fnmatch

import logging
import hashlib
import subprocess

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

from io import StringIO
from io import open

import pygit2

VERSION = '0.5.0-pre'
MANIFEST_LOCKH = None
REPO_LOCKH = {}

BINGIT = '/usr/bin/git'

# default logger. Will probably be overridden.
logger = logging.getLogger(__name__)


def run_git_command(args=(), env=None):
    if env is None:
        env = {}

    args = [BINGIT] + args

    logger.debug('Running: %s' % ' '.join(args))

    if env:
        logger.debug('With env: %s' % env)

    child = subprocess.Popen(args, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=env)
    (stdout, stderror) = child.communicate()

    return child.returncode, stdout, stderror


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


def find_all_gitdirs(toplevel, ignore=None):
    if ignore is None:
        ignore = []

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


class Repository(object):
    def __init__(self, toplevel, gitdir):
        self.toplevel = toplevel.rstrip('/')
        self.gitdir = gitdir.lstrip('/')
        self.fullpath = os.path.join(toplevel, self.gitdir)
        self.repo = pygit2.Repository(self.fullpath)

        self.__timestamp = None
        self.__fingerprint = None
        self.__description = None
        self.__owner = None
        self.__modified = None

    def _lockname(self):
        lockpath = os.path.dirname(self.fullpath)
        lockname = '.%s.lock' % os.path.basename(self.fullpath)

        if not os.path.exists(lockpath):
            logger.debug('Creating parent dirs for %s' % self.fullpath)
            os.makedirs(lockpath)

        repolock = os.path.join(lockpath, lockname)
        logger.debug('repolock=%s' % repolock)

        return repolock

    def lock(self, nonblocking=False):
        repolock = self._lockname()
        logger.debug('Locking repo %s' % self.gitdir)
        logger.debug('Attempting to exclusive-lock %s' % repolock)
        lockfh = open(repolock, 'w')

        if nonblocking:
            flags = LOCK_EX | LOCK_NB
        else:
            flags = LOCK_EX

        lockf(lockfh, flags)

        global REPO_LOCKH
        REPO_LOCKH[self.fullpath] = lockfh
        logger.debug('REPO_LOCKH=%s' % REPO_LOCKH)

    def unlock(self):
        global REPO_LOCKH
        if self.fullpath in REPO_LOCKH.keys():
            logger.debug('Unlocking %s' % self.gitdir)
            lockf(REPO_LOCKH[self.fullpath], LOCK_UN)
            REPO_LOCKH[self.fullpath].close()
            del REPO_LOCKH[self.fullpath]

    @property
    def timestamp(self):
        if self.__timestamp is not None:
            return self.__timestamp

        self.__timestamp = 0

        tsfile = os.path.join(self.fullpath, 'grokmirror.timestamp')
        if os.path.exists(tsfile):
            tsfh = open(tsfile, 'r', encoding='ascii')
            contents = tsfh.read()
            tsfh.close()
            try:
                self.__timestamp = int(contents)
                logger.debug('Timestamp for %s: %s' % (self.gitdir,
                                                       self.__timestamp))
            except ValueError:
                logger.warning('Was not able to parse timestamp in %s' % tsfile)
        else:
            logger.debug('No existing timestamp for %s' % self.gitdir)

        return self.__timestamp

    @timestamp.setter
    def timestamp(self, ts):
        tsfile = os.path.join(self.fullpath, 'grokmirror.timestamp')

        tsfh = open(tsfile, 'w', encoding='ascii')
        tsfh.write(u'%d' % ts)
        tsfh.close()

        logger.debug('Recorded timestamp for %s: %s' % (self.gitdir, ts))
        self.__timestamp = ts

    @timestamp.deleter
    def timestamp(self):
        tsfile = os.path.join(self.fullpath, 'grokmirror.timestamp')
        if os.path.exists(tsfile):
            os.unlink(tsfile)
            logger.debug('Deleted %s' % tsfile)

    def gen_fingerprint(self):
        logger.debug('Generating fingerprint for %s' % self.gitdir)

        refs = self.repo.listall_references()
        if not len(refs):
            logger.debug('No refs in %s, nothing to fingerprint.'
                         % self.gitdir)
            self.__fingerprint = None
            return

        # generate git show-ref compatible output
        fprhash = hashlib.sha1()
        for refname in refs:
            gitobj = self.repo.lookup_reference(refname).target
            line = u'%s %s\n' % (gitobj, refname)
            fprhash.update(line.encode('utf-8'))

        self.fingerprint = fprhash.hexdigest()

        logger.debug('Generated fresh %s fingerprint: %s'
                     % (self.gitdir, self.__fingerprint))

    @property
    def fingerprint(self):
        if self.__fingerprint is not None:
            return self.__fingerprint

        fpfile = os.path.join(self.fullpath, 'grokmirror.fingerprint')

        if os.path.exists(fpfile):
            fpfh = open(fpfile, 'r', encoding='ascii')
            self.__fingerprint = fpfh.read()
            fpfh.close()
            logger.debug('Got %s fingerprint from file: %s'
                         % (self.gitdir, self.__fingerprint))
        else:
            self.gen_fingerprint()

        return self.__fingerprint

    @fingerprint.deleter
    def fingerprint(self):
        fpfile = os.path.join(self.fullpath, 'grokmirror.fingerprint')
        if os.path.exists(fpfile):
            os.unlink(fpfile)
            logger.debug('Deleted %s' % fpfile)
        self.__fingerprint = None

    @fingerprint.setter
    def fingerprint(self, fingerprint):
        if fingerprint is None:
            del self.fingerprint
            return

        fpfile = os.path.join(self.fullpath, 'grokmirror.fingerprint')

        fpfh = open(fpfile, 'w', encoding='ascii')
        fpfh.write(u'%s' % fingerprint)
        fpfh.close()

        logger.debug('Recorded fingerprint for %s: %s' % (self.gitdir,
                                                          fingerprint))
        self.__fingerprint = fingerprint

    @property
    def description(self):
        if self.__description is not None:
            return self.__description

        descfile = os.path.join(self.fullpath, 'description')
        fh = open(descfile, 'r', encoding='utf-8')
        self.__description = fh.read()
        fh.close()

        logger.debug('Read %s description from file: %s' % (self.gitdir,
                                                            self.__description))
        return self.__description

    @description.setter
    def description(self, description):
        descfile = os.path.join(self.fullpath, 'description')
        logger.debug('Setting %s description to: %s' % (self.gitdir,
                                                        description))
        fh = open(descfile, 'w', encoding='utf-8')
        fh.write(description)
        fh.close()

        self.__description = description

    @description.deleter
    def description(self):
        descfile = os.path.join(self.fullpath, 'description')
        if os.path.exists(descfile):
            os.unlink(descfile)
            logger.debug('Deleted %s' % descfile)

        self.__description = None

    @property
    def owner(self):
        if self.__owner is not None:
            return self.__owner

        if 'gitweb.owner' in self.repo.config:
            # grab the first one
            self.__owner = list(
                self.repo.config.get_multivar('gitweb.owner'))[0]
            logger.debug('Got %s owner from config: %s'
                         % (self.gitdir, self.__owner))
        else:
            logger.debug('No owner set for %s, using default' % self.gitdir)
            self.__owner = u'Grokmirror'

        return self.__owner

    @owner.setter
    def owner(self, owner):
        self.repo.config.set_multivar('gitweb.owner', '.*', owner)
        logger.debug('Set %s owner to: %s' % (self.gitdir, owner))
        self.__owner = owner

    @owner.deleter
    def owner(self):
        self.owner = None

    @property
    def modified(self):
        """
        Guess when we were last modified
        """
        if self.__modified is not None:
            return self.__modified

        self.__modified = 0
        for refname in self.repo.listall_references():
            obj = self.repo.lookup_reference(refname).get_object()
            commit_time = obj.commit_time + obj.commit_time_offset
            if commit_time > self.__modified:
                self.__modified = commit_time

        logger.debug('%s last modified: %s' % (self.gitdir, self.__modified))
        return self.__modified


def manifest_lock(manifile):
    global MANIFEST_LOCKH
    if MANIFEST_LOCKH is not None:
        logger.debug('Manifest %s already locked' % manifile)

    manilock = _lockname(manifile)
    MANIFEST_LOCKH = open(manilock, 'w')
    logger.debug('Attempting to lock %s' % manilock)
    lockf(MANIFEST_LOCKH, LOCK_EX)
    logger.debug('Manifest lock obtained')


def manifest_unlock(manifile):
    global MANIFEST_LOCKH
    if MANIFEST_LOCKH is not None:
        logger.debug('Unlocking manifest %s' % manifile)
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
    jdata = fh.read()
    fh.close()

    try:
        manifest = anyjson.deserialize(jdata)
    except:
        # We'll regenerate the file entirely on failure to parse
        logger.critical('Unable to parse %s, will regenerate' % manifile)
        manifest = {}

    logger.debug('Manifest contains %s entries' % len(manifest.keys()))

    return manifest


def write_manifest(manifile, manifest, mtime=None, pretty=False):
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
                import json
                json.dump(manifest, gfh, indent=2, sort_keys=True)
            else:
                jdata = anyjson.serialize(manifest)
                gfh.write(jdata)
            gfh.close()
        else:
            if pretty:
                import json
                json.dump(manifest, fh, indent=2, sort_keys=True)
            else:
                jdata = anyjson.serialize(manifest)
                fh.write(jdata)

        os.fsync(fd)
        fh.close()
        # set mode to current umask
        curmask = os.umask(0)
        os.chmod(tmpfile, 0o0666 ^ curmask)
        os.umask(curmask)
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
