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
    symlinks = {}
    for root, dirs, files in os.walk(toplevel, topdown=True):
        if not len(dirs):
            continue

        torm = []
        for name in dirs:
            # Should we ignore this dir?
            ignored = False
            for ignoredir in ignore:
                if (fnmatch.fnmatch(os.path.join(root, name), ignoredir)
                        or fnmatch.fnmatch(name, ignoredir)):
                    torm.append(name)
                    ignored = True
                    break

            if ignored:
                continue

            fullpath = os.path.join(root, name)
            if not is_bare_git_repo(fullpath):
                continue

            if os.path.islink(fullpath):
                target = os.path.realpath(fullpath)
                if target.find(toplevel) < 0:
                    logger.info('Symlink %s points outside toplevel, ignored'
                                % symlink)
                    continue

                logger.debug('Found symlink %s -> %s' % (fullpath, target))
                if target not in symlinks.keys():
                    symlinks[target] = []
                symlinks[target].append(fullpath)

            else:
                logger.debug('Found %s' % os.path.join(root, name))
                gitdirs.append(os.path.join(root, name))

            torm.append(name)

        for name in torm:
            # don't recurse into the found *.git dirs
            dirs.remove(name)

    return gitdirs, symlinks


def _lockname(fullpath):
    lockpath = os.path.dirname(fullpath)
    lockname = '.%s.lock' % os.path.basename(fullpath)

    if not os.path.exists(lockpath):
        logger.debug('Creating parent dirs for %s' % fullpath)
        os.makedirs(lockpath)

    repolock = os.path.join(lockpath, lockname)
    return repolock


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
        self.__alternates = None
        self.__export_ok = None

    def _lockname(self):
        repolock = _lockname(self.fullpath)
        logger.debug('repolock=%s' % repolock)

        return repolock

    def to_manifest(self):
        # For backwards-compatible "reference" we get either the first entry
        # in the alternates list, or use "None"
        reference = None
        if len(self.alternates):
            reference = self.alternates[0]
        return {
            'description': self.description,
            'fingerprint': self.fingerprint,
            'modified': self.modified,
            'owner': self.owner,
            'reference': reference,
            'alternates': self.alternates,
        }

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
    def alternates(self):
        if self.__alternates is not None:
            return self.__alternates

        altfile = os.path.join(self.fullpath, 'objects/info/alternates')
        if not os.path.exists(altfile):
            logger.debug('No alternates file found, returning []')
            self.__alternates = []
            return self.__alternates

        fh = open(altfile, 'r', encoding='ascii')
        while True:
            line = fh.readline()
            if not len(line):
                break

            altpath = os.path.realpath(line.rstrip())
            if altpath.find(self.toplevel) != 0:
                logger.error('Alternate %s in %s is pointing outside toplevel!'
                             % (altpath, self.gitdir))
                continue

            if not os.path.exists(altpath):
                # TODO: This is a broken repo!
                continue
            reponame = altpath.replace(self.toplevel, '', 1).lstrip('/')
            reponame = reponame.replace('/objects', '')

            if self.__alternates is None:
                self.__alternates = []
            if reponame not in self.__alternates:
                self.__alternates.append(reponame)

        fh.close()
        logger.debug('Loaded %s alternates from file: %s'
                     % (self.gitdir, self.__alternates))

        return self.__alternates

    @alternates.setter
    def alternates(self, alternates):
        # first things first, are you trying to remove alternates?
        # we don't currently support it, as that requires a full repack
        # and can't reasonably be done on huge repos
        for existing_alternate in self.alternates:
            if existing_alternate not in alternates:
                logger.error('Ignoring request to delete %s alternate from %s!'
                             % (existing_alternate, self.gitdir))
                alternates.append(existing_alternate)

        altfile = os.path.join(self.fullpath, 'objects/info/alternates')
        fh = open(altfile, 'w', encoding='ascii')

        self.__alternates = []
        for alternate in alternates:
            altpath = os.path.join(self.toplevel, alternate, 'objects')
            if os.path.exists(altpath):
                self.__alternates.append(alternate)
                logger.debug('Adding verified alternate %s to %s'
                             % (alternate, self.gitdir))
                fh.write(u'%s\n' % altpath)
            else:
                logger.debug('Not adding bogus alternate %s to %s'
                             % (alternate, self.gitdir))
        fh.close()
        logger.debug('Wrote new alternates file with: %s' % self.__alternates)

    @alternates.deleter
    def alternates(self):
        # I can't let you do that, Dave.
        logger.error('Received request to delete all alternates from %s!'
                     % self.gitdir)
        return

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

        self.lock()
        tsfh = open(tsfile, 'w', encoding='ascii')
        tsfh.write(u'%d' % ts)
        tsfh.close()
        self.unlock()

        logger.debug('Recorded timestamp for %s: %s' % (self.gitdir, ts))
        self.__timestamp = ts

    @timestamp.deleter
    def timestamp(self):
        tsfile = os.path.join(self.fullpath, 'grokmirror.timestamp')
        if os.path.exists(tsfile):
            self.lock()
            os.unlink(tsfile)
            self.unlock()
            logger.debug('Deleted %s' % tsfile)

    def gen_fingerprint(self):
        logger.debug('Generating fingerprint for %s' % self.gitdir)

        self.lock()
        refs = self.repo.listall_references()
        if not len(refs):
            logger.debug('No refs in %s, nothing to fingerprint.'
                         % self.gitdir)
            self.__fingerprint = None
            self.unlock()
            return

        # generate git show-ref compatible output
        fprhash = hashlib.sha1()
        for refname in refs:
            gitobj = self.repo.lookup_reference(refname).target
            line = u'%s %s\n' % (gitobj, refname)
            fprhash.update(line.encode('utf-8'))

        self.unlock()
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
            self.lock()
            os.unlink(fpfile)
            logger.debug('Deleted %s' % fpfile)
            self.unlock()

        self.__fingerprint = None

    @fingerprint.setter
    def fingerprint(self, fingerprint):
        if fingerprint is None:
            del self.fingerprint
            return

        fpfile = os.path.join(self.fullpath, 'grokmirror.fingerprint')

        self.lock()
        fpfh = open(fpfile, 'w', encoding='ascii')
        fpfh.write(u'%s' % fingerprint)
        fpfh.close()
        self.unlock()

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
        self.lock()
        fh = open(descfile, 'w', encoding='utf-8')
        fh.write(description)
        fh.close()
        self.unlock()

        self.__description = description

    @description.deleter
    def description(self):
        descfile = os.path.join(self.fullpath, 'description')
        if os.path.exists(descfile):
            self.lock()
            os.unlink(descfile)
            logger.debug('Deleted %s' % descfile)
            self.unlock()

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
        self.lock()
        self.repo.config.set_multivar('gitweb.owner', '.*', owner)
        self.unlock()
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
        self.lock()
        for refname in self.repo.listall_references():
            obj = self.repo.lookup_reference(refname).get_object()
            commit_time = obj.commit_time + obj.commit_time_offset
            if commit_time > self.__modified:
                self.__modified = commit_time
        self.unlock()

        logger.debug('%s last modified: %s' % (self.gitdir, self.__modified))
        return self.__modified

    @property
    def export_ok(self):
        if self.__export_ok is not None:
            return self.__export_ok

        self.__export_ok = False
        expfile = os.path.join(self.fullpath, 'git-daemon-export-ok')
        if os.path.exists(expfile):
            self.__export_ok = True
            logger.debug('Found %s, marking as export ok' % expfile)
        else:
            logger.debug('Did not find %s, marking as not exported' % expfile)
        return self.__export_ok

    @export_ok.setter
    def export_ok(self, is_ok):
        expfile = os.path.join(self.fullpath, 'git-daemon-export-ok')
        if not is_ok and os.path.exists(expfile):
            os.unlink(expfile)
            logger.debug('Removed export permission on %s' % self.gitdir)
        else:
            fh = open(expfile, 'w')
            fh.write(u'Set by grokmirror\n')
            logger.debug('Set export permission on %s' % self.gitdir)
            fh.close()
        self.__export_ok = is_ok


class Manifest(object):
    def __init__(self, manifile, wait=False):
        self.manifile = manifile
        self.wait = wait
        self._lockh = None

        self.__repos = None

    def _lockname(self):
        manilock = _lockname(self.manifile)
        logger.debug('manilock=%s' % manilock)

        return manilock

    def lock(self):
        if self._lockh is not None:
            logger.debug('Manifest %s already locked' % self.manifile)

        self._lockh = open(self._lockname(), 'w')
        logger.debug('Attempting to lock %s' % self.manifile)
        lockf(self._lockh, LOCK_EX)
        logger.debug('Manifest lock obtained')

    def unlock(self):
        if self._lockh is not None:
            logger.debug('Unlocking manifest %s' % self.manifile)
            lockf(self._lockh, LOCK_UN)
            self._lockh.close()
            self._lockh = None

    @property
    def repos(self):
        if self.__repos is not None:
            return self.__repos

        # On NFS, sometimes the file shows up as not present, which is why
        # we're always waiting for it to exist first before we try to do any
        # work with it.
        while True:
            if not self.wait or os.path.exists(self.manifile):
                break
            logger.info('Manifest file not yet found, waiting...')

            # Unlock the manifest so other processes aren't waiting for us
            was_locked = False
            if self._lockh is not None:
                was_locked = True
                self.unlock()
            time.sleep(1)
            if was_locked:
                self.lock()

        if not os.path.exists(self.manifile):
            logger.info('%s not found, assuming initial run' % self.manifile)
            self.__repos = {}
            return self.__repos

        self.lock()
        if self.manifile.find('.gz') > 0:
            import gzip
            fh = gzip.open(self.manifile, 'rb')
            jdata = fh.read().decode('utf-8')
        else:
            fh = open(self.manifile, 'r', encoding='utf-8')
            jdata = fh.read()

        logger.info('Reading %s' % self.manifile)

        fh.close()

        self.__repos = anyjson.deserialize(jdata)

        self.unlock()
        logger.debug('Manifest contains %s entries' % len(self.__repos))

        return self.__repos

    def populate(self, toplevel, ignore=None, only_export_ok=False):
        # Blow away whatever we know about repos
        self.__repos = {}
        (gitdirs, symlinks) = find_all_gitdirs(toplevel, ignore)

        for fullpath in gitdirs:
            gitdir = fullpath.replace(toplevel, '', 1).lstrip('/')
            repo = Repository(toplevel, gitdir)

            if only_export_ok and not repo.export_ok:
                logger.debug('Skipping %s because it is not export_ok'
                             % gitdir)
            else:
                self.__repos[gitdir] = repo.to_manifest()
                # Do any symlinks point to us?
                if fullpath in symlinks.keys():
                    for fullspath in symlinks[fullpath]:
                        symlink = fullspath.replace(toplevel, '', 1).lstrip('/')
                        self.set_symlink(gitdir, symlink)

    def set_symlink(self, gitdir, symlink):
        if 'symlinks' not in self.__repos[gitdir].keys():
            self.__repos[gitdir]['symlinks'] = []

        if symlink not in self.__repos[gitdir]['symlinks']:
            self.__repos[gitdir]['symlinks'].append(symlink)
            logger.info('Recording symlink %s to %s' % (symlink, gitdir))

        # Now go through all repos and fix any references pointing to the
        # symlinked location.
        # TODO: Fix for alternates
        #for gitdir in manifest.keys():
        #    if manifest[gitdir]['reference'] == relative:
        #        logger.info('Adjusted symlinked reference for %s: %s->%s'
        #                    % (gitdir, relative, tgtgitdir))
        #        manifest[gitdir]['reference'] = tgtgitdir

    def save(self, mtime=None, pretty=False):
        import tempfile
        import shutil
        import gzip

        logger.info('Writing new %s' % self.manifile)

        (dirname, basename) = os.path.split(self.manifile)
        (fd, tmpfile) = tempfile.mkstemp(prefix=basename, dir=dirname)
        logger.debug('Created a temporary file in %s' % tmpfile)
        logger.debug('Writing to %s' % tmpfile)
        try:
            if self.manifile.find('.gz') > 0:
                fh = os.fdopen(fd, 'wb')
                gfh = gzip.GzipFile(fileobj=fh, mode='wb')
                if pretty:
                    import json
                    json.dump(self.repos, gfh, indent=2, sort_keys=True)
                else:
                    jdata = anyjson.serialize(self.repos)
                    gfh.write(jdata.encode('utf-8'))
                gfh.close()
            else:
                fh = os.fdopen(fd, 'w')
                if pretty:
                    import json
                    json.dump(self.repos, fh, indent=2, sort_keys=True)
                else:
                    jdata = anyjson.serialize(self.repos)
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
            logger.debug('Moving %s to %s' % (tmpfile, self.manifile))

            self.lock()
            shutil.move(tmpfile, self.manifile)
            self.unlock()

        finally:
            # If something failed, don't leave these trailing around
            if os.path.exists(tmpfile):
                logger.debug('Removing %s' % tmpfile)
                os.unlink(tmpfile)
