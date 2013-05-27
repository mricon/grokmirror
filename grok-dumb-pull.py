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

import grokmirror
import logging
import time
import fnmatch
import subprocess
import shutil
import calendar

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

from git import Repo

logger = logging.getLogger(__name__)

def git_remote_update(args, env):
    logger.debug('Running: GIT_DIR=%s %s' % (env['GIT_DIR'], ' '.join(args)))

    (output, error) = subprocess.Popen(args, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env=env).communicate()

    error = error.strip()

    if error:
        # Put things we recognize into debug
        debug = []
        warn  = []
        for line in error.split('\n'):
            if line.find('From ') == 0:
                debug.append(line)
            elif line.find('-> ') > 0:
                debug.append(line)
            else:
                warn.append(line)
        if debug:
            logger.debug('Stderr: %s' % '\n'.join(debug))
        if warn:
            logger.warning('Stderr: %s' % '\n'.join(warn))

def dumb_pull_repo(gitdir, remote, svn=False):
    # verify it's a git repo and fetch all remotes
    try:
        repo = Repo(gitdir)
        assert repo.bare == True
    except:
        logger.critical('Error opening %s.' % gitdir)
        logger.critical('Make sure it is a bare git repository.')
        sys.exit(1)

    try:
        grokmirror.lock_repo(gitdir, nonblocking=True)
    except IOError, ex:
        logger.info('Could not obtain exclusive lock on %s' % gitdir)
        logger.info('\tAssuming another process is running.')

    env = {'GIT_DIR': gitdir}

    if svn:
        logger.debug('Using git-svn for %s' % gitdir)
        # arghie-argh-argh
        if remote == '*':
            remote = '--all'

        logger.info('Running git-svn fetch %s in %s' % (remote, gitdir))
        args = ['/usr/bin/git', 'svn', 'fetch', remote]
        git_remote_update(args, env)
        grokmirror.unlock_repo(gitdir)

        return

    # Not an svn remote
    pullremotes = []

    hasremotes = repo.git.remote()
    if not len(hasremotes.strip()):
        logger.info('Repository %s has no defined remotes!' % gitdir)
        return

    didwork = False

    logger.debug('existing remotes: %s' % hasremotes)
    for hasremote in hasremotes.split('\n'):
        if fnmatch.fnmatch(hasremote, remote):
            logger.debug('existing remote %s matches requested %s' % (
                hasremote, remote))
            args = ['/usr/bin/git', 'remote', 'update', hasremote]
            logger.info('Updating remote %s in %s' % (hasremote, gitdir))

            git_remote_update(args, env)
            didwork = True

    grokmirror.unlock_repo(gitdir)
    if not didwork:
        logger.info('Could not find any remotes matching %s in %s' % (
            remote, gitdir))

    return didwork

def run_post_update_hook(hookscript, gitdir):
    if hookscript == '':
        return
    if not os.access(hookscript, os.X_OK):
        logger.warning('post_update_hook %s is not executable' % hookscript)
        return

    args = [hookscript, gitdir]
    logger.debug('Running: %s' % ' '.join(args))
    (output, error) = subprocess.Popen(args, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE).communicate()

    error  = error.strip()
    output = output.strip()
    if error:
        # Put hook stderror into warning
        logger.warning('Hook Stderr: %s' % error)
    if output:
        # Put hook stdout into info
        logger.info('Hook Stdout: %s' % output)

if __name__ == '__main__':
    from optparse import OptionParser

    usage = '''usage: %prog [options] /path/to/repos
    Bluntly fetch remotes in specified git repositories.
    '''

    parser = OptionParser(usage=usage, version=grokmirror.VERSION)
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
        default=False,
        help='Be verbose and tell us what you are doing')
    parser.add_option('-s', '--svn', dest='svn', action='store_true',
        default=False,
        help='The remotes for these repositories are Subversion')
    parser.add_option('-r', '--remote-name', dest='remote',
        default='*',
        help='Only fetch remotes matching this name (accepts globbing)')
    parser.add_option('-u', '--post-update-hook', dest='posthook',
        default='',
        help='Run this hook after each repository is updated. Passes '
             'full path to the repository as the sole argument.')
    parser.add_option('-l', '--logfile', dest='logfile',
        default=None,
        help='Put debug logs into this file')

    (opts, args) = parser.parse_args()

    if not len(args):
        parser.error('You must provide at least a path to the repos to pull')

    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)

    if opts.verbose:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.CRITICAL)

    logger.addHandler(ch)

    if opts.logfile is not None:
        ch = logging.FileHandler(opts.logfile)
        formatter = logging.Formatter("[%(process)d] %(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)

        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)

    # push our logger into grokmirror to override the default
    grokmirror.logger = logger

    # Find all repositories we are to pull
    for entry in args:
        if entry[-4:] == '.git':
            if not os.path.exists(entry):
                logger.critical('%s does not exist' % entry)
                continue

            logger.debug('Found %s' % entry)
            didwork = dumb_pull_repo(entry, opts.remote, svn=opts.svn)
            if didwork:
                run_post_update_hook(opts.posthook, entry)

        else:
            logger.debug('Finding all git repos in %s' % entry)
            for founddir in grokmirror.find_all_gitdirs(entry):
                didwork = dumb_pull_repo(founddir, opts.remote, svn=opts.svn)
                if didwork:
                    run_post_update_hook(opts.posthook, founddir)

