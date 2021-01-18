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

import grokmirror
import logging
import fnmatch
import subprocess

logger = logging.getLogger(__name__)


def git_rev_parse_all(gitdir):
    args = ['rev-parse', '--all']
    retcode, output, error = grokmirror.run_git_command(gitdir, args)

    if error:
        # Put things we recognize into debug
        debug = list()
        warn = list()
        for line in error.split('\n'):
            warn.append(line)
        if debug:
            logger.debug('Stderr: %s', '\n'.join(debug))
        if warn:
            logger.warning('Stderr: %s', '\n'.join(warn))

    return output


def git_remote_update(args, fullpath):
    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    if error:
        # Put things we recognize into debug
        debug = list()
        warn = list()
        for line in error.split('\n'):
            if line.find('From ') == 0:
                debug.append(line)
            elif line.find('-> ') > 0:
                debug.append(line)
            else:
                warn.append(line)
        if debug:
            logger.debug('Stderr: %s', '\n'.join(debug))
        if warn:
            logger.warning('Stderr: %s', '\n'.join(warn))


def dumb_pull_repo(gitdir, remotes, svn=False):
    # verify it's a git repo and fetch all remotes
    logger.debug('Will pull %s with following remotes: %s', gitdir, remotes)
    old_revs = git_rev_parse_all(gitdir)

    try:
        grokmirror.lock_repo(gitdir, nonblocking=True)
    except IOError:
        logger.info('Could not obtain exclusive lock on %s', gitdir)
        logger.info('\tAssuming another process is running.')
        return False

    if svn:
        logger.debug('Using git-svn for %s', gitdir)

        for remote in remotes:
            # arghie-argh-argh
            if remote == '*':
                remote = '--all'

            logger.info('Running git-svn fetch %s in %s', remote, gitdir)
            args = ['svn', 'fetch', remote]
            git_remote_update(args, gitdir)

    else:
        # Not an svn remote
        myremotes = grokmirror.list_repo_remotes(gitdir)
        if not len(myremotes):
            logger.info('Repository %s has no defined remotes!', gitdir)
            return False

        logger.debug('existing remotes: %s', myremotes)
        for remote in remotes:
            remotefound = False
            for myremote in myremotes:
                if fnmatch.fnmatch(myremote, remote):
                    remotefound = True
                    logger.debug('existing remote %s matches %s', myremote, remote)
                    args = ['remote', 'update', myremote, '--prune']
                    logger.info('Updating remote %s in %s', myremote, gitdir)

                    git_remote_update(args, gitdir)

            if not remotefound:
                logger.info('Could not find any remotes matching %s in %s', remote, gitdir)

    new_revs = git_rev_parse_all(gitdir)
    grokmirror.unlock_repo(gitdir)

    if old_revs == new_revs:
        logger.debug('No new revs, no updates')
        return False

    logger.debug('New revs found -- new content pulled')
    return True


def run_post_update_hook(hookscript, gitdir):
    if hookscript == '':
        return
    if not os.access(hookscript, os.X_OK):
        logger.warning('post_update_hook %s is not executable', hookscript)
        return

    args = [hookscript, gitdir]
    logger.debug('Running: %s', ' '.join(args))
    (output, error) = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

    error = error.decode().strip()
    output = output.decode().strip()
    if error:
        # Put hook stderror into warning
        logger.warning('Hook Stderr: %s', error)
    if output:
        # Put hook stdout into info
        logger.info('Hook Stdout: %s', output)


def parse_args():
    import argparse
    # noinspection PyTypeChecker
    op = argparse.ArgumentParser(prog='grok-dumb-pull',
                                 description='Fetch remotes in repositories not managed by grokmirror',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    op.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                    default=False,
                    help='Be verbose and tell us what you are doing')
    op.add_argument('-s', '--svn', dest='svn', action='store_true',
                    default=False,
                    help='The remotes for these repositories are Subversion')
    op.add_argument('-r', '--remote-names', dest='remotes', action='append',
                    default=None,
                    help='Only fetch remotes matching this name (accepts shell globbing)')
    op.add_argument('-u', '--post-update-hook', dest='posthook',
                    default='',
                    help='Run this hook after each repository is updated.')
    op.add_argument('-l', '--logfile', dest='logfile',
                    default=None,
                    help='Put debug logs into this file')
    op.add_argument('--version', action='version', version=grokmirror.VERSION)
    op.add_argument('paths', nargs='+', help='Full path(s) of the repos to pull')

    opts = op.parse_args()

    if not len(opts.paths):
        op.error('You must provide at least a path to the repos to pull')

    return opts


def dumb_pull(paths, verbose=False, svn=False, remotes=None, posthook='', logfile=None):
    global logger

    loglevel = logging.INFO
    logger = grokmirror.init_logger('dumb-pull', logfile, loglevel, verbose)

    if remotes is None:
        remotes = ['*']

    # Find all repositories we are to pull
    for entry in paths:
        if entry[-4:] == '.git':
            if not os.path.exists(entry):
                logger.critical('%s does not exist', entry)
                continue

            logger.debug('Found %s', entry)
            didwork = dumb_pull_repo(entry, remotes, svn=svn)
            if didwork:
                run_post_update_hook(posthook, entry)

        else:
            logger.debug('Finding all git repos in %s', entry)
            for founddir in grokmirror.find_all_gitdirs(entry):
                didwork = dumb_pull_repo(founddir, remotes, svn=svn)
                if didwork:
                    run_post_update_hook(posthook, founddir)


def command():
    opts = parse_args()

    return dumb_pull(
        opts.paths, verbose=opts.verbose, svn=opts.svn, remotes=opts.remotes,
        posthook=opts.posthook, logfile=opts.logfile)


if __name__ == '__main__':
    command()
