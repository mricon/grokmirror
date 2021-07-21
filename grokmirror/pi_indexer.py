#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# A hook to properly initialize and index mirrored public-inbox repositories.

import logging
import os
import sys
import re
import shutil

import grokmirror

from fnmatch import fnmatch

# default basic logger. We override it later.
logger = logging.getLogger(__name__)


def get_pi_repos(inboxdir: str) -> list:
    members = list()
    at = 0
    while True:
        repodir = os.path.join(inboxdir, 'git', '%d.git' % at)
        if not os.path.isdir(repodir):
            break
        members.append(repodir)
        at += 1

    return members


def index_pi_inbox(inboxdir: str, opts) -> bool:
    logger.info('Indexing inboxdir %s', inboxdir)
    success = True
    # Check that msgmap.sqlite3 is there
    msgmapdbf = os.path.join(inboxdir, 'msgmap.sqlite3')
    if not os.path.exists(msgmapdbf):
        logger.critical('Inboxdir not initialized: %s', inboxdir)
        return False

    piargs = ['public-inbox-index', inboxdir]
    env = {'PI_CONFIG': opts.piconfig}
    try:
        ec, out, err = grokmirror.run_shell_command(piargs, env=env)
        if ec > 0:
            logger.critical('Unable to index public-inbox repo %s: %s', inboxdir, err)
            success = False
    except Exception as ex:  # noqa
        logger.critical('Unable to index public-inbox repo %s: %s', inboxdir, ex)
        success = False

    return success


def init_pi_inbox(inboxdir: str, opts) -> bool:
    # for boost values, we look at the number of entries
    boosts = list()
    if opts.listid_priority:
        boosts = list(reversed(opts.listid_priority.split(',')))

    logger.info('Initializing inboxdir %s', inboxdir)
    # Lock all member repos so they don't get updated in the process
    pi_repos = get_pi_repos(inboxdir)
    origins = None
    gitargs = ['show', 'refs/meta/origins:i']
    # We reverse because we want to give priority to the latest origins info
    success = True
    for subrepo in reversed(pi_repos):
        grokmirror.lock_repo(subrepo)
        if not origins:
            ec, out, err = grokmirror.run_git_command(subrepo, gitargs)
            if out:
                origins = out
    inboxname = os.path.basename(inboxdir)
    if not origins and opts.origin_host:
        # Attempt to grab the config sample from remote
        origin_host = opts.origin_host.rstrip('/')
        rconfig = f'{origin_host}/{inboxname}/_/text/config/raw'
        try:
            ses = grokmirror.get_requests_session()
            res = ses.get(rconfig)
            res.raise_for_status()
            origins = res.text
        except: # noqa
            logger.critical('ERROR: Not able to get origins info for %s, skipping', inboxdir)
            success = False

    if origins:
        # Okay, let's process it
        # Generate a config entry
        local_host = opts.local_host.rstrip('/')
        local_url = f'{local_host}/{inboxname}/'
        extraopts = list()
        description = None
        newsgroup = None
        addresses = list()
        for line in origins.split('\n'):
            line = line.strip()
            if not line or line.startswith(';') or line.startswith('#') or line.startswith('[publicinbox'):
                continue
            try:
                opt, val = line.split('=', maxsplit=1)
                opt = opt.strip()
                val = val.strip()
                if opt == 'address':
                    addresses.append(val)
                    continue
                if opt == 'description':
                    description = val
                    continue
                if opt == 'newsgroup':
                    newsgroup = val
                    continue
                if opt == 'listid' and boosts:
                    # Calculate the boost value
                    for patt in boosts:
                        if fnmatch(val, patt):
                            extraopts.append(('boost', str(boosts.index(patt) + 1)))
                            break

                if opt not in {'infourl', 'listid'}:
                    continue
                extraopts.append((opt, val))
            except ValueError:
                logger.critical('Invalid config line: %s', line)
                success = False

            if not success:
                break

        if not addresses:
            addresses = [f'{inboxname}@localhost']
        if not description:
            description = f'{inboxname} mirror'

        if success:
            # Now we run public-inbox-init
            piargs = ['public-inbox-init', '-V2', '-L', opts.indexlevel]
            if newsgroup:
                piargs += ['--ng', newsgroup]
            for opt, val in extraopts:
                piargs += ['-c', f'{opt}={val}']
            piargs += [inboxname, inboxdir, local_url]
            piargs += addresses
            print(piargs)

            env = {'PI_CONFIG': opts.piconfig}
            try:
                ec, out, err = grokmirror.run_shell_command(piargs, env=env)
                if ec > 0:
                    logger.critical('Unable to init public-inbox repo %s: %s', inboxdir, err)
                    success = False
            except Exception as ex: # noqa
                logger.critical('Unable to init public-inbox repo %s: %s', inboxdir, ex)
                success = False

        if success:
            with open(os.path.join(inboxdir, 'description', 'w')) as fh:
                fh.write(description)

    # Unlock all members
    for subrepo in pi_repos:
        grokmirror.unlock_repo(subrepo)

    return success


def get_inboxdirs(repos: list) -> set:
    inboxdirs = set()
    for repo in repos:
        # Check that it's a public-inbox repo -- it should have .../git/N.git at the end
        matches = re.search(r'(/.*)/git/\d+\.git', repo)
        if matches:
            inboxdirs.add(matches.groups()[0])

    return inboxdirs


def command():
    import argparse
    global logger

    # noinspection PyTypeChecker
    op = argparse.ArgumentParser(prog='grok-pi-indexer',
                                 description='Properly initialize and update mirrored public-inbox repositories',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    op.add_argument('-v', '--verbose', action='store_true',
                    default=False,
                    help='Be verbose and tell us what you are doing')
    op.add_argument('-c', '--pi-config', dest='piconfig', required=True,
                    help='Location of the public-inbox configuration file')
    op.add_argument('-l', '--logfile',
                    help='Log activity in this log file')
    op.add_argument('--local-hostname', dest='local_host',
                    default='http://localhost/',
                    help='URL of the local mirror toplevel')
    op.add_argument('--origin-hostname', dest='origin_host',
                    default='https://lore.kernel.org/',
                    help='URL of the origin toplevel serving config files')
    op.add_argument('--listid-priority', dest='listid_priority',
                    default='*.linux.dev,*.kernel.org',
                    help='List-Ids priority order (comma-separated, can use shell globbing)')
    op.add_argument('--indexlevel', default='full',
                    help='Indexlevel to use with public-inbox-init (full, medium, basic)')
    op.add_argument('--force-init', dest='forceinit', action='store_true', default=False,
                    help='Force (re-)initialization of the repo passed as argument')
    op.add_argument('repo', nargs='?',
                    help='Full path to foo/git/N.git public-inbox repository')

    opts = op.parse_args()

    logfile = opts.logfile
    if opts.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logger = grokmirror.init_logger('pull', logfile, loglevel, opts.verbose)
    if opts.repo:
        # If we have a positional argument, then this is a post-update hook. We only
        # run the indexer if the inboxdir has already been initialized
        mode = 'update'
        if not opts.repo.endswith('.git'):
            # Assume we're working with toplevel inboxdir
            inboxdirs = [opts.repo]
        else:
            inboxdirs = get_inboxdirs([opts.repo])
    elif not sys.stdin.isatty():
        # This looks like a post_clone_complete_hook invocation
        mode = 'clone'
        repos = list()
        for line in sys.stdin.read().split('\n'):
            if not line:
                continue
            repos.append(line)
        inboxdirs = get_inboxdirs(repos)
    else:
        logger.critical('Pass either the repo to update, or list of freshly cloned repos on stdin')
        sys.exit(1)

    if not len(inboxdirs):
        logger.info('No updated public-inbox repositories, exiting')
        sys.exit(0)

    for inboxdir in inboxdirs:
        # Check if msgmap.sqlite3 is there -- it can be a clone of a new epoch,
        # so no initialization is necessary
        msgmapdbf = os.path.join(inboxdir, 'msgmap.sqlite3')
        if not os.path.exists(msgmapdbf) and mode == 'clone':
            # Initialize this public-inbox repo
            if not init_pi_inbox(inboxdir, opts):
                logger.critical('Could not init %s', inboxdir)
                continue
        elif opts.forceinit and mode == 'update':
            # Delete msgmap and xap15 if present and reinitialize
            if os.path.exists(msgmapdbf):
                logger.critical('Reinitializing %s', inboxdir)
                os.unlink(msgmapdbf)
            if os.path.exists(os.path.join(inboxdir, 'xap15')):
                shutil.rmtree(os.path.join(inboxdir, 'xap15'))
            if not init_pi_inbox(inboxdir, opts):
                logger.critical('Could not init %s', inboxdir)
                continue

        logger.info('Indexing %s', inboxdir)
        if not index_pi_inbox(inboxdir, opts):
            logger.critical('Unable to index %s', inboxdir)


if __name__ == '__main__':
    command()
