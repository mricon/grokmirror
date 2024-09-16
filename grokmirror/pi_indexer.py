#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# A hook to properly initialize and index mirrored public-inbox repositories.

import logging
import os
import sys
import re
import shutil
import pathlib

import grokmirror

from fnmatch import fnmatch
from typing import Tuple

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


def index_pi_inbox(fullpath: str, opts) -> bool:
    gdir, pdir = get_git_pi_dir(opts, fullpath)
    logger.info('pi-index %s', gdir)
    success = True
    # Check that msgmap.sqlite3 is there
    msgmapdbf = os.path.join(pdir, 'msgmap.sqlite3')
    if not os.path.exists(msgmapdbf):
        logger.info('Inboxdir not initialized: %s', pdir)
        return False

    piargs = ['public-inbox-index', '--no-update-extindex']
    if opts.jobs:
        piargs += ['--jobs', str(opts.jobs)]
    if opts.nofsync:
        piargs += ['--no-fsync']

    piargs.append(pdir)

    env = {
        'PI_CONFIG': opts.piconfig,
        'PATH': os.getenv('PATH', '/bin:/usr/bin:/usr/local/bin'),
    }
    try:
        ec, out, err = grokmirror.run_shell_command(piargs, env=env)
        if ec > 0:
            logger.critical('Unable to index public-inbox repo %s: %s', pdir, err)
            success = False
    except Exception as ex:  # noqa
        logger.critical('Unable to index public-inbox repo %s: %s', pdir, ex)
        success = False

    return success


def init_pi_inbox(gdir: str, pdir: str, opts) -> bool:
    # for boost values, we look at the number of entries
    boosts = list()
    if opts.listid_priority:
        boosts = list(reversed(opts.listid_priority.split(',')))

    logger.info('pi-init  %s', gdir)
    # Lock all member repos so they don't get updated in the process
    pi_repos = get_pi_repos(gdir)
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
    inboxname = os.path.basename(gdir)
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
            logger.critical('ERROR: Not able to get origins info for %s, skipping', gdir)
            success = False

    if origins:
        # Okay, let's process it
        # Generate a config entry
        if opts.local_toplevel:
            local_toplevel = opts.local_toplevel.rstrip('/')
            local_url = f'{local_toplevel}/{inboxname}'
        else:
            local_url = inboxname
        extraopts = list()
        acceptopts = {'listid'}
        if opts.extra_cfgopts:
            acceptopts.update(opts.extra_cfgopts.split(','))
        description = None
        newsgroup = None
        listid = None
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
                    listid = val
                    # Calculate the boost value
                    boostval = 1
                    for patt in boosts:
                        if fnmatch(val, patt):
                            boostval = boosts.index(patt) + 10
                            break
                    extraopts.append(('boost', str(boostval)))

                if opt in acceptopts:
                    logger.debug('Accepting extra opt %s=%s', opt, val)
                    extraopts.append((opt, val))

            except ValueError:
                logger.critical('Invalid config line: %s', line)
                success = False

            if not success:
                break

        if not addresses:
            addresses = [f'{inboxname}@localhost']
        if not description:
            if listid:
                description = f'{listid} archive mirror'
            else:
                description = f'{inboxname} archive mirror'

        if success:
            if gdir != pdir:
                # public-inbox databases are separate from the main git trees
                pathlib.Path(pdir).mkdir(parents=True, exist_ok=True)
                # Symlink the git subpath
                if not os.path.islink(os.path.join(pdir, 'git')):
                    os.symlink(os.path.join(gdir, 'git'), os.path.join(pdir, 'git'))

            # Now we run public-inbox-init
            piargs = ['public-inbox-init', '-V2', '-L', opts.indexlevel]
            if newsgroup:
                piargs += ['--ng', newsgroup]
            for opt, val in extraopts:
                piargs += ['-c', f'{opt}={val}']
            piargs += [inboxname, pdir, local_url]
            piargs += addresses
            logger.debug('piargs=%s', piargs)

            env = {
                'PI_CONFIG': opts.piconfig,
                'PATH': os.getenv('PATH', '/bin:/usr/bin:/usr/local/bin'),
            }
            try:
                ec, out, err = grokmirror.run_shell_command(piargs, env=env)
                if ec > 0:
                    logger.critical('Unable to init public-inbox repo %s: %s', pdir, err)
                    success = False
            except Exception as ex: # noqa
                logger.critical('Unable to init public-inbox repo %s: %s', pdir, ex)
                success = False

        if success:
            with open(os.path.join(pdir, 'description'), 'w') as fh:
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


def process_inboxdirs(inboxdirs: set, opts, init: bool = False):
    if not len(inboxdirs):
        logger.info('Nothing to do')
        sys.exit(0)

    # Init all new repos first, and then index them one by one
    toindex = set()
    for inboxdir in inboxdirs:
        gdir, pdir = get_git_pi_dir(opts, inboxdir)
        # Check if msgmap.sqlite3 is there -- it can be a clone of a new epoch,
        # so no initialization is necessary
        msgmapdbf = os.path.join(pdir, 'msgmap.sqlite3')
        if init and not os.path.exists(msgmapdbf):
            # Initialize this public-inbox repo
            if not init_pi_inbox(gdir, pdir, opts):
                logger.critical('Could not init %s', inboxdir)
                continue
        if os.path.exists(msgmapdbf):
            toindex.add(inboxdir)

    for inboxdir in toindex:
        if not index_pi_inbox(inboxdir, opts):
            logger.critical('Unable to index %s', inboxdir)


def get_git_pi_dir(opts, fullpath: str) -> Tuple[str, str]:
    fullpath = os.path.realpath(fullpath)
    if not opts.pitoplevel:
        # Public-inbox is in the same dir
        return fullpath, fullpath
    # Public-inbox is in a separate dir
    pitop = os.path.realpath(opts.pitoplevel)
    groktop = os.path.realpath(opts.toplevel)
    inboxname = os.path.relpath(fullpath, groktop)
    return fullpath, os.path.join(pitop, inboxname)


def cmd_init(opts):
    if opts.inboxdir:
        inboxdirs = get_inboxdirs(opts.inboxdir)
        if opts.forceinit:
            inboxdir = list(inboxdirs)[0] # get first element from the set
            gdir, pdir = get_git_pi_dir(opts, inboxdir)
            msgmapdbf = os.path.join(pdir, 'msgmap.sqlite3')
            # Delete msgmap and xap15 if present and reinitialize
            if os.path.exists(msgmapdbf):
                logger.critical('Reinitializing %s', opts.inboxdir)
                os.unlink(msgmapdbf)
            if os.path.exists(os.path.join(pdir, 'xap15')):
                shutil.rmtree(os.path.join(pdir, 'xap15'))
    elif not sys.stdin.isatty():
        repos = list()
        for line in sys.stdin.read().split('\n'):
            if not line:
                continue
            repos.append(line)
        inboxdirs = get_inboxdirs(repos)
    else:
        logger.info('Nothing to do')
        sys.exit(0)

    process_inboxdirs(inboxdirs, opts, init=True)


def cmd_update(opts):
    inboxdirs = get_inboxdirs(opts.repo)
    process_inboxdirs(inboxdirs, opts)


def cmd_extindex(opts):
    env = {
        'PI_CONFIG': opts.piconfig,
        'PATH': os.getenv('PATH', '/bin:/usr/bin:/usr/local/bin'),
    }
    logger.info('Running extindex --all')
    piargs = ['public-inbox-extindex', '-L', opts.indexlevel, '--all']
    if opts.jobs:
        piargs += ['--jobs', str(opts.jobs)]
    if opts.nofsync:
        piargs += ['--no-fsync']
    try:
        ec, out, err = grokmirror.run_shell_command(piargs, env=env)
        if ec > 0:
            logger.critical('Unable to run public-inbox-extindex: %s', err)
            sys.exit(1)
    except Exception as ex:  # noqa
        logger.critical('Unable to run public-inbox-extindex: %s', ex)
        sys.exit(1)


def command():
    import argparse
    global logger

    # noinspection PyTypeChecker
    ap = argparse.ArgumentParser(prog='grok-pi-indexer',
                                 description='Properly initialize and update mirrored public-inbox repositories',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    ap.add_argument('-v', '--verbose', action='store_true',
                    default=False,
                    help='Be verbose and tell us what you are doing')
    ap.add_argument('-c', '--pi-config', dest='piconfig', required=True,
                    help='Location of the public-inbox configuration file')
    ap.add_argument('-t', '--toplevel', dest='toplevel', required=True,
                    help='Path to git repository mirror toplevel')
    ap.add_argument('-p', '--pi-toplevel', dest='pitoplevel',
                    help='Path to public-inbox toplevel, if separate')
    ap.add_argument('-l', '--logfile',
                    help='Log activity in this log file')
    ap.add_argument('-L', '--indexlevel', default='full',
                    help='Indexlevel to use with public-inbox (full, medium, basic)')
    ap.add_argument('-j', '--jobs', type=int,
                    help='The --jobs parameter to pass to public-inbox')
    ap.add_argument('--no-fsync', dest='nofsync', action='store_true', default=False,
                    help='Use --no-fsync when invoking public-inbox')

    sp = ap.add_subparsers(help='sub-command help', dest='subcmd')
    sp_init = sp.add_parser('init', help='Run public-inbox-init+index on repositories passed via stdin')

    sp_init.add_argument('--local-toplevel', dest='local_toplevel', default='',
                         help='URL of the local mirror toplevel (omit if serving from /)')
    sp_init.add_argument('--origin-hostname', dest='origin_host',
                         default='https://lore.kernel.org/',
                         help='URL of the origin toplevel serving config files')
    sp_init.add_argument('--listid-priority', dest='listid_priority',
                         default='*.linux.dev,*.kernel.org',
                         help='List-Ids priority order (comma-separated, can use shell globbing)')
    sp_init.add_argument('--extra-cfgopts', dest='extra_cfgopts',
                         default='indexheader,replyto',
                         help='Extra config options to accept from remote (comma-separated)')
    sp_init.add_argument('--force-reinit', dest='forceinit', action='store_true', default=False,
                         help='Force a full (re-)init of an inboxdir')
    sp_init.add_argument('inboxdir', nargs='?',
                         help='Path to toplevel inboxdir (non-hook mode)')
    sp_init.set_defaults(func=cmd_init)

    sp_update = sp.add_parser('update', help='Run public-inbox-index on passed repository path')
    sp_update.add_argument('repo', nargs=1,
                           help='Full path to foo/git/N.git public-inbox repository')
    sp_update.set_defaults(func=cmd_update)

    sp_extindex = sp.add_parser('extindex', help='Run extindex on all inboxes')
    sp_extindex.set_defaults(func=cmd_extindex)

    opts = ap.parse_args()
    if 'func' not in opts:
        ap.print_help()
        sys.exit(1)

    logfile = opts.logfile
    if opts.verbose:
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logger = grokmirror.init_logger('pi-indexer', logfile, loglevel, opts.verbose)
    opts.func(opts)


if __name__ == '__main__':
    command()
