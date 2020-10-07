#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# This is a ready-made post_update_hook script for piping messages from
# mirrored public-inbox repositories to arbitrary commands (e.g. procmail).
#

__author__ = 'Konstantin Ryabitsev <konstantin@linuxfoundation.org>'

import os
import sys
import grokmirror
import fnmatch
import logging
import shlex

from typing import Optional

# default basic logger. We override it later.
logger = logging.getLogger(__name__)


def git_get_message_from_pi(fullpath: str, commit_id: str) -> bytes:
    logger.debug('Getting %s:m from %s', commit_id, fullpath)
    args = ['show', f'{commit_id}:m']
    ecode, out, err = grokmirror.run_git_command(fullpath, args, decode=False)
    if ecode > 0:
        logger.debug('Could not get the message, error below')
        logger.debug(err.decode())
        raise KeyError('Could not find %s in %s' % (commit_id, fullpath))
    return out


def git_get_new_revs(fullpath: str, pipelast: Optional[int] = None) -> list:
    statf = os.path.join(fullpath, 'pi-piper.latest')
    if pipelast:
        rev_range = '-n %d' % pipelast
    else:
        with open(statf, 'r') as fh:
            latest = fh.read().strip()
            rev_range = f'{latest}..'

    args = ['rev-list', '--pretty=oneline', '--reverse', rev_range, 'master']
    ecode, out, err = grokmirror.run_git_command(fullpath, args)
    if ecode > 0:
        raise KeyError('Could not iterate %s in %s' % (rev_range, fullpath))

    newrevs = list()
    if out:
        for line in out.split('\n'):
            (commit_id, logmsg) = line.split(' ', 1)
            logger.debug('commit_id=%s, subject=%s', commit_id, logmsg)
            newrevs.append((commit_id, logmsg))

    return newrevs


def reshallow(repo: str, commit_id: str) -> int:
    with open(os.path.join(repo, 'shallow'), 'w') as fh:
        fh.write(commit_id)
        fh.write('\n')
    logger.info('   prune: %s ', repo)
    ecode, out, err = grokmirror.run_git_command(repo, ['gc', '--prune=now'])
    return ecode


def init_piper_tracking(repo: str, shallow: bool) -> bool:
    logger.info('Initial setup for %s', repo)
    args = ['rev-list', '-n', '1', 'master']
    ecode, out, err = grokmirror.run_git_command(repo, args)
    if ecode > 0 or not out:
        logger.info('Could not list revs in %s', repo)
        return False
    # Just write latest into the tracking file and return
    latest = out.strip()
    statf = os.path.join(repo, 'pi-piper.latest')
    with open(statf, 'w') as fh:
        fh.write(latest)
    if shallow:
        reshallow(repo, latest)
    return True


def run_pi_repo(repo: str, pipedef: str, dryrun: bool = False, shallow: bool = False,
                pipelast: Optional[int] = None) -> None:
    logger.info('Checking %s', repo)
    sp = shlex.shlex(pipedef, posix=True)
    sp.whitespace_split = True
    args = list(sp)
    if not os.access(args[0], os.EX_OK):
        logger.critical('Cannot execute %s', pipedef)
        sys.exit(1)

    statf = os.path.join(repo, 'pi-piper.latest')
    if not os.path.exists(statf):
        if dryrun:
            logger.info('Would have set up piper for %s [DRYRUN]', repo)
            return
        if not init_piper_tracking(repo, shallow):
            logger.critical('Unable to set up piper for %s', repo)
        return

    try:
        revlist = git_get_new_revs(repo, pipelast=pipelast)
    except KeyError:
        # this could have happened if the public-inbox repository
        # got rebased, e.g. due to GDPR-induced history editing.
        # For now, bluntly handle this by getting rid of our
        # status file and pretending we just started new.
        # XXX: in reality, we could handle this better by keeping track
        #      of the subject line of the latest message we processed, and
        #      then going through history to find the new commit-id of that
        #      message. Unless, of course, that's the exact message that got
        #      deleted in the first place. :/
        #      This also makes it hard with shallow repos, since we'd have
        #      to unshallow them first in order to find that message.
        logger.critical('Assuming the repository got rebased, dropping all history.')
        os.unlink(statf)
        if not dryrun:
            init_piper_tracking(repo, shallow)
        revlist = git_get_new_revs(repo)

    if not revlist:
        return

    logger.info('Processing %s commits', len(revlist))

    latest_good = None
    ecode = 0
    for commit_id, subject in revlist:
        msgbytes = git_get_message_from_pi(repo, commit_id)
        if msgbytes:
            if dryrun:
                logger.info('  piping: %s (%s b) [DRYRUN]', commit_id, len(msgbytes))
                logger.debug(' subject: %s', subject)
            else:
                logger.info('  piping: %s (%s b)', commit_id, len(msgbytes))
                logger.debug(' subject: %s', subject)
                ecode, out, err = grokmirror.run_shell_command(args, stdin=msgbytes)
                if ecode > 0:
                    logger.info('Error running %s', pipedef)
                    logger.info(err)
                    break
                latest_good = commit_id

    if latest_good and not dryrun:
        with open(statf, 'w') as fh:
            fh.write(latest_good)
            logger.info('Wrote %s', statf)
        if ecode == 0 and shallow:
            reshallow(repo, latest_good)

    sys.exit(ecode)


def command():
    import argparse
    from configparser import ConfigParser, ExtendedInterpolation

    global logger

    # noinspection PyTypeChecker
    op = argparse.ArgumentParser(prog='grok-pi-piper',
                                 description='Pipe new messages from public-inbox repositories to arbitrary commands',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    op.add_argument('-v', '--verbose', action='store_true',
                    default=False,
                    help='Be verbose and tell us what you are doing')
    op.add_argument('-d', '--dry-run', dest='dryrun', action='store_true',
                    default=False,
                    help='Do a dry-run and just show what would be done')
    op.add_argument('-c', '--config', required=True,
                    help='Location of the configuration file')
    op.add_argument('-l', '--pipe-last', dest='pipelast', type=int, default=None,
                    help='Force pipe last NN messages in the list, regardless of tracking')
    op.add_argument('repo',
                    help='Full path to foo/git/N.git public-inbox repository')
    op.add_argument('--version', action='version', version=grokmirror.VERSION)

    opts = op.parse_args()

    cfgfile = os.path.expanduser(opts.config)
    if not cfgfile:
        sys.stderr.write('ERORR: File does not exist: %s\n' % cfgfile)
        sys.exit(1)
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read(os.path.expanduser(cfgfile))

    # Find out the section that we want from the config file
    section = 'DEFAULT'
    for sectname in config.sections():
        if fnmatch.fnmatch(opts.repo, f'*/{sectname}/git/*.git'):
            section = sectname

    pipe = config[section].get('pipe')
    if pipe == 'None':
        # Quick exit
        sys.exit(0)

    logfile = config[section].get('log')
    if config[section].get('loglevel') == 'debug':
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    shallow = config[section].getboolean('shallow', False) # noqa

    logger = grokmirror.init_logger('pull', logfile, loglevel, opts.verbose)

    run_pi_repo(opts.repo, pipe, dryrun=opts.dryrun, shallow=shallow, pipelast=opts.pipelast)


if __name__ == '__main__':
    command()
