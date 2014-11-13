#-*- coding: utf-8 -*-
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
import json
import subprocess
import random

import time
import datetime

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

# default basic logger. We override it later.
logger = logging.getLogger(__name__)


def run_git_repack(fullpath, config):
    if 'repack' not in config.keys() or config['repack'] != 'yes':
        return

    if 'repack_flags' not in config.keys():
        config['repack_flags'] = '-A -d -l -q'

    flags = config['repack_flags'].split()

    env = {'GIT_DIR': fullpath}
    args = ['/usr/bin/git', 'repack'] + flags
    logger.info('Repacking %s' % fullpath)

    logger.debug('Running: GIT_DIR=%s %s' % (env['GIT_DIR'], ' '.join(args)))

    (output, error) = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env).communicate()

    error = error.strip()

    # With newer versions of git, repack may return warnings that are safe to ignore
    # so use the same strategy to weed out things we aren't interested in seeing
    if error:
        # Put things we recognize as fairly benign into debug
        debug = []
        warn = []
        for line in error.split('\n'):
            ignored = False
            for estring in config['ignore_errors']:
                if line.find(estring) != -1:
                    ignored = True
                    debug.append(line)
                    break
            if not ignored:
                warn.append(line)

        if debug:
            logger.debug('Stderr: %s' % '\n'.join(debug))
        if warn:
            logger.critical('Repacking %s returned critical errors:' % fullpath)
            for entry in warn:
                logger.critical("\t%s" % entry)


def run_git_fsck(fullpath, config):
    # Lock the git repository so no other grokmirror process attempts to
    # modify it while we're running git ops. If we miss this window, we
    # may not check the repo again for a long time, so block until the lock
    # is available.
    try:
        grokmirror.lock_repo(fullpath, nonblocking=False)
    except IOError:
        logger.info('Could not obtain exclusive lock on %s' % fullpath)
        logger.info('Will run next time')
        return

    env = {'GIT_DIR': fullpath}
    args = ['/usr/bin/git', 'fsck', '--full']
    logger.info('Checking %s' % fullpath)

    logger.debug('Running: GIT_DIR=%s %s' % (env['GIT_DIR'], ' '.join(args)))

    (output, error) = subprocess.Popen(
        args, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env=env).communicate()

    error = error.strip()

    if error:
        # Put things we recognize as fairly benign into debug
        debug = []
        warn = []
        for line in error.split('\n'):
            ignored = False
            for estring in config['ignore_errors']:
                if line.find(estring) != -1:
                    ignored = True
                    debug.append(line)
                    break
            if not ignored:
                warn.append(line)

        if debug:
            logger.debug('Stderr: %s' % '\n'.join(debug))
        if warn:
            logger.critical('%s has critical errors:' % fullpath)
            for entry in warn:
                logger.critical("\t%s" % entry)

    run_git_repack(fullpath, config)

    grokmirror.unlock_repo(fullpath)


def fsck_mirror(name, config, verbose=False, force=False):
    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if 'log' in config.keys():
        ch = logging.FileHandler(config['log'])
        formatter = logging.Formatter(
            "[%(process)d] %(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        loglevel = logging.INFO

        if 'loglevel' in config.keys():
            if config['loglevel'] == 'debug':
                loglevel = logging.DEBUG

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

    # push it into grokmirror to override the default logger
    grokmirror.logger = logger

    logger.info('Running grok-fsck for [%s]' % name)

    # Lock the tree to make sure we only run one instance
    logger.debug('Attempting to obtain lock on %s' % config['lock'])
    flockh = open(config['lock'], 'w')
    try:
        lockf(flockh, LOCK_EX | LOCK_NB)
    except IOError:
        logger.info('Could not obtain exclusive lock on %s' % config['lock'])
        logger.info('Assuming another process is running.')
        return 0

    manifest = grokmirror.read_manifest(config['manifest'])

    if os.path.exists(config['statusfile']):
        logger.info('Reading status from %s' % config['statusfile'])
        stfh = open(config['statusfile'], 'r')
        try:
            # Format of the status file:
            #  {
            #    '/full/path/to/repository': {
            #      'lastcheck': 'YYYY-MM-DD' or 'never',
            #      'nextcheck': 'YYYY-MM-DD',
            #    },
            #    ...
            #  }

            status = json.load(stfh)
        except:
            # Huai le!
            logger.critical('Failed to parse %s' % config['statusfile'])
            lockf(flockh, LOCK_UN)
            flockh.close()
            return 1
    else:
        status = {}

    frequency = int(config['frequency'])

    today = datetime.datetime.today()

    workdone = False

    # Go through the manifest and compare with status
    for gitdir in manifest.keys():
        fullpath = os.path.join(config['toplevel'], gitdir.lstrip('/'))
        if fullpath not in status.keys():
            # Newly added repository
            # Randomize next check between now and frequency
            delay = random.randint(0, frequency)
            nextdate = today + datetime.timedelta(days=delay)
            nextcheck = nextdate.strftime('%F')
            status[fullpath] = {
                'lastcheck': 'never',
                'nextcheck': nextcheck,
            }
            logger.info('Added new repository %s with next check on %s' % (
                gitdir, nextcheck))
            workdone = True

    # Go through status and queue checks for all the dirs that are due today
    # (unless --force, which is EVERYTHING)
    todayiso = today.strftime('%F')
    for fullpath in status.keys():
        # Check to make sure it's still in the manifest
        gitdir = fullpath.replace(config['toplevel'], '', 1)
        gitdir = '/' + gitdir.lstrip('/')

        if gitdir not in manifest.keys():
            del status[fullpath]
            logger.info('Removed %s which is no longer in manifest' % gitdir)
            continue

        # If nextcheck is before today, set it to today
        # XXX: If a system comes up after being in downtime for a while, this
        #      may cause pain for them, so perhaps use randomization here?
        nextcheck = datetime.datetime.strptime(status[fullpath]['nextcheck'],
                                               '%Y-%m-%d')

        if force or nextcheck <= today:
            logger.debug('Queueing to check %s' % fullpath)
            # Calculate elapsed seconds
            startt = time.time()
            run_git_fsck(fullpath, config)
            endt = time.time()

            status[fullpath]['lastcheck'] = todayiso
            status[fullpath]['s_elapsed'] = round(endt - startt, 2)

            if force:
                # Use randomization for next check, again
                delay = random.randint(1, frequency)
            else:
                delay = frequency

            nextdate = today + datetime.timedelta(days=delay)
            status[fullpath]['nextcheck'] = nextdate.strftime('%F')
            workdone = True

    # Do quickie checks
    if 'quick_checks_max_min' in config.keys():
        # Convert to seconds for ease of tracking
        max_time = int(config['quick_checks_max_min']) * 60
        logger.debug('max_time=%s' % max_time)
        if max_time < 60:
            logger.warning('quick_checks_max_min must be at least 1 minute')
            max_time = 60
        logger.info('Performing quick checks')
        # Find the smallest s_elapsed not yet checked today
        # and run a check on it until we either run out of time
        # or repos to check.
        total_elapsed_time = 0
        quickies_checked = 0
        while True:
            # use this var to track which repo is smallest on s_elapsed
            least_elapsed = None
            repo_to_check = None
            for fullpath in status.keys():
                if status[fullpath]['lastcheck'] in ('never', todayiso):
                    # never been checked or checked today, skip
                    continue
                if 's_elapsed' not in status[fullpath].keys():
                    # something happened to the s_elapsed entry?
                    continue
                prev_elapsed = status[fullpath]['s_elapsed']
                if total_elapsed_time + prev_elapsed > max_time:
                    # would take us too long to check it, skip
                    continue
                if least_elapsed is None or prev_elapsed < least_elapsed:
                    least_elapsed = prev_elapsed
                    repo_to_check = fullpath

            if repo_to_check is None:
                if quickies_checked == 0:
                    logger.info('No repos qualified for quick checks')
                else:
                    logger.info('Quick-checked %s repos in %s seconds' % (
                        quickies_checked, total_elapsed_time))
                break

            # check repo and record the necessary bits
            startt = time.time()
            run_git_fsck(repo_to_check, config)
            endt = time.time()

            # We don't adjust nextcheck, since it kinda becomes meaningless
            status[repo_to_check]['lastcheck'] = todayiso
            status[repo_to_check]['s_elapsed'] = round(endt - startt, 2)

            total_elapsed_time += status[repo_to_check]['s_elapsed']
            quickies_checked += 1
            workdone = True

    # Write out the new status
    if workdone:
        logger.info('Writing new status file in %s' % config['statusfile'])
        stfh = open(config['statusfile'], 'w')
        json.dump(status, stfh, indent=2)
        stfh.close()

    lockf(flockh, LOCK_UN)
    flockh.close()


def parse_args():
    from optparse import OptionParser

    usage = '''usage: %prog -c fsck.conf
    Run a git-fsck check on grokmirror-managed repositories.
    '''

    op = OptionParser(usage=usage, version=grokmirror.VERSION)
    op.add_option('-v', '--verbose', dest='verbose', action='store_true',
                  default=False,
                  help='Be verbose and tell us what you are doing')
    op.add_option('-f', '--force', dest='force',
                  action='store_true', default=False,
                  help='Force immediate run on all repositories.')
    op.add_option('-c', '--config', dest='config',
                  help='Location of fsck.conf')

    opts, args = op.parse_args()

    if not opts.config:
        op.error('You must provide the path to the config file')

    return opts, args


def grok_fsck(config, verbose=False, force=False):
    from ConfigParser import ConfigParser

    ini = ConfigParser()
    ini.read(config)

    for section in ini.sections():
        config = {}
        for (option, value) in ini.items(section):
            config[option] = value

        if 'ignore_errors' not in config:
            config['ignore_errors'] = [
                'dangling commit',
                'dangling blob',
                'notice: HEAD points to an unborn branch',
                'notice: No default references',
                'contains zero-padded file modes',
            ]
        else:
            ignore_errors = []
            for estring in config['ignore_errors'].split('\n'):
                ignore_errors.append(estring.strip())
            config['ignore_errors'] = ignore_errors

        fsck_mirror(section, config, verbose, force)


def command():
    opts, args = parse_args()

    return grok_fsck(opts.config, opts.verbose, opts.force)

if __name__ == '__main__':
    command()
