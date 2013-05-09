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
import json
import subprocess
import random

import time
import datetime

from fcntl import flock, LOCK_EX, LOCK_UN, LOCK_NB

# default basic logger. We override it later.
logger = logging.getLogger(__name__)

def run_git_fsck(fullpath, config):
    env = {'GIT_DIR': fullpath}
    args = ['/usr/bin/git', 'fsck', '--full']
    logger.info('Checking %s' % fullpath)

    # Lock the git repository so no other grokmirror process attempts to
    # modify it while we're running git-fsck. If we miss this window, we
    # may not check the repo again for a long time, so block until the lock
    # is available.
    try:
        grokmirror.lock_repo(fullpath, nonblocking=False)
    except IOError, ex:
        logger.info('Could not obtain exclusive lock on %s' % gitdir)
        logger.info('Will run next time')
        return

    logger.debug('Running: GIT_DIR=%s %s' % (env['GIT_DIR'], ' '.join(args)))

    (output, error) = subprocess.Popen(args, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env=env).communicate()

    grokmirror.unlock_repo(fullpath)

    error = error.strip()

    if error:
        # Put things we recognize as fairly benign into debug
        debug = []
        warn  = []
        for line in error.split('\n'):
            if line.find('dangling ') == 0:
                debug.append(line)
            elif line.find('notice: ') == 0:
                debug.append(line)
            else:
                warn.append(line)
        if debug:
            logger.debug('Stderr: %s' % '\n'.join(debug))
        if warn:
            logger.critical('%s has critical errors:' % fullpath)
            for entry in warn:
                logger.critical("\t%s" % entry)

def fsck_mirror(name, config, opts):
    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if 'log' in config.keys():
        ch = logging.FileHandler(config['log'])
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
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

    if opts.verbose:
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
        flock(flockh, LOCK_EX | LOCK_NB)
    except IOError, ex:
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
            flock(flockh, LOCK_UN)
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
        nextcheck = datetime.datetime.strptime(status[fullpath]['nextcheck'],
                '%Y-%m-%d')

        if opts.force or nextcheck <= today:
            logger.debug('Queueing to check %s' % fullpath)
            # Calculate elapsed seconds
            startt = time.time()
            run_git_fsck(fullpath, config)
            endt = time.time()

            status[fullpath]['lastcheck'] = todayiso
            status[fullpath]['s_elapsed'] = round(endt - startt, 2)

            if opts.force:
                # Use randomization for next check, again
                delay = random.randint(1, frequency)
            else:
                delay = frequency

            nextdate = today + datetime.timedelta(days=delay)
            status[fullpath]['nextcheck'] = nextdate.strftime('%F')
            workdone = True

    # Write out the new status
    if workdone:
        logger.info('Writing new status file in %s' % config['statusfile'])
        stfh = open(config['statusfile'], 'w')
        json.dump(status, stfh, indent=2)
        stfh.close()

    flock(flockh, LOCK_UN)
    flockh.close()


if __name__ == '__main__':
    from optparse import OptionParser
    from ConfigParser import ConfigParser

    usage = '''usage: %prog -c fsck.conf
    Run a git-fsck check on grokmirror-managed repositories.
    '''

    parser = OptionParser(usage=usage, version=grokmirror.VERSION)
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
        default=False,
        help='Be verbose and tell us what you are doing')
    parser.add_option('-f', '--force', dest='force',
        action='store_true', default=False,
        help='Force immediate run on all repositories.')
    parser.add_option('-c', '--config', dest='config',
        help='Location of fsck.conf')

    (opts, args) = parser.parse_args()

    ini = ConfigParser()
    ini.read(opts.config)

    retval = 0

    for section in ini.sections():
        config = {}
        for (option, value) in ini.items(section):
            config[option] = value

        fsck_mirror(section, config, opts)


