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
import urllib2
import time
import gzip
import json
import fnmatch
import subprocess
import shutil
import calendar

from fcntl import flock, LOCK_EX, LOCK_UN, LOCK_NB
from StringIO import StringIO

# default basic logger. We override it later.
logger = logging.getLogger(__name__)

def pull_repo(toplevel, gitdir):
    env = {'GIT_DIR': os.path.join(toplevel, gitdir.lstrip('/'))}
    args = ['/usr/bin/git', 'remote', 'update']
    logger.info('Updating %s' % gitdir)

    logger.debug('Running: GIT_DIR=%s %s' % (env['GIT_DIR'], ' '.join(args)))

    (output, error) = subprocess.Popen(args, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE, env=env).communicate()

    if error.strip():
        logger.warning('Stderr: %s' % error.strip())

def clone_repo(toplevel, gitdir, site, reference=None):
    source = os.path.join(site, gitdir.lstrip('/'))
    dest   = os.path.join(toplevel, gitdir.lstrip('/'))

    args = ['/usr/bin/git', 'clone', '--mirror']
    if reference is not None:
        reference = os.path.join(toplevel, reference.lstrip('/'))
        args.append('--reference')
        args.append(reference)

    args.append(source)
    args.append(dest)

    logger.info('Cloning %s into %s' % (source, dest))
    if reference is not None:
        logger.info('With reference to %s' % reference)

    logger.debug('Running: %s' % ' '.join(args))

    (output, error) = subprocess.Popen(args, stdout=subprocess.PIPE,
        stderr=subprocess.PIPE).communicate()

    if error.strip():
        logger.warning('Stderr: %s' % error.strip())

def clone_order(to_clone, manifest, to_clone_sorted, existing):
    # recursively go through the list and resolve dependencies
    new_to_clone = []
    num_received = len(to_clone)
    logger.debug('Another clone_order loop')
    for gitdir in to_clone:
        reference = manifest[gitdir]['reference']
        logger.debug('reference: %s' % reference)
        if (reference in existing
                or reference in to_clone_sorted
                or reference is None):
            logger.debug('%s: reference found in existing' % gitdir)
            to_clone_sorted.append(gitdir)
        else:
            logger.debug('%s: reference not found' % gitdir)
            new_to_clone.append(gitdir)
    if len(new_to_clone) == 0 or len(new_to_clone) == num_received:
        # we can resolve no more dependencies, break out
        logger.debug('Finished resolving dependencies, quitting')
        if len(new_to_clone):
            logger.debug('Unresolved: %s' % new_to_clone)
            to_clone_sorted.extend(new_to_clone)
        return

    logger.debug('Going for another clone_order loop')
    clone_order(new_to_clone, manifest, to_clone_sorted, existing)

def pull_mirror(name, config, opts):
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

    logger.info('Updating mirror for [%s]' % name)

    # Lock the tree to make sure we only run one instance
    logger.debug('Attempting to obtain lock on %s' % config['lock'])
    flockh = open(config['lock'], 'w')
    try:
        flock(flockh, LOCK_EX | LOCK_NB)
    except IOError, ex:
        logger.info('Could not obtain exclusive lock on %s' % config['lock'])
        logger.info('Assuming another process is running.')
        return

    mymanifest = config['mymanifest']
    logger.info('Fetching remote manifest from %s' % config['manifest'])
    request = urllib2.Request(config['manifest'])
    opener  = urllib2.build_opener()

    # Find out if we need to run at all first
    if not (opts.force or opts.nomtime) and os.path.exists(mymanifest):
        fstat = os.stat(mymanifest)
        mtime = fstat[8]
        logger.debug('mtime on %s is: %s' % (mymanifest, mtime))
        my_last_modified = time.strftime('%a, %d %b %Y %H:%M:%S GMT',
            time.gmtime(mtime))
        logger.debug('Our last-modified is: %s' % my_last_modified)
        request.add_header('If-Modified-Since', my_last_modified)

    try:
        ufh = opener.open(request)
    except urllib2.HTTPError, ex:
        if ex.code == 304:
            logger.info('Server says we have the latest manifest. Quitting.')
            flock(flockh, LOCK_UN)
            flockh.close()
            return
        logger.critical('Could not fetch %s' % config['manifest'])
        logger.critical('Server returned: %s' % ex)
        flock(flockh, LOCK_UN)
        flockh.close()
        return
    except urllib2.URLError, ex:
        logger.critical('Could not fetch %s' % config['manifest'])
        logger.critical('Error was: %s' % ex)
        flock(flockh, LOCK_UN)
        flockh.close()
        return

    last_modified = ufh.headers.get('Last-Modified')
    last_modified = time.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
    last_modified = calendar.timegm(last_modified)

    # We don't use read_manifest for the remote manifest, as it can be
    # anything, really. For now, blindly open it with gzipfile if it ends
    # with .gz. XXX: some http servers will auto-deflate such files.
    if config['manifest'].find('.gz') > 0:
        fh = gzip.GzipFile(fileobj=StringIO(ufh.read()))
    else:
        fh = ufh

    try:
        manifest = json.load(fh)
    except:
        logger.critical('Failed to parse %s' % config['manifest'])
        flock(flockh, LOCK_UN)
        flockh.close()
        return

    mymanifest = grokmirror.read_manifest(mymanifest)

    includes = config['include'].split('\n')
    excludes = config['exclude'].split('\n')

    # We keep culled, because that becomes our new manifest
    culled = {}

    for gitdir in manifest.keys():
        # does it fall under include?
        for include in includes:
            if fnmatch.fnmatch(gitdir, include):
                # Yes, but does it fall under excludes?
                excluded = False
                for exclude in excludes:
                    if fnmatch.fnmatch(gitdir, exclude):
                        excluded = True
                        break
                if excluded:
                    continue

                culled[gitdir] = manifest[gitdir]

    to_clone = []
    to_pull  = []
    existing = []

    toplevel = config['toplevel']
    for gitdir in culled.keys():
        if gitdir in mymanifest.keys():
            # Is the directory in place, too?
            if os.path.exists(os.path.join(toplevel, gitdir.lstrip('/'))):
                # Is it newer than what we have in our old manifest?
                if gitdir in mymanifest.keys():
                    if (opts.force or culled[gitdir]['modified']
                            > mymanifest[gitdir]['modified']):
                        to_pull.append(gitdir)
                    else:
                        logger.info('Repo %s unchanged in manifest' % gitdir)
                        existing.append(gitdir)
                continue

        # do we have the dir in place?
        if os.path.exists(os.path.join(toplevel, gitdir.lstrip('/'))):
            # blindly assume it's kosher and pull it
            to_pull.append(gitdir)
            continue

        to_clone.append(gitdir)

    for gitdir in to_pull:
        pull_repo(toplevel, gitdir)

    # we use "existing" to track which repos can be used as references
    existing.extend(to_pull)

    to_clone_sorted = []
    clone_order(to_clone, manifest, to_clone_sorted, existing)

    for gitdir in to_clone_sorted:
        reference = culled[gitdir]['reference']
        if reference is not None and reference in existing:
            clone_repo(toplevel, gitdir, config['site'], reference=reference)
        else:
            clone_repo(toplevel, gitdir, config['site'])

        # check dir to make sure cloning succeeded and then add to existing
        if os.path.exists(os.path.join(toplevel, gitdir.lstrip('/'))):
            logger.debug('Cloning of %s succeeded, adding to existing' % gitdir)
            existing.append(gitdir)

    # loop through all entries and find any symlinks we need to set
    for gitdir in culled.keys():
        if 'symlinks' in culled[gitdir].keys():
            source = os.path.join(config['toplevel'], gitdir.lstrip('/'))
            for symlink in culled[gitdir]['symlinks']:
                target = os.path.join(config['toplevel'], symlink.lstrip('/'))
                logger.info('Symlinking %s -> %s' % (target, source))
                os.symlink(source, target)

    if opts.purge:
        for founddir in grokmirror.find_all_gitdirs(config['toplevel']):
            gitdir = founddir.replace(config['toplevel'], '')
            if gitdir not in culled.keys():
                logger.info('Purging %s' % gitdir)
                shutil.rmtree(founddir)

    # Once we're done, save culled as our new manifest
    grokmirror.write_manifest(config['mymanifest'], culled, last_modified)
    logger.debug('Unlocking %s' % config['lock'])
    flock(flockh, LOCK_UN)
    flockh.close()


if __name__ == '__main__':
    from optparse import OptionParser
    from ConfigParser import ConfigParser

    usage = '''usage: %prog -c repos.conf
    Create a grok mirror using the repository configuration found in repos.conf
    '''

    parser = OptionParser(usage=usage, version='0.1')
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
        default=False,
        help='Be verbose and tell us what you are doing')
    parser.add_option('-n', '--no-mtime-check', dest='nomtime',
        action='store_true', default=False,
        help='Run without checking manifest mtime.')
    parser.add_option('-f', '--force', dest='force',
        action='store_true', default=False,
        help='Force full git update regardless of last-modified times. '
        'Also useful when repos.conf has changed.')
    parser.add_option('-p', '--purge', dest='purge',
        action='store_true', default=False,
        help='Remove any git trees that are no longer in manifest.')
    parser.add_option('-c', '--config', dest='config',
        help='Location of repos.conf')

    (opts, args) = parser.parse_args()

    ini = ConfigParser()
    ini.read(opts.config)
    for section in ini.sections():
        config = {}
        for (option, value) in ini.items(section):
            config[option] = value

        pull_mirror(section, config, opts)
