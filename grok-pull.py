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

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB
from StringIO import StringIO

from git import Repo

# default basic logger. We override it later.
logger = logging.getLogger(__name__)

def fix_remotes(gitdir, toplevel, site):
    # Remove all existing remotes and set new origin
    repo = Repo(os.path.join(toplevel, gitdir.lstrip('/')))
    remotes = repo.git.remote()
    if len(remotes.strip()):
        logger.debug('existing remotes: %s' % remotes)
        for name in remotes.split('\n'):
            logger.debug('\tremoving remote: %s' % name)
            repo.git.remote('rm', name)

    # set my origin
    origin = os.path.join(site, gitdir.lstrip('/'))
    repo.git.remote('add', '--mirror', 'origin', origin)
    logger.debug('\tset new origin as %s' % origin)

def set_owner_description(toplevel, gitdir, owner, description):
    if owner is None and description is None:
        # Let the default git values be there, then
        return

    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    repo = Repo(fullpath)

    if description is not None and repo.description != description:
        logger.debug('Setting %s description to: %s' % (gitdir, description))
        repo.description = description

    if owner is not None:
        logger.debug('Setting %s owner to: %s' % (gitdir, owner))
        repo.git.config('gitweb.owner', owner)

def set_agefile(toplevel, gitdir, last_modified):
    # set agefile, which can be used by cgit to show idle times
    # cgit recommends it to be yyyy-mm-dd hh:mm:ss
    cgit_fmt = time.strftime('%F %T', time.gmtime(last_modified))
    agefile = os.path.join(toplevel, gitdir.lstrip('/'),
            'info/web/last-modified')
    if not os.path.exists(os.path.dirname(agefile)):
        os.makedirs(os.path.dirname(agefile))
    fh = open(agefile, 'w')
    fh.write('%s\n' % cgit_fmt)
    fh.close()
    logger.debug('Wrote "%s" into %s' % (cgit_fmt, agefile))

def run_post_update_hook(hookscript, toplevel, gitdir):
    if hookscript == '':
        return
    if not os.access(hookscript, os.X_OK):
        logger.warning('post_update_hook %s is not executable' % hookscript)
        return

    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    args = [hookscript, fullpath]
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


def pull_repo(toplevel, gitdir):
    env = {'GIT_DIR': os.path.join(toplevel, gitdir.lstrip('/'))}
    args = ['/usr/bin/git', 'remote', 'update', '--prune']
    logger.info('Updating %s' % gitdir)

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

    error = error.strip()

    if error:
        # Put things we recognize into debug
        debug = []
        warn  = []
        for line in error.split('\n'):
            if line.find('cloned an empty repository') > 0:
                debug.append(line)
            else:
                warn.append(line)
        if debug:
            logger.debug('Stderr: %s' % '\n'.join(debug))
        if warn:
            logger.warning('Stderr: %s' % '\n'.join(warn))


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

def write_projects_list(manifest, config):
    import tempfile
    import shutil

    if 'projectslist' not in config.keys():
        return

    if config['projectslist'] == '':
        return

    plpath  = config['projectslist']
    trimtop = ''

    if 'projectslist_trimtop' in config.keys():
        trimtop = config['projectslist_trimtop']

    (dirname, basename) = os.path.split(plpath)
    (fd, tmpfile) = tempfile.mkstemp(prefix=basename, dir=dirname)
    logger.info('Writing new %s' % plpath)

    try:
        fh = open(tmpfile, 'w')
        for gitdir in manifest.keys():
            if trimtop and gitdir.find(trimtop) == 0:
                gitdir = gitdir[len(trimtop):]

            # Always remove leading slash, otherwise cgit breaks
            gitdir = gitdir.lstrip('/')
            fh.write('%s\n' % gitdir)

        fh.close()
        os.chmod(tmpfile, 0644)
        shutil.move(tmpfile, plpath)

    finally:
        # If something failed, don't leave tempfiles trailing around
        if os.path.exists(tmpfile):
            os.unlink(tmpfile)


def pull_mirror(name, config, opts):
    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    if 'log' in config.keys():
        ch = logging.FileHandler(config['log'])
        formatter = logging.Formatter("[%(process)d] %(asctime)s - %(levelname)s - %(message)s")
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
        lockf(flockh, LOCK_EX | LOCK_NB)
    except IOError, ex:
        logger.info('Could not obtain exclusive lock on %s' % config['lock'])
        logger.info('Assuming another process is running.')
        return 0

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
            lockf(flockh, LOCK_UN)
            flockh.close()
            return 0
        logger.critical('Could not fetch %s' % config['manifest'])
        logger.critical('Server returned: %s' % ex)
        lockf(flockh, LOCK_UN)
        flockh.close()
        return 1
    except urllib2.URLError, ex:
        logger.critical('Could not fetch %s' % config['manifest'])
        logger.critical('Error was: %s' % ex)
        lockf(flockh, LOCK_UN)
        flockh.close()
        return 1

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
        lockf(flockh, LOCK_UN)
        flockh.close()
        return 1

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
    if not os.access(toplevel, os.W_OK):
        logger.critical('Toplevel %s does not exist or is not writable' % toplevel)
        sys.exit(1)

    for gitdir in culled.keys():
        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        if gitdir in mymanifest.keys():
            # Is the directory in place, too?
            if os.path.exists(fullpath):
                # Is it newer than what we have in our old manifest?
                if gitdir in mymanifest.keys():
                    # This code is hurky and needs to be cleaned up
                    desc    = culled[gitdir].get('description')
                    owner   = culled[gitdir].get('owner')
                    mydesc  = mymanifest[gitdir].get('description')
                    myowner = mymanifest[gitdir].get('owner')
                    if owner is None:
                        owner = config['default_owner']
                    if myowner is None:
                        myowner = config['default_owner']
                    if (owner != myowner or desc != mydesc):
                        # we can do this right away without waiting
                        set_owner_description(toplevel, gitdir, owner, desc)

                    if (opts.force or culled[gitdir]['modified']
                            > mymanifest[gitdir]['modified']):
                        to_pull.append(gitdir)
                    else:
                        logger.debug('Repo %s unchanged in manifest' % gitdir)
                        existing.append(gitdir)
                continue

        # do we have the dir in place?
        if os.path.exists(fullpath):
            if not opts.reuse:
                logger.critical('Found existing repository in %s' % fullpath)
                logger.critical('Run with -r to use existing repos')
                sys.exit(1)
            logger.info('Found existing %s, will set new origin' % gitdir)
            # Accept it, but fix remotes so they are pointing to our origin
            fix_remotes(gitdir, toplevel, config['site'])
            to_pull.append(gitdir)
            continue

        to_clone.append(gitdir)

    hookscript = config['post_update_hook']

    for gitdir in to_pull:
        # Check in case grok-fsck is checking this repo right now
        try:
            fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
            grokmirror.lock_repo(fullpath, nonblocking=True)
            pull_repo(toplevel, gitdir)
            set_agefile(toplevel, gitdir, culled[gitdir]['modified'])
            run_post_update_hook(hookscript, toplevel, gitdir)
            grokmirror.unlock_repo(fullpath)
        except IOError, ex:
            logger.info('Could not obtain exclusive lock on %s' % gitdir)
            logger.info('\tAssuming grok-fsck is running, will try later.')
            # To make sure we check this again during next run,
            # fudge the manifest accordingly.
            culled[gitdir] = mymanifest[gitdir]
            # this is rather hackish, but effective
            last_modified -= 1

    if to_clone:
        # we use "existing" to track which repos can be used as references
        existing.extend(to_pull)

        to_clone_sorted = []
        clone_order(to_clone, manifest, to_clone_sorted, existing)

        for gitdir in to_clone_sorted:
            reference = culled[gitdir]['reference']
            if reference is not None and reference in existing:
                clone_repo(toplevel, gitdir, config['site'],
                        reference=reference)
            else:
                clone_repo(toplevel, gitdir, config['site'])

            # check dir to make sure cloning succeeded and then add to existing
            if os.path.exists(os.path.join(toplevel, gitdir.lstrip('/'))):
                logger.debug('Cloning of %s succeeded, adding to existing'
                        % gitdir)
                existing.append(gitdir)

                desc    = culled[gitdir].get('description')
                owner   = culled[gitdir].get('owner')
                if owner is None:
                    owner = config['default_owner']
                set_owner_description(toplevel, gitdir, owner, desc)
                set_agefile(toplevel, gitdir, culled[gitdir]['modified'])
                run_post_update_hook(hookscript, toplevel, gitdir)

    # loop through all entries and find any symlinks we need to set
    # We also collect all symlinks to do purging correctly
    symlinks = []
    for gitdir in culled.keys():
        if 'symlinks' in culled[gitdir].keys():
            source = os.path.join(config['toplevel'], gitdir.lstrip('/'))
            for symlink in culled[gitdir]['symlinks']:
                if symlink not in symlinks:
                    symlinks.append(symlink)
                target = os.path.join(config['toplevel'], symlink.lstrip('/'))
                if not os.path.exists(target) and os.path.exists(source):
                    logger.info('Symlinking %s -> %s' % (target, source))
                    # Make sure the leading dirs are in place
                    if not os.path.exists(os.path.dirname(target)):
                        os.makedirs(os.path.dirname(target))
                    os.symlink(source, target)

    if opts.purge:
        for founddir in grokmirror.find_all_gitdirs(config['toplevel']):
            gitdir = founddir.replace(config['toplevel'], '')
            if gitdir not in culled.keys() and gitdir not in symlinks:
                if os.path.islink(founddir):
                    logger.info('Removing unreferenced symlink %s' % gitdir)
                    os.unlink(founddir)
                else:
                    logger.info('Purging %s' % gitdir)
                    shutil.rmtree(founddir)

    # Once we're done, save culled as our new manifest
    grokmirror.write_manifest(config['mymanifest'], culled, last_modified)

    # write out projects.list, if asked to
    write_projects_list(culled, config)

    logger.debug('Unlocking %s' % config['lock'])
    lockf(flockh, LOCK_UN)
    flockh.close()

    return 127


if __name__ == '__main__':
    from optparse import OptionParser
    from ConfigParser import ConfigParser

    usage = '''usage: %prog -c repos.conf
    Create a grok mirror using the repository configuration found in repos.conf
    '''

    parser = OptionParser(usage=usage, version=grokmirror.VERSION)
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
    parser.add_option('-r', '--reuse-existing-repos', dest='reuse',
        action='store_true', default=False,
        help='If any existing repositories are found on disk, set new '
        'remote origin and reuse')
    parser.add_option('-c', '--config', dest='config',
        help='Location of repos.conf')

    (opts, args) = parser.parse_args()

    ini = ConfigParser()
    ini.read(opts.config)

    retval = 0

    for section in ini.sections():
        config = {}
        for (option, value) in ini.items(section):
            config[option] = value

        if 'default_owner' not in config.keys():
            config['default_owner'] = 'Grokmirror User'
        if 'post_update_hook' not in config.keys():
            config['post_update_hook'] = ''

        sect_retval = pull_mirror(section, config, opts)
        if sect_retval == 1:
            # Fatal error encountered at some point
            retval = 1
        elif sect_retval == 127:
            # Successful run with contents modified
            retval = 127

    sys.exit(retval)
