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
import sys

import grokmirror
import logging
try:
    import urllib.request as urllib_request
    from urllib.error import HTTPError, URLError
    from urllib.parse import urlparse
except ImportError:
    import urllib2 as urllib_request
    from urllib2 import HTTPError, URLError
    from urlparse import urlparse

import ssl
import time
import gzip
import anyjson
import fnmatch
import subprocess
import shutil
import calendar

import threading
try:
    from queue import Queue
except ImportError:
    from Queue import Queue

from io import BytesIO

from git import Repo

import enlighten

# default basic logger. We override it later.
logger = logging.getLogger(__name__)

# We use it to bluntly track if there were any repos we couldn't lock
lock_fails = []
# The same repos that didn't clone/pull successfully
git_fails = []
# The same for repos that didn't verify successfully
verify_fails = []


class PullerThread(threading.Thread):
    def __init__(self, in_queue, out_queue, config, thread_name, e_bar):
        threading.Thread.__init__(self)
        self.in_queue = in_queue
        self.out_queue = out_queue
        self.toplevel = config['toplevel']
        self.hookscript = config['post_update_hook']
        self.myname = thread_name
        self.e_bar = e_bar

    def run(self):
        # XXX: This is not thread-safe, but okay for now,
        #      as we only use this for very blunt throttling
        global lock_fails
        global git_fails
        while True:
            (gitdir, fingerprint, modified) = self.in_queue.get()
            self.e_bar.refresh()
            # Do we still need to update it, or has another process
            # already done this for us?
            todo = True
            success = False
            logger.debug('[Thread-%s] gitdir=%s, figerprint=%s, modified=%s',
                         self.myname, gitdir, fingerprint, modified)

            fullpath = os.path.join(self.toplevel, gitdir.lstrip('/'))

            try:
                grokmirror.lock_repo(fullpath, nonblocking=True)
                # First, get fingerprint as reported in grokmirror.fingerprint
                my_fingerprint = grokmirror.get_repo_fingerprint(
                    self.toplevel, gitdir, force=False)

                # We never rely on timestamps if fingerprints are in play
                if fingerprint is None:
                    ts = grokmirror.get_repo_timestamp(self.toplevel, gitdir)
                    if ts >= modified:
                        logger.debug('[Thread-%s] TS same or newer, '
                                     'not pulling %s', self.myname, gitdir)
                        todo = False
                else:
                    # Recheck the real fingerprint to make sure there is no
                    # divergence between grokmirror.fingerprint and real repo
                    logger.debug('[Thread-%s] Rechecking fingerprint in %s',
                                 self.myname, gitdir)
                    my_fingerprint = grokmirror.get_repo_fingerprint(
                        self.toplevel, gitdir, force=True)

                    # Update the fingerprint stored in-repo
                    grokmirror.set_repo_fingerprint(
                        self.toplevel, gitdir, fingerprint=my_fingerprint)

                    if fingerprint == my_fingerprint:
                        logger.debug('[Thread-%s] FP match, not pulling %s',
                                     self.myname, gitdir)
                        todo = False

                if not todo:
                    logger.debug('[Thread-%s] %s already latest, skipping',
                                 self.myname, gitdir)
                    set_agefile(self.toplevel, gitdir, modified)
                    grokmirror.unlock_repo(fullpath)
                    self.out_queue.put((gitdir, my_fingerprint, True))
                    self.in_queue.task_done()
                    continue

                logger.info('[Thread-%s] updating %s', self.myname, gitdir)
                success = pull_repo(self.toplevel, gitdir, threadid=self.myname)
                logger.debug('[Thread-%s] done pulling %s',
                             self.myname, gitdir)

                if success:
                    set_agefile(self.toplevel, gitdir, modified)
                    run_post_update_hook(self.hookscript, self.toplevel, gitdir,
                                         threadid=self.myname)
                else:
                    logger.warning('[Thread-%s] pulling %s unsuccessful',
                                   self.myname, gitdir)
                    git_fails.append(gitdir)

                # Record our current fingerprint and return it
                my_fingerprint = grokmirror.set_repo_fingerprint(
                    self.toplevel, gitdir)

                grokmirror.unlock_repo(fullpath)
            except IOError:
                my_fingerprint = fingerprint
                logger.info('[Thread-%s] Could not lock %s, skipping',
                            self.myname, gitdir)
                lock_fails.append(gitdir)

            self.out_queue.put((gitdir, my_fingerprint, success))
            self.e_bar.update()
            self.in_queue.task_done()


def cull_manifest(manifest, config):
    includes = config['include'].split('\n')
    excludes = config['exclude'].split('\n')

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

    return culled


def fix_remotes(gitdir, toplevel, site):
    # Remove all existing remotes and set new origin
    repo = Repo(os.path.join(toplevel, gitdir.lstrip('/')))
    remotes = repo.git.remote()
    if len(remotes.strip()):
        logger.debug('existing remotes: %s', remotes)
        for name in remotes.split('\n'):
            logger.debug('\tremoving remote: %s', name)
            repo.git.remote('rm', name)

    # set my origin
    origin = os.path.join(site, gitdir.lstrip('/'))
    repo.git.remote('add', '--mirror', 'origin', origin)
    logger.debug('\tset new origin as %s', origin)


def set_repo_params(toplevel, gitdir, owner, description, reference):
    if owner is None and description is None and reference is None:
        # Let the default git values be there, then
        return

    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    repo = Repo(fullpath)

    if description is not None:
        try:
            if repo.description != description:
                logger.debug('Setting %s description to: %s',
                             gitdir, description)
                repo.description = description
        except IOError:
            # Bug in git-python will throw an exception if description
            # file is not found
            logger.debug('%s description file missing, setting to: %s',
                         gitdir, description)
            repo.description = description

    if owner is not None:
        logger.debug('Setting %s owner to: %s', gitdir, owner)
        repo.git.config('gitweb.owner', owner)

    if reference is not None:
        # XXX: Removing alternates involves git repack, so we don't support it
        #      at this point. We also cowardly refuse to change an existing
        #      alternates entry, as this has high chance of resulting in
        #      broken git repositories. Only do this when we're going from
        #      none to some value.
        if len(repo.alternates) > 0:
            return

        objects = os.path.join(toplevel, reference.lstrip('/'), 'objects')
        altfile = os.path.join(fullpath, 'objects', 'info', 'alternates')
        logger.info('Setting %s alternates to: %s', gitdir, objects)
        with open(altfile, 'wt') as altfh:
            altfh.write('%s\n' % objects)


def set_agefile(toplevel, gitdir, last_modified):
    grokmirror.set_repo_timestamp(toplevel, gitdir, last_modified)

    # set agefile, which can be used by cgit to show idle times
    # cgit recommends it to be yyyy-mm-dd hh:mm:ss
    cgit_fmt = time.strftime('%F %T', time.localtime(last_modified))
    agefile = os.path.join(toplevel, gitdir.lstrip('/'),
                           'info/web/last-modified')
    if not os.path.exists(os.path.dirname(agefile)):
        os.makedirs(os.path.dirname(agefile))
    with open(agefile, 'wt') as fh:
        fh.write('%s\n' % cgit_fmt)
    logger.debug('Wrote "%s" into %s', cgit_fmt, agefile)


def run_post_update_hook(hookscript, toplevel, gitdir, threadid='X'):
    if hookscript == '':
        return
    if not os.access(hookscript, os.X_OK):
        logger.warning('[Thread-%s] post_update_hook %s is not executable',
                       threadid, hookscript)
        return

    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    args = [hookscript, fullpath]
    logger.debug('[Thread-%s] Running: %s', threadid, ' '.join(args))
    (output, error) = subprocess.Popen(args, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE).communicate()

    error = error.decode().strip()
    output = output.decode().strip()
    if error:
        # Put hook stderror into warning
        logger.warning('[Thread-%s] Hook Stderr: %s', threadid, error)
    if output:
        # Put hook stdout into info
        logger.info('[Thread-%s] Hook Stdout: %s', threadid, output)


def pull_repo(toplevel, gitdir, threadid='X'):
    env = {'GIT_DIR': os.path.join(toplevel, gitdir.lstrip('/'))}
    args = ['/usr/bin/git', 'remote', 'update', '--prune']

    logger.debug('[Thread-%s] Running: GIT_DIR=%s %s',
                 threadid, env['GIT_DIR'], ' '.join(args))

    child = subprocess.Popen(args, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, env=env)
    (output, error) = child.communicate()

    error = error.decode().strip()

    success = False
    if child.returncode == 0:
        success = True

    if error:
        # Put things we recognize into debug
        debug = []
        warn = []
        for line in error.split('\n'):
            if line.find('From ') == 0:
                debug.append(line)
            elif line.find('-> ') > 0:
                debug.append(line)
            else:
                warn.append(line)
        if debug:
            logger.debug('[Thread-%s] Stderr: %s', threadid, '\n'.join(debug))
        if warn:
            logger.warning('[Thread-%s] Stderr: %s', threadid, '\n'.join(warn))

    return success


def clone_repo(toplevel, gitdir, site, reference=None):
    source = os.path.join(site, gitdir.lstrip('/'))
    dest = os.path.join(toplevel, gitdir.lstrip('/'))

    args = ['/usr/bin/git', 'clone', '--mirror']
    if reference is not None:
        reference = os.path.join(toplevel, reference.lstrip('/'))
        args.append('--reference')
        args.append(reference)

    args.append(source)
    args.append(dest)

    logger.info('Cloning %s into %s', source, dest)
    if reference is not None:
        logger.info('With reference to %s', reference)

    logger.debug('Running: %s', ' '.join(args))

    child = subprocess.Popen(args, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE)
    (output, error) = child.communicate()

    success = False
    if child.returncode == 0:
        success = True

    error = error.decode().strip()

    if error:
        # Put things we recognize into debug
        debug = []
        warn = []
        for line in error.split('\n'):
            if line.find('cloned an empty repository') > 0:
                debug.append(line)
            if line.find('into bare repository') > 0:
                debug.append(line)
            else:
                warn.append(line)
        if debug:
            logger.debug('Stderr: %s', '\n'.join(debug))
        if warn:
            logger.warning('Stderr: %s', '\n'.join(warn))

    return success


def clone_order(to_clone, manifest, to_clone_sorted, existing):
    # recursively go through the list and resolve dependencies
    new_to_clone = []
    num_received = len(to_clone)
    logger.debug('Another clone_order loop')
    for gitdir in to_clone:
        reference = manifest[gitdir]['reference']
        logger.debug('reference: %s', reference)
        if (reference in existing
            or reference in to_clone_sorted
                or reference is None):
            logger.debug('%s: reference found in existing', gitdir)
            to_clone_sorted.append(gitdir)
        else:
            logger.debug('%s: reference not found', gitdir)
            new_to_clone.append(gitdir)
    if len(new_to_clone) == 0 or len(new_to_clone) == num_received:
        # we can resolve no more dependencies, break out
        logger.debug('Finished resolving dependencies, quitting')
        if len(new_to_clone):
            logger.debug('Unresolved: %s', new_to_clone)
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

    plpath = config['projectslist']
    trimtop = ''

    if 'projectslist_trimtop' in config.keys():
        trimtop = config['projectslist_trimtop']

    add_symlinks = False
    if ('projectslist_symlinks' in config.keys()
            and config['projectslist_symlinks'] == 'yes'):
        add_symlinks = True

    (dirname, basename) = os.path.split(plpath)
    (fd, tmpfile) = tempfile.mkstemp(prefix=basename, dir=dirname)
    logger.info('Writing new %s', plpath)

    try:
        with open(tmpfile, 'wt') as fh:
            for gitdir in manifest:
                if trimtop and gitdir.startswith(trimtop):
                    pgitdir = gitdir[len(trimtop):]
                else:
                    pgitdir = gitdir

                # Always remove leading slash, otherwise cgit breaks
                pgitdir = pgitdir.lstrip('/')
                fh.write('%s\n' % pgitdir)

                if add_symlinks and 'symlinks' in manifest[gitdir]:
                    # Do the same for symlinks
                    # XXX: Should make this configurable, perhaps
                    for symlink in manifest[gitdir]['symlinks']:
                        if trimtop and symlink.startswith(trimtop):
                            symlink = symlink[len(trimtop):]

                        symlink = symlink.lstrip('/')
                        fh.write('%s\n' % symlink)

        fh.close()
        # set mode to current umask
        curmask = os.umask(0)
        os.chmod(tmpfile, 0o0666 ^ curmask)
        os.umask(curmask)
        shutil.move(tmpfile, plpath)

    finally:
        # If something failed, don't leave tempfiles trailing around
        if os.path.exists(tmpfile):
            os.unlink(tmpfile)


def pull_mirror(name, config, verbose=False, force=False, nomtime=False,
                verify=False, verify_subpath='*', noreuse=False,
                purge=False, pretty=False, forcepurge=False):
    global logger
    global lock_fails

    # noinspection PyTypeChecker
    em = enlighten.get_manager(series=' -=#')

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
        em.enabled = False

    logger.addHandler(ch)

    # push it into grokmirror to override the default logger
    grokmirror.logger = logger

    logger.info('Checking [%s]', name)
    mymanifest = config['mymanifest']

    if verify:
        logger.info('Verifying mirror against %s', config['manifest'])
        nomtime = True

    if config['manifest'].find('file:///') == 0:
        manifile = config['manifest'].replace('file://', '')
        if not os.path.exists(manifile):
            logger.critical('Remote manifest not found in %s! Quitting!',
                            config['manifest'])
            return 1

        fstat = os.stat(manifile)
        last_modified = fstat[8]
        logger.debug('mtime on %s is: %s', manifile, fstat[8])

        if os.path.exists(config['mymanifest']):
            fstat = os.stat(config['mymanifest'])
            my_last_modified = fstat[8]
            logger.debug('Our last-modified is: %s', my_last_modified)
            if not (force or nomtime) and last_modified <= my_last_modified:
                logger.info('Manifest file unchanged. Quitting.')
                return 0

        logger.info('Reading new manifest from %s', manifile)
        manifest = grokmirror.read_manifest(manifile)
        # Don't accept empty manifests -- that indicates something is wrong
        if not len(manifest.keys()):
            logger.critical('Remote manifest empty or unparseable! Quitting.')
            return 1

    else:
        # Load it from remote host using http and header magic
        logger.info('Fetching remote manifest from %s', config['manifest'])

        # Do we have username:password@ in the URL?
        chunks = urlparse(config['manifest'])
        if chunks.netloc.find('@') > 0:
            logger.debug('Taking username/password from the URL for basic auth')
            (upass, netloc) = chunks.netloc.split('@')
            if upass.find(':') > 0:
                (username, password) = upass.split(':')
            else:
                username = upass
                password = ''

            manifesturl = config['manifest'].replace(chunks.netloc, netloc)
            logger.debug('manifesturl=%s', manifesturl)
            request = urllib_request.Request(manifesturl)

            password_mgr = urllib_request.HTTPPasswordMgrWithDefaultRealm()
            password_mgr.add_password(None, manifesturl, username, password)
            auth_handler = urllib_request.HTTPBasicAuthHandler(password_mgr)
            opener = urllib_request.build_opener(auth_handler)

        else:
            request = urllib_request.Request(config['manifest'])
            opener = urllib_request.build_opener()

        # Find out if we need to run at all first
        if not (force or nomtime) and os.path.exists(mymanifest):
            fstat = os.stat(mymanifest)
            mtime = fstat[8]
            logger.debug('mtime on %s is: %s', mymanifest, mtime)
            my_last_modified = time.strftime('%a, %d %b %Y %H:%M:%S GMT',
                                             time.gmtime(mtime))
            logger.debug('Our last-modified is: %s', my_last_modified)
            request.add_header('If-Modified-Since', my_last_modified)

        try:
            ufh = opener.open(request, timeout=30)
        except HTTPError as ex:
            if ex.code == 304:
                logger.info('Server says we have the latest manifest. '
                            'Quitting.')
                return 0
            logger.warning('Could not fetch %s', config['manifest'])
            logger.warning('Server returned: %s', ex)
            return 1
        except (URLError, ssl.SSLError, ssl.CertificateError) as ex:
            logger.warning('Could not fetch %s', config['manifest'])
            logger.warning('Error was: %s', ex)
            return 1

        last_modified = ufh.headers.get('Last-Modified')
        last_modified = time.strptime(last_modified, '%a, %d %b %Y %H:%M:%S %Z')
        last_modified = calendar.timegm(last_modified)

        # We don't use read_manifest for the remote manifest, as it can be
        # anything, really. For now, blindly open it with gzipfile if it ends
        # with .gz. XXX: some http servers will auto-deflate such files.
        try:
            if config['manifest'].find('.gz') > 0:
                fh = gzip.GzipFile(fileobj=BytesIO(ufh.read()))
            else:
                fh = ufh

            jdata = fh.read().decode('utf-8')
            fh.close()

            manifest = anyjson.deserialize(jdata)

        except Exception as ex:
            logger.warning('Failed to parse %s', config['manifest'])
            logger.warning('Error was: %s', ex)
            return 1

    mymanifest = grokmirror.read_manifest(mymanifest)

    culled = cull_manifest(manifest, config)

    to_clone = []
    to_pull = []
    existing = []

    toplevel = config['toplevel']
    if not os.access(toplevel, os.W_OK):
        logger.critical('Toplevel %s does not exist or is not writable',
                        toplevel)
        sys.exit(1)

    if 'pull_threads' in config.keys():
        pull_threads = int(config['pull_threads'])
        if pull_threads < 1:
            logger.info('pull_threads is less than 1, forcing to 1')
            pull_threads = 1
    else:
        # be conservative
        logger.info('pull_threads is not set, consider setting it')
        pull_threads = 5

    # noinspection PyTypeChecker
    e_cmp = em.counter(total=len(culled), desc='Comparing:', unit='repos', leave=False)

    for gitdir in list(culled):
        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        e_cmp.update()

        # fingerprints were added in later versions, so deal if the upstream
        # manifest doesn't have a fingerprint
        if 'fingerprint' not in culled[gitdir]:
            culled[gitdir]['fingerprint'] = None

        # Attempt to lock the repo
        try:
            grokmirror.lock_repo(fullpath, nonblocking=True)
        except IOError:
            logger.info('Could not lock %s, skipping', gitdir)
            lock_fails.append(gitdir)
            # Force the fingerprint to what we have in mymanifest,
            # if we have it.
            culled[gitdir]['fingerprint'] = None
            if gitdir in mymanifest and 'fingerprint' in mymanifest[gitdir]:
                culled[gitdir]['fingerprint'] = mymanifest[gitdir][
                    'fingerprint']
            if len(lock_fails) >= pull_threads:
                logger.info('Too many repositories locked (%s). Exiting.',
                            len(lock_fails))
                return 0
            continue

        if verify:
            if culled[gitdir]['fingerprint'] is None:
                logger.debug('No fingerprint for %s, not verifying', gitdir)
                grokmirror.unlock_repo(fullpath)
                continue

            if not fnmatch.fnmatch(gitdir, verify_subpath):
                grokmirror.unlock_repo(fullpath)
                continue

            logger.debug('Verifying %s', gitdir)
            if not os.path.exists(fullpath):
                verify_fails.append(gitdir)
                logger.info('Verify: %s ABSENT', gitdir)
                grokmirror.unlock_repo(fullpath)
                continue

            my_fingerprint = grokmirror.get_repo_fingerprint(
                toplevel, gitdir, force=force)

            if my_fingerprint == culled[gitdir]['fingerprint']:
                logger.info('Verify: %s OK', gitdir)
            else:
                logger.critical('Verify: %s FAILED', gitdir)
                verify_fails.append(gitdir)

            grokmirror.unlock_repo(fullpath)
            continue

        # Is the directory in place?
        if os.path.exists(fullpath):
            # Fix owner and description, if necessary
            if gitdir in mymanifest.keys():
                # This code is hurky and needs to be cleaned up
                desc = culled[gitdir].get('description')
                owner = culled[gitdir].get('owner')
                ref = None
                if config['ignore_repo_references'] != 'yes':
                    ref = culled[gitdir].get('reference')

                # dirty hack to force on-disk owner/description checks
                # when we're called with -n, in case our manifest
                # differs from what is on disk for owner/description/alternates
                myref = None
                if nomtime:
                    mydesc = None
                    myowner = None
                else:
                    mydesc = mymanifest[gitdir].get('description')
                    myowner = mymanifest[gitdir].get('owner')

                    if config['ignore_repo_references'] != 'yes':
                        myref = mymanifest[gitdir].get('reference')

                    if myowner is None:
                        myowner = config['default_owner']

                if owner is None:
                    owner = config['default_owner']

                if desc != mydesc or owner != myowner or ref != myref:
                    # we can do this right away without waiting
                    set_repo_params(toplevel, gitdir, owner, desc, ref)

            else:
                # It exists on disk, but not in my manifest?
                if noreuse:
                    logger.critical('Found existing git repo in %s', fullpath)
                    logger.critical('But you asked NOT to reuse repos')
                    logger.critical('Skipping %s', gitdir)
                    grokmirror.unlock_repo(fullpath)
                    continue

                logger.info('Setting new origin for %s', gitdir)
                fix_remotes(gitdir, toplevel, config['site'])
                to_pull.append(gitdir)
                grokmirror.unlock_repo(fullpath)
                continue

            # fingerprints were added late, so if we don't have them
            # in the remote manifest, fall back on using timestamps
            changed = False
            if culled[gitdir]['fingerprint'] is not None:
                logger.debug('Will use fingerprints to compare %s', gitdir)
                my_fingerprint = grokmirror.get_repo_fingerprint(toplevel,
                                                                 gitdir,
                                                                 force=force)

                if my_fingerprint != culled[gitdir]['fingerprint']:
                    logger.debug('No fingerprint match, will pull %s', gitdir)
                    changed = True
                else:
                    logger.debug('Fingerprints match, skipping %s', gitdir)
            else:
                logger.debug('Will use timestamps to compare %s', gitdir)
                if force:
                    logger.debug('Will force-pull %s', gitdir)
                    changed = True
                    # set timestamp to 0 as well
                    grokmirror.set_repo_timestamp(toplevel, gitdir, 0)
                else:
                    ts = grokmirror.get_repo_timestamp(toplevel, gitdir)
                    if ts < culled[gitdir]['modified']:
                        changed = True

            if changed:
                to_pull.append(gitdir)
                grokmirror.unlock_repo(fullpath)
                continue
            else:
                logger.debug('Repo %s unchanged', gitdir)
                # if we don't have a fingerprint for it, add it now
                if culled[gitdir]['fingerprint'] is None:
                    fpr = grokmirror.get_repo_fingerprint(toplevel, gitdir)
                    culled[gitdir]['fingerprint'] = fpr
                existing.append(gitdir)
                grokmirror.unlock_repo(fullpath)
                continue

        else:
            # Newly incoming repo
            to_clone.append(gitdir)
            grokmirror.unlock_repo(fullpath)
            continue

        # If we got here, something is odd.
        # noinspection PyUnreachableCode
        logger.critical('Could not figure out what to do with %s', gitdir)
        grokmirror.unlock_repo(fullpath)

    logger.info('Compared new manifest against %s repositories in %0.2fs', len(culled), e_cmp.elapsed)
    e_cmp.close()

    if verify:
        if len(verify_fails):
            logger.critical('%s repos failed to verify', len(verify_fails))
            return 1
        else:
            logger.info('Verification successful')
            return 0

    hookscript = config['post_update_hook']

    if len(to_pull):

        if len(lock_fails) > 0:
            pull_threads -= len(lock_fails)

        # Don't spin up more threads than we need
        if pull_threads > len(to_pull):
            pull_threads = len(to_pull)

        # exit if we're ever at 0 pull_threads. Shouldn't happen, but some extra
        # precaution doesn't hurt
        if pull_threads <= 0:
            logger.info('Too many repositories locked. Exiting.')
            return 0

        logger.info('Will use %d threads to pull repos', pull_threads)

        # noinspection PyTypeChecker
        e_pull = em.counter(total=len(to_pull), desc='Updating :', unit='repos', leave=False)
        logger.info('Updating %s repos from %s', len(to_pull), config['site'])
        in_queue = Queue()
        out_queue = Queue()

        for gitdir in to_pull:
            in_queue.put((gitdir, culled[gitdir]['fingerprint'],
                          culled[gitdir]['modified']))

        for i in range(pull_threads):
            logger.debug('Spun up thread %s', i)
            t = PullerThread(in_queue, out_queue, config, i, e_pull)
            t.setDaemon(True)
            t.start()

        # wait till it's all done
        in_queue.join()
        logger.info('All threads finished.')

        while not out_queue.empty():
            # see if any of it failed
            (gitdir, my_fingerprint, status) = out_queue.get()
            # We always record our fingerprint in our manifest
            culled[gitdir]['fingerprint'] = my_fingerprint
            if not status:
                # To make sure we check this again during next run,
                # fudge the manifest accordingly.
                logger.debug('Will recheck %s during next run', gitdir)
                culled[gitdir] = mymanifest[gitdir]
                # this is rather hackish, but effective
                last_modified -= 1

        logger.info('Updates completed in %0.2fs', e_pull.elapsed)
        e_pull.close()
    else:
        logger.info('No repositories need updating')

    # how many lockfiles have we seen?
    # If there are more lock_fails than there are
    # pull_threads configured, we skip cloning out of caution
    if len(to_clone) and len(lock_fails) > pull_threads:
        logger.info('Too many repositories locked. Skipping cloning new repos.')
        to_clone = []

    if len(to_clone):
        # noinspection PyTypeChecker
        e_clone = em.counter(total=len(to_clone), desc='Cloning  :', unit='repos', leave=False)
        logger.info('Cloning %s repos from %s', len(to_clone), config['site'])
        # we use "existing" to track which repos can be used as references
        existing.extend(to_pull)

        to_clone_sorted = []
        clone_order(to_clone, manifest, to_clone_sorted, existing)

        for gitdir in to_clone_sorted:
            e_clone.refresh()
            # Do we still need to clone it, or has another process
            # already done this for us?
            ts = grokmirror.get_repo_timestamp(toplevel, gitdir)

            if ts > 0:
                logger.debug('Looks like %s already cloned, skipping', gitdir)
                continue

            fullpath = os.path.join(toplevel, gitdir.lstrip('/'))

            try:
                grokmirror.lock_repo(fullpath, nonblocking=True)
            except IOError:
                logger.info('Could not lock %s, skipping', gitdir)
                lock_fails.append(gitdir)
                e_clone.update()
                continue

            reference = None
            if config['ignore_repo_references'] != 'yes':
                reference = culled[gitdir]['reference']

            if reference is not None and reference in existing:
                # Make sure we can lock the reference repo
                refrepo = os.path.join(toplevel, reference.lstrip('/'))
                try:
                    grokmirror.lock_repo(refrepo, nonblocking=True)
                    success = clone_repo(toplevel, gitdir, config['site'],
                                         reference=reference)
                    grokmirror.unlock_repo(refrepo)
                except IOError:
                    logger.info('Cannot lock reference repo %s, skipping %s',
                                reference, gitdir)
                    if reference not in lock_fails:
                        lock_fails.append(reference)

                    grokmirror.unlock_repo(fullpath)
                    e_clone.update()
                    continue
            else:
                success = clone_repo(toplevel, gitdir, config['site'])

            # check dir to make sure cloning succeeded and then add to existing
            if os.path.exists(fullpath) and success:
                logger.debug('Cloning of %s succeeded, adding to existing',
                             gitdir)
                existing.append(gitdir)

                desc = culled[gitdir].get('description')
                owner = culled[gitdir].get('owner')
                ref = culled[gitdir].get('reference')

                if owner is None:
                    owner = config['default_owner']
                set_repo_params(toplevel, gitdir, owner, desc, ref)
                set_agefile(toplevel, gitdir, culled[gitdir]['modified'])
                my_fingerprint = grokmirror.set_repo_fingerprint(toplevel,
                                                                 gitdir)
                culled[gitdir]['fingerprint'] = my_fingerprint
                run_post_update_hook(hookscript, toplevel, gitdir)
            else:
                logger.critical('Was not able to clone %s', gitdir)
                # Remove it from our manifest so we can try re-cloning
                # next time grok-pull runs
                del culled[gitdir]
                git_fails.append(gitdir)

            grokmirror.unlock_repo(fullpath)
            e_clone.update()

        logger.info('Clones completed in %0.2fs' % e_clone.elapsed)
        e_clone.close()

    else:
        logger.info('No repositories need cloning')

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

                if os.path.exists(source):
                    if os.path.islink(target):
                        # are you pointing to where we need you?
                        if os.path.realpath(target) != source:
                            # Remove symlink and recreate below
                            logger.debug('Removed existing wrong symlink %s',
                                         target)
                            os.unlink(target)
                    elif os.path.exists(target):
                        logger.warning('Deleted repo %s, because it is now'
                                       ' a symlink to %s' % (target, source))
                        shutil.rmtree(target)

                    # Here we re-check if we still need to do anything
                    if not os.path.exists(target):
                        logger.info('Symlinking %s -> %s', target, source)
                        # Make sure the leading dirs are in place
                        if not os.path.exists(os.path.dirname(target)):
                            os.makedirs(os.path.dirname(target))
                        os.symlink(source, target)

    manifile = config['mymanifest']
    grokmirror.manifest_lock(manifile)

    # Is the local manifest newer than last_modified? That would indicate
    # that another process has run and "culled" is no longer the latest info
    if os.path.exists(manifile):
        fstat = os.stat(manifile)
        if fstat[8] > last_modified:
            logger.info('Local manifest is newer, not saving.')
            grokmirror.manifest_unlock(manifile)
            return 0

    if purge:
        to_purge = []
        found_repos = 0
        for founddir in grokmirror.find_all_gitdirs(config['toplevel']):
            gitdir = founddir.replace(config['toplevel'], '')
            found_repos += 1

            if gitdir not in culled.keys() and gitdir not in symlinks:
                to_purge.append(founddir)

        if len(to_purge):
            # Purge-protection engage
            try:
                purge_limit = int(config['purgeprotect'])
                assert 1 <= purge_limit <= 99
            except (ValueError, AssertionError):
                logger.critical('Warning: "%s" is not valid for purgeprotect.',
                                config['purgeprotect'])
                logger.critical('Please set to a number between 1 and 99.')
                logger.critical('Defaulting to purgeprotect=5.')
                purge_limit = 5

            purge_pc = len(to_purge) * 100 / found_repos
            logger.debug('purgeprotect=%s', purge_limit)
            logger.debug('purge prercentage=%s', purge_pc)

            if not forcepurge and purge_pc >= purge_limit:
                logger.critical('Refusing to purge %s repos (%s%%)',
                                len(to_purge), purge_pc)
                logger.critical('Set purgeprotect to a higher percentage, or'
                                ' override with --force-purge.')
                logger.info('Not saving local manifest')
                return 1
            else:
                # noinspection PyTypeChecker
                e_purge = em.counter(total=len(to_purge), desc='Purging  :', unit='repos', leave=False)
                for founddir in to_purge:
                    e_purge.refresh()
                    if os.path.islink(founddir):
                        logger.info('Removing unreferenced symlink %s', gitdir)
                        os.unlink(founddir)
                    else:
                        # is anything using us for alternates?
                        gitdir = '/' + os.path.relpath(founddir, toplevel).lstrip('/')
                        if grokmirror.is_alt_repo(toplevel, gitdir):
                            logger.info('Not purging %s because it is used by '
                                        'other repos via alternates', founddir)
                        else:
                            try:
                                logger.info('Purging %s', founddir)
                                grokmirror.lock_repo(founddir, nonblocking=True)
                                shutil.rmtree(founddir)
                            except IOError:
                                lock_fails.append(gitdir)
                                logger.info('%s is locked, not purging',
                                            gitdir)
                    e_purge.update()

                logger.info('Purging completed in %0.2fs', e_purge.elapsed)
                e_purge.close()

        else:
            logger.info('No repositories need purging')

    # Done with progress bars
    em.stop()

    # Go through all repos in culled and get the latest local timestamps.
    for gitdir in culled:
        ts = grokmirror.get_repo_timestamp(toplevel, gitdir)
        culled[gitdir]['modified'] = ts

    # If there were any lock failures, we fudge last_modified to always
    # be older than the server, which will force the next grokmirror run.
    if len(lock_fails):
        logger.info('%s repos could not be locked. Forcing next run.',
                    len(lock_fails))
        last_modified -= 1
    elif len(git_fails):
        logger.info('%s repos failed. Forcing next run.', len(git_fails))
        last_modified -= 1

    # Once we're done, save culled as our new manifest
    grokmirror.write_manifest(manifile, culled, mtime=last_modified,
                              pretty=pretty)

    grokmirror.manifest_unlock(manifile)

    # write out projects.list, if asked to
    write_projects_list(culled, config)

    return 127


def parse_args():
    from optparse import OptionParser

    usage = '''usage: %prog -c repos.conf
    Create a grok mirror using the repository configuration found in repos.conf
    '''

    op = OptionParser(usage=usage, version=grokmirror.VERSION)
    op.add_option('-v', '--verbose', dest='verbose', action='store_true',
                  default=False,
                  help='Be verbose and tell us what you are doing')
    op.add_option('-n', '--no-mtime-check', dest='nomtime',
                  action='store_true', default=False,
                  help='Run without checking manifest mtime.')
    op.add_option('-f', '--force', dest='force',
                  action='store_true', default=False,
                  help='Force full git update regardless of last-modified time.'
                       ' Also useful when repos.conf has changed.')
    op.add_option('-p', '--purge', dest='purge',
                  action='store_true', default=False,
                  help='Remove any git trees that are no longer in manifest.')
    op.add_option('', '--force-purge', dest='forcepurge',
                  action='store_true', default=False,
                  help='Force purge despite significant repo deletions.')
    op.add_option('-y', '--pretty', dest='pretty', action='store_true',
                  default=False,
                  help='Pretty-print manifest (sort keys and add indentation)')
    op.add_option('-r', '--no-reuse-existing-repos', dest='noreuse',
                  action='store_true', default=False,
                  help='If any existing repositories are found on disk, do NOT '
                       'update origin and reuse')
    op.add_option('-m', '--verify-mirror', dest='verify',
                  action='store_true', default=False,
                  help='Do not perform any updates, just verify that mirror '
                       'matches upstream manifest.')
    op.add_option('-s', '--verify-subpath', dest='verify_subpath',
                  default='*',
                  help='Only verify a subpath (accepts shell globbing)')
    op.add_option('-c', '--config', dest='config',
                  help='Location of repos.conf')

    opts, args = op.parse_args()

    if not opts.config:
        op.error('You must provide the path to the config file')

    return opts, args


def grok_pull(config, verbose=False, force=False, nomtime=False,
              verify=False, verify_subpath='*', noreuse=False,
              purge=False, pretty=False, forcepurge=False):
    try:
        from configparser import ConfigParser
    except ImportError:
        from ConfigParser import ConfigParser

    ini = ConfigParser()
    ini.read(config)

    retval = 0

    for section in ini.sections():
        # Reset fail trackers for each section
        global lock_fails
        global git_fails

        lock_fails = []
        git_fails = []

        config = {
            'default_owner': 'Grokmirror User',
            'post_update_hook': '',
            'include': '*',
            'exclude': '',
            'ignore_repo_references': 'no',
            'purgeprotect': '5',
        }

        for (option, value) in ini.items(section):
            config[option] = value

        sect_retval = pull_mirror(
            section, config, verbose, force, nomtime, verify, verify_subpath,
            noreuse, purge, pretty, forcepurge)
        if sect_retval == 1:
            # Fatal error encountered at some point
            retval = 1
        elif sect_retval == 127 and retval != 1:
            # Successful run with contents modified
            retval = 127
    return retval


def command():
    opts, args = parse_args()

    retval = grok_pull(
        opts.config, opts.verbose, opts.force, opts.nomtime, opts.verify,
        opts.verify_subpath, opts.noreuse, opts.purge, opts.pretty,
        opts.forcepurge)

    sys.exit(retval)


if __name__ == '__main__':
    command()
