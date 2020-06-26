# -*- coding: utf-8 -*-
# Copyright (C) 2013-2020 by The Linux Foundation and contributors
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
import requests
import time
import gzip
import json
import fnmatch
import subprocess
import shutil
import calendar
import enlighten
import pathlib
import uuid

import multiprocessing as mp

# default basic logger. We override it later.
logger = logging.getLogger(__name__)

# used for tracking pending objstore repo fetches
pending_obstrepos = set()


def queue_worker(config, gitdir, repoinfo, action, obstrepo, is_private):
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    try:
        grokmirror.lock_repo(fullpath, nonblocking=True)
    except IOError:
        # Let the next run deal with this one
        return False, gitdir, action, None, obstrepo, is_private

    logger.info('  Working: %s (%s)', gitdir, action)
    desc = repoinfo.get('description', '')
    owner = repoinfo.get('owner', None)
    if owner is None:
        owner = config['pull'].get('default_owner', 'Grokmirror')

    head = repoinfo.get('head', None)

    next_action = None
    success = True
    orig_action = action
    site = config['remote'].get('site')

    if action == 'fix_remotes':
        success = fix_remotes(toplevel, gitdir, site)
        action = 'fix_params'

    if action == 'fix_params':
        set_repo_params(toplevel, gitdir, owner, desc, head)
        next_action = 'pull'

    if action == 'reclone':
        try:
            shutil.move(fullpath, '%s.reclone' % fullpath)
            shutil.rmtree('%s.reclone' % fullpath)
            next_action = 'init'
        except (PermissionError, IOError) as ex:
            logger.critical('Unable to remove %s: %s', fullpath, str(ex))
            success = False

    if action == 'init':
        args = ['init', '--bare', fullpath]
        ecode, out, err = grokmirror.run_git_command(fullpath, args)
        if ecode > 0:
            # assume another process has beaten us to it
            logger.critical('Unable to bare-init %s', fullpath)
            success = False
        else:
            # Remove .sample files from hooks, because they are just dead weight
            hooksdir = os.path.join(fullpath, 'hooks')
            for child in pathlib.Path(hooksdir).iterdir():
                if child.suffix == '.sample':
                    child.unlink()
            grokmirror.set_git_config(fullpath, 'gc.auto', '0')
            fix_remotes(toplevel, gitdir, site)
            set_repo_params(toplevel, gitdir, owner, desc, head)

            if obstrepo:
                grokmirror.set_altrepo(fullpath, obstrepo)

            next_action = 'pull'

    if action == 'pull':
        r_fp = repoinfo.get('fingerprint')
        my_fp = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=True)

        if r_fp != my_fp:
            success = pull_repo(toplevel, gitdir)
            if success:
                run_post_update_hook(toplevel, gitdir, config['pull'].get('post_update_hook', ''))
                my_fp = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=True)
                if obstrepo and not is_private:
                    next_action = 'objstore'
        else:
            logger.debug('FP match, not pulling %s', gitdir)

        if success:
            set_agefile(toplevel, gitdir, repoinfo.get('modified'))
            if my_fp is not None:
                grokmirror.set_repo_fingerprint(toplevel, gitdir, fingerprint=my_fp)

    if action == 'objstore':
        if obstrepo and not is_private:
            try:
                grokmirror.lock_repo(obstrepo, nonblocking=True)
                grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                grokmirror.unlock_repo(obstrepo)
            except IOError:
                # Locked by external process. Don't block here and let the next run fix this.
                logger.debug('Could not lock %s, not fetching objects into it from %s', obstrepo, fullpath)

    if action == 'repack':
        # Should only trigger after initial clone with objstore repo support, in order
        # to remove a lot of duplicate objects. All other repacking should be done as part of grok-fsck
        logger.debug('quick-repacking %s', fullpath)
        args = ['repack', '-Adlq']
        ecode, out, err = grokmirror.run_git_command(fullpath, args)
        if ecode > 0:
            logger.debug('Could not repack %s', fullpath)
        args = ['pack-refs', '--all']
        ecode, out, err = grokmirror.run_git_command(fullpath, args)
        if ecode > 0:
            logger.debug('Could not pack-refs in %s', fullpath)

    grokmirror.unlock_repo(fullpath)

    return success, gitdir, orig_action, next_action, obstrepo, is_private


def cull_manifest(manifest, config):
    includes = config['pull'].get('include', '*').split('\n')
    excludes = config['pull'].get('exclude', '').split('\n')

    culled = dict()

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


def fix_remotes(toplevel, gitdir, site):
    # Remove all existing remotes and set new origin
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    ecode, out, err = grokmirror.run_git_command(fullpath, ['remote'])
    if len(out):
        for remote in out.split('\n'):
            logger.debug('\tremoving remote: %s', remote)
            ecode, out, err = grokmirror.run_git_command(fullpath, ['remote', 'remove', remote])
            if ecode > 0:
                logger.critical('FATAL: Could not remove remote %s from %s', remote, fullpath)
                return False

    # set my origin
    origin = os.path.join(site, gitdir.lstrip('/'))
    ecode, out, err = grokmirror.run_git_command(fullpath, ['remote', 'add', '--mirror', 'origin', origin])
    if ecode > 0:
        logger.critical('FATAL: Could not set origin to %s in %s', origin, fullpath)
        return False

    logger.debug('\tset new origin as %s', origin)
    return True


def set_repo_params(toplevel, gitdir, owner, description, head):
    if owner is None and description is None and head is None:
        # Let the default git values be there, then
        return

    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    if description is not None:
        descfile = os.path.join(fullpath, 'description')
        contents = None
        if os.path.exists(descfile):
            with open(descfile) as fh:
                contents = fh.read()
        if contents != description:
            logger.debug('Setting %s description to: %s', gitdir, description)
            with open(descfile, 'w') as fh:
                fh.write(description)

    if owner is not None:
        logger.debug('Setting %s owner to: %s', gitdir, owner)
        grokmirror.set_git_config(fullpath, 'gitweb.owner', owner)

    if head is not None:
        headfile = os.path.join(fullpath, 'HEAD')
        contents = None
        if os.path.exists(headfile):
            with open(headfile) as fh:
                contents = fh.read()
        if contents != head:
            logger.debug('Setting %s HEAD to: %s', gitdir, head)
            with open(headfile, 'w') as fh:
                fh.write(head)


def set_agefile(toplevel, gitdir, last_modified):
    grokmirror.set_repo_timestamp(toplevel, gitdir, last_modified)

    # set agefile, which can be used by cgit to show idle times
    # cgit recommends it to be yyyy-mm-dd hh:mm:ss
    cgit_fmt = time.strftime('%F %T', time.localtime(last_modified))
    agefile = os.path.join(toplevel, gitdir.lstrip('/'), 'info/web/last-modified')
    if not os.path.exists(os.path.dirname(agefile)):
        os.makedirs(os.path.dirname(agefile))
    with open(agefile, 'wt') as fh:
        fh.write('%s\n' % cgit_fmt)
    logger.debug('Wrote "%s" into %s', cgit_fmt, agefile)


def run_post_update_hook(toplevel, gitdir, hookscript):
    if not len(hookscript):
        return

    if not os.access(hookscript, os.X_OK):
        logger.warning('post_update_hook %s is not executable', hookscript)
        return

    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    args = [hookscript, fullpath]
    logger.debug('Running: %s', ' '.join(args))
    (output, error) = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE).communicate()

    error = error.decode().strip()
    output = output.decode().strip()
    if error:
        # Put hook stderror into warning
        logger.warning('Hook Stderr (%s): %s', gitdir, error)
    if output:
        # Put hook stdout into info
        logger.info('Hook Stdout (%s): %s', gitdir, output)


def pull_repo(toplevel, gitdir):
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    args = ['remote', 'update', '--prune']

    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    success = False
    if retcode == 0:
        success = True

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
            logger.debug('Stderr (%s): %s', gitdir, '\n'.join(debug))
        if warn:
            logger.warning('Stderr (%s): %s', gitdir, '\n'.join(warn))

    return success


def write_projects_list(manifest, config):
    import tempfile
    import shutil

    plpath = config['pull'].get('projectslist', '')
    if not plpath:
        return

    trimtop = config['pull'].get('projectslist_trimtop', '')
    add_symlinks = config['pull'].get('projectslist_symlinks', False)

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


def find_next_best_actions(toplevel, todo, obstdir, privmasks, forkgroups, mapping, maxactions=60):
    global pending_obstrepos
    candidates = set()
    ignore = set()
    for c_gitdir, c_action in todo:
        if len(candidates) >= maxactions:
            return candidates

        c_fullpath = os.path.join(toplevel, c_gitdir.lstrip('/'))
        if c_fullpath in ignore:
            continue

        is_private = False
        for privmask in privmasks:
            # Does this repo match privrepo
            if fnmatch.fnmatch(c_gitdir, privmask):
                is_private = True
                break

        if c_action != 'init':
            # We don't have to be fancy with this one
            obstrepo = grokmirror.get_altrepo(c_fullpath)
            if obstrepo and obstrepo.find(obstdir) != 0:
                obstrepo = None
            candidates.add((c_gitdir, c_action, obstrepo, is_private))
            continue

        forkgroup = None
        for fg, srs in forkgroups.items():
            if c_fullpath in srs:
                forkgroup = fg
                break

        if forkgroup is None:
            logger.debug('no-sibling clone: %s', c_gitdir)
            candidates.add((c_gitdir, c_action, None, is_private))
            continue

        obstrepo = os.path.join(obstdir, '%s.git' % forkgroup)
        if not os.path.isdir(obstrepo):
            # No siblings matched an existing objstore repo, we are the first
            # But wait, are we a private repo?
            obstrepo = grokmirror.setup_objstore_repo(obstdir, name=forkgroup)
            if is_private:
                # do we have any non-private siblings? If so, clone that repo first.
                found = False
                for s_fullpath in forkgroups[forkgroup]:
                    is_private_sibling = False
                    for privmask in privmasks:
                        # Does this repo match privrepo
                        if fnmatch.fnmatch(s_fullpath, privmask):
                            is_private_sibling = True
                            break
                    if not is_private_sibling:
                        found = True
                        break
                if not found:
                    # clone as a non-sibling repo, then
                    logger.debug('all siblings are private, so clone as individual repo')
                    candidates.add((c_gitdir, 'clone', None, True))
                    continue
                else:
                    # We'll add it when we come to it
                    continue

            try:
                logger.debug('cloning %s with new obstrepo %s', c_gitdir, obstrepo)
                # We want to prevent other siblings from being fetched until
                # we have some objects available
                pending_obstrepos.add(obstrepo)
                logger.debug('locked obstrepo %s', obstrepo)
                mapping[c_fullpath] = obstrepo
                candidates.add((c_gitdir, c_action, obstrepo, is_private))
                if not is_private:
                    grokmirror.add_repo_to_objstore(obstrepo, c_fullpath)
            except IOError:
                # External process is keeping it locked
                logger.debug('cannot clone %s until %s is available', c_gitdir, obstrepo)
                # mark all siblings as ignore
                for s_fullpath in forkgroups[forkgroup]:
                    ignore.add(s_fullpath)

            continue

        if obstrepo in pending_obstrepos:
            logger.debug('cannot clone %s until %s is available (internal)', c_gitdir, obstrepo)
            # mark all siblings as ignore
            for s_fullpath in forkgroups[forkgroup]:
                ignore.add(s_fullpath)
            continue

        try:
            mapping[c_fullpath] = obstrepo
            candidates.add((c_gitdir, c_action, obstrepo, is_private))
            logger.debug('cloning %s with obstrepo %s', c_gitdir, obstrepo)
            if not is_private:
                grokmirror.add_repo_to_objstore(obstrepo, c_fullpath)
            continue
        except IOError:
            # External process is keeping it locked
            logger.debug('cannot clone %s until %s is available (external)', c_gitdir, obstrepo)
            # mark all siblings as ignore
            for s_fullpath in forkgroups[forkgroup]:
                ignore.add(s_fullpath)
            continue

    return candidates


def pull_mirror(config, verbose=False, force=False, nomtime=False,
                verify=False, verify_subpath='*', noreuse=False,
                purge=False, pretty=False, forcepurge=False):
    global logger
    global pending_obstrepos

    # noinspection PyTypeChecker
    em = enlighten.get_manager(series=' -=#')

    logger = logging.getLogger('pull')
    logger.setLevel(logging.DEBUG)

    logfile = config['core'].get('log', None)
    if logfile:
        ch = logging.FileHandler(logfile)
        formatter = logging.Formatter("[%(process)d] %(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        loglevel = logging.INFO

        if config['core'].get('loglevel', 'info') == 'debug':
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

    logger.info('Checking %s', config['remote'].get('site'))
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    if not os.access(toplevel, os.W_OK):
        logger.critical('Toplevel %s does not exist or is not writable', toplevel)
        sys.exit(1)

    obstdir = config['core'].get('objstore', None)
    if obstdir is None:
        obstdir = os.path.join(toplevel, '_alternates')
        config['core']['objstore'] = obstdir
    else:
        obstdir = os.path.realpath(obstdir)

    # l_ = local, r_ = remote
    l_manifest_path = config['core'].get('manifest')
    l_last_modified = 0
    r_last_modified = 0
    if os.path.exists(l_manifest_path):
        fstat = os.stat(l_manifest_path)
        l_last_modified = fstat[8]
        logger.debug('Our last-modified is: %s', l_last_modified)

    if verify:
        nomtime = True

    r_manifest_url = config['remote'].get('manifest')
    if r_manifest_url.find('file:///') == 0:
        r_manifest_url = r_manifest_url.replace('file://', '')
        if not os.path.exists(r_manifest_url):
            logger.critical('Remote manifest not found in %s! Quitting!', r_manifest_url)
            return 1

        fstat = os.stat(r_manifest_url)
        if l_last_modified:
            r_last_modified = fstat[8]
            logger.debug('mtime on %s is: %s', r_manifest_url, fstat[8])
            if not (force or nomtime) and r_last_modified <= l_last_modified:
                logger.info('Manifest file unchanged. Quitting.')
                return 0

        logger.info('Reading new manifest from %s', r_manifest_url)
        r_manifest = grokmirror.read_manifest(r_manifest_url)
        # Don't accept empty manifests -- that indicates something is wrong
        if not len(r_manifest.keys()):
            logger.warning('Remote manifest empty or unparseable! Quitting.')
            return 1

    else:
        # Load it from remote host using http and header magic
        logger.info('Fetching remote manifest from %s', r_manifest_url)

        session = grokmirror.get_requests_session()

        # Find out if we need to run at all first
        headers = dict()
        if l_last_modified and not nomtime:
            last_modified_h = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(l_last_modified))
            logger.debug('Our last-modified is: %s', last_modified_h)
            headers['If-Modified-Since'] = last_modified_h

        try:
            res = session.get(r_manifest_url, headers=headers)
        except requests.exceptions.RequestException as ex:
            logger.warning('Could not fetch %s', r_manifest_url)
            logger.warning('Server returned: %s', ex)
            return 1

        if res.status_code == 304:
            logger.info('Server says we have the latest manifest. Quitting.')
            return 0

        if res.status_code > 200:
            logger.warning('Could not fetch %s', r_manifest_url)
            logger.warning('Server returned status: %s', res.status_code)
            return 1

        r_last_modified = res.headers['Last-Modified']
        r_last_modified = time.strptime(r_last_modified, '%a, %d %b %Y %H:%M:%S %Z')
        r_last_modified = calendar.timegm(r_last_modified)

        # We don't use read_manifest for the remote manifest, as it can be
        # anything, really. For now, blindly open it with gzipfile if it ends
        # with .gz. XXX: some http servers will auto-deflate such files.
        try:
            if r_manifest_url.find('.gz') > 0:
                import io
                fh = gzip.GzipFile(fileobj=io.BytesIO(res.content))
                jdata = fh.read().decode('utf-8')
            else:
                jdata = res.content

            res.close()
            r_manifest = json.loads(jdata)

        except Exception as ex:
            logger.warning('Failed to parse %s', r_manifest_url)
            logger.warning('Error was: %s', ex)
            return 1

    l_manifest = grokmirror.read_manifest(l_manifest_path)
    r_culled = cull_manifest(r_manifest, config)

    if verify:
        logger.info('Verifying mirror against %s', r_manifest_url)
        f_count = 0
        for gitdir in r_culled:
            fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
            if r_culled[gitdir]['fingerprint'] is None:
                logger.debug('No fingerprint for %s, not verifying', gitdir)
                grokmirror.unlock_repo(fullpath)
                continue

            if not fnmatch.fnmatch(gitdir, verify_subpath):
                grokmirror.unlock_repo(fullpath)
                continue

            logger.debug('Verifying %s', gitdir)
            if not os.path.exists(fullpath):
                f_count += 0
                logger.info('Verify: %s ABSENT', gitdir)
                grokmirror.unlock_repo(fullpath)
                continue

            my_fingerprint = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=force)

            if my_fingerprint == r_culled[gitdir]['fingerprint']:
                logger.info('Verify: %s OK', gitdir)
            else:
                logger.critical('Verify: %s FAILED', gitdir)
                f_count += 0

            grokmirror.unlock_repo(fullpath)

        if f_count > 0:
            logger.critical('%s repos failed to verify', f_count)
            return 1
        else:
            logger.info('Verification successful')
            return 0

    pull_threads = config['pull'].getint('pull_threads', 0)
    if pull_threads < 1:
        # take half of available CPUs by default
        logger.info('pull_threads is not set, consider setting it')
        pull_threads = int(mp.cpu_count() / 2)

    # noinspection PyTypeChecker
    e_cmp = em.counter(total=len(r_culled), desc='Comparing', unit='repos', leave=False)

    # First run through to identify repositories that may need work
    todo = set()
    r_forkgroups = dict()
    for gitdir in set(r_culled.keys()):
        e_cmp.update()
        if r_culled[gitdir]['fingerprint'] is None:
            logger.critical('Manifest files without fingeprints no longer supported.')
            continue

        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        # our forkgroup info wins, because our own grok-fcsk may have found better siblings
        # unless we're cloning, in which case we have nothing to go by except remote info
        if gitdir in l_manifest:
            reference = l_manifest[gitdir].get('reference', None)
            forkgroup = l_manifest[gitdir].get('forkgroup', None)
            if reference is not None:
                r_culled[gitdir]['reference'] = reference
            if forkgroup is not None:
                r_culled[gitdir]['forkgroup'] = forkgroup
        else:
            reference = r_culled[gitdir].get('reference', None)
            forkgroup = r_culled[gitdir].get('forkgroup', None)

        if reference and not forkgroup:
            # probably a grokmirror-1.x manifest
            r_fullpath = os.path.join(toplevel, reference.lstrip('/'))
            for fg, fps in r_forkgroups.items():
                if r_fullpath in fps:
                    forkgroup = fg
                    break
            if not forkgroup:
                # I guess we get to make a new one!
                forkgroup = str(uuid.uuid4())
                r_forkgroups[forkgroup] = {r_fullpath}

        if forkgroup is not None:
            if forkgroup not in r_forkgroups:
                r_forkgroups[forkgroup] = set()
            r_forkgroups[forkgroup].add(fullpath)

        # Is the directory in place?
        if os.path.exists(fullpath):
            # Did grok-fsck request to reclone it?
            rfile = os.path.join(fullpath, 'grokmirror.reclone')
            if os.path.exists(rfile):
                logger.info('Reclone requested for %s:', gitdir)
                todo.add((gitdir, 'reclone'))
                with open(rfile, 'r') as rfh:
                    reason = rfh.read()
                    logger.info('  %s', reason)

            if gitdir not in l_manifest:
                if noreuse:
                    logger.critical('Found existing git repo in %s', fullpath)
                    logger.critical('But you asked NOT to reuse repos')
                    logger.critical('Skipping %s', gitdir)
                    continue
                todo.add((gitdir, 'fix_remotes'))
                continue

            # Fix owner and description, if necessary
            # This code is hurky and needs to be cleaned up
            r_desc = r_culled[gitdir].get('description', None)
            r_owner = r_culled[gitdir].get('owner', None)
            r_head = r_culled[gitdir].get('head', None)

            l_desc = l_manifest[gitdir].get('description', None)
            l_owner = l_manifest[gitdir].get('owner', None)
            l_head = l_manifest[gitdir].get('head', None)

            if l_owner is None:
                l_owner = config['pull'].get('default_owner', 'Grokmirror')
            if r_owner is None:
                r_owner = config['pull'].get('default_owner', 'Grokmirror')

            if r_desc != l_desc or r_owner != l_owner or r_head != l_head:
                todo.add((gitdir, 'fix_params'))

            my_fingerprint = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=force)

            if my_fingerprint == r_culled[gitdir]['fingerprint']:
                logger.debug('Fingerprints match, skipping %s', gitdir)
                continue

            logger.debug('No fingerprint match, will pull %s', gitdir)
            todo.add((gitdir, 'pull'))
            continue

        todo.add((gitdir, 'init'))

    e_cmp.close()
    # Compare their forkgroups and my forkgroups in case we have a more optimal strategy
    l_forkgroups = grokmirror.get_forkgroups(obstdir, toplevel)
    for r_fg, r_siblings in r_forkgroups.items():
        # if we have an intersection between their forkgroups and our forkgroups, then we use ours
        found = False
        for l_fg, l_siblings in l_forkgroups.items():
            if l_siblings == r_siblings:
                # No changes there
                continue
            if len(l_siblings.intersection(r_siblings)):
                l_siblings.update(r_siblings)
                found = True
                break
        if not found:
            # We don't have any matches in existing repos, so make a new forkgroup
            l_forkgroups[r_fg] = r_siblings
    forkgroups = l_forkgroups

    failures = 0
    if len(todo):
        logger.info('Found updates to %s repositories', len(todo))
        # Run through the repos that need work and attempt to lock them.
        # If too many locks fail, we'll know that another process is already working
        # on them and we should quit.
        for gitdir, action in todo:
            fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
            try:
                grokmirror.lock_repo(fullpath, nonblocking=True)
                grokmirror.unlock_repo(fullpath)
            except IOError:
                logger.info('Could not lock %s, skipping', gitdir)
                pull_threads -= 1
                # Force the fingerprint to what we have in l_manifest, if we have it.
                r_culled[gitdir]['fingerprint'] = None
                if gitdir in l_manifest and 'fingerprint' in l_manifest[gitdir]:
                    r_culled[gitdir]['fingerprint'] = l_manifest[gitdir]['fingerprint']
                if pull_threads <= 0:
                    # Blunt exit without any writes to the manifest
                    logger.info('Too many repositories locked. Exiting.')
                    em.stop()
                    return 0
                continue

        if len(todo) < pull_threads:
            pull_threads = len(todo)

        barfmt = ('{desc}{desc_pad}{percentage_1:3.0f}%|{bar}| {count_1:{len_total}d}/{total:d} '
                  '[{elapsed}<{eta_1}, {rate_1:.2f}{unit_pad}{unit}/s]')
        # noinspection PyTypeChecker
        e_que = em.counter(total=len(todo), desc='Working', unit='repos',
                           color='yellow', bar_format=barfmt, leave=False)
        e_fin = e_que.add_subcounter(color='white', all_fields=True)
        logger.info('Starting work pool with %s workers', pull_threads)
        obstdir = os.path.realpath(config['core'].get('objstore'))
        privmasks = config['core'].get('private', '').split('\n')
        mapping = grokmirror.get_obstrepo_mapping(obstdir)
        freshclones = set()
        with mp.Pool(pull_threads) as wpool:
            results = list()
            while len(results) or len(todo):
                if len(todo) and len(results) < 60:
                    candidates = find_next_best_actions(toplevel, todo, obstdir, privmasks, forkgroups,
                                                        mapping, maxactions=60-len(results))
                    for gitrepo, action, refrepo, is_private in candidates:
                        if action == 'init':
                            freshclones.add(gitrepo)
                        todo.remove((gitrepo, action))
                        logger.info('   Queued: %s', gitrepo)
                        e_que.update()
                        res = wpool.apply_async(queue_worker, (config, gitrepo, r_culled[gitrepo],
                                                               action, refrepo, is_private))
                        results.append(res)
                e_que.refresh()

                try:
                    res = results.pop(0)
                    success, gitrepo, action, next_action, obstrepo, is_private = res.get(timeout=0.1)
                    logger.debug('result: repo=%s, action=%s, next=%s', gitrepo, action, next_action)
                    if not success:
                        logger.info('   Failed: %s', gitrepo)
                        failures += 1
                        # To make sure we check this again during next run,
                        # fudge the manifest accordingly.
                        if gitrepo in l_manifest:
                            r_culled[gitrepo] = l_manifest[gitrepo]
                        # this is rather hackish, but effective
                        r_last_modified -= 1
                        if obstrepo and obstrepo in pending_obstrepos:
                            pending_obstrepos.remove(obstrepo)
                            logger.debug('marked available obstrepo %s', obstrepo)
                        continue

                    if action == 'objstore' and gitrepo in freshclones:
                        freshclones.remove(gitrepo)
                        next_action = 'repack'
                        if obstrepo and obstrepo in pending_obstrepos:
                            pending_obstrepos.remove(obstrepo)
                            logger.debug('marked available obstrepo %s', obstrepo)

                    if next_action is None:
                        e_fin.update_from(e_que)
                        e_que.refresh()
                        if success:
                            logger.info(' Finished: %s', gitrepo)

                    if next_action is not None:
                        res = wpool.apply_async(queue_worker, (config, gitrepo, r_culled[gitrepo],
                                                               next_action, obstrepo, is_private))
                        results.append(res)

                except mp.TimeoutError:
                    results.append(res)
                    pass

        e_que.close()

    # loop through all entries and find any symlinks we need to set
    # We also collect all symlinks to do purging correctly
    symlinks = set()
    for gitdir in r_culled.keys():
        if 'symlinks' in r_culled[gitdir].keys():
            source = os.path.join(toplevel, gitdir.lstrip('/'))
            for symlink in r_culled[gitdir]['symlinks']:
                symlinks.add(symlink)
                target = os.path.join(toplevel, symlink.lstrip('/'))

                if os.path.exists(source):
                    if os.path.islink(target):
                        # are you pointing to where we need you?
                        if os.path.realpath(target) != source:
                            # Remove symlink and recreate below
                            logger.debug('Removed existing wrong symlink %s', target)
                            os.unlink(target)
                    elif os.path.exists(target):
                        logger.warning('Deleted repo %s, because it is now a symlink to %s' % (target, source))
                        shutil.rmtree(target)

                    # Here we re-check if we still need to do anything
                    if not os.path.exists(target):
                        logger.info('Symlinking %s -> %s', target, source)
                        # Make sure the leading dirs are in place
                        if not os.path.exists(os.path.dirname(target)):
                            os.makedirs(os.path.dirname(target))
                        os.symlink(source, target)

    grokmirror.manifest_lock(l_manifest_path)

    # Is the local manifest newer than r_last_modified? That would indicate
    # that another process has run and "r_culled" is no longer the latest info
    if os.path.exists(l_manifest_path):
        fstat = os.stat(l_manifest_path)
        if fstat[8] > r_last_modified:
            logger.info('Local manifest is newer, not saving.')
            grokmirror.manifest_unlock(l_manifest_path)
            em.stop()
            return 0

    if purge:
        to_purge = set()
        found_repos = 0
        for founddir in grokmirror.find_all_gitdirs(toplevel, ignore='%s/*' % obstdir):
            gitdir = founddir.replace(toplevel, '')
            found_repos += 1

            if gitdir not in r_culled and gitdir not in symlinks:
                to_purge.add(founddir)

        if len(to_purge):
            # Purge-protection engage
            purge_limit = int(config['pull'].getint('purgeprotect', 5))
            if purge_limit < 1 or purge_limit > 99:
                logger.critical('Warning: "%s" is not valid for purgeprotect.', purge_limit)
                logger.critical('Please set to a number between 1 and 99.')
                logger.critical('Defaulting to purgeprotect=5.')
                purge_limit = 5

            purge_pc = int(len(to_purge) * 100 / found_repos)
            logger.debug('purgeprotect=%s', purge_limit)
            logger.debug('purge prercentage=%s', purge_pc)

            if not forcepurge and purge_pc >= purge_limit:
                logger.critical('Refusing to purge %s repos (%s%%)', len(to_purge), purge_pc)
                logger.critical('Set purgeprotect to a higher percentage, or override with --force-purge.')
                logger.info('Not saving local manifest')
                return 1
            else:
                # noinspection PyTypeChecker
                e_purge = em.counter(total=len(r_culled), desc='Purging', unit='repos', leave=False)
                for founddir in to_purge:
                    e_purge.refresh()
                    if os.path.islink(founddir):
                        logger.info('Removing unreferenced symlink %s', founddir)
                        os.unlink(founddir)
                    else:
                        # is anything using us for alternates?
                        gitdir = '/' + os.path.relpath(founddir, toplevel).lstrip('/')
                        if grokmirror.is_alt_repo(toplevel, gitdir):
                            logger.info('Not purging %s because it is used by other repos via alternates', founddir)
                        else:
                            try:
                                logger.info('Purging %s', founddir)
                                grokmirror.lock_repo(founddir, nonblocking=True)
                                shutil.rmtree(founddir)
                            except IOError:
                                failures += 1
                                logger.info('%s is locked, not purging', gitdir)
                    e_purge.update()

                logger.info('Purging completed in %0.2fs', e_purge.elapsed)
                e_purge.close()

        else:
            logger.info('No repositories need purging')

    # Done with progress bars
    em.stop()

    # Go through all repos in r_culled and get the latest local timestamps.
    for gitdir in r_culled:
        ts = grokmirror.get_repo_timestamp(toplevel, gitdir)
        r_culled[gitdir]['modified'] = ts

    # If there were any lock failures, we fudge last_modified to always
    # be older than the server, which will force the next grokmirror run.
    if failures:
        logger.info('%s repo updates failed. Forcing next run.', failures)
        r_last_modified -= 1

    # Once we're done, save r_culled as our new manifest
    grokmirror.write_manifest(l_manifest_path, r_culled, mtime=r_last_modified, pretty=pretty)
    grokmirror.manifest_unlock(l_manifest_path)

    # write out projects.list, if asked to
    write_projects_list(r_culled, config)

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
                  help='Location of the configuration file')

    opts, args = op.parse_args()

    if not opts.config:
        op.error('You must provide the path to the config file')

    return opts, args


def grok_pull(cfgfile, verbose=False, force=False, nomtime=False,
              verify=False, verify_subpath='*', noreuse=False,
              purge=False, pretty=False, forcepurge=False):

    config = grokmirror.load_config_file(cfgfile)

    return pull_mirror(config, verbose, force, nomtime, verify, verify_subpath,
                       noreuse, purge, pretty, forcepurge)


def command():
    opts, args = parse_args()

    retval = grok_pull(
        opts.config, opts.verbose, opts.force, opts.nomtime, opts.verify,
        opts.verify_subpath, opts.noreuse, opts.purge, opts.pretty,
        opts.forcepurge)

    sys.exit(retval)


if __name__ == '__main__':
    command()
