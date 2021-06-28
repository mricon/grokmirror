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
import stat
import sys

import grokmirror
import logging
import requests
import time
import gzip
import json
import fnmatch
import shutil
import tempfile
import signal
import shlex

import calendar
import uuid

import multiprocessing as mp
import queue

from socketserver import UnixStreamServer, StreamRequestHandler, ThreadingMixIn

# default basic logger. We override it later.
logger = logging.getLogger(__name__)


class SignalHandler:

    def __init__(self, config, sw, dws, pws, done):
        self.config = config
        self.sw = sw
        self.dws = dws
        self.pws = pws
        self.done = done
        self.killed = False

    def _handler(self, signum, frame):
        self.killed = True
        logger.debug('Received signum=%s, frame=%s', signum, frame)
        # if self.sw:
        #    self.sw.terminate()
        #    self.sw.join()

        # for dw in self.dws:
        #    if dw and dw.is_alive():
        #        dw.terminate()
        #        dw.join()

        # for pw in self.pws:
        #    if pw and pw.is_alive():
        #        pw.terminate()
        #        pw.join()

        if len(self.done):
            update_manifest(self.config, self.done)

        logger.info('Exiting on signal %s', signum)
        sys.exit(0)

    def __enter__(self):
        self.old_sigint = signal.signal(signal.SIGINT, self._handler)
        self.old_sigterm = signal.signal(signal.SIGTERM, self._handler)

    def __exit__(self, sigtype, value, traceback):
        if self.killed:
            sys.exit(0)
        signal.signal(signal.SIGINT, self.old_sigint)
        signal.signal(signal.SIGTERM, self.old_sigterm)


class Handler(StreamRequestHandler):

    def handle(self):
        config = self.server.config
        manifile = config['core'].get('manifest')
        while True:
            # noinspection PyBroadException
            try:
                gitdir = self.rfile.readline().strip().decode()
                # Do we know anything about this path?
                manifest = grokmirror.read_manifest(manifile)
                if gitdir in manifest:
                    logger.info(' listener: %s', gitdir)
                    repoinfo = manifest[gitdir]
                    # Set fingerprint to None to force a run
                    repoinfo['fingerprint'] = None
                    repoinfo['modified'] = int(time.time())
                    self.server.q_mani.put((gitdir, repoinfo, 'pull'))
                elif gitdir:
                    logger.info(' listener: %s (not known, ignored)', gitdir)
                    return
                else:
                    return
            except:
                return


class ThreadedUnixStreamServer(ThreadingMixIn, UnixStreamServer):
    pass


def build_optimal_forkgroups(l_manifest, r_manifest, toplevel, obstdir):
    r_forkgroups = dict()
    for gitdir in set(r_manifest.keys()):
        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        # our forkgroup info wins, because our own grok-fcsk may have found better siblings
        # unless we're cloning, in which case we have nothing to go by except remote info
        if gitdir in l_manifest:
            reference = l_manifest[gitdir].get('reference', None)
            forkgroup = l_manifest[gitdir].get('forkgroup', None)
            if reference is not None:
                r_manifest[gitdir]['reference'] = reference
            if forkgroup is not None:
                r_manifest[gitdir]['forkgroup'] = forkgroup
        else:
            reference = r_manifest[gitdir].get('reference', None)
            forkgroup = r_manifest[gitdir].get('forkgroup', None)

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

    # Compare their forkgroups and my forkgroups in case we have a more optimal strategy
    forkgroups = grokmirror.get_forkgroups(obstdir, toplevel)
    for r_fg, r_siblings in r_forkgroups.items():
        # if we have an intersection between their forkgroups and our forkgroups, then we use ours
        found = False
        for l_fg, l_siblings in forkgroups.items():
            if l_siblings == r_siblings:
                # No changes there
                continue
            if len(l_siblings.intersection(r_siblings)):
                l_siblings.update(r_siblings)
                found = True
                break
        if not found:
            # We don't have any matches in existing repos, so make a new forkgroup
            forkgroups[r_fg] = r_siblings

    return forkgroups


def spa_worker(config, q_spa, pauseonload):
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    cpus = mp.cpu_count()
    saidpaused = False
    while True:
        if pauseonload:
            load = os.getloadavg()
            if load[0] > cpus:
                if not saidpaused:
                    logger.info('      spa: paused (system load), %s waiting', q_spa.qsize())
                    saidpaused = True
                time.sleep(5)
                continue
            saidpaused = False

        try:
            (gitdir, actions) = q_spa.get(timeout=1)
        except queue.Empty:
            sys.exit(0)

        logger.debug('spa_worker: gitdir=%s, actions=%s', gitdir, actions)
        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        try:
            grokmirror.lock_repo(fullpath, nonblocking=True)
        except IOError:
            # We'll get it during grok-fsck
            continue

        if not q_spa.empty():
            logger.info('      spa: 1 active, %s waiting', q_spa.qsize())
        else:
            logger.info('      spa: 1 active')

        done = list()
        for action in actions:
            if action in done:
                continue
            done.append(action)
            if action == 'objstore':
                altrepo = grokmirror.get_altrepo(fullpath)
                # Should we use plumbing for this?
                use_plumbing = config['core'].getboolean('objstore_uses_plumbing', False)
                grokmirror.fetch_objstore_repo(altrepo, fullpath, use_plumbing=use_plumbing)

            elif action == 'repack':
                logger.debug('quick-repacking %s', fullpath)
                args = ['repack', '-Adlq']
                if 'fsck' in config:
                    extraflags = config['fsck'].get('extra_repack_flags', '').split()
                    if len(extraflags):
                        args += extraflags
                ecode, out, err = grokmirror.run_git_command(fullpath, args)
                if ecode > 0:
                    logger.debug('Could not repack %s', fullpath)

            elif action == 'packrefs':
                args = ['pack-refs']
                ecode, out, err = grokmirror.run_git_command(fullpath, args)
                if ecode > 0:
                    logger.debug('Could not pack-refs %s', fullpath)

            elif action == 'packrefs-all':
                args = ['pack-refs', '--all']
                ecode, out, err = grokmirror.run_git_command(fullpath, args)
                if ecode > 0:
                    logger.debug('Could not pack-refs %s', fullpath)

        grokmirror.unlock_repo(fullpath)
        logger.info('      spa: %s (done: %s)', gitdir, ', '.join(done))


def objstore_repo_preload(config, obstrepo):
    purl = config['remote'].get('preload_bundle_url')
    if not purl:
        return
    bname = os.path.basename(obstrepo)[:-4]
    obstdir = os.path.realpath(config['core'].get('objstore'))
    burl = '%s/%s.bundle' % (purl.rstrip('/'), bname)
    bfile = os.path.join(obstdir, '%s.bundle' % bname)
    try:
        sess = grokmirror.get_requests_session()
        resp = sess.get(burl, stream=True)
        resp.raise_for_status()
        logger.info(' objstore: downloading %s.bundle', bname)
        with open(bfile, 'wb') as fh:
            for chunk in resp.iter_content(chunk_size=8192):
                fh.write(chunk)
        resp.close()
    except: # noqa
        # Make sure we don't leave .bundle files lying around
        # Should we add logic to resume downloads here in the future?
        if os.path.exists(bfile):
            os.unlink(bfile)
        return

    # Now we clone from it into the objstore repo
    ecode, out, err = grokmirror.run_git_command(obstrepo, ['remote', 'add', '--mirror=fetch', '_preload', bfile])
    if ecode == 0:
        logger.info(' objstore: preloading %s.bundle', bname)
        args = ['remote', 'update', '_preload']
        ecode, out, err = grokmirror.run_git_command(obstrepo, args)
        if ecode > 0:
            logger.info(' objstore: failed to preload from %s.bundle', bname)
        else:
            # now pack refs and generate a commit graph
            grokmirror.run_git_command(obstrepo, ['pack-refs', '--all'])
            if grokmirror.git_newer_than('2.18.0'):
                grokmirror.run_git_command(obstrepo, ['commit-graph', 'write'])
            logger.info(' objstore: successful preload from %s.bundle', bname)
    # Regardless of what happened, we remove _preload and the bundle, then move on
    grokmirror.run_git_command(obstrepo, ['remote', 'rm', '_preload'])
    os.unlink(bfile)


def pull_worker(config, q_pull, q_spa, q_done):
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    obstdir = os.path.realpath(config['core'].get('objstore'))
    maxretries = config['pull'].getint('retries', 3)
    site = config['remote'].get('site')
    remotename = config['pull'].get('remotename', '_grokmirror')
    # Should we use plumbing for objstore operations?
    objstore_uses_plumbing = config['core'].getboolean('objstore_uses_plumbing', False)

    while True:
        try:
            (gitdir, repoinfo, action, q_action) = q_pull.get(timeout=1)
        except queue.Empty:
            sys.exit(0)

        logger.debug('pull_worker: gitdir=%s, action=%s', gitdir, action)
        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        success = True
        spa_actions = list()

        try:
            grokmirror.lock_repo(fullpath, nonblocking=True)
        except IOError:
            # Take a quick nap and put it back into queue
            logger.info('    defer: %s (locked)', gitdir)
            time.sleep(5)
            q_pull.put((gitdir, repoinfo, action, q_action))
            continue

        altrepo = grokmirror.get_altrepo(fullpath)
        obstrepo = None
        if altrepo and grokmirror.is_obstrepo(altrepo, obstdir):
            obstrepo = altrepo

        if action == 'purge':
            # Is it a symlink?
            if os.path.islink(fullpath):
                logger.info('    purge: %s', gitdir)
                os.unlink(fullpath)
            else:
                # is anything using us for alternates?
                if grokmirror.is_alt_repo(toplevel, gitdir):
                    logger.debug('Not purging %s because it is used by other repos via alternates', fullpath)
                else:
                    logger.info('    purge: %s', gitdir)
                    shutil.rmtree(fullpath)

        if action == 'fix_params':
            logger.info(' reconfig: %s', gitdir)
            set_repo_params(fullpath, repoinfo)

        if action == 'fix_remotes':
            logger.info(' reorigin: %s', gitdir)
            success = fix_remotes(toplevel, gitdir, site, config)
            if success:
                set_repo_params(fullpath, repoinfo)
                action = 'pull'
            else:
                success = False

        if action == 'reclone':
            logger.info('  reclone: %s', gitdir)
            try:
                altrepo = grokmirror.get_altrepo(fullpath)
                shutil.move(fullpath, '%s.reclone' % fullpath)
                shutil.rmtree('%s.reclone' % fullpath)
                grokmirror.setup_bare_repo(fullpath)
                fix_remotes(toplevel, gitdir, site, config)
                set_repo_params(fullpath, repoinfo)
                if altrepo:
                    grokmirror.set_altrepo(fullpath, altrepo)
                action = 'pull'
            except (PermissionError, IOError) as ex:
                logger.critical('Unable to remove %s: %s', fullpath, str(ex))
                success = False

        if action in ('pull', 'objstore_migrate'):
            r_fp = repoinfo.get('fingerprint')
            my_fp = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=True)
            if obstrepo:
                o_obj_info = grokmirror.get_repo_obj_info(obstrepo)
                if o_obj_info.get('count') == '0' and o_obj_info.get('in-pack') == '0' and not my_fp:
                    # Try to preload the objstore repo directly
                    objstore_repo_preload(config, obstrepo)

            if r_fp != my_fp:
                # Make sure we have the remote set up
                if action == 'pull' and remotename not in grokmirror.list_repo_remotes(fullpath):
                    logger.info(' reorigin: %s', gitdir)
                    fix_remotes(toplevel, gitdir, site, config)
                logger.info('    fetch: %s', gitdir)
                retries = 1
                while True:
                    success = pull_repo(fullpath, remotename)
                    if success:
                        break
                    retries += 1
                    if retries > maxretries:
                        break
                    logger.info('  refetch: %s (try #%s)', gitdir, retries)

                if success:
                    run_post_update_hook(toplevel, gitdir, config['pull'].get('post_update_hook', ''))
                    post_pull_fp = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=True)
                    repoinfo['fingerprint'] = post_pull_fp
                    altrepo = grokmirror.get_altrepo(fullpath)
                    if post_pull_fp != my_fp:
                        grokmirror.set_repo_fingerprint(toplevel, gitdir, fingerprint=post_pull_fp)
                        if altrepo and grokmirror.is_obstrepo(altrepo, obstdir) and not repoinfo.get('private'):
                            # do we have any objects in the objstore repo?
                            o_obj_info = grokmirror.get_repo_obj_info(altrepo)
                            if o_obj_info.get('count') == '0' and o_obj_info.get('in-pack') == '0':
                                # We fetch right now, as other repos may be waiting on these objects
                                logger.info(' objstore: %s', gitdir)
                                grokmirror.fetch_objstore_repo(altrepo, fullpath, use_plumbing=objstore_uses_plumbing)
                                if not objstore_uses_plumbing:
                                    spa_actions.append('repack')
                            else:
                                # We lazy-fetch in the spa
                                spa_actions.append('objstore')
                                if my_fp is None and not objstore_uses_plumbing:
                                    # Initial clone, trigger a repack after objstore
                                    spa_actions.append('repack')

                        if my_fp is None:
                            # This was the initial clone, so pack all refs
                            spa_actions.append('packrefs-all')

                        if not grokmirror.is_precious(fullpath):
                            # See if doing a quick repack would be beneficial
                            obj_info = grokmirror.get_repo_obj_info(fullpath)
                            if grokmirror.get_repack_level(obj_info):
                                # We only do quick repacks, so we don't care about precise level
                                spa_actions.append('repack')
                                spa_actions.append('packrefs')

                    modified = repoinfo.get('modified')
                    if modified is not None:
                        set_agefile(toplevel, gitdir, modified)
            else:
                logger.debug('FP match, not pulling %s', gitdir)

        if action == 'objstore_migrate':
            spa_actions.append('objstore')
            spa_actions.append('repack')

        grokmirror.unlock_repo(fullpath)

        symlinks = repoinfo.get('symlinks')
        if os.path.exists(fullpath) and symlinks:
            for symlink in symlinks:
                target = os.path.join(toplevel, symlink.lstrip('/'))

                if os.path.islink(target):
                    # are you pointing to where we need you?
                    if os.path.realpath(target) != fullpath:
                        # Remove symlink and recreate below
                        logger.debug('Removed existing wrong symlink %s', target)
                        os.unlink(target)
                elif os.path.exists(target):
                    logger.warning('Deleted repo %s, because it is now a symlink to %s' % (target, fullpath))
                    shutil.rmtree(target)

                # Here we re-check if we still need to do anything
                if not os.path.exists(target):
                    logger.info('  symlink: %s -> %s', symlink, gitdir)
                    # Make sure the leading dirs are in place
                    if not os.path.exists(os.path.dirname(target)):
                        os.makedirs(os.path.dirname(target))
                    os.symlink(fullpath, target)

        q_done.put((gitdir, repoinfo, q_action, success))
        if spa_actions:
            q_spa.put((gitdir, spa_actions))


def cull_manifest(manifest, config):
    includes = config['pull'].get('include', '*').split('\n')
    excludes = config['pull'].get('exclude', '').split('\n')

    culled = dict()

    for gitdir, repoinfo in manifest.items():
        if not repoinfo.get('fingerprint'):
            logger.critical('Repo without fingerprint info (skipped): %s', gitdir)
            continue
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


def fix_remotes(toplevel, gitdir, site, config):
    remotename = config['pull'].get('remotename', '_grokmirror')
    fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
    # Set our remote
    if remotename in grokmirror.list_repo_remotes(fullpath):
        logger.debug('\tremoving remote: %s', remotename)
        ecode, out, err = grokmirror.run_git_command(fullpath, ['remote', 'remove', remotename])
        if ecode > 0:
            logger.critical('FATAL: Could not remove remote %s from %s', remotename, fullpath)
            return False

    # set my remote URL
    url = os.path.join(site, gitdir.lstrip('/'))
    ecode, out, err = grokmirror.run_git_command(fullpath, ['remote', 'add', '--mirror=fetch', remotename, url])
    if ecode > 0:
        logger.critical('FATAL: Could not set %s to %s in %s', remotename, url, fullpath)
        return False

    ffonly = False
    for globpatt in set([x.strip() for x in config['pull'].get('ffonly', '').split('\n')]):
        if fnmatch.fnmatch(gitdir, globpatt):
            ffonly = True
            break
    if ffonly:
        grokmirror.set_git_config(fullpath, 'remote.{}.fetch'.format(remotename), 'refs/*:refs/*')
        logger.debug('\tset %s as %s (ff-only)', remotename, url)
    else:
        logger.debug('\tset %s as %s', remotename, url)
    return True


def set_repo_params(fullpath, repoinfo):
    owner = repoinfo.get('owner')
    description = repoinfo.get('description')
    head = repoinfo.get('head')
    if owner is None and description is None and head is None:
        # Let the default git values be there, then
        return

    if description is not None:
        descfile = os.path.join(fullpath, 'description')
        contents = None
        if os.path.exists(descfile):
            with open(descfile) as fh:
                contents = fh.read()
        if contents != description:
            logger.debug('Setting %s description to: %s', fullpath, description)
            with open(descfile, 'w') as fh:
                fh.write(description)

    if owner is not None:
        logger.debug('Setting %s owner to: %s', fullpath, owner)
        grokmirror.set_git_config(fullpath, 'gitweb.owner', owner)

    if head is not None:
        headfile = os.path.join(fullpath, 'HEAD')
        contents = None
        if os.path.exists(headfile):
            with open(headfile) as fh:
                contents = fh.read().rstrip()
        if contents != head:
            logger.debug('Setting %s HEAD to: %s', fullpath, head)
            with open(headfile, 'w') as fh:
                fh.write('{}\n'.format(head))


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


def run_post_clone_complete_hook(config, clones):
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    stdin = '\n'.join(clones).encode() + b'\n'
    hookscripts = config['pull'].get('post_clone_complete_hook', '')
    for hookscript in hookscripts.split('\n'):
        hookscript = os.path.expanduser(hookscript.strip())
        sp = shlex.shlex(hookscript, posix=True)
        sp.whitespace_split = True
        args = list(sp)
        if not os.access(args[0], os.X_OK):
            logger.warning('post_update_hook %s is not executable', hookscript)
            continue
        logger.info(' inithook: %s', ' '.join(args))
        logger.debug('Running: %s', ' '.join(args))
        args.append(toplevel)
        ecode, output, error = grokmirror.run_shell_command(args, stdin=stdin)
        if error:
            # Put hook stderror into warning
            logger.warning('Hook Stderr: %s', error)
        if output:
            # Put hook stdout into info
            logger.info('Hook Stdout: %s', output)


def run_post_update_hook(toplevel, gitdir, hookscripts):
    if not len(hookscripts):
        return

    for hookscript in hookscripts.split('\n'):
        hookscript = os.path.expanduser(hookscript.strip())
        sp = shlex.shlex(hookscript, posix=True)
        sp.whitespace_split = True
        args = list(sp)

        logger.info('     hook: %s', ' '.join(args))
        if not os.access(args[0], os.X_OK):
            logger.warning('post_update_hook %s is not executable', hookscript)
            continue

        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        args.append(fullpath)
        logger.debug('Running: %s', ' '.join(args))
        ecode, output, error = grokmirror.run_shell_command(args)

        if error:
            # Put hook stderror into warning
            logger.warning('Hook Stderr (%s): %s', gitdir, error)
        if output:
            # Put hook stdout into info
            logger.info('Hook Stdout (%s): %s', gitdir, output)


def pull_repo(fullpath, remotename):
    args = ['remote', 'update', remotename, '--prune']

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
            elif line.find('remote: warning:') == 0:
                debug.append(line)
            elif line.find('ControlSocket') >= 0:
                debug.append(line)
            elif not success:
                warn.append(line)
            else:
                debug.append(line)
        if debug:
            logger.debug('Stderr (%s): %s', fullpath, '\n'.join(debug))
        if warn:
            logger.warning('Stderr (%s): %s', fullpath, '\n'.join(warn))

    return success


def write_projects_list(config, manifest):
    plpath = config['pull'].get('projectslist', '')
    if not plpath:
        return

    trimtop = config['pull'].get('projectslist_trimtop', '')
    add_symlinks = config['pull'].getboolean('projectslist_symlinks', False)

    (dirname, basename) = os.path.split(plpath)
    (fd, tmpfile) = tempfile.mkstemp(prefix=basename, dir=dirname)

    try:
        fh = os.fdopen(fd, 'wb', 0)
        for gitdir in manifest:
            if trimtop and gitdir.startswith(trimtop):
                pgitdir = gitdir[len(trimtop):]
            else:
                pgitdir = gitdir

            # Always remove leading slash, otherwise cgit breaks
            pgitdir = pgitdir.lstrip('/')
            fh.write('{}\n'.format(pgitdir).encode())

            if add_symlinks and 'symlinks' in manifest[gitdir]:
                # Do the same for symlinks
                # XXX: Should make this configurable, perhaps
                for symlink in manifest[gitdir]['symlinks']:
                    if trimtop and symlink.startswith(trimtop):
                        symlink = symlink[len(trimtop):]

                    symlink = symlink.lstrip('/')
                    fh.write('{}\n'.format(symlink).encode())

        os.fsync(fd)
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

    logger.info(' projlist: wrote %s', plpath)


def fill_todo_from_manifest(config, q_mani, nomtime=False, forcepurge=False):
    # l_ = local, r_ = remote
    l_mani_path = config['core'].get('manifest')
    r_mani_cmd = config['remote'].get('manifest_command')

    if r_mani_cmd:
        if not os.access(r_mani_cmd, os.X_OK):
            logger.critical('Remote manifest command is not executable: %s', r_mani_cmd)
            sys.exit(1)
        logger.info(' manifest: executing %s', r_mani_cmd)
        cmdargs = [r_mani_cmd]
        if nomtime:
            cmdargs += ['--force']
        (ecode, output, error) = grokmirror.run_shell_command(cmdargs)
        if ecode == 0:
            try:
                r_manifest = json.loads(output)
            except json.JSONDecodeError as ex:
                logger.warning('Failed to parse output from %s', r_mani_cmd)
                logger.warning('Error was: %s', ex)
                raise IOError('Failed to parse output from %s (%s)' % (r_mani_cmd, ex))
        elif ecode == 127:
            logger.info(' manifest: unchanged')
            return
        elif ecode == 1:
            logger.warning('Executing %s failed, exiting', r_mani_cmd, ecode)
            raise IOError('Failed executing %s' % r_mani_cmd)
        else:
            # Non-fatal errors for all other exit codes
            logger.warning(' manifest: executing %s returned %s', r_mani_cmd, ecode)
            return

        if not len(r_manifest):
            logger.warning(' manifest: empty, ignoring')
            raise IOError('Empty manifest returned by %s' % r_mani_cmd)

    else:
        r_mani_status_path = os.path.join(os.path.dirname(l_mani_path), '.%s.remote' % os.path.basename(l_mani_path))
        try:
            with open(r_mani_status_path, 'r') as fh:
                r_mani_status = json.loads(fh.read())
        except (IOError, json.JSONDecodeError):
            logger.debug('Could not read %s', r_mani_status_path)
            r_mani_status = dict()
        r_last_fetched = r_mani_status.get('last-fetched', 0)
        config_last_modified = r_mani_status.get('config-last-modified', 0)
        if config_last_modified != config.last_modified:
            nomtime = True
        r_mani_url = config['remote'].get('manifest')
        logger.info(' manifest: fetching %s', r_mani_url)
        if r_mani_url.find('file:///') == 0:
            r_mani_url = r_mani_url.replace('file://', '')
            if not os.path.exists(r_mani_url):
                logger.critical('Remote manifest not found in %s! Quitting!', r_mani_url)
                raise IOError('Remote manifest not found in %s' % r_mani_url)

            fstat = os.stat(r_mani_url)
            r_last_modified = fstat[8]
            if r_last_fetched:
                logger.debug('mtime on %s is: %s', r_mani_url, fstat[8])
                if not nomtime and r_last_modified <= r_last_fetched:
                    logger.info(' manifest: unchanged')
                    return

            logger.info('Reading new manifest from %s', r_mani_url)
            r_manifest = grokmirror.read_manifest(r_mani_url)
            # Don't accept empty manifests -- that indicates something is wrong
            if not len(r_manifest):
                logger.warning('Remote manifest empty or unparseable! Quitting.')
                raise IOError('Empty manifest in %s' % r_mani_url)

        else:
            session = grokmirror.get_requests_session()

            # Find out if we need to run at all first
            headers = dict()
            if r_last_fetched and not nomtime:
                last_modified_h = time.strftime('%a, %d %b %Y %H:%M:%S GMT', time.gmtime(r_last_fetched))
                logger.debug('Our last-modified is: %s', last_modified_h)
                headers['If-Modified-Since'] = last_modified_h

            try:
                # 30 seconds to connect, 5 minutes between reads
                res = session.get(r_mani_url, headers=headers, timeout=(30, 300))
            except requests.exceptions.RequestException as ex:
                logger.warning('Could not fetch %s', r_mani_url)
                logger.warning('Server returned: %s', ex)
                raise IOError('Remote server returned an error: %s' % ex)

            if res.status_code == 304:
                # No change to the manifest, nothing to do
                logger.info(' manifest: unchanged')
                return

            if res.status_code > 200:
                logger.warning('Could not fetch %s', r_mani_url)
                logger.warning('Server returned status: %s', res.status_code)
                raise IOError('Remote server returned an error: %s' % res.status_code)

            r_last_modified = res.headers['Last-Modified']
            r_last_modified = time.strptime(r_last_modified, '%a, %d %b %Y %H:%M:%S %Z')
            r_last_modified = calendar.timegm(r_last_modified)

            # We don't use read_manifest for the remote manifest, as it can be
            # anything, really. For now, blindly open it with gzipfile if it ends
            # with .gz. XXX: some http servers will auto-deflate such files.
            try:
                if r_mani_url.rfind('.gz') > 0:
                    import io
                    fh = gzip.GzipFile(fileobj=io.BytesIO(res.content))
                    jdata = fh.read().decode()
                else:
                    jdata = res.content

                res.close()
                # Don't hold session open, since we don't refetch manifest very frequently
                session.close()
                r_manifest = json.loads(jdata)

            except Exception as ex:
                logger.warning('Failed to parse %s', r_mani_url)
                logger.warning('Error was: %s', ex)
                raise IOError('Failed to parse %s (%s)' % (r_mani_url, ex))

        # Record for the next run
        with open(r_mani_status_path, 'w') as fh:
            r_mani_status = {
                'source': r_mani_url,
                'last-fetched': r_last_modified,
                'config-last-modified': config.last_modified,
            }
            json.dump(r_mani_status, fh)

    l_manifest = grokmirror.read_manifest(l_mani_path)
    r_culled = cull_manifest(r_manifest, config)
    logger.info(' manifest: %s relevant entries', len(r_culled))

    toplevel = os.path.realpath(config['core'].get('toplevel'))

    obstdir = os.path.realpath(config['core'].get('objstore'))
    forkgroups = build_optimal_forkgroups(l_manifest, r_culled, toplevel, obstdir)
    privmasks = set([x.strip() for x in config['core'].get('private', '').split('\n')])

    # populate private/forkgroup info in r_culled
    for forkgroup, siblings in forkgroups.items():
        for s_fullpath in siblings:
            s_gitdir = '/' + os.path.relpath(s_fullpath, toplevel)

            is_private = False
            for privmask in privmasks:
                # Does this repo match privrepo
                if fnmatch.fnmatch(s_gitdir, privmask):
                    is_private = True
                    break
            if s_gitdir in r_culled:
                r_culled[s_gitdir]['forkgroup'] = forkgroup
                r_culled[s_gitdir]['private'] = is_private

    seen = set()
    to_migrate = set()
    # Used to track symlinks so we can properly avoid purging them
    all_symlinks = set()

    for gitdir, repoinfo in r_culled.items():
        symlinks = repoinfo.get('symlinks')
        if symlinks and isinstance(symlinks, list):
            all_symlinks.update(set(symlinks))

        if gitdir in seen:
            continue
        seen.add(gitdir)
        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        forkgroup = repoinfo.get('forkgroup')

        # Is the directory in place?
        if os.path.exists(fullpath):
            # Did grok-fsck request to reclone it?
            rfile = os.path.join(fullpath, 'grokmirror.reclone')
            if os.path.exists(rfile):
                logger.debug('Reclone requested for %s:', gitdir)
                q_mani.put((gitdir, repoinfo, 'reclone'))
                with open(rfile, 'r') as rfh:
                    reason = rfh.read()
                    logger.debug('  %s', reason)
                continue

            if gitdir not in l_manifest:
                q_mani.put((gitdir, repoinfo, 'fix_remotes'))
                continue

            r_desc = r_culled[gitdir].get('description')
            r_owner = r_culled[gitdir].get('owner')
            r_head = r_culled[gitdir].get('head')

            l_desc = l_manifest[gitdir].get('description')
            l_owner = l_manifest[gitdir].get('owner')
            l_head = l_manifest[gitdir].get('head')

            if l_owner is None:
                l_owner = config['pull'].get('default_owner', 'Grokmirror')
            if r_owner is None:
                r_owner = config['pull'].get('default_owner', 'Grokmirror')

            if r_desc != l_desc or r_owner != l_owner or r_head != l_head:
                q_mani.put((gitdir, repoinfo, 'fix_params'))

            if symlinks and isinstance(symlinks, list):
                # Are all symlinks in place?
                for symlink in symlinks:
                    linkpath = os.path.join(toplevel, symlink.lstrip('/'))
                    if not os.path.islink(linkpath) or os.path.realpath(linkpath) != fullpath:
                        q_mani.put((gitdir, repoinfo, 'fix_params'))
                        break

            my_fingerprint = grokmirror.get_repo_fingerprint(toplevel, gitdir)
            if my_fingerprint != l_manifest[gitdir].get('fingerprint'):
                logger.debug('Fingerprint discrepancy, forcing a fetch')
                q_mani.put((gitdir, repoinfo, 'pull'))
                continue

            if my_fingerprint == r_culled[gitdir]['fingerprint']:
                logger.debug('Fingerprints match, skipping %s', gitdir)
                continue

            logger.debug('No fingerprint match, will pull %s', gitdir)
            q_mani.put((gitdir, repoinfo, 'pull'))
            continue

        if not forkgroup:
            # no-sibling repo
            q_mani.put((gitdir, repoinfo, 'init'))
            continue

        obstrepo = os.path.join(obstdir, '%s.git' % forkgroup)
        if os.path.isdir(obstrepo):
            # Init with an existing obstrepo, easy case
            q_mani.put((gitdir, repoinfo, 'init'))
            continue

        # Do we have any existing siblings that were cloned without obstrepo?
        # This would happen when an initial fork is created of an existing repo.
        found_existing = False
        public_siblings = set()
        for s_fullpath in forkgroups[forkgroup]:
            s_gitdir = '/' + os.path.relpath(s_fullpath, toplevel)
            if s_gitdir == gitdir:
                continue

            # can't simply rely on r_culled 'private' info, as this repo may only exist locally
            is_private = False
            for privmask in privmasks:
                # Does this repo match privrepo
                if fnmatch.fnmatch(s_gitdir, privmask):
                    is_private = True
                    break
            if is_private:
                # Can't use this sibling for anything, as it's private
                continue

            if os.path.isdir(s_fullpath):
                found_existing = True
                if s_gitdir not in to_migrate:
                    # Plan to migrate it to objstore
                    logger.debug('reusing existing %s as new obstrepo %s', s_gitdir, obstrepo)
                    s_repoinfo = grokmirror.get_repo_defs(toplevel, s_gitdir, usenow=True)
                    s_repoinfo['forkgroup'] = forkgroup
                    s_repoinfo['private'] = False
                    # Stick it into queue before the new clone
                    q_mani.put((s_gitdir, s_repoinfo, 'objstore_migrate'))
                    seen.add(s_gitdir)
                    to_migrate.add(s_gitdir)
                break
            if s_gitdir in r_culled:
                public_siblings.add(s_gitdir)

        if found_existing:
            q_mani.put((gitdir, repoinfo, 'init'))
            continue

        if repoinfo['private'] and len(public_siblings):
            # Clone public siblings first
            for s_gitdir in public_siblings:
                if s_gitdir not in seen:
                    q_mani.put((s_gitdir, r_culled[s_gitdir], 'init'))
                    seen.add(s_gitdir)
        # Finally, clone ourselves.
        q_mani.put((gitdir, repoinfo, 'init'))

    if config['pull'].getboolean('purge', False):
        nopurge = config['pull'].get('nopurge', '').split('\n')
        to_purge = set()
        found_repos = 0
        for founddir in grokmirror.find_all_gitdirs(toplevel, exclude_objstore=True):
            gitdir = '/' + os.path.relpath(founddir, toplevel)
            found_repos += 1

            if gitdir not in r_culled and gitdir not in all_symlinks:
                exclude = False
                for entry in nopurge:
                    if fnmatch.fnmatch(gitdir, entry):
                        exclude = True
                        break
                # Refuse to purge ffonly repos
                for globpatt in set([x.strip() for x in config['pull'].get('ffonly', '').split('\n')]):
                    if fnmatch.fnmatch(gitdir, globpatt):
                        # Woah, these are not supposed to be deleted, ever
                        logger.critical('Refusing to purge ffonly repo %s', gitdir)
                        exclude = True
                        break
                if not exclude:
                    logger.debug('Adding %s to to_purge', gitdir)
                    to_purge.add(gitdir)

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
            else:
                for gitdir in to_purge:
                    logger.debug('Queued %s for purging', gitdir)
                    q_mani.put((gitdir, None, 'purge'))
        else:
            logger.debug('No repositories need purging')


def update_manifest(config, entries):
    manifile = config['core'].get('manifest')
    grokmirror.manifest_lock(manifile)
    manifest = grokmirror.read_manifest(manifile)
    changed = False
    while len(entries):
        gitdir, repoinfo, action, success = entries.pop()
        if not success:
            continue
        if action == 'purge':
            # Remove entry from manifest
            try:
                manifest.pop(gitdir)
                changed = True
            except KeyError:
                pass
            continue

        try:
            # does not belong in the manifest
            repoinfo.pop('private')
        except KeyError:
            pass
        for key, val in dict(repoinfo).items():
            # Clean up grok-2.0 null values
            if key in ('head', 'forkgroup') and val is None:
                repoinfo.pop(key)
        # Make sure 'reference' is present to prevent grok-1.x breakage
        if 'reference' not in repoinfo:
            repoinfo['reference'] = None
        manifest[gitdir] = repoinfo
        changed = True
    if changed:
        if 'manifest' in config:
            pretty = config['manifest'].getboolean('pretty', False)
        else:
            pretty = False
        grokmirror.write_manifest(manifile, manifest, pretty=pretty)
        logger.info(' manifest: wrote %s (%d entries)', manifile, len(manifest))
        # write out projects.list, if asked to
        write_projects_list(config, manifest)

    grokmirror.manifest_unlock(manifile)


def socket_worker(config, q_mani, sockfile):
    logger.info(' listener: listening on socket %s', sockfile)
    curmask = os.umask(0)
    with ThreadedUnixStreamServer(sockfile, Handler) as server:
        os.umask(curmask)
        # Stick some objects into the server
        server.q_mani = q_mani
        server.config = config
        server.serve_forever()


def showstats(q_todo, q_pull, q_spa, good, bad, pws, dws):
    stats = list()
    if good:
        stats.append('%s fetched' % good)
    if pws:
        stats.append('%s active' % len(pws))
    if not q_pull.empty():
        stats.append('%s queued' % q_pull.qsize())
    if not q_todo.empty():
        stats.append('%s waiting' % q_todo.qsize())
    if len(dws) or not q_spa.empty():
        stats.append('%s in spa' % (q_spa.qsize() + len(dws)))
    if bad:
        stats.append('%s errors' % bad)

    logger.info('      ---:  %s', ', '.join(stats))


def manifest_worker(config, q_mani, nomtime=False):
    starttime = int(time.time())
    fill_todo_from_manifest(config, q_mani, nomtime=nomtime)
    refresh = config['pull'].getint('refresh', 300)
    left = refresh - int(time.time() - starttime)
    if left > 0:
        logger.info(' manifest: sleeping %ss', left)


def pull_mirror(config, nomtime=False, forcepurge=False, runonce=False):
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    obstdir = os.path.realpath(config['core'].get('objstore'))
    refresh = config['pull'].getint('refresh', 300)

    q_mani = mp.Queue()
    q_todo = mp.Queue()
    q_pull = mp.Queue()
    q_done = mp.Queue()
    q_spa = mp.Queue()

    sw = None
    sockfile = config['pull'].get('socket')
    if sockfile and not runonce:
        if os.path.exists(sockfile):
            mode = os.stat(sockfile).st_mode
            if stat.S_ISSOCK(mode):
                os.unlink(sockfile)
            else:
                raise IOError('File exists but is not a socket: %s' % sockfile)

        sw = mp.Process(target=socket_worker, args=(config, q_mani, sockfile))
        sw.daemon = True
        sw.start()

    pws = list()
    dws = list()
    mws = list()
    actions = set()
    # Run in the main thread if we have runonce
    if runonce:
        fill_todo_from_manifest(config, q_mani, nomtime=nomtime, forcepurge=forcepurge)
        if not q_mani.qsize():
            return 0
    else:
        # force nomtime to True the first time
        nomtime = True
    lastrun = 0

    pull_threads = config['pull'].getint('pull_threads', 0)
    if pull_threads < 1:
        # take half of available CPUs by default
        pull_threads = int(mp.cpu_count() / 2)

    busy = set()
    done = list()
    cloned = list()
    good = 0
    bad = 0
    loopmark = None
    post_clone_hook = config['pull'].get('post_clone_complete_hook')
    with SignalHandler(config, sw, dws, pws, done):
        while True:
            for pw in pws:
                if pw and not pw.is_alive():
                    pws.remove(pw)
                    logger.info('   worker: terminated (%s remaining)', len(pws))
                    showstats(q_todo, q_pull, q_spa, good, bad, pws, dws)

            for dw in dws:
                if dw and not dw.is_alive():
                    dws.remove(dw)
                    showstats(q_todo, q_pull, q_spa, good, bad, pws, dws)

            for mw in mws:
                if mw and not mw.is_alive():
                    mws.remove(mw)

            if not q_spa.empty() and not len(dws):
                if runonce:
                    pauseonload = False
                else:
                    pauseonload = True
                dw = mp.Process(target=spa_worker, args=(config, q_spa, pauseonload))
                dw.daemon = True
                dw.start()
                dws.append(dw)

            if not q_pull.empty() and len(pws) < pull_threads:
                pw = mp.Process(target=pull_worker, args=(config, q_pull, q_spa, q_done))
                pw.daemon = True
                pw.start()
                pws.append(pw)
                logger.info('   worker: started (%s running)', len(pws))

            # Any new results?
            try:
                while True:
                    gitdir, repoinfo, q_action, success = q_done.get_nowait()
                    try:
                        actions.remove((gitdir, q_action))
                    except KeyError:
                        pass
                    # Was it a clone, and are all other clones done?
                    if post_clone_hook and q_action == 'init':
                        cloned.append(gitdir)
                        more_clones = False
                        for qgd, qqa in actions:
                            if qqa == 'init':
                                more_clones = True
                                break
                        if not more_clones:
                            # Fire the post_clone hook
                            run_post_clone_complete_hook(config, cloned)
                            cloned = list()

                    forkgroup = repoinfo.get('forkgroup')
                    if forkgroup and forkgroup in busy:
                        busy.remove(forkgroup)
                    done.append((gitdir, repoinfo, q_action, success))
                    if success:
                        good += 1
                    else:
                        bad += 1
                    logger.info('     done: %s', gitdir)
                    showstats(q_todo, q_pull, q_spa, good, bad, pws, dws)
                    if len(done) >= 100:
                        # Write manifest every 100 repos
                        update_manifest(config, done)

            except queue.Empty:
                pass

            # Anything new in the manifest queue?
            try:
                new_updates = 0
                while True:
                    gitdir, repoinfo, action = q_mani.get_nowait()
                    if (gitdir, action) in actions:
                        logger.debug('already in the queue: %s, %s', gitdir, action)
                        continue
                    if action == 'pull' and (gitdir, 'init') in actions:
                        logger.debug('already in the queue as init: %s, %s', gitdir, action)
                        continue

                    actions.add((gitdir, action))
                    q_todo.put((gitdir, repoinfo, action))
                    new_updates += 1
                    logger.debug('queued: %s, %s', gitdir, action)

                if new_updates:
                    logger.info(' manifest: %s new updates', new_updates)

            except queue.Empty:
                pass

            if not runonce and not len(mws) and q_todo.empty() and q_pull.empty() and time.time() - lastrun >= refresh:
                if done:
                    update_manifest(config, done)
                mw = mp.Process(target=manifest_worker, args=(config, q_mani, nomtime))
                nomtime = False
                mw.daemon = True
                mw.start()
                mws.append(mw)
                lastrun = int(time.time())

            # Finally, deal with q_todo
            try:
                gitdir, repoinfo, q_action = q_todo.get_nowait()
                logger.debug('main_thread: got %s/%s from q_todo', gitdir, q_action)
            except queue.Empty:
                if q_mani.empty() and q_done.empty():
                    if not len(pws):
                        if done:
                            update_manifest(config, done)
                            if runonce:
                                # Wait till spa is done
                                while True:
                                    if q_spa.empty():
                                        for dw in dws:
                                            dw.join()
                                        return 0
                                    time.sleep(1)
                    if len(pws):
                        # Don't run a hot loop waiting on results
                        time.sleep(5)
                    else:
                        # Shorter sleep if everything is idle
                        time.sleep(1)
                continue

            if repoinfo is None:
                repoinfo = dict()

            fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
            forkgroup = repoinfo.get('forkgroup')
            if gitdir in busy or (forkgroup is not None and forkgroup in busy):
                # Stick it back into the queue
                q_todo.put((gitdir, repoinfo, q_action))
                if loopmark is None:
                    loopmark = gitdir
                elif loopmark == gitdir:
                    # We've looped around all waiting repos, so back off and don't run
                    # a hot waiting loop.
                    time.sleep(5)
                continue

            if gitdir == loopmark:
                loopmark = None

            if q_action == 'objstore_migrate':
                # Add forkgroup to busy, so we don't run any pulls until it's done
                busy.add(repoinfo['forkgroup'])
                obstrepo = grokmirror.setup_objstore_repo(obstdir, name=forkgroup)
                grokmirror.add_repo_to_objstore(obstrepo, fullpath)
                grokmirror.set_altrepo(fullpath, obstrepo)

            if q_action != 'init':
                # Easy actions that don't require priority logic
                q_pull.put((gitdir, repoinfo, q_action, q_action))
                continue

            try:
                grokmirror.lock_repo(fullpath, nonblocking=True)
            except IOError:
                if not runonce:
                    q_todo.put((gitdir, repoinfo, q_action))
                continue

            if not grokmirror.setup_bare_repo(fullpath):
                logger.critical('Unable to bare-init %s', fullpath)
                q_done.put((gitdir, repoinfo, q_action, False))
                continue

            fix_remotes(toplevel, gitdir, config['remote'].get('site'), config)
            set_repo_params(fullpath, repoinfo)
            grokmirror.unlock_repo(fullpath)

            forkgroup = repoinfo.get('forkgroup')
            if not forkgroup:
                logger.debug('no-sibling clone: %s', gitdir)
                q_pull.put((gitdir, repoinfo, 'pull', q_action))
                continue

            obstrepo = os.path.join(obstdir, '%s.git' % forkgroup)
            if os.path.isdir(obstrepo):
                logger.debug('clone %s with existing obstrepo %s', gitdir, obstrepo)
                grokmirror.set_altrepo(fullpath, obstrepo)
                if not repoinfo['private']:
                    grokmirror.add_repo_to_objstore(obstrepo, fullpath)
                q_pull.put((gitdir, repoinfo, 'pull', q_action))
                continue

            # Set up a new obstrepo and make sure it's not used until the initial
            # pull is done
            logger.debug('cloning %s with new obstrepo %s', gitdir, obstrepo)
            busy.add(forkgroup)
            obstrepo = grokmirror.setup_objstore_repo(obstdir, name=forkgroup)
            grokmirror.set_altrepo(fullpath, obstrepo)
            if not repoinfo['private']:
                grokmirror.add_repo_to_objstore(obstrepo, fullpath)
            q_pull.put((gitdir, repoinfo, 'pull', q_action))

    return 0


def parse_args():
    import argparse
    # noinspection PyTypeChecker
    op = argparse.ArgumentParser(prog='grok-pull',
                                 description='Create or update a git repository collection mirror',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    op.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                    default=False,
                    help='Be verbose and tell us what you are doing')
    op.add_argument('-n', '--no-mtime-check', dest='nomtime',
                    action='store_true', default=False,
                    help='Run without checking manifest mtime')
    op.add_argument('-p', '--purge', dest='purge',
                    action='store_true', default=False,
                    help='Remove any git trees that are no longer in manifest')
    op.add_argument('--force-purge', dest='forcepurge',
                    action='store_true', default=False,
                    help='Force purge despite significant repo deletions')
    op.add_argument('-o', '--continuous', dest='runonce',
                    action='store_false', default=True,
                    help='Run continuously (no effect if refresh is not set in config)')
    op.add_argument('-c', '--config', dest='config',
                    required=True,
                    help='Location of the configuration file')
    op.add_argument('--version', action='version', version=grokmirror.VERSION)

    return op.parse_args()


def grok_pull(cfgfile, verbose=False, nomtime=False, purge=False, forcepurge=False, runonce=False):
    global logger

    config = grokmirror.load_config_file(cfgfile)
    if config['pull'].get('refresh', None) is None:
        runonce = True

    logfile = config['core'].get('log', None)
    if config['core'].get('loglevel', 'info') == 'debug':
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    if purge:
        # Override the pull.purge setting
        config['pull']['purge'] = 'yes'

    logger = grokmirror.init_logger('pull', logfile, loglevel, verbose)

    return pull_mirror(config, nomtime, forcepurge, runonce)


def command():
    opts = parse_args()

    retval = grok_pull(
        opts.config, opts.verbose, opts.nomtime, opts.purge, opts.forcepurge, opts.runonce)

    sys.exit(retval)


if __name__ == '__main__':
    command()
