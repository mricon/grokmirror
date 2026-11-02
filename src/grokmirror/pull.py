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

from __future__ import annotations

import argparse
import calendar
import gzip
import io
import json
import logging
import os
import queue
import shlex
import shutil
import signal
import stat
import sys
import tempfile
import threading
import time
import uuid
from collections import deque
from concurrent import futures
from pathlib import Path
from socketserver import StreamRequestHandler, ThreadingMixIn, UnixStreamServer
from types import FrameType, TracebackType
from typing import cast

import requests

import grokmirror
from grokmirror import configcheck

# default basic logger. We override it later.
logger = logging.getLogger(__name__)

# What travels on the internal queues and worklists. All of them are some
# variation on "this repository, its manifest entry, and what to do with it".
#
# A work item carries a second action as well: an 'init' turns into a 'pull'
# once the bare repository exists, but note_done() still has to account for it
# as the init it was queued as, so both are passed along.
ManiItem = tuple[str, grokmirror.RepoInfo, str]
WorkItem = tuple[str, grokmirror.RepoInfo, str, str]
# A finished work item and whether it succeeded, for update_manifest().
DoneItem = tuple[str, grokmirror.RepoInfo, str, bool]
# A repository and the post-pull treatments queued for it. None is the
# sentinel that asks the spa worker to exit.
SpaItem = tuple[str, list[str]]


class SignalHandler:
    """Flush accumulated manifest updates and exit on SIGINT/SIGTERM."""

    def __init__(self, config: grokmirror.GrokConfigParser, done: list[DoneItem]) -> None:
        self.config = config
        self.done = done

    def _handler(self, signum: int, frame: FrameType | None) -> None:
        logger.debug('Received signum=%s, frame=%s', signum, frame)
        if self.done:
            try:
                update_manifest(self.config, self.done)
            except grokmirror.GrokError as ex:
                # The signal may have caught the main thread in the middle of
                # its own manifest update, in which case the manifest is
                # already locked by us. Whatever was not written will be
                # picked up again on the next run.
                logger.warning('Could not write the manifest on exit: %s', ex)

        logger.info('Exiting on signal %s', signum)
        # Exit without the interpreter shutdown dance: waiting for the worker
        # threads would mean waiting out any in-flight git fetches. When the
        # signal went to the whole process group (Ctrl-C, systemd) those git
        # processes are dying with us; when it was aimed at us alone, they
        # finish on their own, exactly as they did when the workers were
        # daemon processes.
        os._exit(0)

    def __enter__(self) -> None:
        self.old_sigint = signal.signal(signal.SIGINT, self._handler)
        self.old_sigterm = signal.signal(signal.SIGTERM, self._handler)

    def __exit__(
        self, sigtype: type[BaseException] | None, value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        signal.signal(signal.SIGINT, self.old_sigint)
        signal.signal(signal.SIGTERM, self.old_sigterm)


class ThreadedUnixStreamServer(ThreadingMixIn, UnixStreamServer):
    # pull_mirror() sticks these onto the instance for Handler to pick up
    q_mani: queue.Queue[ManiItem]
    config: grokmirror.GrokConfigParser


class Handler(StreamRequestHandler):
    def handle(self) -> None:
        # socketserver types self.server as the base class, so narrow it to the
        # one socket_worker() actually hands us
        server = cast('ThreadedUnixStreamServer', self.server)
        config = server.config
        # load_config_file() guarantees [core]manifest is set
        manifile = config['core']['manifest']
        # The try is outside the loop, not inside it: every way out of the
        # handler is a return, so catching per-iteration bought nothing.
        # noinspection PyBroadException
        try:
            while True:
                gitdir = self.rfile.readline().strip().decode()
                # Do we know anything about this path?
                manifest = grokmirror.read_manifest(manifile)
                if gitdir in manifest:
                    logger.info(' listener: %s', gitdir)
                    repoinfo = manifest[gitdir]
                    # Set fingerprint to None to force a run
                    repoinfo['fingerprint'] = None
                    repoinfo['modified'] = int(time.time())
                    server.q_mani.put((gitdir, repoinfo, 'pull'))
                elif gitdir:
                    logger.info(' listener: %s (not known, ignored)', gitdir)
                    return
                else:
                    return
        except Exception:  # noqa: BLE001
            # Anything at all going wrong on this connection (a short read,
            # undecodable input, an unreadable manifest) just drops it.
            return


def build_optimal_forkgroups(
    l_manifest: grokmirror.Manifest, r_manifest: grokmirror.Manifest, toplevel: str, obstdir: str
) -> dict[str, set[str]]:
    r_forkgroups: dict[str, set[str]] = {}
    for gitdir in set(r_manifest.keys()):
        # str(), not the Path: these sets get compared and merged with the ones
        # get_forkgroups() builds, which are sets of strings. A Path in here
        # would never match one and every fork would look like a new group.
        fullpath = str(grokmirror.gitdir_to_fullpath(toplevel, gitdir))
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
            r_fullpath = str(grokmirror.gitdir_to_fullpath(toplevel, reference))
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
        for l_siblings in forkgroups.values():
            if l_siblings == r_siblings:
                # No changes there
                continue
            if l_siblings.intersection(r_siblings):
                l_siblings.update(r_siblings)
                found = True
                break
        if not found:
            # We don't have any matches in existing repos, so make a new forkgroup
            forkgroups[r_fg] = r_siblings

    return forkgroups


def spa_worker(config: grokmirror.GrokConfigParser, q_spa: queue.Queue[SpaItem | None], pauseonload: bool) -> None:
    """Run the queued post-pull treatments (objstore fetch, repack, pack-refs).

    Runs as a single thread for the life of the run, so the spa operations
    stay serialized. Every queued item is accounted with q_spa.task_done(),
    which lets the supervisor drain the spa with q_spa.join(); a None item
    asks the worker to exit.
    """
    toplevel = os.path.realpath(config['core']['toplevel'])
    cpus = os.cpu_count() or 1
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

        item = q_spa.get()
        try:
            if item is None:
                return
            (gitdir, actions) = item
            _spa_repo(config, toplevel, gitdir, actions, waiting=q_spa.qsize())
        # Deliberately broad: a failed treatment must not take the spa thread
        # down with it, or every later spa action would sit in the queue
        # forever and a --runonce drain would never finish.
        except Exception:
            logger.exception('      spa: failed on %s', item)
        finally:
            q_spa.task_done()


def _spa_repo(
    config: grokmirror.GrokConfigParser, toplevel: str, gitdir: str, actions: list[str], waiting: int = 0
) -> None:
    logger.debug('spa_worker: gitdir=%s, actions=%s', gitdir, actions)
    fullpath = grokmirror.gitdir_to_fullpath(toplevel, gitdir)
    try:
        with grokmirror.locked_repo(fullpath, nonblocking=True):
            if waiting:
                logger.info('      spa: 1 active, %s waiting', waiting)
            else:
                logger.info('      spa: 1 active')

            done = []
            for action in actions:
                if action in done:
                    continue
                done.append(action)
                if action == 'objstore':
                    altrepo = grokmirror.get_altrepo(fullpath)
                    if not altrepo:
                        # Whatever queued this action expected us to have alternates,
                        # and we don't. Passing None along would make git look at the
                        # cwd instead of at an objstore repo.
                        logger.debug('%s: no alternates, skipping objstore fetch', gitdir)
                        continue
                    # Should we use plumbing for this?
                    use_plumbing = config.get_bool('core', 'objstore_uses_plumbing', False)
                    grokmirror.fetch_objstore_repo(altrepo, fullpath, use_plumbing=use_plumbing)

                elif action == 'repack':
                    logger.debug('quick-repacking %s', fullpath)
                    args = ['repack', '-Adlq']
                    if 'fsck' in config:
                        extraflags = config['fsck'].get('extra_repack_flags', '').split()
                        if extraflags:
                            args += extraflags
                    ecode, _out, _err = grokmirror.run_git_command(fullpath, args)
                    if ecode > 0:
                        logger.debug('Could not repack %s', fullpath)

                elif action == 'packrefs':
                    args = ['pack-refs']
                    ecode, _out, _err = grokmirror.run_git_command(fullpath, args)
                    if ecode > 0:
                        logger.debug('Could not pack-refs %s', fullpath)

                elif action == 'packrefs-all':
                    args = ['pack-refs', '--all']
                    ecode, _out, _err = grokmirror.run_git_command(fullpath, args)
                    if ecode > 0:
                        logger.debug('Could not pack-refs %s', fullpath)
    except grokmirror.GrokLockError:
        # We'll get it during grok-fsck
        return

    logger.info('      spa: %s (done: %s)', gitdir, ', '.join(done))


def objstore_repo_preload(ses: grokmirror.GrokSession, config: grokmirror.GrokConfigParser, obstrepo: str) -> None:
    purl = config['remote'].get('preload_bundle_url')
    if not purl:
        return
    bname = Path(obstrepo).name.removesuffix('.git')
    obstdir = os.path.realpath(config['core']['objstore'])
    burl = '{}/{}.bundle'.format(purl.rstrip('/'), bname)
    bfile = Path(obstdir, f'{bname}.bundle')
    try:
        resp = ses.get_requests_session().get(burl, stream=True)
        resp.raise_for_status()
        logger.info(' objstore: downloading %s.bundle', bname)
        with bfile.open('wb') as fh:
            fh.writelines(resp.iter_content(chunk_size=8192))
        resp.close()
    # Deliberately broad: whatever went wrong mid-download, clean up and fall
    # back to a regular clone. But no longer a bare except: a Ctrl-C or a
    # SystemExit must not be swallowed here.
    except Exception:  # noqa: BLE001
        # Make sure we don't leave .bundle files lying around
        # Should we add logic to resume downloads here in the future?
        if bfile.exists():
            bfile.unlink()
        return

    # Now we clone from it into the objstore repo
    args = ['remote', 'add', '--mirror=fetch', '_preload', str(bfile)]
    ecode, _out, _err = grokmirror.run_git_command(obstrepo, args)
    if ecode == 0:
        logger.info(' objstore: preloading %s.bundle', bname)
        args = ['remote', 'update', '_preload']
        ecode, _out, _err = grokmirror.run_git_command(obstrepo, args)
        if ecode > 0:
            logger.info(' objstore: failed to preload from %s.bundle', bname)
        else:
            # now pack refs and generate a commit graph
            grokmirror.run_git_command(obstrepo, ['pack-refs', '--all'])
            if grokmirror.git_newer_than('2.18.0'):
                grokmirror.run_git_command(obstrepo, ['commit-graph', 'write', '--reachable'])
            logger.info(' objstore: successful preload from %s.bundle', bname)
    # Regardless of what happened, we remove _preload and the bundle, then move on
    grokmirror.run_git_command(obstrepo, ['remote', 'rm', '_preload'])
    bfile.unlink()


def run_pull_action(
    ses: grokmirror.GrokSession,
    config: grokmirror.GrokConfigParser,
    gitdir: str,
    fullpath: grokmirror.StrPath,
    action: str,
    repoinfo: grokmirror.RepoInfo,
    spa_actions: list[str],
) -> bool:
    """Handle the 'pull' and 'objstore_migrate' actions for pull_worker().

    Recomputes the fingerprint, fetches if it moved (retrying up to
    [pull] retries times), and on a successful fetch works out which
    post-pull spa treatments (objstore/repack/packrefs) the repo now needs.
    Appends to `spa_actions` in place, matching pull_worker()'s own
    accumulation style, rather than returning a new list to merge back.
    """
    toplevel = os.path.realpath(config['core']['toplevel'])
    obstdir = os.path.realpath(config['core']['objstore'])
    site = config['remote']['site']
    remotename = config['pull'].get('remotename', '_grokmirror')
    maxretries = config.get_int('pull', 'retries', 3)
    objstore_uses_plumbing = config.get_bool('core', 'objstore_uses_plumbing', False)

    success = True
    altrepo = grokmirror.get_altrepo(fullpath)
    obstrepo = altrepo if altrepo and grokmirror.is_obstrepo(altrepo, obstdir) else None

    ignorerefs = grokmirror.get_ignorerefs(config)

    r_fp = repoinfo.get('fingerprint')
    my_fp = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=True, ignorerefs=ignorerefs)
    if obstrepo:
        o_obj_info = grokmirror.get_repo_obj_info(obstrepo)
        if o_obj_info.get('count') == '0' and o_obj_info.get('in-pack') == '0' and not my_fp:
            # Try to preload the objstore repo directly
            objstore_repo_preload(ses, config, obstrepo)

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
            run_post_update_hook(config, fullpath)
            post_pull_fp = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=True, ignorerefs=ignorerefs)
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
                        spa_actions.extend(('repack', 'packrefs'))

            modified = repoinfo.get('modified')
            if modified is not None:
                set_agefile(toplevel, gitdir, modified)
    else:
        logger.debug('FP match, not pulling %s', gitdir)

    if action == 'objstore_migrate':
        spa_actions.extend(('objstore', 'repack'))

    return success


def sync_repo_symlinks(toplevel: str, gitdir: str, fullpath: grokmirror.StrPath, symlinks: list[str]) -> None:
    """Make sure each of `symlinks` on disk actually points at `fullpath`.

    A symlink already pointing elsewhere is recreated; a real directory
    sitting where a symlink now belongs is deleted first, since otherwise
    the same repository would stick around twice under different names.
    """
    for symlink in symlinks:
        target = grokmirror.gitdir_to_fullpath(toplevel, symlink)

        if target.is_symlink():
            # are you pointing to where we need you?
            # os.fspath() on the right-hand side: realpath() hands back a
            # str, and a Path never compares equal to one, so every correct
            # symlink would look wrong and get recreated on every run.
            if os.path.realpath(target) != os.fspath(fullpath):
                # Remove symlink and recreate below
                logger.debug('Removed existing wrong symlink %s', target)
                target.unlink()
        elif target.exists():
            logger.warning(f'Deleted repo {target}, because it is now a symlink to {fullpath}')
            shutil.rmtree(target)

        # Here we re-check if we still need to do anything
        if not target.exists():
            logger.info('  symlink: %s -> %s', symlink, gitdir)
            # Make sure the leading dirs are in place; another worker may
            # be placing a sibling symlink there at this very moment.
            target.parent.mkdir(parents=True, exist_ok=True)
            target.symlink_to(fullpath)


def pull_worker(
    ses: grokmirror.GrokSession,
    config: grokmirror.GrokConfigParser,
    item: WorkItem,
    q_spa: queue.Queue[SpaItem | None],
) -> bool | None:
    """Carry out one queued repository action in a pool thread.

    Returns whether the action succeeded, or None if the repository was
    locked by another grokmirror process, in which case the supervisor
    requeues the item.
    """
    (gitdir, repoinfo, action, _q_action) = item
    toplevel = os.path.realpath(config['core']['toplevel'])
    # pull_mirror() checked these before starting us up
    site = config['remote']['site']

    logger.debug('pull_worker: gitdir=%s, action=%s', gitdir, action)
    fullpath = grokmirror.gitdir_to_fullpath(toplevel, gitdir)
    success = True
    spa_actions: list[str] = []

    try:
        with grokmirror.locked_repo(fullpath, nonblocking=True):
            if action == 'purge':
                # Is it a symlink?
                if fullpath.is_symlink():
                    logger.info('    purge: %s', gitdir)
                    fullpath.unlink()
                # is anything using us for alternates?
                elif ses.is_alt_repo(toplevel, gitdir):
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
                    shutil.move(fullpath, f'{fullpath}.reclone')
                    shutil.rmtree(f'{fullpath}.reclone')
                    grokmirror.setup_bare_repo(fullpath)
                    fix_remotes(toplevel, gitdir, site, config)
                    set_repo_params(fullpath, repoinfo)
                    if altrepo:
                        grokmirror.set_altrepo(fullpath, altrepo)
                    action = 'pull'
                except (OSError, PermissionError) as ex:
                    logger.critical('Unable to remove %s: %s', fullpath, str(ex))
                    success = False

            if action in ('pull', 'objstore_migrate'):
                success = run_pull_action(ses, config, gitdir, fullpath, action, repoinfo, spa_actions)

    except grokmirror.GrokLockError:
        # Take a quick nap before letting the supervisor requeue this item.
        logger.info('    defer: %s (locked)', gitdir)
        time.sleep(5)
        return None

    symlinks = repoinfo.get('symlinks')
    if fullpath.exists() and symlinks:
        sync_repo_symlinks(toplevel, gitdir, fullpath, symlinks)

    if spa_actions:
        q_spa.put((gitdir, spa_actions))
    return success


def cull_manifest(manifest: grokmirror.Manifest, config: grokmirror.GrokConfigParser) -> grokmirror.Manifest:
    # Compiled once for the whole manifest: this used to be every include
    # times every exclude, for every repository the origin publishes.
    included = grokmirror.compile_globs(config['pull'].get('include', '*').splitlines())
    excluded = grokmirror.compile_globs(config['pull'].get('exclude', '').splitlines())

    culled = {}

    for gitdir, repoinfo in manifest.items():
        if not repoinfo.get('fingerprint'):
            logger.critical('Repo without fingerprint info (skipped): %s', gitdir)
            continue
        if included.match(gitdir) and not excluded.match(gitdir):
            culled[gitdir] = repoinfo

    return culled


def fix_remotes(toplevel: str, gitdir: str, site: str, config: grokmirror.GrokConfigParser) -> bool:
    remotename = config['pull'].get('remotename', '_grokmirror')
    fullpath = grokmirror.gitdir_to_fullpath(toplevel, gitdir)
    # Set our remote
    if remotename in grokmirror.list_repo_remotes(fullpath):
        logger.debug('\tremoving remote: %s', remotename)
        ecode, _out, _err = grokmirror.run_git_command(fullpath, ['remote', 'remove', remotename])
        if ecode > 0:
            logger.critical('FATAL: Could not remove remote %s from %s', remotename, fullpath)
            return False

    # set my remote URL. Joined by hand, the same way objstore_repo_preload()
    # builds its bundle URL: site is a URL, not a path, and os.path.join()
    # only happens to be right for one because the separator matches. Path()
    # is not even that -- it collapses the '//' in 'https://' to a single
    # slash and hands back a remote nothing can fetch from.
    url = '{}/{}'.format(site.rstrip('/'), gitdir.lstrip('/'))
    ecode, _out, _err = grokmirror.run_git_command(fullpath, ['remote', 'add', '--mirror=fetch', remotename, url])
    if ecode > 0:
        logger.critical('FATAL: Could not set %s to %s in %s', remotename, url, fullpath)
        return False

    if grokmirror.compile_globs(config['pull'].get('ffonly', '').splitlines()).match(gitdir):
        grokmirror.set_git_config(fullpath, f'remote.{remotename}.fetch', 'refs/*:refs/*')
        logger.debug('\tset %s as %s (ff-only)', remotename, url)
    else:
        logger.debug('\tset %s as %s', remotename, url)
    return True


def set_repo_params(fullpath: grokmirror.StrPath, repoinfo: grokmirror.RepoInfo) -> None:
    owner = repoinfo.get('owner')
    description = repoinfo.get('description')
    head = repoinfo.get('head')
    if owner is None and description is None and head is None:
        # Let the default git values be there, then
        return

    if description is not None:
        descfile = Path(fullpath, 'description')
        contents = None
        if descfile.exists():
            contents = descfile.read_text(encoding='utf-8')
        if contents != description:
            logger.debug('Setting %s description to: %s', fullpath, description)
            descfile.write_text(description, encoding='utf-8')

    if owner is not None:
        logger.debug('Setting %s owner to: %s', fullpath, owner)
        grokmirror.set_git_config(fullpath, 'gitweb.owner', owner)

    if head is not None:
        headfile = Path(fullpath, 'HEAD')
        contents = None
        if headfile.exists():
            contents = headfile.read_text(encoding='utf-8').rstrip()
        if contents != head:
            logger.debug('Setting %s HEAD to: %s', fullpath, head)
            headfile.write_text(f'{head}\n', encoding='utf-8')


def set_agefile(toplevel: str, gitdir: str, last_modified: int) -> None:
    grokmirror.set_repo_timestamp(toplevel, gitdir, last_modified)

    # set agefile, which can be used by cgit to show idle times
    # cgit recommends it to be yyyy-mm-dd hh:mm:ss
    cgit_fmt = time.strftime('%F %T', time.localtime(last_modified))
    agefile = grokmirror.gitdir_to_fullpath(toplevel, gitdir) / 'info' / 'web' / 'last-modified'
    agefile.parent.mkdir(parents=True, exist_ok=True)
    agefile.write_text(f'{cgit_fmt}\n', encoding='utf-8')
    logger.debug('Wrote "%s" into %s', cgit_fmt, agefile)


def get_hookscripts(config: grokmirror.GrokConfigParser, hookname: str) -> list[list[str]]:
    hookscripts = []
    # And sinker!
    hookline = config['pull'].get(hookname, '')
    for rawscript in hookline.splitlines():
        # Deliberately os.path.expanduser() and not Path().expanduser(): this
        # is a whole command line, arguments and all, not a path. Round-tripping
        # it through Path would rewrite the arguments too -- '//' collapsed, a
        # trailing slash dropped -- and only the leading '~' needs expanding.
        hookscript = os.path.expanduser(rawscript.strip())  # noqa: PTH111
        args = shlex.split(hookscript)
        if not args:
            continue
        if not os.access(args[0], os.X_OK):
            logger.warning('hook not executable: %s', hookscript)
            continue
        hookscripts.append(args)
    return hookscripts


def run_post_clone_complete_hook(config: grokmirror.GrokConfigParser, clones: list[str]) -> None:
    stdin = '\n'.join(clones) + '\n'
    hookscripts = get_hookscripts(config, 'post_clone_complete_hook')
    for args in hookscripts:
        logger.info(' inithook: %s', ' '.join(args))
        logger.debug('Running: %s', ' '.join(args))
        logger.debug('Stdin: ---start---')
        logger.debug(stdin)
        logger.debug('Stdin: ---end---')
        _ecode, output, error = grokmirror.run_shell_command(args, stdin=stdin.encode())
        if error:
            logger.warning('Hook Stderr: %s', error)
        if output:
            logger.info('Hook Stdout: %s', output)


def run_post_work_complete_hook(config: grokmirror.GrokConfigParser) -> None:
    hookscripts = get_hookscripts(config, 'post_work_complete_hook')
    for args in hookscripts:
        logger.info(' workhook: %s', ' '.join(args))
        logger.debug('Running: %s', ' '.join(args))
        _ecode, output, error = grokmirror.run_shell_command(args)
        if error:
            logger.warning('Hook Stderr: %s', error)
        if output:
            logger.info('Hook Stdout: %s', output)


def run_post_update_hook(config: grokmirror.GrokConfigParser, fullpath: grokmirror.StrPath) -> None:
    hookscripts = get_hookscripts(config, 'post_update_hook')
    for args in hookscripts:
        logger.info('     hook: %s', ' '.join(args))
        # os.fspath(): the hook argv is logged with ' '.join()
        args.append(os.fspath(fullpath))
        logger.debug('Running: %s', ' '.join(args))
        _ecode, output, error = grokmirror.run_shell_command(args)
        if error:
            logger.warning('Hook Stderr (%s): %s', fullpath, error)
        if output:
            logger.info('Hook Stdout (%s): %s', fullpath, output)


def pull_repo(fullpath: grokmirror.StrPath, remotename: str) -> bool:
    args = ['remote', 'update', remotename, '--prune']

    retcode, _output, error = grokmirror.run_git_command(fullpath, args, timeout=grokmirror.REMOTE_TIMEOUT)

    success = False
    if retcode == 0:
        success = True

    if error:
        # Put things we recognize into debug
        debug = []
        warn = []
        for line in error.splitlines():
            if line.startswith(('From ', 'remote: warning:')) or '-> ' in line or 'ControlSocket' in line:
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


def write_projects_list(config: grokmirror.GrokConfigParser, manifest: grokmirror.Manifest) -> None:
    plpath = config['pull'].get('projectslist', '')
    if not plpath:
        return

    trimtop = config['pull'].get('projectslist_trimtop', '')
    add_symlinks = config.get_bool('pull', 'projectslist_symlinks', False)

    plfile = Path(plpath)
    (fd, tmpname) = tempfile.mkstemp(prefix=plfile.name, dir=plfile.parent)
    tmpfile = Path(tmpname)

    try:
        fh = os.fdopen(fd, 'wb', 0)
        for gitdir, repoinfo in manifest.items():
            # removeprefix() is a no-op both when trimtop is unset and when
            # this particular gitdir does not start with it.
            pgitdir = gitdir.removeprefix(trimtop)

            # Always remove leading slash, otherwise cgit breaks
            pgitdir = pgitdir.lstrip('/')
            fh.write(f'{pgitdir}\n'.encode())

            if add_symlinks and 'symlinks' in repoinfo:
                # Do the same for symlinks
                # XXX: Should make this configurable, perhaps
                for symlink in repoinfo['symlinks']:
                    # Same two steps as the gitdir above, spelled the same way
                    psymlink = symlink.removeprefix(trimtop).lstrip('/')
                    fh.write(f'{psymlink}\n'.encode())

        os.fsync(fd)
        fh.close()
        # mkstemp() always creates 0600, so put the umask back on
        tmpfile.chmod(grokmirror.file_mode())
        # Path.replace for an actually atomic swap; see write_manifest()
        tmpfile.replace(plfile)

    finally:
        # If something failed, don't leave tempfiles trailing around
        if tmpfile.exists():
            tmpfile.unlink()

    logger.info(' projlist: wrote %s', plpath)


def fetch_remote_manifest(
    ses: grokmirror.GrokSession,
    config: grokmirror.GrokConfigParser,
    nomtime: bool = False,
) -> grokmirror.Manifest | None:
    """Fetch the remote manifest, or None if it's unchanged since last time.

    Covers both ways of getting it: running manifest_command, or fetching the
    manifest URL (a local file:// or over HTTP, using If-Modified-Since/mtime
    to skip the work when nothing changed).
    """
    r_mani_cmd = config['remote'].get('manifest_command')

    if r_mani_cmd:
        # Same splitting and the same executability rule --config-check
        # applies, so a config it passed cannot fail here.
        try:
            cmdargs = configcheck.command_argv(r_mani_cmd)
        except ValueError as ex:
            logger.critical('Remote manifest command %s', ex)
            raise grokmirror.GrokManifestError(f'Remote manifest command {ex}') from ex
        logger.info(' manifest: executing %s', r_mani_cmd)
        if nomtime:
            cmdargs += ['--force']
        # This one usually crosses the network (e.g. ssh to a gitolite
        # server), so unlike the hooks it runs under the remote ceiling.
        (ecode, output, _error) = grokmirror.run_shell_command(cmdargs, timeout=grokmirror.REMOTE_TIMEOUT)
        if ecode == 0:
            try:
                r_manifest = json.loads(output)
            except json.JSONDecodeError as ex:
                logger.warning('Failed to parse output from %s', r_mani_cmd)
                logger.warning('Error was: %s', ex)
                raise grokmirror.GrokManifestError(f'Failed to parse output from {r_mani_cmd} ({ex})') from ex
        elif ecode == 127:
            logger.info(' manifest: unchanged')
            return None
        elif ecode == 1:
            logger.warning('Executing %s failed with exit code %s, exiting', r_mani_cmd, ecode)
            raise grokmirror.GrokManifestError(f'Failed executing {r_mani_cmd}')
        else:
            # Non-fatal errors for all other exit codes
            logger.warning(' manifest: executing %s returned %s', r_mani_cmd, ecode)
            return None

        if not r_manifest:
            logger.warning(' manifest: empty, ignoring')
            raise grokmirror.GrokManifestError(f'Empty manifest returned by {r_mani_cmd}')

        return cast('grokmirror.Manifest', r_manifest)

    l_mani_path = config['core']['manifest']
    l_mani_file = Path(l_mani_path)
    r_mani_status_path = l_mani_file.parent / f'.{l_mani_file.name}.remote'
    try:
        r_mani_status = json.loads(r_mani_status_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError):
        logger.debug('Could not read %s', r_mani_status_path)
        r_mani_status = {}
    r_last_fetched = r_mani_status.get('last-fetched', 0)
    config_last_modified = r_mani_status.get('config-last-modified', 0)
    if config_last_modified != config.last_modified:
        nomtime = True
    # No manifest_command, so pull_mirror() made sure we have a manifest URL
    r_mani_url = config['remote']['manifest']
    logger.info(' manifest: fetching %s', r_mani_url)
    if r_mani_url.startswith('file:///'):
        r_mani_url = r_mani_url.removeprefix('file://')
        if not Path(r_mani_url).exists():
            logger.critical('Remote manifest not found in %s! Quitting!', r_mani_url)
            raise grokmirror.GrokManifestError(f'Remote manifest not found in {r_mani_url}')

        fstat = Path(r_mani_url).stat()
        r_last_modified = fstat[8]
        if r_last_fetched:
            logger.debug('mtime on %s is: %s', r_mani_url, fstat[8])
            if not nomtime and r_last_modified <= r_last_fetched:
                logger.info(' manifest: unchanged')
                return None

        logger.info('Reading new manifest from %s', r_mani_url)
        r_manifest = grokmirror.read_manifest(r_mani_url)
        # Don't accept empty manifests -- that indicates something is wrong
        if not r_manifest:
            logger.warning('Remote manifest empty or unparseable! Quitting.')
            raise grokmirror.GrokManifestError(f'Empty manifest in {r_mani_url}')

    else:
        session = ses.get_requests_session()

        # Find out if we need to run at all first
        headers = {}
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
            raise grokmirror.GrokManifestError(f'Remote server returned an error: {ex}') from ex

        if res.status_code == 304:
            # No change to the manifest, nothing to do
            logger.info(' manifest: unchanged')
            return None

        if res.status_code > 200:
            logger.warning('Could not fetch %s', r_mani_url)
            logger.warning('Server returned status: %s', res.status_code)
            raise grokmirror.GrokManifestError(f'Remote server returned an error: {res.status_code}')

        r_mtime = time.strptime(res.headers['Last-Modified'], '%a, %d %b %Y %H:%M:%S %Z')
        r_last_modified = calendar.timegm(r_mtime)

        # We don't use read_manifest for the remote manifest, as it can be
        # anything, really. For now, blindly open it with gzipfile if it ends
        # with .gz. XXX: some http servers will auto-deflate such files.
        jdata: str | bytes
        try:
            if r_mani_url.endswith('.gz'):
                with gzip.GzipFile(fileobj=io.BytesIO(res.content)) as gzfh:
                    jdata = gzfh.read().decode()
            else:
                jdata = res.content

            res.close()
            # Don't hold the session open, since we don't refetch the
            # manifest very frequently. The session object forgets the
            # closed requests.Session, so a later call starts a fresh one.
            ses.close_requests_session()
            r_manifest = json.loads(jdata)

        # Deliberately broad: anything at all going wrong while fetching or
        # decoding the manifest is reported as a single error below.
        except Exception as ex:
            logger.warning('Failed to parse %s', r_mani_url)
            logger.warning('Error was: %s', ex)
            raise grokmirror.GrokManifestError(f'Failed to parse {r_mani_url} ({ex})') from ex

    # Record for the next run
    with r_mani_status_path.open('w', encoding='utf-8') as fh:
        r_mani_status = {
            'source': r_mani_url,
            'last-fetched': r_last_modified,
            'config-last-modified': config.last_modified,
        }
        json.dump(r_mani_status, fh)

    return cast('grokmirror.Manifest', r_manifest)


def purge_stale_repos(
    ses: grokmirror.GrokSession,
    config: grokmirror.GrokConfigParser,
    toplevel: str,
    r_culled: grokmirror.Manifest,
    all_symlinks: set[str],
    forcepurge: bool,
    q_mani: queue.Queue[ManiItem],
) -> None:
    """Queue for purging any repo on disk that the remote manifest no longer lists.

    Two protections apply first: ffonly repos are never purged, no matter
    what, and purgeprotect refuses the whole batch (unless overridden with
    forcepurge) when the deletions are a large enough fraction of the tree to
    look like a mistake rather than a deliberate cleanup.
    """
    if not config.get_bool('pull', 'purge', False):
        return

    nopurgematch = grokmirror.compile_globs(config['pull'].get('nopurge', '').splitlines())
    ffonlymatch = grokmirror.compile_globs(config['pull'].get('ffonly', '').splitlines())
    to_purge = set()
    found_repos = 0
    for founddir in ses.find_all_gitdirs(toplevel, exclude_objstore=True):
        gitdir = grokmirror.fullpath_to_gitdir(toplevel, founddir)
        found_repos += 1

        if gitdir not in r_culled and gitdir not in all_symlinks:
            # Refuse to purge ffonly repos
            if ffonlymatch.match(gitdir):
                # Woah, these are not supposed to be deleted, ever
                logger.critical('Refusing to purge ffonly repo %s', gitdir)
            elif not nopurgematch.match(gitdir):
                logger.debug('Adding %s to to_purge', gitdir)
                to_purge.add(gitdir)

    if not to_purge:
        logger.debug('No repositories need purging')
        return

    # Purge-protection engage
    purge_limit = config.get_int('pull', 'purgeprotect', 5)
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
        return

    for gitdir in to_purge:
        logger.debug('Queued %s for purging', gitdir)
        # An empty entry: there is nothing left on disk to describe, and a
        # purge only ever needs the path.
        q_mani.put((gitdir, {}, 'purge'))


def fill_todo_from_manifest(
    ses: grokmirror.GrokSession,
    config: grokmirror.GrokConfigParser,
    q_mani: queue.Queue[ManiItem],
    nomtime: bool = False,
    forcepurge: bool = False,
) -> None:
    r_manifest = fetch_remote_manifest(ses, config, nomtime)
    if r_manifest is None:
        return

    # l_ = local, r_ = remote
    l_mani_path = config['core']['manifest']
    l_manifest = grokmirror.read_manifest(l_mani_path)
    r_culled = cull_manifest(r_manifest, config)
    logger.info(' manifest: %s relevant entries', len(r_culled))

    toplevel = os.path.realpath(config['core']['toplevel'])

    obstdir = os.path.realpath(config['core']['objstore'])
    forkgroups = build_optimal_forkgroups(l_manifest, r_culled, toplevel, obstdir)
    privmatch = grokmirror.compile_globs(config['core'].get('private', '').splitlines())
    ignorerefs = grokmirror.get_ignorerefs(config)

    # populate private/forkgroup info in r_culled
    for fg, siblings in forkgroups.items():
        for s_fullpath in siblings:
            s_gitdir = grokmirror.fullpath_to_gitdir(toplevel, s_fullpath)
            if s_gitdir in r_culled:
                r_culled[s_gitdir]['forkgroup'] = fg
                r_culled[s_gitdir]['private'] = bool(privmatch.match(s_gitdir))

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
        fullpath = grokmirror.gitdir_to_fullpath(toplevel, gitdir)
        forkgroup = repoinfo.get('forkgroup')

        # Is the directory in place?
        if fullpath.exists():
            # Did grok-fsck request to reclone it?
            rfile = fullpath / 'grokmirror.reclone'
            if rfile.exists():
                logger.debug('Reclone requested for %s:', gitdir)
                q_mani.put((gitdir, repoinfo, 'reclone'))
                reason = rfile.read_text(encoding='utf-8')
                logger.debug('  %s', reason)
                continue

            if gitdir not in l_manifest:
                q_mani.put((gitdir, repoinfo, 'fix_remotes'))
                continue

            r_desc = repoinfo.get('description')
            r_owner = repoinfo.get('owner')
            r_head = repoinfo.get('head')

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
                    linkpath = grokmirror.gitdir_to_fullpath(toplevel, symlink)
                    # os.fspath(), because realpath() returns a str and a Path
                    # is never equal to one: see pull_worker()
                    if not linkpath.is_symlink() or os.path.realpath(linkpath) != os.fspath(fullpath):
                        q_mani.put((gitdir, repoinfo, 'fix_params'))
                        break

            my_fingerprint = grokmirror.get_repo_fingerprint(toplevel, gitdir, ignorerefs=ignorerefs)
            if my_fingerprint != l_manifest[gitdir].get('fingerprint'):
                logger.debug('Fingerprint discrepancy, forcing a fetch')
                q_mani.put((gitdir, repoinfo, 'pull'))
                continue

            if my_fingerprint == repoinfo.get('fingerprint'):
                logger.debug('Fingerprints match, skipping %s', gitdir)
                continue

            logger.debug('No fingerprint match, will pull %s', gitdir)
            q_mani.put((gitdir, repoinfo, 'pull'))
            continue

        if not forkgroup:
            # no-sibling repo
            q_mani.put((gitdir, repoinfo, 'init'))
            continue

        obstrepo = Path(obstdir, f'{forkgroup}.git')
        if obstrepo.is_dir():
            # Init with an existing obstrepo, easy case
            q_mani.put((gitdir, repoinfo, 'init'))
            continue

        # Do we have any existing siblings that were cloned without obstrepo?
        # This would happen when an initial fork is created of an existing repo.
        found_existing = False
        public_siblings = set()
        for s_fullpath in forkgroups[forkgroup]:
            s_gitdir = grokmirror.fullpath_to_gitdir(toplevel, s_fullpath)
            if s_gitdir == gitdir:
                continue

            # can't simply rely on r_culled 'private' info, as this repo may only exist locally
            if privmatch.match(s_gitdir):
                # Can't use this sibling for anything, as it's private
                continue

            if Path(s_fullpath).is_dir():
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

        if repoinfo.get('private') and public_siblings:
            # Clone public siblings first
            for s_gitdir in public_siblings:
                if s_gitdir not in seen:
                    q_mani.put((s_gitdir, r_culled[s_gitdir], 'init'))
                    seen.add(s_gitdir)
        # Finally, clone ourselves.
        q_mani.put((gitdir, repoinfo, 'init'))

    purge_stale_repos(ses, config, toplevel, r_culled, all_symlinks, forcepurge, q_mani)


def update_manifest(config: grokmirror.GrokConfigParser, entries: list[DoneItem]) -> None:
    manifile = config['core']['manifest']
    with grokmirror.locked_manifest(manifile):
        manifest = grokmirror.read_manifest(manifile)
        changed = False
        while entries:
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

            # Our own local judgement about the mirror's config; it does not
            # belong in the manifest we publish.
            repoinfo.pop('private', None)
            # Clean up grok-2.0 null values
            if repoinfo.get('head') is None:
                repoinfo.pop('head', None)
            if repoinfo.get('forkgroup') is None:
                repoinfo.pop('forkgroup', None)
            # Make sure 'reference' is present to prevent grok-1.x breakage
            if 'reference' not in repoinfo:
                repoinfo['reference'] = None
            manifest[gitdir] = repoinfo
            changed = True
        if changed:
            pretty = config.get_bool('manifest', 'pretty', False)
            grokmirror.write_manifest(manifile, manifest, pretty=pretty)
            logger.info(' manifest: wrote %s (%d entries)', manifile, len(manifest))
            # write out projects.list, if asked to
            write_projects_list(config, manifest)


def socket_worker(server: ThreadedUnixStreamServer, sockfile: str) -> None:
    # The socket itself was bound by pull_mirror(), before any workers exist.
    logger.info(' listener: listening on socket %s', sockfile)
    with server:
        server.serve_forever()


def showstats(waiting: int, queued: int, active: int, spa: int, good: int, bad: int) -> None:
    stats = []
    if good:
        stats.append(f'{good} fetched')
    if active:
        stats.append(f'{active} active')
    if queued:
        stats.append(f'{queued} queued')
    if waiting:
        stats.append(f'{waiting} waiting')
    if spa:
        stats.append(f'{spa} in spa')
    if bad:
        stats.append(f'{bad} errors')

    logger.info('      ---:  %s', ', '.join(stats))


def manifest_worker(
    ses: grokmirror.GrokSession,
    config: grokmirror.GrokConfigParser,
    q_mani: queue.Queue[ManiItem],
    nomtime: bool = False,
) -> None:
    starttime = int(time.time())
    try:
        fill_todo_from_manifest(ses, config, q_mani, nomtime=nomtime)
    except (OSError, grokmirror.GrokManifestError) as ex:
        # Whatever went wrong was already logged in detail where it happened,
        # so just say so and fall through to the usual pacing below: a broken
        # origin must not turn this into a hot retry loop.
        logger.critical('Could not get the remote manifest: %s', ex)
    refresh = config.get_int('pull', 'refresh', 300)
    left = refresh - int(time.time() - starttime)
    if left > 0:
        logger.info(' manifest: sleeping %ss', left)


def validate_pull_config(config: grokmirror.GrokConfigParser) -> bool:
    """Check that [remote] is complete enough for pull_mirror() to run.

    Every worker started below assumes these are set, so check plainly here
    instead of crashing much later inside a worker with a TypeError.

    The rule itself lives in configcheck, which is also what --config-check
    reports, so the two cannot drift apart and tell an admin different
    things about the same file.
    """
    diagnostics = configcheck.check_remote_completeness(config)
    for diag in diagnostics:
        logger.critical('Section [%s] %s', diag.section, diag.message)
    return not diagnostics


def start_socket_listener(config: grokmirror.GrokConfigParser, q_mani: queue.Queue[ManiItem]) -> None:
    """Start the daemon socket listener that lets other processes queue work.

    See contrib/pubsubv1.py for a producer that talks to this socket.
    """
    sockfile = config['pull'].get('socket')
    if not sockfile:
        return

    if Path(sockfile).exists():
        mode = Path(sockfile).stat().st_mode
        if stat.S_ISSOCK(mode):
            Path(sockfile).unlink()
        else:
            raise grokmirror.GrokError(f'File exists but is not a socket: {sockfile}')

    server = ThreadedUnixStreamServer(sockfile, Handler)
    # Deliberately world-writable: anyone able to reach the socket may ask
    # the daemon to check a repository. Set after the bind rather than by
    # zeroing the process umask, which would have applied to every other
    # file the process creates for as long as the window was open.
    Path(sockfile).chmod(0o777)
    # Stick some objects into the server
    server.q_mani = q_mani
    server.config = config
    listener = threading.Thread(target=socket_worker, args=(server, sockfile), daemon=True)
    listener.start()


def pull_mirror(
    config: grokmirror.GrokConfigParser, nomtime: bool = False, forcepurge: bool = False, runonce: bool = False
) -> int:
    if not validate_pull_config(config):
        return 1
    if 'pull' not in config:
        # Same as in grok_pull(), for the benefit of anyone calling us directly:
        # all of [pull] is optional, but the section must be there to read from.
        config['pull'] = {}

    toplevel = os.path.realpath(config['core']['toplevel'])
    obstdir = os.path.realpath(config['core']['objstore'])
    refresh = config.get_int('pull', 'refresh', 300)

    # The worker threads all share this, so the alternates map only gets
    # walked once for everybody.
    ses = grokmirror.GrokSession()

    q_mani: queue.Queue[ManiItem] = queue.Queue()
    q_spa: queue.Queue[SpaItem | None] = queue.Queue()

    if not runonce:
        start_socket_listener(config, q_mani)

    # Run in the main thread if we have runonce
    if runonce:
        try:
            fill_todo_from_manifest(ses, config, q_mani, nomtime=nomtime, forcepurge=forcepurge)
        except (OSError, grokmirror.GrokManifestError) as ex:
            # Already logged in detail. A mirror run from cron should report the
            # problem and exit non-zero, not print a traceback at the admin.
            logger.critical('Could not get the remote manifest: %s', ex)
            return 1
        if not q_mani.qsize():
            return 0
    else:
        # force nomtime to True the first time
        nomtime = True
    lastrun = 0

    pull_threads = config.get_int('pull', 'pull_threads', 0)
    if pull_threads < 1:
        # take half of available CPUs by default
        pull_threads = max(1, (os.cpu_count() or 1) // 2)

    # The spa thread lives for the whole run: a None sentinel tells it to
    # exit, and q_spa.join() waits out everything that was queued.
    dw = threading.Thread(target=spa_worker, args=(config, q_spa, not runonce), daemon=True)
    dw.start()

    pool = futures.ThreadPoolExecutor(max_workers=pull_threads, thread_name_prefix='pull_worker')
    # Submitted but unfinished work, mapping each future back to the item it
    # carries: a repository locked by another process goes back in line.
    pending: dict[futures.Future[bool | None], WorkItem] = {}
    # Work that is ready to be handed to the pool. Plain supervisor-local
    # state: everything is put here by the loop below.
    todo: deque[tuple[str, grokmirror.RepoInfo, str]] = deque()
    mws: list[threading.Thread] = []
    actions: set[tuple[str, str]] = set()
    busy: set[str] = set()
    done: list[DoneItem] = []
    cloned: list[str] = []
    good = 0
    bad = 0
    post_clone_hook = config['pull'].get('post_clone_complete_hook')
    post_work_hook = config['pull'].get('post_work_complete_hook')

    def enqueue(gitdir: str, repoinfo: grokmirror.RepoInfo, action: str) -> bool:
        if (gitdir, action) in actions:
            logger.debug('already in the queue: %s, %s', gitdir, action)
            return False
        if action == 'pull' and (gitdir, 'init') in actions:
            logger.debug('already in the queue as init: %s, %s', gitdir, action)
            return False
        actions.add((gitdir, action))
        todo.append((gitdir, repoinfo, action))
        logger.debug('queued: %s, %s', gitdir, action)
        return True

    def submit(item: WorkItem) -> None:
        pending[pool.submit(pull_worker, ses, config, item, q_spa)] = item

    def show_pool_stats() -> None:
        active = sum(1 for f in pending if f.running())
        showstats(len(todo), len(pending) - active, active, q_spa.unfinished_tasks, good, bad)

    def note_done(gitdir: str, repoinfo: grokmirror.RepoInfo, q_action: str, success: bool) -> None:
        nonlocal good, bad, cloned
        actions.discard((gitdir, q_action))
        # Was it a clone, and are all other clones done?
        if post_clone_hook and q_action == 'init':
            # str(), because the list is handed to the hook as its stdin
            cloned.append(str(grokmirror.gitdir_to_fullpath(toplevel, gitdir)))
            more_clones = False
            for _qgd, qqa in actions:
                if qqa == 'init':
                    more_clones = True
                    break
            if not more_clones:
                # Fire the post_clone hook
                run_post_clone_complete_hook(config, cloned)
                cloned = []

        forkgroup = repoinfo.get('forkgroup')
        if forkgroup and forkgroup in busy:
            busy.remove(forkgroup)
        done.append((gitdir, repoinfo, q_action, success))
        if success:
            good += 1
        else:
            bad += 1
        logger.info('     done: %s', gitdir)
        show_pool_stats()
        if len(done) >= 100:
            # Write manifest every 100 repos
            update_manifest(config, done)

    def dispatch_todo_item(
        gitdir: str,
        repoinfo: grokmirror.RepoInfo,
        q_action: str,
        held: deque[tuple[str, grokmirror.RepoInfo, str]],
    ) -> None:
        """Act on one item popped off `todo`, or put it back on `held`.

        Priority logic lives only in the 'init' case below: an easy action
        (pull, reclone, fix_remotes, fix_params, purge, ...) is submitted as
        soon as its forkgroup is free, but a fresh clone has to decide
        whether to join an existing obstrepo or set up a new one first.
        """
        fullpath = grokmirror.gitdir_to_fullpath(toplevel, gitdir)
        forkgroup = repoinfo.get('forkgroup')
        if gitdir in busy or (forkgroup is not None and forkgroup in busy):
            # Hold it until the repository blocking it is done
            held.append((gitdir, repoinfo, q_action))
            return

        if q_action == 'objstore_migrate':
            if not forkgroup:
                # fill_todo_from_manifest() only ever queues a migration for a
                # repository it has just given a forkgroup to, so this should
                # not be reachable -- but losing the whole daemon to a
                # KeyError over it would be worse.
                logger.critical('No forkgroup for %s, skipping objstore migration', gitdir)
                note_done(gitdir, repoinfo, q_action, False)
                return
            # Add forkgroup to busy, so we don't run any pulls until it's done
            busy.add(forkgroup)
            obstrepo = grokmirror.setup_objstore_repo(obstdir, name=forkgroup)
            grokmirror.add_repo_to_objstore(obstrepo, fullpath)
            grokmirror.set_altrepo(fullpath, obstrepo)

        if q_action != 'init':
            # Easy actions that don't require priority logic
            submit((gitdir, repoinfo, q_action, q_action))
            return

        try:
            with grokmirror.locked_repo(fullpath, nonblocking=True):
                if not grokmirror.setup_bare_repo(fullpath):
                    logger.critical('Unable to bare-init %s', fullpath)
                    note_done(gitdir, repoinfo, q_action, False)
                    return

                fix_remotes(toplevel, gitdir, config['remote']['site'], config)
                set_repo_params(fullpath, repoinfo)
        except grokmirror.GrokLockError:
            if not runonce:
                held.append((gitdir, repoinfo, q_action))
            return

        forkgroup = repoinfo.get('forkgroup')
        if not forkgroup:
            logger.debug('no-sibling clone: %s', gitdir)
            submit((gitdir, repoinfo, 'pull', q_action))
            return

        # str(), to match what setup_objstore_repo() returns above
        obstrepo = str(Path(obstdir, f'{forkgroup}.git'))
        if Path(obstrepo).is_dir():
            logger.debug('clone %s with existing obstrepo %s', gitdir, obstrepo)
            grokmirror.set_altrepo(fullpath, obstrepo)
            if not repoinfo.get('private'):
                grokmirror.add_repo_to_objstore(obstrepo, fullpath)
            submit((gitdir, repoinfo, 'pull', q_action))
            return

        # Set up a new obstrepo and make sure it's not used until the initial
        # pull is done
        logger.debug('cloning %s with new obstrepo %s', gitdir, obstrepo)
        busy.add(forkgroup)
        obstrepo = grokmirror.setup_objstore_repo(obstdir, name=forkgroup)
        grokmirror.set_altrepo(fullpath, obstrepo)
        if not repoinfo.get('private'):
            grokmirror.add_repo_to_objstore(obstrepo, fullpath)
        submit((gitdir, repoinfo, 'pull', q_action))

    with SignalHandler(config, done):
        while True:
            # Any new results?
            for fut in [fut for fut in pending if fut.done()]:
                (gitdir, repoinfo, action, q_action) = pending.pop(fut)
                try:
                    success = fut.result()
                # Deliberately broad: an unexpected failure gets logged with its
                # traceback and counted against the run instead of silently
                # wedging this repository until the daemon is restarted.
                except Exception:
                    logger.exception('unexpected failure while processing %s', gitdir)
                    success = False
                if success is None:
                    # The repository is locked by another process; the worker
                    # already took a nap, so it goes straight back in line.
                    submit((gitdir, repoinfo, action, q_action))
                    continue
                note_done(gitdir, repoinfo, q_action, success)

            # Anything new in the manifest queue?
            new_updates = 0
            while True:
                try:
                    (gitdir, repoinfo, action) = q_mani.get_nowait()
                except queue.Empty:
                    break
                if enqueue(gitdir, repoinfo, action):
                    new_updates += 1
            if new_updates:
                logger.info(' manifest: %s new updates', new_updates)

            # Time to refetch the remote manifest?
            mws = [mw for mw in mws if mw.is_alive()]
            if not runonce and not mws and not todo and not pending and time.time() - lastrun >= refresh:
                if done:
                    update_manifest(config, done)
                    if post_work_hook:
                        run_post_work_complete_hook(config)
                mw = threading.Thread(target=manifest_worker, args=(ses, config, q_mani, nomtime), daemon=True)
                nomtime = False
                mw.start()
                mws.append(mw)
                lastrun = int(time.time())

            # Finally, deal with the todo list
            held: deque[tuple[str, grokmirror.RepoInfo, str]] = deque()
            while todo:
                (gitdir, repoinfo, q_action) = todo.popleft()
                dispatch_todo_item(gitdir, repoinfo, q_action, held)
            todo.extend(held)

            if pending:
                # Block until at least one worker is done, but check in on the
                # other queues at least once a second.
                futures.wait(list(pending), timeout=1, return_when=futures.FIRST_COMPLETED)
                continue

            # Nothing in flight; is more work already waiting?
            if q_mani.qsize():
                continue

            if not todo:
                if done:
                    update_manifest(config, done)
                    if post_work_hook:
                        run_post_work_complete_hook(config)
                if runonce:
                    # Wait till spa is done, then wrap up
                    q_spa.put(None)
                    q_spa.join()
                    dw.join()
                    pool.shutdown()
                    return 0

            # Wait for the listener or the next manifest refresh. Everything in
            # the todo list at this point is waiting out a lock held by another
            # process, so look in on those every 5 seconds.
            waittime = refresh - (time.time() - lastrun)
            if todo:
                waittime = min(waittime, 5)
            try:
                (gitdir, repoinfo, action) = q_mani.get(timeout=max(1, waittime))
            except queue.Empty:
                continue
            enqueue(gitdir, repoinfo, action)


def parse_args() -> argparse.Namespace:

    # noinspection PyTypeChecker
    op = argparse.ArgumentParser(
        prog='grok-pull',
        description='Create or update a git repository collection mirror',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    op.add_argument(
        '-v',
        '--verbose',
        dest='verbose',
        action='store_true',
        default=False,
        help='Be verbose and tell us what you are doing',
    )
    op.add_argument(
        '-n',
        '--no-mtime-check',
        dest='nomtime',
        action='store_true',
        default=False,
        help='Run without checking manifest mtime',
    )
    op.add_argument(
        '-p',
        '--purge',
        dest='purge',
        action='store_true',
        default=False,
        help='Remove any git trees that are no longer in manifest',
    )
    op.add_argument(
        '--force-purge',
        dest='forcepurge',
        action='store_true',
        default=False,
        help='Force purge despite significant repo deletions',
    )
    op.add_argument(
        '-o',
        '--continuous',
        dest='runonce',
        action='store_false',
        default=True,
        help='Run continuously (no effect if refresh is not set in config)',
    )
    op.add_argument('-c', '--config', dest='config', required=True, help='Location of the configuration file')
    configcheck.add_check_arguments(op)
    op.add_argument('--version', action='version', version=grokmirror.VERSION)

    opts = op.parse_args()
    configcheck.check_arguments(op, opts)
    return opts


def grok_pull(
    cfgfile: str,
    verbose: bool = False,
    nomtime: bool = False,
    purge: bool = False,
    forcepurge: bool = False,
    runonce: bool = False,
) -> int:
    config = grokmirror.load_config_file(cfgfile)
    if 'pull' not in config:
        # Every setting in [pull] has a default, so mirroring without the
        # section is perfectly fine -- but it has to exist for the lookups
        # here and in the workers, which otherwise raise KeyError.
        config['pull'] = {}
    if 'refresh' not in config['pull']:
        # Asking whether it is set, not what it is set to -- the value itself
        # is read with get_int() where it is actually used.
        runonce = True

    logfile = config['core'].get('log', None)
    loglevel = logging.DEBUG if config['core'].get('loglevel', 'info') == 'debug' else logging.INFO

    if purge:
        # Override the pull.purge setting
        config['pull']['purge'] = 'yes'

    grokmirror.init_logger('pull', logfile, loglevel, verbose)

    return pull_mirror(config, nomtime, forcepurge, runonce)


# The sections grok-pull reads. [fsck] and [manifest] are real sections it
# has no business opining about, so they are recognised but not checked.
CHECKED_SECTIONS = {'core', 'remote', 'pull'}


def command() -> None:
    opts = parse_args()

    if opts.config_check:
        sys.exit(configcheck.run_check(opts.config, CHECKED_SECTIONS, as_json=opts.as_json, online=not opts.no_network))

    try:
        retval = grok_pull(opts.config, opts.verbose, opts.nomtime, opts.purge, opts.forcepurge, opts.runonce)
    except grokmirror.GrokError as ex:
        sys.stderr.write(f'ERROR: {ex}\n')
        retval = 1

    sys.exit(retval)


if __name__ == '__main__':
    command()
