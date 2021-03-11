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
import logging
import datetime

import grokmirror

logger = logging.getLogger(__name__)

objstore_uses_plumbing = False


def update_manifest(manifest, toplevel, fullpath, usenow, ignorerefs):
    logger.debug('Examining %s', fullpath)
    if not grokmirror.is_bare_git_repo(fullpath):
        logger.critical('Error opening %s.', fullpath)
        logger.critical('Make sure it is a bare git repository.')
        sys.exit(1)

    gitdir = '/' + os.path.relpath(fullpath, toplevel)
    repoinfo = grokmirror.get_repo_defs(toplevel, gitdir, usenow=usenow, ignorerefs=ignorerefs)
    # Ignore it if it's an empty git repository
    if not repoinfo['fingerprint']:
        logger.info(' manifest: ignored %s (no heads)', gitdir)
        return

    if gitdir not in manifest:
        # In grokmirror-1.x we didn't normalize paths to be always with a leading '/', so
        # check the manifest for both and make sure we only save the path with a leading /
        if gitdir.lstrip('/') in manifest:
            manifest[gitdir] = manifest.pop(gitdir.lstrip('/'))
            logger.info(' manifest: updated %s', gitdir)
        else:
            logger.info(' manifest: added %s', gitdir)
            manifest[gitdir] = dict()
    else:
        logger.info(' manifest: updated %s', gitdir)

    altrepo = grokmirror.get_altrepo(fullpath)
    reference = None
    if manifest[gitdir].get('forkgroup', None) != repoinfo.get('forkgroup', None):
        # Use the first remote listed in the forkgroup as our reference, just so
        # grokmirror-1.x clients continue to work without doing full clones
        remotes = grokmirror.list_repo_remotes(altrepo, withurl=True)
        if len(remotes):
            urls = list(x[1] for x in remotes)
            urls.sort()
            reference = '/' + os.path.relpath(urls[0], toplevel)
    else:
        reference = manifest[gitdir].get('reference', None)

    if altrepo and not reference and not repoinfo.get('forkgroup'):
        # Not an objstore repo
        reference = '/' + os.path.relpath(altrepo, toplevel)

    manifest[gitdir].update(repoinfo)
    # Always write a reference entry even if it's None, as grok-1.x clients expect it
    manifest[gitdir]['reference'] = reference


def set_symlinks(manifest, toplevel, symlinks):
    for symlink in symlinks:
        target = os.path.realpath(symlink)
        if not os.path.exists(target):
            logger.critical(' manifest: symlink %s is broken, ignored', symlink)
            continue
        relative = '/' + os.path.relpath(symlink, toplevel)
        if target.find(toplevel) < 0:
            logger.critical(' manifest: symlink %s points outside toplevel, ignored', relative)
            continue
        tgtgitdir = '/' + os.path.relpath(target, toplevel)
        if tgtgitdir not in manifest:
            logger.critical(' manifest: symlink %s points to %s, which we do not recognize', relative, tgtgitdir)
            continue
        if 'symlinks' in manifest[tgtgitdir]:
            if relative not in manifest[tgtgitdir]['symlinks']:
                logger.info(' manifest: symlinked %s->%s', relative, tgtgitdir)
                manifest[tgtgitdir]['symlinks'].append(relative)
            else:
                logger.info(' manifest: %s->%s is already in manifest', relative, tgtgitdir)
        else:
            manifest[tgtgitdir]['symlinks'] = [relative]
            logger.info(' manifest: symlinked %s->%s', relative, tgtgitdir)

        # Now go through all repos and fix any references pointing to the
        # symlinked location. We shouldn't need to do anything with forkgroups.
        for gitdir in manifest:
            if manifest[gitdir] == relative:
                logger.info(' manifest: removing %s (replaced by a symlink)', gitdir)
                manifest.pop(gitdir)
                continue
            if manifest[gitdir]['reference'] == relative:
                logger.info(' manifest: symlinked %s->%s', relative, tgtgitdir)
                manifest[gitdir]['reference'] = tgtgitdir


def purge_manifest(manifest, toplevel, gitdirs):
    for oldrepo in list(manifest):
        if os.path.join(toplevel, oldrepo.lstrip('/')) not in gitdirs:
            logger.info(' manifest: purged %s (gone)', oldrepo)
            manifest.remove(oldrepo)


def parse_args():
    global objstore_uses_plumbing

    import argparse
    # noinspection PyTypeChecker
    op = argparse.ArgumentParser(prog='grok-manifest',
                                 description='Create or update a manifest file',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    op.add_argument('--cfgfile', dest='cfgfile',
                    default=None,
                    help='Path to grokmirror.conf containing at least a [core] section')
    op.add_argument('-m', '--manifest', dest='manifile',
                    help='Location of manifest.js or manifest.js.gz')
    op.add_argument('-t', '--toplevel', dest='toplevel',
                    help='Top dir where all repositories reside')
    op.add_argument('-l', '--logfile', dest='logfile',
                    default=None,
                    help='When specified, will put debug logs in this location')
    op.add_argument('-n', '--use-now', dest='usenow', action='store_true',
                    default=False,
                    help='Use current timestamp instead of parsing commits')
    op.add_argument('-c', '--check-export-ok', dest='check_export_ok',
                    action='store_true', default=False,
                    help='Export only repositories marked as git-daemon-export-ok')
    op.add_argument('-p', '--purge', dest='purge', action='store_true',
                    default=False,
                    help='Purge deleted git repositories from manifest')
    op.add_argument('-x', '--remove', dest='remove', action='store_true',
                    default=False,
                    help='Remove repositories passed as arguments from manifest')
    op.add_argument('-y', '--pretty', dest='pretty', action='store_true',
                    default=False,
                    help='Pretty-print manifest (sort keys and add indentation)')
    op.add_argument('-i', '--ignore-paths', dest='ignore', action='append',
                    default=None,
                    help='When finding git dirs, ignore these paths (accepts shell-style globbing)')
    op.add_argument('-r', '--ignore-refs', dest='ignore_refs', action='append', default=None,
                    help='Refs to exclude from fingerprint calculation (e.g. refs/meta/*)')
    op.add_argument('-w', '--wait-for-manifest', dest='wait',
                    action='store_true', default=False,
                    help='When running with arguments, wait if manifest is not there '
                         '(can be useful when multiple writers are writing the manifest)')
    op.add_argument('-o', '--fetch-objstore', dest='fetchobst',
                    action='store_true', default=False,
                    help='Fetch updates into objstore repo (if used)')
    op.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                    default=False,
                    help='Be verbose and tell us what you are doing')
    op.add_argument('--version', action='version', version=grokmirror.VERSION)
    op.add_argument('paths', nargs='*', help='Full path(s) to process')

    opts = op.parse_args()

    if opts.cfgfile:
        config = grokmirror.load_config_file(opts.cfgfile)
        if not opts.manifile:
            opts.manifile = config['core'].get('manifest')
        if not opts.toplevel:
            opts.toplevel = os.path.realpath(config['core'].get('toplevel'))
        if not opts.logfile:
            opts.logfile = config['core'].get('logfile')

        objstore_uses_plumbing = config['core'].getboolean('objstore_uses_plumbing', False)

        if 'manifest' in config:
            if not opts.ignore:
                opts.ignore = [x.strip() for x in config['manifest'].get('ignore', '').split('\n')]
            if not opts.check_export_ok:
                opts.check_export_ok = config['manifest'].getboolean('check_export_ok', False)
            if not opts.pretty:
                opts.pretty = config['manifest'].getboolean('pretty', False)
            if not opts.fetchobst:
                opts.fetchobst = config['manifest'].getboolean('fetch_objstore', False)

    if not opts.manifile:
        op.error('You must provide the path to the manifest file')
    if not opts.toplevel:
        op.error('You must provide the toplevel path')
    if opts.ignore is None:
        opts.ignore = list()

    if not len(opts.paths) and opts.wait:
        op.error('--wait option only makes sense when dirs are passed')

    return opts


def grok_manifest(manifile, toplevel, paths=None, logfile=None, usenow=False,
                  check_export_ok=False, purge=False, remove=False,
                  pretty=False, ignore=None, wait=False, verbose=False, fetchobst=False,
                  ignorerefs=None):
    global logger
    loglevel = logging.INFO
    logger = grokmirror.init_logger('manifest', logfile, loglevel, verbose)

    startt = datetime.datetime.now()
    if paths is None:
        paths = list()
    if ignore is None:
        ignore = list()

    grokmirror.manifest_lock(manifile)
    manifest = grokmirror.read_manifest(manifile, wait=wait)

    toplevel = os.path.realpath(toplevel)

    # If manifest is empty, don't use current timestamp
    if not len(manifest):
        usenow = False

    if remove and len(paths):
        # Remove the repos as required, write new manfiest and exit
        for fullpath in paths:
            repo = '/' + os.path.relpath(fullpath, toplevel)
            if repo in manifest:
                manifest.pop(repo)
                logger.info(' manifest: removed %s', repo)
            else:
                # Is it in any of the symlinks?
                found = False
                for gitdir in manifest:
                    if 'symlinks' in manifest[gitdir] and repo in manifest[gitdir]['symlinks']:
                        found = True
                        manifest[gitdir]['symlinks'].remove(repo)
                        if not len(manifest[gitdir]['symlinks']):
                            manifest[gitdir].pop('symlinks')
                        logger.info(' manifest: removed symlink %s->%s', repo, gitdir)
                if not found:
                    logger.info(' manifest: %s not in manifest', repo)

        # XXX: need to add logic to make sure we don't break the world
        #      by removing a repository used as a reference for others
        grokmirror.write_manifest(manifile, manifest, pretty=pretty)
        grokmirror.manifest_unlock(manifile)
        return 0

    gitdirs = list()

    if purge or not len(paths) or not len(manifest):
        # We automatically purge when we do a full tree walk
        for gitdir in grokmirror.find_all_gitdirs(toplevel, ignore=ignore, exclude_objstore=True):
            gitdirs.append(gitdir)
        purge_manifest(manifest, toplevel, gitdirs)

    if len(manifest) and len(paths):
        # limit ourselves to passed dirs only when there is something
        # in the manifest. This precaution makes sure we regenerate the
        # whole file when there is nothing in it or it can't be parsed.
        for apath in paths:
            arealpath = os.path.realpath(apath)
            if apath != arealpath and os.path.islink(apath):
                gitdirs.append(apath)
            else:
                gitdirs.append(arealpath)

    symlinks = list()
    tofetch = set()
    for gitdir in gitdirs:
        # check to make sure this gitdir is ok to export
        if check_export_ok and not os.path.exists(os.path.join(gitdir, 'git-daemon-export-ok')):
            # is it curently in the manifest?
            repo = '/' + os.path.relpath(gitdir, toplevel)
            if repo in list(manifest):
                logger.info(' manifest: removed %s (no longer exported)', repo)
                manifest.pop(repo)

            # XXX: need to add logic to make sure we don't break the world
            #      by removing a repository used as a reference for others
            #      also make sure we clean up any dangling symlinks
            continue

        if os.path.islink(gitdir):
            symlinks.append(gitdir)
        else:
            update_manifest(manifest, toplevel, gitdir, usenow, ignorerefs)
            if fetchobst:
                # Do it after we're done with manifest, to avoid keeping it locked
                tofetch.add(gitdir)

    if len(symlinks):
        set_symlinks(manifest, toplevel, symlinks)

    grokmirror.write_manifest(manifile, manifest, pretty=pretty)
    grokmirror.manifest_unlock(manifile)

    fetched = set()
    for gitdir in tofetch:
        altrepo = grokmirror.get_altrepo(gitdir)
        if altrepo in fetched:
            continue
        if altrepo and grokmirror.is_obstrepo(altrepo):
            try:
                grokmirror.lock_repo(altrepo, nonblocking=True)
                logger.info(' manifest: objstore %s -> %s', gitdir, os.path.basename(altrepo))
                grokmirror.fetch_objstore_repo(altrepo, gitdir, use_plumbing=objstore_uses_plumbing)
                grokmirror.unlock_repo(altrepo)
                fetched.add(altrepo)
            except IOError:
                # grok-fsck will fetch this one, then
                pass

    elapsed = datetime.datetime.now() - startt
    if len(gitdirs) > 1:
        logger.info('Updated %s records in %ds', len(gitdirs), elapsed.total_seconds())
    else:
        logger.info('Done in %0.2fs', elapsed.total_seconds())


def command():
    opts = parse_args()

    return grok_manifest(
        opts.manifile, opts.toplevel, paths=opts.paths, logfile=opts.logfile,
        usenow=opts.usenow, check_export_ok=opts.check_export_ok,
        purge=opts.purge, remove=opts.remove, pretty=opts.pretty,
        ignore=opts.ignore, wait=opts.wait, verbose=opts.verbose,
        fetchobst=opts.fetchobst, ignorerefs=opts.ignore_refs)


if __name__ == '__main__':
    command()
