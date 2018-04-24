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
import logging
import time
import enlighten

import grokmirror

from git import Repo

logger = logging.getLogger(__name__)


def update_manifest(manifest, toplevel, gitdir, usenow):
    path = gitdir.replace(toplevel, '', 1)

    # Try to open git dir
    logger.debug('Examining %s', gitdir)
    try:
        repo = Repo(gitdir)
        assert repo.bare is True
    except:
        logger.critical('Error opening %s.', gitdir)
        logger.critical('Make sure it is a bare git repository.')
        sys.exit(1)

    # Ignore it if it's an empty git repository
    try:
        if len(repo.heads) == 0:
            logger.info('%s has no heads, ignoring', gitdir)
            return
    except:
        # Errors when listing heads usually means repository is no good
        logger.info('Error listing heads in %s, ignoring', gitdir)
        return

    try:
        description = repo.description
    except:
        description = 'Unnamed repository'

    try:
        rcr = repo.config_reader()
        owner = rcr.get('gitweb', 'owner')
    except:
        owner = None

    modified = 0

    if not usenow:
        # noinspection PyTypeChecker
        for branch in repo.branches:
            try:
                if branch.commit.committed_date > modified:
                    modified = branch.commit.committed_date
                    # Older versions of GitPython returned time.struct_time
                    if type(modified) == time.struct_time:
                        modified = int(time.mktime(modified))

            except:
                pass

    if modified == 0:
        modified = int(time.time())

    reference = None

    if len(repo.alternates) == 1:
        # use this to hint which repo to use as reference when cloning
        alternate = repo.alternates[0]
        if alternate.find(toplevel) == 0:
            reference = alternate.replace(toplevel, '').replace('/objects', '')

    if path not in manifest.keys():
        logger.info('Adding %s to manifest', path)
        manifest[path] = {}
    else:
        logger.info('Updating %s in the manifest', path)

    # we need a way to quickly compare whether mirrored repositories match
    # what is in the master manifest. To this end, we calculate a so-called
    # "state fingerprint" -- basically the output of "git show-ref | sha1sum".
    # git show-ref output is deterministic and should accurately list all refs
    # and their relation to heads/tags/etc.
    fingerprint = grokmirror.get_repo_fingerprint(toplevel, path, force=True)
    # Record it in the repo for other use
    grokmirror.set_repo_fingerprint(toplevel, path, fingerprint)

    manifest[path]['owner'] = owner
    manifest[path]['description'] = description
    manifest[path]['reference'] = reference
    manifest[path]['modified'] = modified
    manifest[path]['fingerprint'] = fingerprint


def set_symlinks(manifest, toplevel, symlinks):
    for symlink in symlinks:
        target = os.path.realpath(symlink)
        if target.find(toplevel) < 0:
            logger.info('Symlink %s points outside toplevel, ignored', symlink)
            continue
        tgtgitdir = target.replace(toplevel, '')
        if tgtgitdir not in manifest.keys():
            logger.info('Symlink %s points to %s, which we do not recognize',
                        symlink, target)
            continue
        relative = symlink.replace(toplevel, '')
        if 'symlinks' in manifest[tgtgitdir].keys():
            if relative not in manifest[tgtgitdir]['symlinks']:
                logger.info('Recording symlink %s->%s', relative, tgtgitdir)
                manifest[tgtgitdir]['symlinks'].append(relative)
        else:
            manifest[tgtgitdir]['symlinks'] = [relative]
            logger.info('Recording symlink %s to %s', relative, tgtgitdir)

        # Now go through all repos and fix any references pointing to the
        # symlinked location.
        for gitdir in manifest.keys():
            if manifest[gitdir]['reference'] == relative:
                logger.info('Adjusted symlinked reference for %s: %s->%s',
                            gitdir, relative, tgtgitdir)
                manifest[gitdir]['reference'] = tgtgitdir


def purge_manifest(manifest, toplevel, gitdirs):
    for oldrepo in list(manifest):
        if os.path.join(toplevel, oldrepo.lstrip('/')) not in gitdirs:
            logger.info('Purged deleted %s', oldrepo)
            del manifest[oldrepo]


def parse_args():
    from optparse import OptionParser

    usage = '''usage: %prog -m manifest.js[.gz] -t /path [/path/to/bare.git]
    Create or update manifest.js with the latest repository information.
    '''

    op = OptionParser(usage=usage, version=grokmirror.VERSION)
    op.add_option('-m', '--manifest', dest='manifile',
                  help='Location of manifest.js or manifest.js.gz')
    op.add_option('-t', '--toplevel', dest='toplevel',
                  help='Top dir where all repositories reside')
    op.add_option('-l', '--logfile', dest='logfile',
                  default=None,
                  help='When specified, will put debug logs in this location')
    op.add_option('-n', '--use-now', dest='usenow', action='store_true',
                  default=False,
                  help='Use current timestamp instead of parsing commits')
    op.add_option('-c', '--check-export-ok', dest='check_export_ok',
                  action='store_true', default=False,
                  help='Export only repositories marked as '
                       'git-daemon-export-ok')
    op.add_option('-p', '--purge', dest='purge', action='store_true',
                  default=False,
                  help='Purge deleted git repositories from manifest')
    op.add_option('-x', '--remove', dest='remove', action='store_true',
                  default=False,
                  help='Remove repositories passed as arguments from manifest')
    op.add_option('-y', '--pretty', dest='pretty', action='store_true',
                  default=False,
                  help='Pretty-print manifest (sort keys and add indentation)')
    op.add_option('-i', '--ignore-paths', dest='ignore', action='append',
                  default=[],
                  help='When finding git dirs, ignore these paths '
                       '(can be used multiple times, accepts shell-style '
                       'globbing wildcards)')
    op.add_option('-w', '--wait-for-manifest', dest='wait',
                  action='store_true', default=False,
                  help='When running with arguments, wait if manifest is not '
                       'there (can be useful when multiple writers are writing '
                       'the manifest)')
    op.add_option('-v', '--verbose', dest='verbose', action='store_true',
                  default=False,
                  help='Be verbose and tell us what you are doing')

    opts, args = op.parse_args()

    if not opts.manifile:
        op.error('You must provide the path to the manifest file')
    if not opts.toplevel:
        op.error('You must provide the toplevel path')
    if not len(args) and opts.wait:
        op.error('--wait option only makes sense when dirs are passed')

    return opts, args


def grok_manifest(manifile, toplevel, args=None, logfile=None, usenow=False,
                  check_export_ok=False, purge=False, remove=False,
                  pretty=False, ignore=None, wait=False, verbose=False):

    if args is None:
        args = []
    if ignore is None:
        ignore = []

    logger.setLevel(logging.DEBUG)
    # noinspection PyTypeChecker
    em = enlighten.get_manager(series=' -=#')

    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)

    if verbose:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.CRITICAL)
        em.enabled = False

    logger.addHandler(ch)

    if logfile is not None:
        ch = logging.FileHandler(logfile)
        formatter = logging.Formatter(
            "[%(process)d] %(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)

        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)

    # push our logger into grokmirror to override the default
    grokmirror.logger = logger

    grokmirror.manifest_lock(manifile)
    manifest = grokmirror.read_manifest(manifile, wait=wait)

    # If manifest is empty, don't use current timestamp
    if not len(manifest.keys()):
        usenow = False

    if remove and len(args):
        # Remove the repos as required, write new manfiest and exit
        for fullpath in args:
            repo = fullpath.replace(toplevel, '', 1)
            if repo in manifest.keys():
                del manifest[repo]
                logger.info('Repository %s removed from manifest', repo)
            else:
                logger.info('Repository %s not in manifest', repo)

        # XXX: need to add logic to make sure we don't break the world
        #      by removing a repository used as a reference for others
        #      also make sure we clean up any dangling symlinks

        grokmirror.write_manifest(manifile, manifest, pretty=pretty)
        grokmirror.manifest_unlock(manifile)
        return 0

    gitdirs = []

    if purge or not len(args) or not len(manifest.keys()):
        # We automatically purge when we do a full tree walk
        gitdirs = grokmirror.find_all_gitdirs(toplevel, ignore=ignore)
        purge_manifest(manifest, toplevel, gitdirs)

    if len(manifest) and len(args):
        # limit ourselves to passed dirs only when there is something
        # in the manifest. This precaution makes sure we regenerate the
        # whole file when there is nothing in it or it can't be parsed.
        gitdirs = args
        # Don't draw a progress bar for a single repo
        em.enabled = False

    symlinks = []
    # noinspection PyTypeChecker
    run = em.counter(total=len(gitdirs), desc='Processing:', unit='repos', leave=False)
    for gitdir in gitdirs:
        run.update()
        # check to make sure this gitdir is ok to export
        if (check_export_ok and not
                os.path.exists(os.path.join(gitdir, 'git-daemon-export-ok'))):
            # is it curently in the manifest?
            repo = gitdir.replace(toplevel, '', 1)
            if repo in list(manifest):
                logger.info('Repository %s is no longer exported, '
                            'removing from manifest', repo)
                del manifest[repo]

            # XXX: need to add logic to make sure we don't break the world
            #      by removing a repository used as a reference for others
            #      also make sure we clean up any dangling symlinks
            continue

        if os.path.islink(gitdir):
            symlinks.append(gitdir)
        else:
            update_manifest(manifest, toplevel, gitdir, usenow)

    logger.info('Updated %s records in %0.2fs', len(gitdirs), run.elapsed)
    run.close()
    em.stop()


    if len(symlinks):
        set_symlinks(manifest, toplevel, symlinks)

    grokmirror.write_manifest(manifile, manifest, pretty=pretty)
    grokmirror.manifest_unlock(manifile)


def command():

    opts, args = parse_args()

    return grok_manifest(
        opts.manifile, opts.toplevel, args=args, logfile=opts.logfile,
        usenow=opts.usenow, check_export_ok=opts.check_export_ok,
        purge=opts.purge, remove=opts.remove, pretty=opts.pretty,
        ignore=opts.ignore, wait=opts.wait, verbose=opts.verbose)

if __name__ == '__main__':
    command()
