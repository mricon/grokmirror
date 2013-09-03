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
import logging
import time

import grokmirror

from git import Repo

logger = logging.getLogger(__name__)

def update_manifest(manifest, toplevel, gitdir, usenow):
    path = gitdir.replace(toplevel, '', 1)

    # Try to open git dir
    logger.debug('Examining %s' % gitdir)
    try:
        repo = Repo(gitdir)
        assert repo.bare == True
    except:
        logger.critical('Error opening %s.' % gitdir)
        logger.critical('Make sure it is a bare git repository.')
        sys.exit(1)

    # Ignore it if it's an empty git repository
    try:
        if len(repo.heads) == 0:
            logger.info('%s has no heads, ignoring' % gitdir)
            return
    except:
        # Errors when listing heads usually means repository is no good
        logger.info('Error listing heads in %s, ignoring' % gitdir)
        return

    try:
        description = repo.description
    except:
        description = 'Unnamed repository'

    try:
        rcr   = repo.config_reader()
        owner = rcr.get('gitweb', 'owner')
    except:
        owner = None

    modified = 0

    if not usenow:
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
        logger.info('Adding %s to manifest' % path)
        manifest[path] = {}
    else:
        logger.info('Updating %s in the manifest' % path)

    manifest[path]['owner']       = owner
    manifest[path]['description'] = description
    manifest[path]['reference']   = reference
    manifest[path]['modified']    = modified

def set_symlinks(manifest, toplevel, symlinks):
    for symlink in symlinks:
        target = os.path.realpath(symlink)
        if target.find(toplevel) < 0:
            logger.info('Symlink %s points outside toplevel, ignored' % symlink)
            continue
        tgtgitdir = target.replace(toplevel, '')
        if tgtgitdir not in manifest.keys():
            logger.info('Symlink %s points to %s, which we do not recognize'
                    % (symlink, target))
            continue
        relative = symlink.replace(toplevel, '')
        if 'symlinks' in manifest[tgtgitdir].keys():
            if relative not in manifest[tgtgitdir]['symlinks']:
                logger.info('Recording symlink %s->%s' % (relative, tgtgitdir))
                manifest[tgtgitdir]['symlinks'].append(relative)
        else:
            manifest[tgtgitdir]['symlinks'] = [relative]
            logger.info('Recording symlink %s to %s' % (relative, tgtgitdir))

        # Now go through all repos and fix any references pointing to the
        # symlinked location.
        for gitdir in manifest.keys():
            if manifest[gitdir]['reference'] == relative:
                logger.info('Adjusted symlinked reference for %s: %s->%s'
                        % (gitdir, relative, tgtgitdir))
                manifest[gitdir]['reference'] = tgtgitdir

def purge_manifest(manifest, toplevel, gitdirs):
    for oldrepo in manifest.keys():
        if os.path.join(toplevel, oldrepo.lstrip('/')) not in gitdirs:
            logger.info('Purged deleted %s\n' % oldrepo)
            del manifest[oldrepo]

if __name__ == '__main__':
    from optparse import OptionParser

    usage = '''usage: %prog -m manifest.js[.gz] -t /path [/path/to/bare.git]
    Create or update manifest.js with the latest repository information.
    '''

    parser = OptionParser(usage=usage, version=grokmirror.VERSION)
    parser.add_option('-m', '--manifest', dest='manifile',
        help='Location of manifest.js or manifest.js.gz')
    parser.add_option('-t', '--toplevel', dest='toplevel',
        help='Top dir where all repositories reside')
    parser.add_option('-l', '--logfile', dest='logfile',
        default=None,
        help='When specified, will put debug logs in this location')
    parser.add_option('-n', '--use-now', dest='usenow', action='store_true',
        default=False,
        help='Use current timestamp instead of parsing commits')
    parser.add_option('-c', '--check-export-ok', dest='check_export_ok',
        action='store_true', default=False,
        help='Export only repositories marked as git-daemon-export-ok')
    parser.add_option('-p', '--purge', dest='purge', action='store_true',
        default=False,
        help='Purge deleted git repositories from manifest')
    parser.add_option('-x', '--remove', dest='remove', action='store_true',
        default=False,
        help='Remove repositories passed as arguments from manifest')
    parser.add_option('-y', '--pretty', dest='pretty', action='store_true',
        default=False,
        help='Pretty-print manifest (sort keys and add indentation)')
    parser.add_option('-i', '--ignore-paths', dest='ignore', action='append',
        default=[],
        help='When finding git dirs, ignore these paths '
             '(can be used multiple times, accepts shell-style globbing)')
    parser.add_option('-w', '--wait-for-manifest', dest='wait',
        action='store_true', default=False,
        help='When running with arguments, wait if manifest is not there '
             '(can be useful when multiple writers are writing the manifest)')
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
        default=False,
        help='Be verbose and tell us what you are doing')

    (opts, args) = parser.parse_args()

    if not opts.manifile:
        parser.error('You must provide the path to the manifest file')
    if not opts.toplevel:
        parser.error('You must provide the toplevel path')
    if not len(args) and opts.wait:
        parser.error('--wait option only makes sense when dirs are passed')

    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)

    if opts.verbose:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.CRITICAL)

    logger.addHandler(ch)

    if opts.logfile is not None:
        ch = logging.FileHandler(opts.logfile)
        formatter = logging.Formatter("[%(process)d] %(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)

        ch.setLevel(logging.DEBUG)
        logger.addHandler(ch)

    # push our logger into grokmirror to override the default
    grokmirror.logger = logger

    grokmirror.manifest_lock(opts.manifile)
    manifest = grokmirror.read_manifest(opts.manifile, wait=opts.wait)

    # If manifest is empty, don't use current timestamp
    if not len(manifest.keys()):
        opts.usenow = False

    #Keep gitdir consistent by ensuring toplevel has trailing slash
    if not opts.toplevel.endswith('/'):
        opts.toplevel += '/'

    if opts.remove and len(args):
        # Remove the repos as required, write new manfiest and exit
        for fullpath in args:
            repo = fullpath.replace(opts.toplevel, '', 1)
            if repo in manifest.keys():
                del manifest[repo]
                logger.info('Repository %s removed from manifest' % repo)
            else:
                logger.info('Repository %s not in manifest' % repo)

        # XXX: need to add logic to make sure we don't break the world
        #      by removing a repository used as a reference for others
        #      also make sure we clean up any dangling symlinks

        grokmirror.write_manifest(opts.manifile, manifest, pretty=opts.pretty)
        grokmirror.manifest_unlock(opts.manifile)
        sys.exit(0)

    if opts.purge or not len(args) or not len(manifest.keys()):
        # We automatically purge when we do a full tree walk
        gitdirs = grokmirror.find_all_gitdirs(opts.toplevel, ignore=opts.ignore)
        purge_manifest(manifest, opts.toplevel, gitdirs)

    if len(manifest.keys()) and len(args):
        # limit ourselves to passed dirs only when there is something
        # in the manifest. This precaution makes sure we regenerate the
        # whole file when there is nothing in it or it can't be parsed.
        gitdirs = args

    symlinks = []
    for gitdir in gitdirs:

        #keep gitdir consistent be ensuring it does not have trailing slash
        gitdir = gitdir.rstrip('/')

        # check to make sure this gitdir is ok to export
        if (opts.check_export_ok and
            not os.path.exists(os.path.join(gitdir, 'git-daemon-export-ok'))):
            # is it curently in the manifest?
            repo = gitdir.replace(opts.toplevel, '', 1)
            if repo in manifest.keys():
                logger.info('Repository %s is no longer exported, '
                    'removing from manifest' % repo)
                del manifest[repo]

            # XXX: need to add logic to make sure we don't break the world
            #      by removing a repository used as a reference for others
            #      also make sure we clean up any dangling symlinks
            continue

        if os.path.islink(gitdir):
            symlinks.append(gitdir)
        else:
            update_manifest(manifest, opts.toplevel, gitdir, opts.usenow)

    if len(symlinks):
        set_symlinks(manifest, opts.toplevel, symlinks)

    grokmirror.write_manifest(opts.manifile, manifest, pretty=opts.pretty)
    grokmirror.manifest_unlock(opts.manifile)

