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
    path = gitdir.replace(toplevel, '')

    if usenow and path in manifest.keys():
        # Is it already in the manifest? If so, just update the
        # modified timestamp and get out.
        logger.info('Updating timestamp for %s' % gitdir)
        manifest[path]['modified'] = int(time.time())
        return

    # Try to open git dir
    logger.debug('Examining %s' % gitdir)
    try:
        repo = Repo(gitdir)
        assert repo.bare == True
    except:
        logger.critical('Error opening %s.' % gitdir)
        logger.critical('Make sure it is a bare git repository.')
        sys.exit(1)

    modified = 0

    if not usenow:
        for branch in repo.branches:
            try:
                if branch.commit.committed_date > modified:
                    modified = branch.commit.committed_date
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

    try:
        description = repo.description
    except:
        description = 'Unnamed repository'

    entry = {
            'description': description,
            'reference':   reference,
            'modified':    modified,
            }

    manifest[path] = entry

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
                manifest[gitdir]['reference'] == tgtgitdir

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

    parser = OptionParser(usage=usage, version='0.1')
    parser.add_option('-m', '--manifest', dest='manifile',
        help='Location of manifest.js or manifest.js.gz')
    parser.add_option('-t', '--toplevel', dest='toplevel',
        help='Top dir where all repositories reside')
    parser.add_option('-n', '--use-now', dest='usenow', action='store_true',
        default=False,
        help='Use current timestamp instead of parsing commits')
    parser.add_option('-p', '--purge', dest='purge', action='store_true',
        default=False,
        help='Purge deleted git repositories from manifest')
    parser.add_option('-v', '--verbose', dest='verbose', action='store_true',
        default=False,
        help='Be verbose and tell us what you are doing')

    (opts, args) = parser.parse_args()

    if not opts.manifile:
        parser.error('You must provide the path to the manifest file')
    if not opts.toplevel:
        parser.error('You must provide the toplevel path')

    logger.setLevel(logging.DEBUG)

    ch = logging.StreamHandler()
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)

    if opts.verbose:
        ch.setLevel(logging.INFO)
    else:
        ch.setLevel(logging.CRITICAL)

    logger.addHandler(ch)

    # push our logger into grokmirror to override the default
    grokmirror.logger = logger

    manifest = grokmirror.read_manifest(opts.manifile)

    if opts.purge or not len(args) or not len(manifest.keys()):
        # We automatically purge when we do a full tree walk
        gitdirs = grokmirror.find_all_gitdirs(opts.toplevel)
        purge_manifest(manifest, opts.toplevel, gitdirs)

    if len(manifest.keys()) and len(args):
        # limit ourselves to passed dirs only when there is something
        # in the manifest. This precaution makes sure we regenerate the
        # whole file when there is nothing in it or it can't be parsed.
        gitdirs = args

    symlinks = []
    for gitdir in gitdirs:
        if os.path.islink(gitdir):
            symlinks.append(gitdir)
        else:
            update_manifest(manifest, opts.toplevel, gitdir, opts.usenow)

    if len(symlinks):
        set_symlinks(manifest, opts.toplevel, symlinks)

    grokmirror.write_manifest(opts.manifile, manifest)

