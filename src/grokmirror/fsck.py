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
import contextlib
import dataclasses
import datetime
import gc
import io
import json
import logging
import os
import platform
import random
import re
import shutil
import smtplib
import sys
import time
from email.message import EmailMessage
from fcntl import LOCK_EX, LOCK_NB, LOCK_UN, lockf
from pathlib import Path
from typing import Any

import grokmirror
from grokmirror import configcheck

# default basic logger. We override it later.
logger = logging.getLogger(__name__)

# Object IDs as git prints them, sha1 or sha256.
OID_RE = re.compile(r'\b[0-9a-f]{40,64}\b')

# git fsck's way of saying "this ref does not resolve to an object".
NULL_REF_RE = re.compile(r'^(?:error: )?(?P<ref>\S+): invalid sha1 pointer 0{40,64}$')

# How many repositories a report block names before it just gives a count.
REPORT_CAP = 10


def check_manifest_fingerprint(
    toplevel: str,
    gitdir: str,
    minfo: grokmirror.RepoInfo,
    stentry: dict[str, Any],
    ignorerefs: list[str] | None,
) -> tuple[str, str] | None:
    """Compare the repository on disk against what the manifest says about it.

    The fingerprint is the whole of grokmirror's "does this need updating?"
    logic, so a repository that disagrees with its manifest entry is one that
    is not being replicated -- and nothing else in grokmirror ever looks.

    Two different things turn up here. Either git will not list the refs at
    all, usually because a ref file is damaged, in which case nothing can
    fingerprint the repository and grok-manifest has been quietly leaving its
    entry frozen at whatever it last said. Or the refs list fine and simply do
    not match: on an origin that means grok-manifest is not being run, on a
    replica that grok-pull is not getting through.

    Returns ('broken'|'stale', detail), or None when all is well. A replica is
    legitimately behind for a while after a push, so a mismatch has to still be
    there on the next run before it counts.
    """
    fullpath = grokmirror.gitdir_to_fullpath(toplevel, gitdir)
    fingerprint = grokmirror.get_repo_fingerprint(toplevel, gitdir, force=True, ignorerefs=ignorerefs)
    if fingerprint is None:
        _retcode, _out, error = grokmirror.run_git_command(fullpath, ['show-ref'])
        stentry.pop('fp_mismatch', None)
        if not error.strip():
            # No refs at all -- show-ref exits non-zero for that too, but says
            # nothing. grok-manifest does not fingerprint those either.
            return None
        return 'broken', error.strip().splitlines()[-1].strip()

    m_fp = minfo.get('fingerprint')
    if not m_fp or fingerprint == m_fp:
        stentry.pop('fp_mismatch', None)
        return None

    if stentry.get('fp_mismatch') != m_fp:
        stentry['fp_mismatch'] = m_fp
        logger.debug('%s does not match the manifest yet', gitdir)
        return None

    return 'stale', f'manifest says {m_fp}, on disk {fingerprint}'


def report_fingerprint_mismatches(broken: list[tuple[str, str]], stale: list[tuple[str, str]]) -> None:
    """Put everything the fingerprint comparison found into the report."""

    def log_block(header: str, entries: list[tuple[str, str]], explanation: list[str]) -> None:
        if not entries:
            return
        logger.critical('%s', header)
        for fullpath, detail in entries[:REPORT_CAP]:
            logger.critical('\t%s', fullpath)
            logger.critical('\t\t%s', detail)
        if len(entries) > REPORT_CAP:
            logger.critical('\t [ %s more not listed ]', len(entries) - REPORT_CAP)
        for line in explanation:
            logger.critical('\t%s', line)

    log_block(
        'Repositories that git cannot fingerprint:',
        broken,
        [
            'Grok-manifest cannot update these, so their manifest entries stay',
            'frozen and no replica will ever see them change again.',
        ],
    )
    log_block(
        'Repositories that do not match the manifest:',
        stale,
        [
            'On an origin this means grok-manifest is not running or not finishing;',
            'on a replica, that grok-pull is not getting through. If this lists most',
            'of the tree, check that [manifest]ignore_refs matches the origin.',
        ],
    )


def log_errors(fullpath: grokmirror.StrPath, cmdargs: list[str], lines: list[str]) -> None:
    logger.critical('%s reports errors:', fullpath)
    with Path(fullpath, 'grokmirror.fsck.err').open('w', encoding='utf-8') as fh:
        fh.write('# Date: {}\n'.format(datetime.datetime.today().strftime('%F')))  # noqa: DTZ002
        fh.write('# Cmd : git {}\n'.format(' '.join(cmdargs)))
        for count, line in enumerate(lines, start=1):
            fh.write(f'{line}\n')
            logger.critical('\t%s', line)
            if count > 10:
                logger.critical('\t [ %s more lines skipped ]', len(lines) - 10)
                logger.critical('\t [ see %s/grokmirror.fsck.err ]', Path(fullpath).name)
                break


def gen_preload_bundle(fullpath: grokmirror.StrPath, config: grokmirror.GrokConfigParser) -> None:
    # Only called when the caller has already checked this is set
    outdir = Path(config['fsck']['preload_bundle_outdir'])
    outdir.mkdir(parents=True, exist_ok=True)
    bname = f'{Path(fullpath).name.removesuffix(".git")}.bundle'
    args = ['bundle', 'create', str(outdir / bname), '--all']
    logger.info(' bundling: %s', bname)
    grokmirror.run_git_command(fullpath, args)


def get_blob_set(fullpath: grokmirror.StrPath) -> tuple[set[tuple[str, int]], int]:
    bset = set()
    size = 0
    blobcache = Path(fullpath, 'grokmirror.blobs')
    if blobcache.exists():
        # Did it age out? Hardcode to 30 days.
        expage = time.time() - 86400 * 30
        st = blobcache.stat()
        if st.st_mtime < expage:
            blobcache.unlink()
    try:
        with blobcache.open(encoding='utf-8') as fh:
            while True:
                line = fh.readline()
                if not line:
                    break
                if line[0] == '#':
                    continue
                chunks = line.strip().split()
                bhash = chunks[0]
                bsize = int(chunks[1])
                size += bsize
                bset.add((bhash, bsize))
    except FileNotFoundError:
        pass
    else:
        return bset, size

    # This only makes sense for repos not using alternates, so make sure you check first
    logger.info(' bloblist: %s', fullpath)
    gitargs = ['cat-file', '--batch-all-objects', '--batch-check', '--unordered']
    retcode, output, _error = grokmirror.run_git_command(fullpath, gitargs)
    if retcode == 0:
        with blobcache.open('w', encoding='utf-8') as fh:
            fh.write('# Blobs and sizes used for sibling calculation\n')
            for line in output.splitlines():
                if ' blob ' not in line:
                    continue
                chunks = line.strip().split()
                fh.write(f'{chunks[0]} {chunks[2]}\n')
                bhash = chunks[0]
                bsize = int(chunks[2])
                size += bsize
                bset.add((bhash, bsize))

    return bset, size


def check_sibling_repos_by_blobs(
    bset1: set[tuple[str, int]], bsize1: int, bset2: set[tuple[str, int]], bsize2: int, ratio: int
) -> bool:
    iset = bset1.intersection(bset2)
    if not iset:
        return False
    isize = sum(bsize for _bhash, bsize in iset)
    # Both repos should share at least ratio % of blobs in them
    ratio1 = int(isize / bsize1 * 100)
    logger.debug('isize=%s, bsize1=%s, ratio1=%s', isize, bsize1, ratio1)
    ratio2 = int(isize / bsize2 * 100)
    logger.debug('isize=%s, bsize2=%s ratio2=%s', isize, bsize2, ratio1)
    return ratio1 >= ratio and ratio2 >= ratio


def find_siblings_by_blobs(ses: grokmirror.GrokSession, obstrepo: str, obstdir: str, ratio: int = 75) -> set[str]:
    siblings = set()
    oset, osize = get_blob_set(obstrepo)
    for srepo in ses.find_all_gitdirs(obstdir, normalize=True, exclude_objstore=False):
        if srepo == obstrepo:
            continue
        logger.debug('Comparing blobs between %s and %s', obstrepo, srepo)
        sset, ssize = get_blob_set(srepo)
        if check_sibling_repos_by_blobs(oset, osize, sset, ssize, ratio):
            logger.info(' siblings: %s and %s', Path(obstrepo).name, Path(srepo).name)
            siblings.add(srepo)

    return siblings


def merge_siblings(siblings: set[str], amap: dict[str, set[str]]) -> str | None:
    mdest = None
    rcount = 0
    # Who has the most remotes? Remember each answer: the merge loop below
    # wants the very same list again, and every call forks a git.
    remotes: dict[str, list[tuple[str, ...]]] = {}
    for sibling in set(siblings):
        if not amap.get(sibling):
            # Orphaned sibling, ignore it -- it will get cleaned up
            siblings.remove(sibling)
            continue
        remotes[sibling] = grokmirror.list_repo_remotes(sibling, withurl=True)
        if len(remotes[sibling]) > rcount:
            mdest = sibling
            rcount = len(remotes[sibling])

    if mdest is None:
        # Every sibling turned out to be orphaned, so there is nothing to merge
        # into. They will get cleaned up separately.
        return None

    # Migrate all siblings into the repo with most remotes
    siblings.remove(mdest)
    for sibling in siblings:
        logger.info('%s: merging into %s', Path(sibling).name, Path(mdest).name)
        for virtref, childpath in remotes[sibling]:
            if childpath not in amap[sibling]:
                # The child repo isn't even using us
                args = ['remote', 'remove', virtref]
                grokmirror.run_git_command(sibling, args)
                continue
            logger.info('   moving: %s', childpath)

            success = grokmirror.add_repo_to_objstore(mdest, childpath)
            if not success:
                logger.critical('Could not add %s to %s', childpath, mdest)
                continue

            logger.info('         : fetching into %s', Path(mdest).name)
            success = grokmirror.fetch_objstore_repo(mdest, childpath)
            if not success:
                logger.critical('Failed to fetch %s from %s to %s', childpath, Path(sibling).name, Path(mdest).name)
                continue
            logger.info('         : repointing alternates')
            grokmirror.set_altrepo(childpath, mdest)
            amap[sibling].remove(childpath)
            amap[mdest].add(childpath)
            args = ['remote', 'remove', virtref]
            grokmirror.run_git_command(sibling, args)
            logger.info('         : done')

    return mdest


def can_reclone(fullpath: grokmirror.StrPath, config: grokmirror.GrokConfigParser) -> bool:
    """Is there anywhere to reclone this repository from?

    A reclone request is a message left for grok-pull, which refetches the
    repository from the remote it maintains. An origin server runs grok-fsck
    without grok-pull and the repositories have no such remote, so the marker
    file would never be picked up and the same "requested auto-reclone" line
    would show up in every report.
    """
    remotename = config['pull'].get('remotename', '_grokmirror') if 'pull' in config else '_grokmirror'
    return remotename in grokmirror.list_repo_remotes(fullpath)


def check_reclone_error(
    ses: grokmirror.GrokSession, fullpath: grokmirror.StrPath, config: grokmirror.GrokConfigParser, errors: list[str]
) -> None:
    reclone = None
    toplevel = os.path.realpath(config['core']['toplevel'])
    # Drop blank patterns: every line "contains" the empty string, so a blank
    # entry (or an unset option) would request a reclone for any error at all.
    errlist = [x.strip() for x in config['fsck'].get('reclone_on_errors', '').splitlines() if x.strip()]
    for line in errors:
        if any(estring in line for estring in errlist):
            reclone = line
            break
    if reclone is None:
        return

    # is this repo used for alternates?
    # No lstrip('/') on the way out: relpath() never returns a leading slash of
    # its own, so the one we prepend is the only one there ever was.
    gitdir = grokmirror.fullpath_to_gitdir(toplevel, fullpath)
    if ses.is_alt_repo(toplevel, gitdir):
        logger.critical('\tused for alternates, not requesting auto-reclone')
        return

    if not can_reclone(fullpath, config):
        logger.critical('\tno mirror remote, this repository needs manual attention')
        return

    logger.critical('\trequested auto-reclone')
    set_repo_reclone(fullpath, reclone)


def get_repo_size(fullpath: grokmirror.StrPath) -> int:
    oi = grokmirror.get_repo_obj_info(fullpath)
    kbsize = 0
    for field in ['size', 'size-pack', 'size-garbage']:
        # Suppressed, not .get(): git only reports size-garbage when there is
        # garbage to report, and a value we can't parse should not make the
        # whole repo look zero-sized.
        with contextlib.suppress(KeyError, ValueError):
            kbsize += int(oi[field])
    logger.debug('%s size: %s kb', fullpath, kbsize)
    return kbsize


def get_human_size(kbsize: int) -> str:
    num = float(kbsize)
    for unit in ['Ki', 'Mi', 'Gi']:
        if abs(num) < 1024.0:
            return f'{num:3.2f} {unit}B'
        num /= 1024.0
    return f'{num:.2f} TiB'


def set_repo_reclone(fullpath: grokmirror.StrPath, reason: str) -> None:
    rfile = Path(fullpath, 'grokmirror.reclone')
    # Have we already requested a reclone?
    if rfile.exists():
        logger.debug('Already requested repo reclone for %s', fullpath)
        return

    rfile.write_text(f'Requested by grok-fsck due to error: {reason}', encoding='utf-8')


def run_git_prune(ses: grokmirror.GrokSession, fullpath: str, config: grokmirror.GrokConfigParser) -> bool:
    # WARNING: We assume you've already verified that it's safe to do so
    prune_ok = True
    isprecious = grokmirror.is_precious(fullpath)
    if isprecious:
        set_precious_objects(fullpath, False)

    # We set expire to yesterday in order to avoid race conditions
    # in repositories that are actively being accessed at the time of
    # running the prune job.
    args = ['prune', '--expire=yesterday']
    logger.info('    prune: pruning')
    _retcode, _output, error = grokmirror.run_git_command(fullpath, args)

    if error:
        warn = remove_ignored_errors(error, config)
        if warn:
            prune_ok = False
            log_errors(fullpath, args, warn)
            check_reclone_error(ses, fullpath, config, warn)

    if isprecious:
        set_precious_objects(fullpath, True)

    return prune_ok


def is_safe_to_prune(ses: grokmirror.GrokSession, fullpath: str, config: grokmirror.GrokConfigParser) -> bool:
    if not config.get_bool('fsck', 'prune', True):
        logger.debug('Pruning disabled in config file')
        return False
    toplevel = os.path.realpath(config['core']['toplevel'])
    obstdir = os.path.realpath(config['core']['objstore'])
    gitdir = grokmirror.fullpath_to_gitdir(toplevel, fullpath)
    if grokmirror.is_obstrepo(fullpath, obstdir):
        # We only prune if all repos pointing to us are public
        urls = set(grokmirror.list_repo_remotes(fullpath, withurl=True))
        mine = {x[1] for x in urls}
        amap = ses.get_altrepo_map(toplevel)
        if mine != amap[fullpath]:
            logger.debug('Cannot prune %s because it is used by non-public repos', gitdir)
            return False
    elif ses.is_alt_repo(toplevel, gitdir):
        logger.debug('Cannot prune %s because it is used as alternates by other repos', gitdir)
        return False

    logger.debug('%s should be safe to prune', gitdir)
    return True


def remove_ignored_errors(output: str, config: grokmirror.GrokConfigParser) -> list[str]:
    # Drop blank patterns: every line "contains" the empty string, so a blank
    # entry (or an unset option) would silence every error the run produced.
    ierrors = {x.strip() for x in config['fsck'].get('ignore_errors', '').splitlines() if x.strip()}
    debug = []
    warn = []
    for rawline in output.splitlines():
        line = rawline.strip()
        # ignore any blank linkes
        if not line:
            continue
        ignored = False
        for estring in ierrors:
            if estring in line:
                ignored = True
                debug.append(line)
                break
        if not ignored:
            warn.append(line)
    if debug:
        logger.debug('Stderr: %s', '\n'.join(debug))

    return warn


def run_git_repack(
    ses: grokmirror.GrokSession, fullpath: str, config: grokmirror.GrokConfigParser, level: int = 1, prune: bool = True
) -> bool:
    # Returns false if we hit any errors on the way
    repack_ok = True
    obstdir = os.path.realpath(config['core']['objstore'])
    toplevel = os.path.realpath(config['core']['toplevel'])
    gitdir = grokmirror.fullpath_to_gitdir(toplevel, fullpath)

    if prune:
        # Make sure it's safe to do so
        prune = is_safe_to_prune(ses, fullpath, config)

    if config['fsck'].get('precious', '') == 'always':
        always_precious = True
        set_precious_objects(fullpath, enabled=True)
    else:
        always_precious = False
        set_precious_objects(fullpath, enabled=False)

    set_precious_after = False

    # Figure out what our repack flags should be.
    repack_flags = []
    rregular = config['fsck'].get('extra_repack_flags', '').split()
    if rregular:
        repack_flags += rregular

    # Full repacks used to pass -f to recompute every delta from scratch.
    # Delta islands are enforced even on reused deltas, so -f buys nothing for
    # correctness -- only marginally better deltas at an enormous CPU cost on
    # large repos. Anyone who wants the old behavior back can put -f in
    # extra_repack_flags_full.
    full_repack_flags = ['--pack-kept-objects']
    rfull = config['fsck'].get('extra_repack_flags_full', '').split()
    if rfull:
        full_repack_flags += rfull

    # A quick repack used to be an all-into-one repack, same as a full one
    # minus the delta recomputation -- so every nightly run rewrote every
    # multi-gigabyte pack on the server in its entirety. Geometric repacking
    # only rolls up the packs (and loose objects) at the small end of a
    # geometric progression, so steady-state quick repacks touch megabytes
    # instead of gigabytes, and drop nothing. Full repacks still consolidate
    # everything into a single pack the old way.
    #
    # Bitmaps require everything reachable in one pack, which a geometric
    # repack does not promise -- with several packs they have to live in a
    # multi-pack-index instead, hence --write-midx wherever we used to rely
    # on single-pack bitmaps. Git 2.41 is the first version where geometric
    # repacking of repositories that borrow from alternates has no known
    # corner-case bugs (see ps/fix-geom-repack-with-alternates), so anything
    # older keeps the historical all-into-one behavior.
    #
    # The same version boundary switches full repacks of the repositories
    # that expire unreachable objects (standalone repos and children) from -A
    # to --cruft: instead of exploding every unreachable object into a loose
    # file -- inode churn now, a pile of work for git-prune later -- they go
    # into a single cruft pack with their expiration times riding along in
    # its .mtimes file.
    modern_git = grokmirror.git_newer_than('2.41.0')
    geometric = level <= 1 and modern_git

    if grokmirror.is_obstrepo(fullpath, obstdir):
        set_precious_after = True
        # Grokmirror used to force pack.compression=9 on objstore repos. The
        # last percent of pack size it wins over the zlib default is not worth
        # what it costs in CPU on every repack of the largest repos we manage,
        # so remove the old forced setting on the way past. The pattern limits
        # this to the value we used to set; a different level someone chose on
        # purpose stays.
        grokmirror.set_git_config(fullpath, 'pack.compression', '^9$', operation='--unset')
        if geometric:
            # Geometric repacking never deletes objects, so the -k that
            # protects unreachable objects from an all-into-one rewrite has
            # nothing to protect against here (and -k does not combine with
            # the pack list a geometric repack feeds to pack-objects anyway).
            repack_flags += ['--geometric=2', '--write-midx']
        else:
            repack_flags.append('-a')
            if not prune and not always_precious:
                repack_flags.append('-k')

    elif ses.is_alt_repo(toplevel, gitdir):
        set_precious_after = True
        if grokmirror.get_altrepo(fullpath):
            logger.warning(' warning : has alternates and is used by others for alternates')
            logger.warning('         : this can cause grandchild corruption')
            repack_flags.append('-A')
            repack_flags.append('-l')
            # Bitmaps can never work in a repo that borrows objects it does
            # not repack, and git says so with a warning that would count as
            # a failed repack unless ignore_errors happens to cover it. Say
            # up front that we know, here and on every other -l repack.
            repack_flags.append('--no-write-bitmap-index')
        elif geometric:
            repack_flags += ['--geometric=2', '--write-midx', '-b']
        else:
            repack_flags.append('-a')
            repack_flags.append('-b')
            if not always_precious:
                repack_flags.append('-k')

    elif grokmirror.get_altrepo(fullpath):
        # we are a "child repo"
        if geometric:
            # No --write-midx: the objstore fetch hardlinks pack files out of
            # child repos, and a multi-pack-index makes no sense outside the
            # repository it was written in. Keeping children midx-free keeps
            # that path simple. Nothing is dropped, so prune's
            # --unpack-unreachable grace period does not apply.
            repack_flags += ['--geometric=2', '-l', '--no-write-bitmap-index']
        elif modern_git:
            # Full repack. Without prune the cruft pack simply holds the
            # unreachables forever, like -A used to keep them loose forever.
            repack_flags.append('--cruft')
            if prune:
                repack_flags.append('--cruft-expiration=yesterday')
            repack_flags.append('-l')
            repack_flags.append('--no-write-bitmap-index')
        else:
            repack_flags.append('-l')
            repack_flags.append('--no-write-bitmap-index')
            repack_flags.append('-A')
            if prune:
                repack_flags.append('--unpack-unreachable=yesterday')

    elif geometric:
        # we have no relationships with other repos
        repack_flags += ['--geometric=2', '--write-midx', '-b']

    elif modern_git:
        # No relationships with other repos; full repack. --cruft implies -a,
        # and as a bonus over the old flags, running without prune no longer
        # silently drops packed unreachable objects -- they stay in the cruft
        # pack.
        repack_flags.append('--cruft')
        if prune:
            repack_flags.append('--cruft-expiration=yesterday')
        repack_flags.append('-b')

    else:
        # we have no relationships with other repos
        repack_flags.append('-a')
        repack_flags.append('-b')
        if prune:
            repack_flags.append('--unpack-unreachable=yesterday')

    if level > 1:
        logger.info('   repack: performing a full consolidating repack')
        repack_flags += full_repack_flags

    if not always_precious:
        repack_flags.append('-d')

    # If we have a logs dir, then run reflog expire
    if Path(fullpath, 'logs').is_dir():
        args = ['reflog', 'expire', '--all', '--stale-fix']
        logger.info('   reflog: expiring reflogs')
        grokmirror.run_git_command(fullpath, args)

    args = ['repack', *repack_flags]
    logger.info('   repack: repacking with "%s"', ' '.join(repack_flags))

    # We always tack on -q
    args.append('-q')

    _retcode, _output, error = grokmirror.run_git_command(fullpath, args)

    # With newer versions of git, repack may return warnings that are safe to ignore
    # so use the same strategy to weed out things we aren't interested in seeing
    if error:
        warn = remove_ignored_errors(error, config)
        if warn:
            repack_ok = False
            log_errors(fullpath, args, warn)
            check_reclone_error(ses, fullpath, config, warn)

    if not repack_ok:
        # No need to repack refs if repo is broken
        if set_precious_after:
            set_precious_objects(fullpath, enabled=True)
        return False

    if config.get_bool('fsck', 'commitgraph', True):
        grokmirror.set_git_config(fullpath, 'core.commitgraph', 'true')
        # --reachable makes the graph list exactly the commits the refs
        # reach, which cgit's reachability check relies on. A graph built
        # from packs would also list commits only a reflog keeps.
        run_git_commit_graph(fullpath, ['--reachable'])

    # repacking refs requires a separate command, so run it now
    args = ['pack-refs']
    if level > 1:
        logger.info(' packrefs: repacking all refs')
        args.append('--all')
    else:
        logger.info(' packrefs: repacking refs')
    _retcode, _output, error = grokmirror.run_git_command(fullpath, args)

    # pack-refs shouldn't return anything, but use the same ignore_errors block
    # to weed out any future potential benign warnings
    if error:
        warn = remove_ignored_errors(error, config)
        if warn:
            repack_ok = False
            log_errors(fullpath, args, warn)
            check_reclone_error(ses, fullpath, config, warn)

    if prune:
        repack_ok = run_git_prune(ses, fullpath, config)

    if set_precious_after:
        set_precious_objects(fullpath, enabled=True)

    return repack_ok


def split_commit_graph_errors(errors: list[str]) -> tuple[list[str], list[str]]:
    """Separate stale commit-graph complaints from real fsck errors.

    Git fsck verifies the commit-graph when core.commitGraph is set, and a
    graph listing commits the object database no longer has produces a pair of
    lines for each of them::

        error: Could not read 01fbb5471cca68fd07bc020e2da2b0bbb9a011dd
        failed to parse commit 01fbb5471cca68fd07bc020e2da2b0bbb9a011dd from object database for commit-graph

    Only the second line says what it is about, so the first is recognized by
    the object id it shares with one of the graph complaints. Anything else
    stays in the error list: if those commits were also reachable from a ref,
    fsck says so separately and that is real damage.
    """

    def is_graph_line(line: str) -> bool:
        return 'commit-graph' in line or 'commit graph' in line

    if not any(is_graph_line(x) for x in errors):
        return [], errors

    graph_oids: set[str] = set()
    for line in errors:
        if is_graph_line(line):
            graph_oids.update(OID_RE.findall(line))

    def is_stale_graph_error(line: str) -> bool:
        if is_graph_line(line):
            return True
        # The other half of the pair only says an object could not be read, so
        # go by the object id it shares with one of the graph complaints.
        oids = set(OID_RE.findall(line))
        return line.startswith('error: Could not read ') and bool(oids) and oids <= graph_oids

    graph = []
    rest = []
    for line in errors:
        if is_stale_graph_error(line):
            graph.append(line)
        else:
            rest.append(line)
    return graph, rest


def repair_commit_graph(fullpath: str, config: grokmirror.GrokConfigParser, errors: list[str]) -> list[str]:
    """Rebuild a stale commit-graph and return the errors that remain.

    A commit-graph that has fallen behind the object database is not damage and
    needs neither a reclone nor a human -- the commits it still lists became
    unreachable and were pruned. Throw the graph away and write a new one.
    """
    graph, rest = split_commit_graph_errors(errors)
    if not graph:
        return errors

    logger.info('    graph: commit-graph is out of date, rebuilding')
    logger.debug('%s: %s stale commit-graph complaints', fullpath, len(graph))
    # Remove it first, so that a graph git refuses to even read cannot keep the
    # rewrite from happening. Both layouts, since a split graph may have been
    # written by something other than grokmirror.
    with contextlib.suppress(OSError):
        Path(fullpath, 'objects', 'info', 'commit-graph').unlink()
    shutil.rmtree(Path(fullpath, 'objects', 'info', 'commit-graphs'), ignore_errors=True)

    if not config.get_bool('fsck', 'commitgraph', True):
        # Graphs are turned off, so a missing one is the correct end state.
        return rest

    if not run_git_commit_graph(fullpath, ['--reachable']):
        logger.critical('%s: could not rebuild the commit-graph', fullpath)

    return rest


def explain_unresolvable_refs(fullpath: str, errors: list[str]) -> list[str]:
    """Say what "invalid sha1 pointer 000...0" is actually about.

    Git hands fsck the null object id for every ref it could not resolve, and
    fsck reports that as a bad object pointer without saying why it did not
    resolve. The usual reason on a server is a symbolic ref whose target has
    been deleted, which git itself considers perfectly legal -- an unborn
    branch looks exactly the same -- so neither newer git nor "git refs verify"
    has anything more to say about it. Ask for the target ourselves, so the
    report says what is actually wrong with the ref.

    Refs that are not symrefs are left alone: an empty ref file, or one holding
    the null object id, is real local damage and keeps its original error.
    """
    explained = []
    for line in errors:
        match = NULL_REF_RE.match(line)
        if not match:
            explained.append(line)
            continue
        ref = match.group('ref')
        retcode, output, _error = grokmirror.run_git_command(fullpath, ['symbolic-ref', ref])
        target = output.strip()
        if retcode != 0 or not target:
            explained.append(line)
            continue
        explained.append(f'error: {ref}: symbolic ref pointing at {target}, which does not exist')
    return explained


def run_git_fsck(
    ses: grokmirror.GrokSession, fullpath: str, config: grokmirror.GrokConfigParser, conn_only: bool = False
) -> None:
    args = ['fsck', '--no-progress', '--no-dangling', '--no-reflogs']
    obstdir = os.path.realpath(config['core']['objstore'])
    # If it's got an obstrepo, always run as connectivity-only
    altrepo = grokmirror.get_altrepo(fullpath)
    if altrepo and grokmirror.is_obstrepo(altrepo, obstdir):
        logger.debug('Repo uses objstore, forcing connectivity-only')
        conn_only = True
    if conn_only:
        args.append('--connectivity-only')
        logger.info('     fsck: running with --connectivity-only')
    else:
        logger.info('     fsck: running full checks')

    _retcode, output, error = grokmirror.run_git_command(fullpath, args)
    output = output + '\n' + error

    if output:
        warn = remove_ignored_errors(output, config)
        if warn:
            warn = explain_unresolvable_refs(fullpath, repair_commit_graph(fullpath, config, warn))
        if warn:
            log_errors(fullpath, args, warn)
            check_reclone_error(ses, fullpath, config, warn)


def run_git_commit_graph(fullpath: str, extraflags: list[str] | None = None) -> bool:
    # Does our version of git support commit-graph?
    if not grokmirror.git_newer_than('2.18.0'):
        logger.debug('Git version too old, not generating commit-graph')
        return False
    logger.info('    graph: generating commit-graph')
    args = ['commit-graph', 'write']
    if extraflags:
        args += extraflags
    retcode, _output, _error = grokmirror.run_git_command(fullpath, args)
    return retcode == 0


def set_precious_objects(fullpath: str, enabled: bool = True) -> None:
    # It's better to just set it blindly without checking first,
    # as this results in one fewer shell-out.
    logger.debug('Setting preciousObjects for %s', fullpath)
    poval = 'true' if enabled else 'false'
    grokmirror.set_git_config(fullpath, 'extensions.preciousObjects', poval)


def check_precious_objects(fullpath: str) -> bool:
    return grokmirror.is_precious(fullpath)


def refresh_obst_roots(obst_roots: dict[str, set[str]], obstrepo: str) -> set[str] | None:
    # Re-read the root commits of an objstore repo we just modified. Every
    # consumer of this map ignores repos with no roots, so drop such repos
    # instead of caching an empty entry -- an objstore repo whose contents were
    # just migrated elsewhere must not be offered as a sibling target again.
    roots = grokmirror.get_repo_roots(obstrepo, force=True)
    if roots:
        obst_roots[obstrepo] = roots
    else:
        obst_roots.pop(obstrepo, None)
    return roots


@dataclasses.dataclass
class FsckOptions:
    """The command-line knobs for a single grok-fsck run.

    These travel together through every stage of a run, so they travel as one
    object. GrokSession does the same thing for per-run state.
    """

    force: bool = False
    repack_only: bool = False
    conn_only: bool = False
    repack_all_quick: bool = False
    repack_all_full: bool = False

    def __post_init__(self) -> None:
        # Both --repack-all-* flags say "(Assumes --force)" in their help, and
        # the implication has to hold everywhere force is read, not just where
        # the flags themselves are.
        if self.repack_all_quick or self.repack_all_full:
            self.force = True

    @property
    def repack_all(self) -> bool:
        """True when every repository is being repacked, schedule or no."""
        return self.repack_all_quick or self.repack_all_full


def decide_repo_action(
    fullpath: str,
    gitdir: str,
    toplevel: str,
    entry: dict[str, str],
    today: datetime.datetime,
    checkdelay: int,
    cfg_repack: bool,
    cfg_precious: str,
    options: FsckOptions,
    ses: grokmirror.GrokSession,
    config: grokmirror.GrokConfigParser,
) -> tuple[str, int | None] | None:
    """Decide what a toplevel repository needs done this run, if anything.

    Returns ('repack', level) or ('fsck', None) for the caller to queue, or
    None for "nothing to do". This is the densest logic in fsck_mirror(): it
    weighs the config, the command-line flags, the object counts, the
    fingerprint and the check schedule against each other. See
    tests/test_fsck.py's characterization tests for what each of those inputs
    is supposed to do -- they were written against this code before it moved.

    Not pure: `entry['nextcheck']` is updated in place whenever the schedule
    advances (status is a plain dict of dicts, and that is how every other
    reader/writer of it works too), and a repository found to be carrying
    unreferenced garbage is pruned synchronously when nothing else is already
    going to repack it.
    """
    obj_info = grokmirror.get_repo_obj_info(fullpath)
    try:
        packs = int(obj_info['packs'])
        count_loose = int(obj_info['count'])
    except KeyError:
        logger.warning('Unable to count objects in %s, skipping', fullpath)
        return None

    schedcheck = datetime.datetime.strptime(entry['nextcheck'], '%Y-%m-%d')  # noqa: DTZ007
    nextcheck = today + datetime.timedelta(days=checkdelay)
    ignorerefs = grokmirror.get_ignorerefs(config)

    if not cfg_repack:
        # don't look at me if you turned off repack
        logger.debug('Not repacking because repack=no in config')
        repack_level = None
    elif options.repack_all_full and (count_loose > 0 or packs > 1):
        logger.debug('repack_level=2 due to repack_all_full')
        repack_level = 2
    elif options.repack_all_quick and count_loose > 0:
        logger.debug('repack_level=1 due to repack_all_quick')
        repack_level = 1
    elif entry.get('fingerprint') != grokmirror.get_repo_fingerprint(toplevel, gitdir, ignorerefs=ignorerefs):
        logger.debug('Checking repack level of %s', fullpath)
        repack_level = grokmirror.get_repack_level(obj_info)
    else:
        repack_level = None

    # trigger a level-1 repack if it's regular check time and the fingerprint has changed
    if (
        not repack_level
        and schedcheck <= today
        and entry.get('fingerprint') != grokmirror.get_repo_fingerprint(toplevel, gitdir, ignorerefs=ignorerefs)
    ):
        entry['nextcheck'] = nextcheck.strftime('%F')
        logger.info('     aged: %s (forcing repack)', fullpath)
        repack_level = 1

    # If we're not already repacking the repo, run a prune if we find garbage in it
    if obj_info['garbage'] != '0' and not repack_level and is_safe_to_prune(ses, fullpath, config):
        logger.info('  garbage: %s (%s files, %s KiB)', gitdir, obj_info['garbage'], obj_info['size-garbage'])
        try:
            with grokmirror.locked_repo(fullpath, nonblocking=True):
                run_git_prune(ses, fullpath, config)
        except (OSError, grokmirror.GrokLockError):
            pass

    if repack_level and (cfg_precious == 'always' and check_precious_objects(fullpath)):
        # if we have preciousObjects, then we only repack based on the same
        # schedule as fsck.
        logger.debug('preciousObjects is set')
        # for repos with preciousObjects, we use the fsck schedule for repacking
        if schedcheck <= today:
            logger.debug('Time for a full periodic repack of a preciousObjects repo')
            entry['nextcheck'] = nextcheck.strftime('%F')
            repack_level = 2
        else:
            logger.debug('Not repacking preciousObjects repo outside of schedule')
            repack_level = None

    if repack_level:
        if repack_level > 1:
            logger.info('   queued: %s (full repack)', fullpath)
        else:
            logger.info('   queued: %s (repack)', fullpath)
        return 'repack', repack_level
    if options.repack_only or options.repack_all:
        return None
    if schedcheck <= today or options.force:
        logger.info('   queued: %s (fsck)', fullpath)
        return 'fsck', None
    return None


def fsck_mirror(config: grokmirror.GrokConfigParser, options: FsckOptions) -> int:

    statusfile = config['fsck'].get('statusfile')
    if not statusfile:
        logger.critical('Please define fsck.statusfile in the config')
        return 1

    st_file = Path(statusfile)
    if not st_file.parent.is_dir():
        logger.critical('Directory %s is absent', st_file.parent)
        return 1

    # Lock the tree to make sure we only run one instance
    lockfile = st_file.parent / f'.{st_file.name}.lock'
    logger.debug('Attempting to obtain lock on %s', lockfile)
    # Deliberately not a context manager: the lock must stay held for the rest
    # of the run and is released explicitly further down.
    flockh = lockfile.open('w', encoding='utf-8')
    try:
        lockf(flockh, LOCK_EX | LOCK_NB)
    except OSError:
        logger.info('Could not obtain exclusive lock on %s', lockfile)
        logger.info('Assuming another process is running.')
        flockh.close()
        return 0

    ses = grokmirror.GrokSession()

    manifile = config['core']['manifest']
    logger.info('Analyzing %s', manifile)
    with grokmirror.locked_manifest(manifile):
        manifest = grokmirror.read_manifest(manifile)

        if st_file.exists():
            logger.info('   status: reading %s', statusfile)
            try:
                # Format of the status file:
                #  {
                #    '/full/path/to/repository': {
                #      'lastcheck': 'YYYY-MM-DD' or 'never',
                #      'nextcheck': 'YYYY-MM-DD',
                #      'lastrepack': 'YYYY-MM-DD',
                #      'fingerprint': 'sha-1',
                #      'fp_mismatch': 'sha-1' (manifest fingerprint we already
                #                     disagreed with on the previous run),
                #      's_elapsed': seconds,
                #      'quick_repack_count': times,
                #    },
                #    ...
                #  }

                status = json.loads(st_file.read_text(encoding='utf-8'))
            except (OSError, ValueError):
                # The manifest lock is released by the with block; this used
                # to leave it held for the rest of the (aborted) run.
                logger.critical('Failed to parse %s', statusfile)
                lockf(flockh, LOCK_UN)
                flockh.close()
                return 1
        else:
            status = {}

        frequency = config.get_int('fsck', 'frequency', 30)

        # Naive local dates throughout: fsck scheduling is in whole calendar
        # days in the admin's own timezone, and the dates in the status file
        # are stored the same way.
        today = datetime.datetime.today()  # noqa: DTZ002
        todayiso = today.strftime('%F')

        # Use randomization for the next check, again
        checkdelay = random.randint(1, frequency) if options.force else frequency

        commitgraph = config.get_bool('fsck', 'commitgraph', True)

        # Is our git version new enough to support it?
        if commitgraph and not grokmirror.git_newer_than('2.18.0'):
            logger.info('Git version too old to support commit graphs, disabling')
            config['fsck']['commitgraph'] = 'no'

        # Go through the manifest and compare with status
        toplevel = os.path.realpath(config['core']['toplevel'])
        # Whatever grok-manifest fingerprints with, we have to fingerprint with
        # too, or every comparison against the manifest disagrees for ever.
        ignorerefs = grokmirror.get_ignorerefs(config)
        changed = False
        for gitdir in list(manifest):
            # str(), because fullpath is the key the status file is written
            # with; a Path here would never match an entry read back from it.
            fullpath = str(grokmirror.gitdir_to_fullpath(toplevel, gitdir))
            # Does it exist?
            if not Path(fullpath).is_dir():
                # Remove it from manifest and status
                manifest.pop(gitdir)
                status.pop(fullpath, None)
                changed = True
                continue

            if fullpath not in status:
                # Newly added repository
                if not options.force:
                    # Randomize next check between now and frequency
                    delay = random.randint(0, frequency)
                    nextdate = today + datetime.timedelta(days=delay)
                    nextcheckiso = nextdate.strftime('%F')
                else:
                    nextcheckiso = todayiso

                status[fullpath] = {
                    'lastcheck': 'never',
                    'nextcheck': nextcheckiso,
                    'fingerprint': grokmirror.get_repo_fingerprint(toplevel, gitdir, ignorerefs=ignorerefs),
                }
                logger.info('%s:', fullpath)
                logger.info('    added: next check on %s', nextcheckiso)

        pretty = config.get_bool('manifest', 'pretty', False)

        if changed:
            grokmirror.write_manifest(manifile, manifest, pretty=pretty)

    # record newly found repos in the status file
    logger.debug('Updating status file in %s', statusfile)
    Path(statusfile).write_text(json.dumps(status, indent=2), encoding='utf-8')

    # Go through status and find all repos that need work done on them.
    # Entries are (fullpath, action, repack_level), with no level for fsck runs.
    to_process: set[tuple[str, str, int | None]] = set()

    total_checked = 0
    total_elapsed = 0
    space_saved = 0

    cfg_repack = config.get_bool('fsck', 'repack', True)
    # Can be "always", which is why we do not read it as a boolean
    cfg_precious = config['fsck'].get('precious', 'yes')

    obstdir = os.path.realpath(config['core']['objstore'])
    logger.info('   search: getting parent commit info from all repos, may take a while')
    top_roots, obst_roots = grokmirror.get_rootsets(ses, toplevel, obstdir)
    amap = ses.get_altrepo_map(toplevel)

    obst_changes = False
    analyzed = 0
    queued = 0
    logger.info('Analyzing %s (%s repos)', toplevel, len(status))
    stattime = time.time()
    baselines = [x.strip() for x in config['fsck'].get('baselines', '').splitlines()]
    fp_broken: list[tuple[str, str]] = []
    fp_stale: list[tuple[str, str]] = []
    for fullpath in list(status):
        # Give me a status every 5 seconds
        if time.time() - stattime >= 5:
            logger.info('      ---: %s/%s analyzed, %s queued', analyzed, len(status), queued)
            stattime = time.time()
        start_size = get_repo_size(fullpath)
        analyzed += 1
        # We do obstrepos separately below, as logic is different
        if grokmirror.is_obstrepo(fullpath, obstdir):
            logger.debug('Skipping %s (obstrepo)')
            continue

        # Check to make sure it's still in the manifest
        gitdir = grokmirror.fullpath_to_gitdir(toplevel, fullpath)

        if gitdir not in manifest:
            status.pop(fullpath)
            logger.debug('%s is gone, no longer in manifest', gitdir)
            continue

        # Is the manifest still telling the truth about this repository?
        verdict = check_manifest_fingerprint(toplevel, gitdir, manifest[gitdir], status[fullpath], ignorerefs)
        if verdict is not None:
            kind, detail = verdict
            (fp_broken if kind == 'broken' else fp_stale).append((fullpath, detail))

        # Make sure FETCH_HEAD is pointing to /dev/null
        fetch_headf = Path(fullpath, 'FETCH_HEAD')
        if not fetch_headf.is_symlink():
            logger.debug('  replacing FETCH_HEAD with symlink to /dev/null')
            fetch_headf.unlink(missing_ok=True)
            fetch_headf.symlink_to('/dev/null')

        # Objstore migration routines
        # Are we using objstore?
        altdir = grokmirror.get_altrepo(fullpath)
        is_private = grokmirror.is_private_repo(config, gitdir)
        # Don't prune any repos that are parents -- until migration is fully complete
        m_prune = not ses.is_alt_repo(toplevel, gitdir)

        if not altdir and Path(fullpath, 'grokmirror.do-not-objstore').exists():
            # The admin excluded this repo from object sharing and it has no
            # alternates, so there is nothing to migrate. Every branch below
            # assumes altdir is set, so don't fall through into them.
            logger.debug('%s: excluded from objstore and has no alternates', gitdir)

        elif not altdir:
            # Do we match any obstdir repos?
            obstrepo = grokmirror.find_best_obstrepo(fullpath, obst_roots, toplevel, baselines)
            if obstrepo:
                obst_changes = True
                # Yes, set ourselves up to be using that obstdir
                logger.info('%s: can use %s', gitdir, Path(obstrepo).name)
                grokmirror.set_altrepo(fullpath, obstrepo)
                if not is_private:
                    grokmirror.add_repo_to_objstore(obstrepo, fullpath)
                    # Fetch into the obstrepo
                    logger.info('    fetch: fetching %s', gitdir)
                    grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                    refresh_obst_roots(obst_roots, obstrepo)
                run_git_repack(ses, fullpath, config, level=1, prune=m_prune)
                space_saved += start_size - get_repo_size(fullpath)
            else:
                # Do we have any toplevel siblings?
                obstrepo = None
                my_roots = grokmirror.get_repo_roots(fullpath)
                top_siblings = grokmirror.find_siblings(fullpath, my_roots, top_roots)
                if top_siblings:
                    # Am I a private repo?
                    if is_private:
                        # Are there any non-private siblings?
                        for top_sibling in top_siblings:
                            # Are you a private repo?
                            if grokmirror.is_private_repo(config, top_sibling):
                                continue
                            # Great, make an objstore repo out of this sibling
                            obstrepo = grokmirror.setup_objstore_repo(obstdir)
                            logger.info('%s: can use %s', gitdir, Path(obstrepo).name)
                            logger.info('     init: new objstore repo %s', Path(obstrepo).name)
                            grokmirror.add_repo_to_objstore(obstrepo, top_sibling)
                            # Fetch into the obstrepo
                            logger.info('    fetch: fetching %s', top_sibling)
                            grokmirror.fetch_objstore_repo(obstrepo, top_sibling)
                            refresh_obst_roots(obst_roots, obstrepo)
                            # It doesn't matter if this fails, because repacking is still safe
                            # Other siblings will match in their own due course
                            break
                    else:
                        # Make an objstore repo out of myself
                        obstrepo = grokmirror.setup_objstore_repo(obstdir)
                        logger.info('%s: can use %s', gitdir, Path(obstrepo).name)
                        logger.info('     init: new objstore repo %s', Path(obstrepo).name)
                        grokmirror.add_repo_to_objstore(obstrepo, fullpath)

                if obstrepo:
                    obst_changes = True
                    # Set alternates to the obstrepo
                    grokmirror.set_altrepo(fullpath, obstrepo)
                    if not is_private:
                        # Fetch into the obstrepo
                        logger.info('    fetch: fetching %s', gitdir)
                        grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                    run_git_repack(ses, fullpath, config, level=1, prune=m_prune)
                    space_saved += start_size - get_repo_size(fullpath)
                    refresh_obst_roots(obst_roots, obstrepo)

        elif not Path(altdir).is_dir():
            if can_reclone(fullpath, config):
                logger.critical('  reclone: %s (alternates repo gone)', gitdir)
                set_repo_reclone(fullpath, 'Alternates repository gone')
            else:
                logger.critical('   BROKEN: %s (alternates repo gone, needs manual attention)', gitdir)
            continue

        elif not grokmirror.is_obstrepo(altdir, obstdir):
            # Alternates pointing at something that isn't an objstore repo is the
            # grokmirror-1.x arrangement, where a fork borrowed objects straight
            # from another toplevel repository. Grokmirror-3 no longer migrates
            # those automatically; see UPGRADING.rst. We leave the repo alone
            # apart from the usual repack/fsck scheduling below, because taking
            # its alternates away would be a destructive guess.
            logger.warning('%s: alternates point at %s, which is not an objstore repo', gitdir, altdir)

        elif not is_private:
            # Make sure this repo is properly set up with obstrepo
            # (e.g. it could have been cloned/copied and obstrepo is not tracking it yet)
            obstrepo = altdir
            s_remotes = grokmirror.list_repo_remotes(obstrepo, withurl=True)
            found = any(childpath == fullpath for _virtref, childpath in s_remotes)
            if not found:
                # Set it up properly
                grokmirror.add_repo_to_objstore(obstrepo, fullpath)
                logger.info(' reconfig: %s to fetch into %s', gitdir, Path(obstrepo).name)

        decision = decide_repo_action(
            fullpath,
            gitdir,
            toplevel,
            status[fullpath],
            today,
            checkdelay,
            cfg_repack,
            cfg_precious,
            options,
            ses,
            config,
        )
        if decision is None:
            continue
        kind, level = decision
        queued += 1
        to_process.add((fullpath, kind, level))

    logger.info('     done: %s analyzed, %s queued', analyzed, queued)
    report_fingerprint_mismatches(fp_broken, fp_stale)

    if obst_changes:
        # Refresh the alt repo map cache
        amap = ses.get_altrepo_map(toplevel, refresh=True)

    obstrepos = ses.find_all_gitdirs(obstdir, normalize=True, exclude_objstore=False)

    analyzed = 0
    queued = 0
    logger.info('Analyzing %s (%s repos)', obstdir, len(obstrepos))
    objstore_uses_plumbing = config.get_bool('core', 'objstore_uses_plumbing', False)
    islandcorematch = grokmirror.compile_globs(config['fsck'].get('islandcores', '').splitlines())
    baselinematch = grokmirror.compile_globs(baselines)
    stattime = time.time()
    for obstrepo in obstrepos:
        if time.time() - stattime >= 5:
            logger.info('      ---: %s/%s analyzed, %s queued', analyzed, len(obstrepos), queued)
            stattime = time.time()
        analyzed += 1
        logger.debug('Processing objstore repo: %s', Path(obstrepo).name)
        my_roots = grokmirror.get_repo_roots(obstrepo)
        if amap.get(obstrepo):
            # Is it redundant with any other objstore repos?
            strategy = config['fsck'].get('obstrepo_merge_strategy', 'exact')
            if strategy == 'blobs':
                siblings = find_siblings_by_blobs(ses, obstrepo, obstdir, ratio=75)
            else:
                exact_merge = True
                if strategy == 'loose':
                    exact_merge = False
                siblings = grokmirror.find_siblings(obstrepo, my_roots, obst_roots, exact=exact_merge)
            if siblings:
                siblings.add(obstrepo)
                mdest = merge_siblings(siblings, amap)
                obst_changes = True
                if mdest in status:
                    # Force full repack of merged obstrepos
                    status[mdest]['nextcheck'] = todayiso

                # Recalculate my roots
                my_roots = refresh_obst_roots(obst_roots, obstrepo)

        # Not an else, because the previous step may have migrated things
        if not amap.get(obstrepo):
            obst_changes = True
            # XXX: Is there a possible race condition here if grok-pull cloned a new repo
            #      while we were migrating this one?
            logger.info('%s: deleting (no longer used by anything)', Path(obstrepo).name)
            if obstrepo in amap:
                amap.pop(obstrepo)
            shutil.rmtree(obstrepo)
            continue

        # Record the latest sibling info in the tracking file
        telltale = Path(obstrepo, 'grokmirror.objstore')
        telltale.write_text(grokmirror.OBST_PREAMBULE + '\n'.join(sorted(amap[obstrepo])) + '\n', encoding='utf-8')

        my_remotes = grokmirror.list_repo_remotes(obstrepo, withurl=True)
        # Use the first child repo as our "reference" entry in manifest
        refrepo = None
        # Use for the alternateRefsPrefixes value
        baseline_refs = set()
        set_islandcore = False
        new_islandcore = False
        valid_virtrefs = set()
        for virtref, childpath in my_remotes:
            # Is it still relevant?
            if childpath not in amap[obstrepo]:
                # Remove it and let prune take care of it
                grokmirror.remove_from_objstore(obstrepo, childpath)
                logger.info('%s: removed remote %s (no longer used)', Path(obstrepo).name, childpath)
                continue
            valid_virtrefs.add(virtref)

            # Does it need fetching?
            fetch = True
            l_fpf = Path(obstrepo, f'grokmirror.{virtref}.fingerprint')
            r_fpf = Path(childpath, 'grokmirror.fingerprint')
            try:
                l_fp = l_fpf.read_text(encoding='utf-8').strip()
                r_fp = r_fpf.read_text(encoding='utf-8').strip()
                if l_fp == r_fp:
                    fetch = False
            except OSError:
                pass

            gitdir = grokmirror.fullpath_to_gitdir(toplevel, childpath)
            if fetch:
                with grokmirror.locked_repo(obstrepo):
                    logger.info('    fetch: %s -> %s', gitdir, Path(obstrepo).name)
                    success = grokmirror.fetch_objstore_repo(obstrepo, childpath, use_plumbing=objstore_uses_plumbing)
                    if not success and objstore_uses_plumbing:
                        # Try using git porcelain
                        grokmirror.fetch_objstore_repo(obstrepo, childpath)

            if gitdir not in manifest:
                continue

            # Do we need to set any alternateRefsPrefixes?
            if baselinematch.match(gitdir):
                baseline_refs.add(f'refs/virtual/{virtref}/heads/')

            # Do we need to set islandCore?
            if not set_islandcore and islandcorematch.match(gitdir):
                set_islandcore = True
                # is it already set to that?
                entries = grokmirror.get_config_from_git(obstrepo, r'pack\.island*')
                if entries.get('islandcore') != virtref:
                    new_islandcore = True
                    logger.info(' reconfig: %s (islandCore to %s)', Path(obstrepo).name, virtref)
                    grokmirror.set_git_config(obstrepo, 'pack.islandCore', virtref)

            if refrepo is None:
                # Legacy "reference=" setting in manifest
                refrepo = gitdir
                manifest[gitdir]['reference'] = None
            else:
                manifest[gitdir]['reference'] = refrepo

            manifest[gitdir]['forkgroup'] = Path(obstrepo).name.removesuffix('.git')

        if baseline_refs:
            # sort the list, so we have deterministic value
            br = list(baseline_refs)
            br.sort()
            refpref = ' '.join(br)
            # Go through all remotes and set their alternateRefsPrefixes
            for _s_virtref, s_childpath in my_remotes:
                # is it already set to that?
                entries = grokmirror.get_config_from_git(s_childpath, r'core\.alternate*')
                if entries.get('alternaterefsprefixes') != refpref:
                    s_gitdir = grokmirror.fullpath_to_gitdir(toplevel, s_childpath)
                    logger.info(' reconfig: %s (baseline)', s_gitdir)
                    grokmirror.set_git_config(s_childpath, 'core.alternateRefsPrefixes', refpref)
        repack_requested = False
        if Path(obstrepo, 'grokmirror.repack').exists():
            repack_requested = True

        # Go through all our refs and find all stale virtrefs
        args = ['for-each-ref', '--format=%(refname)', 'refs/virtual/']
        trimmed_virtrefs = set()
        ecode, out, _err = grokmirror.run_git_command(obstrepo, args)
        if ecode == 0:
            for line in out.splitlines():
                chunks = line.split('/')
                if len(chunks) < 3:
                    # Where did this come from?
                    logger.debug('Weird ref %s in objstore repo %s', line, obstrepo)
                    continue
                virtref = chunks[2]
                if virtref not in valid_virtrefs and virtref not in trimmed_virtrefs:
                    logger.info('     trim: stale virtref %s', virtref)
                    grokmirror.objstore_trim_virtref(obstrepo, virtref)
                    trimmed_virtrefs.add(virtref)

        if obstrepo not in status or new_islandcore or trimmed_virtrefs or repack_requested:
            # We don't use obstrepo fingerprints, so we set it to None
            status[obstrepo] = {
                'lastcheck': 'never',
                'nextcheck': todayiso,
                'fingerprint': None,
            }
            # Always full-repack brand new obstrepos
            obst_repack_level = 2
        else:
            obj_info = grokmirror.get_repo_obj_info(obstrepo)
            obst_repack_level = grokmirror.get_repack_level(obj_info)

        nextcheck = datetime.datetime.strptime(status[obstrepo]['nextcheck'], '%Y-%m-%d')  # noqa: DTZ007
        if obst_repack_level > 1 and nextcheck > today:
            # Don't do full repacks outside of schedule
            obst_repack_level = 1

        if obst_repack_level:
            queued += 1
            to_process.add((obstrepo, 'repack', obst_repack_level))
            if obst_repack_level > 1:
                logger.info('   queued: %s (full repack)', Path(obstrepo).name)
            else:
                logger.info('   queued: %s (repack)', Path(obstrepo).name)
        elif options.repack_only or options.repack_all:
            continue
        elif (nextcheck <= today or options.force) and not options.repack_only:
            queued += 1
            status[obstrepo]['nextcheck'] = nextcheck.strftime('%F')
            to_process.add((obstrepo, 'fsck', None))
            logger.info('   queued: %s (fsck)', Path(obstrepo).name)

    logger.info('     done: %s analyzed, %s queued', analyzed, queued)

    if obst_changes:
        # We keep the same mtime, because the repos themselves haven't changed
        with grokmirror.locked_manifest(manifile):
            # Re-read manifest, so we can update reference and forkgroup data
            disk_manifest = grokmirror.read_manifest(manifile)
            # Go through my manifest and update and changes in forkgroup data
            for gitdir, repoinfo in manifest.items():
                if gitdir not in disk_manifest:
                    # What happened here?
                    continue
                if 'reference' in repoinfo:
                    disk_manifest[gitdir]['reference'] = repoinfo['reference']
                if 'forkgroup' in repoinfo:
                    disk_manifest[gitdir]['forkgroup'] = repoinfo['forkgroup']

            grokmirror.write_manifest(manifile, disk_manifest, pretty=pretty)

    if not to_process:
        logger.info('No repos need attention.')
        return 0

    # Delete some vars that are huge for large repo sets -- we no longer need them and the
    # next step will likely eat lots of ram.
    del obst_roots
    del top_roots
    gc.collect()

    logger.info('Processing %s repositories', len(to_process))

    for fullpath, action, repack_level in to_process:
        logger.info('%s:', fullpath)
        start_size = get_repo_size(fullpath)
        checkdelay = frequency if not options.force else random.randint(1, frequency)
        nextcheck = today + datetime.timedelta(days=checkdelay)

        # Calculate elapsed seconds
        startt = time.time()

        # Wait till the repo is available and lock it for the duration of checks,
        # otherwise there may be false-positives if a mirrored repo is updated
        # in the middle of fsck or repack.
        with grokmirror.locked_repo(fullpath):
            if action == 'repack':
                # to_process only ever queues repacks with a level, so pin it down
                # here rather than treating it as optional throughout. Falling back
                # to a quick repack keeps this working if that ever stops being true.
                level = repack_level or 1
                if run_git_repack(ses, fullpath, config, level):
                    status[fullpath]['lastrepack'] = todayiso
                    if level > 1:
                        Path(fullpath, 'grokmirror.repack').unlink(missing_ok=True)
                        status[fullpath]['lastfullrepack'] = todayiso
                        status[fullpath]['lastcheck'] = todayiso
                        status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
                        # Do we need to generate a preload bundle?
                        if config['fsck'].get('preload_bundle_outdir') and grokmirror.is_obstrepo(fullpath, obstdir):
                            gen_preload_bundle(fullpath, config)
                        logger.info('     next: %s', status[fullpath]['nextcheck'])
                else:
                    logger.warning('Repacking %s was unsuccessful', fullpath)
                    continue

            elif action == 'fsck':
                run_git_fsck(ses, fullpath, config, options.conn_only)
                status[fullpath]['lastcheck'] = todayiso
                status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
                logger.info('     next: %s', status[fullpath]['nextcheck'])

            gitdir = grokmirror.fullpath_to_gitdir(toplevel, fullpath)
            status[fullpath]['fingerprint'] = grokmirror.get_repo_fingerprint(toplevel, gitdir, ignorerefs=ignorerefs)

            # noinspection PyTypeChecker
            elapsed = int(time.time() - startt)
            status[fullpath]['s_elapsed'] = elapsed

        # We're done with the repo now
        total_checked += 1
        total_elapsed += elapsed
        saved = start_size - get_repo_size(fullpath)
        space_saved += saved
        if saved > 0:
            logger.info('     done: %ss, %s saved', elapsed, get_human_size(saved))
        else:
            logger.info('     done: %ss', elapsed)
        if space_saved > 0:
            logger.info(
                '      ---: %s done, %s queued, %s saved',
                total_checked,
                len(to_process) - total_checked,
                get_human_size(space_saved),
            )
        else:
            logger.info('      ---: %s done, %s queued', total_checked, len(to_process) - total_checked)

        # Write status file after each check, so if the process dies, we won't
        # have to recheck all the repos we've already checked
        logger.debug('Updating status file in %s', statusfile)
        Path(statusfile).write_text(json.dumps(status, indent=2), encoding='utf-8')

    logger.info('Processed %s repos in %0.2fs', total_checked, total_elapsed)

    Path(statusfile).write_text(json.dumps(status, indent=2), encoding='utf-8')

    lockf(flockh, LOCK_UN)
    flockh.close()

    return 0


def parse_args() -> argparse.Namespace:

    # noinspection PyTypeChecker
    op = argparse.ArgumentParser(
        prog='grok-fsck',
        description='Optimize and check mirrored repositories',
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
        '-f',
        '--force',
        dest='force',
        action='store_true',
        default=False,
        help='Force immediate run on all repositories',
    )
    op.add_argument('-c', '--config', dest='config', required=True, help='Location of the configuration file')
    op.add_argument(
        '--repack-only',
        dest='repack_only',
        action='store_true',
        default=False,
        help='Only find and repack repositories that need optimizing',
    )
    op.add_argument(
        '--connectivity-only',
        dest='conn_only',
        action='store_true',
        default=False,
        help='Only check connectivity when running fsck checks',
    )
    op.add_argument(
        '--repack-all-quick',
        dest='repack_all_quick',
        action='store_true',
        default=False,
        help='(Assumes --force): Do a quick repack of all repos',
    )
    op.add_argument(
        '--repack-all-full',
        dest='repack_all_full',
        action='store_true',
        default=False,
        help='(Assumes --force): Do a full repack of all repos',
    )
    configcheck.add_check_arguments(op)
    op.add_argument('--version', action='version', version=grokmirror.VERSION)

    opts = op.parse_args()
    configcheck.check_arguments(op, opts)

    if opts.repack_all_quick and opts.repack_all_full:
        op.error('Pick either --repack-all-full or --repack-all-quick')

    return opts


def grok_fsck(cfgfile: str, verbose: bool = False, options: FsckOptions | None = None) -> None:
    if options is None:
        options = FsckOptions()
    # Objstore repositories are configured with delta islands, which appeared
    # in git 2.20 -- refuse to run with anything older, up front and with a
    # clear message, instead of failing halfway through the tree with baffling
    # git errors.
    if not grokmirror.git_newer_than('2.20.0'):
        raise grokmirror.GrokError('grok-fsck requires git 2.20.0 or newer')
    config = grokmirror.load_config_file(cfgfile)

    obstdir = config['core'].get('objstore', None)
    if obstdir is None:
        # load_config_file() guarantees [core]toplevel is set
        obstdir = str(Path(config['core']['toplevel'], 'objstore'))
        config['core']['objstore'] = obstdir

    logfile = config['core'].get('log', None)
    loglevel = logging.DEBUG if config['core'].get('loglevel', 'info') == 'debug' else logging.INFO

    root_logger = grokmirror.init_logger('fsck', logfile, loglevel, verbose)

    rh = io.StringIO()
    ch = logging.StreamHandler(stream=rh)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    ch.setLevel(logging.CRITICAL)
    # The report must capture messages from every grokmirror module, so it
    # attaches to the shared parent logger, not this module's child.
    root_logger.addHandler(ch)

    if not grokmirror.git_newer_than('2.41.0'):
        logger.info('Git older than 2.41 detected: quick repacks will be all-into-one instead of geometric')

    fsck_mirror(config, options)

    report = rh.getvalue()
    if report:
        msg = EmailMessage()
        msg.set_content(report)
        subject = config['fsck'].get('report_subject')
        if not subject:
            subject = f'grok-fsck errors on {platform.node()} ({cfgfile})'
        msg['Subject'] = subject
        from_addr = config['fsck'].get('report_from', 'root')
        msg['From'] = from_addr
        report_to = config['fsck'].get('report_to', 'root')
        msg['To'] = report_to
        mailhost = config['fsck'].get('report_mailhost', 'localhost')
        try:
            s = smtplib.SMTP(mailhost)
            s.send_message(msg)
            s.quit()
        except (OSError, smtplib.SMTPException) as ex:
            # Plenty of hosts have no local MTA, and losing the report entirely
            # (plus a traceback in the cron mail) is the worst possible outcome,
            # since by definition we only get here when something went wrong.
            logger.critical('Could not send the report to %s: %s', mailhost, ex)
            logger.critical('Report follows:\n%s', report)


# [fsck] plus the [core] it shares with everything else. grok-fsck never
# reads [remote] or [pull], so it says nothing about them.
CHECKED_SECTIONS = {'core', 'fsck'}


def command() -> None:
    opts = parse_args()

    if opts.config_check:
        sys.exit(configcheck.run_check(opts.config, CHECKED_SECTIONS, as_json=opts.as_json, online=not opts.no_network))

    try:
        grok_fsck(
            opts.config,
            opts.verbose,
            FsckOptions(
                force=opts.force,
                repack_only=opts.repack_only,
                conn_only=opts.conn_only,
                repack_all_quick=opts.repack_all_quick,
                repack_all_full=opts.repack_all_full,
            ),
        )
    except grokmirror.GrokError as ex:
        sys.stderr.write(f'ERROR: {ex}\n')
        sys.exit(1)


if __name__ == '__main__':
    command()
