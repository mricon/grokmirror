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

import grokmirror
import logging

import time
import json
import random
import datetime
import shutil
import gc
import fnmatch
import io
import smtplib

from pathlib import Path

from email.message import EmailMessage
from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

# default basic logger. We override it later.
logger = logging.getLogger(__name__)


def log_errors(fullpath, cmdargs, lines):
    logger.critical('%s reports errors:', fullpath)
    with open(os.path.join(fullpath, 'grokmirror.fsck.err'), 'w') as fh:
        fh.write('# Date: %s\n' % datetime.datetime.today().strftime('%F'))
        fh.write('# Cmd : git %s\n' % ' '.join(cmdargs))
        count = 0
        for line in lines:
            fh.write('%s\n' % line)
            logger.critical('\t%s', line)
            count += 1
            if count > 10:
                logger.critical('\t [ %s more lines skipped ]', len(lines) - 10)
                logger.critical('\t [ see %s/grokmirror.fsck.err ]', os.path.basename(fullpath))
                break


def gen_preload_bundle(fullpath, config):
    outdir = config['fsck'].get('preload_bundle_outdir')
    Path(outdir).mkdir(parents=True, exist_ok=True)
    bname = '%s.bundle' % os.path.basename(fullpath)[:-4]
    args = ['bundle', 'create', os.path.join(outdir, bname), '--all']
    logger.info(' bundling: %s', bname)
    grokmirror.run_git_command(fullpath, args)


def get_blob_set(fullpath):
    bset = set()
    size = 0
    blobcache = os.path.join(fullpath, 'grokmirror.blobs')
    if os.path.exists(blobcache):
        # Did it age out? Hardcode to 30 days.
        expage = time.time() - 86400*30
        st = os.stat(blobcache)
        if st.st_mtime < expage:
            os.unlink(blobcache)
    try:
        with open(blobcache) as fh:
            while True:
                line = fh.readline()
                if not len(line):
                    break
                if line[0] == '#':
                    continue
                chunks = line.strip().split()
                bhash = chunks[0]
                bsize = int(chunks[1])
                size += bsize
                bset.add((bhash, bsize))
        return bset, size
    except FileNotFoundError:
        pass

    # This only makes sense for repos not using alternates, so make sure you check first
    logger.info(' bloblist: %s', fullpath)
    gitargs = ['cat-file', '--batch-all-objects', '--batch-check', '--unordered']
    retcode, output, error = grokmirror.run_git_command(fullpath, gitargs)
    if retcode == 0:
        with open(blobcache, 'w') as fh:
            fh.write('# Blobs and sizes used for sibling calculation\n')
            for line in output.split('\n'):
                if line.find(' blob ') < 0:
                    continue
                chunks = line.strip().split()
                fh.write(f'{chunks[0]} {chunks[2]}\n')
                bhash = chunks[0]
                bsize = int(chunks[2])
                size += bsize
                bset.add((bhash, bsize))

    return bset, size


def check_sibling_repos_by_blobs(bset1, bsize1, bset2, bsize2, ratio):
    iset = bset1.intersection(bset2)
    if not len(iset):
        return False
    isize = 0
    for bhash, bsize in iset:
        isize += bsize
    # Both repos should share at least ratio % of blobs in them
    ratio1 = int(isize / bsize1 * 100)
    logger.debug('isize=%s, bsize1=%s, ratio1=%s', isize, bsize1, ratio1)
    ratio2 = int(isize / bsize2 * 100)
    logger.debug('isize=%s, bsize2=%s ratio2=%s', isize, bsize2, ratio1)
    if ratio1 >= ratio and ratio2 >= ratio:
        return True

    return False


def find_siblings_by_blobs(obstrepo, obstdir, ratio=75):
    siblings = set()
    oset, osize = get_blob_set(obstrepo)
    for srepo in grokmirror.find_all_gitdirs(obstdir, normalize=True, exclude_objstore=False):
        if srepo == obstrepo:
            continue
        logger.debug('Comparing blobs between %s and %s', obstrepo, srepo)
        sset, ssize = get_blob_set(srepo)
        if check_sibling_repos_by_blobs(oset, osize, sset, ssize, ratio):
            logger.info(' siblings: %s and %s', os.path.basename(obstrepo), os.path.basename(srepo))
            siblings.add(srepo)

    return siblings


def merge_siblings(siblings, amap):
    mdest = None
    rcount = 0
    # Who has the most remotes?
    for sibling in set(siblings):
        if sibling not in amap or not len(amap[sibling]):
            # Orphaned sibling, ignore it -- it will get cleaned up
            siblings.remove(sibling)
            continue
        s_remotes = grokmirror.list_repo_remotes(sibling)
        if len(s_remotes) > rcount:
            mdest = sibling
            rcount = len(s_remotes)

    # Migrate all siblings into the repo with most remotes
    siblings.remove(mdest)
    for sibling in siblings:
        logger.info('%s: merging into %s', os.path.basename(sibling), os.path.basename(mdest))
        s_remotes = grokmirror.list_repo_remotes(sibling, withurl=True)
        for virtref, childpath in s_remotes:
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

            logger.info('         : fetching into %s', os.path.basename(mdest))
            success = grokmirror.fetch_objstore_repo(mdest, childpath)
            if not success:
                logger.critical('Failed to fetch %s from %s to %s', childpath, os.path.basename(sibling),
                                os.path.basename(mdest))
                continue
            logger.info('         : repointing alternates')
            grokmirror.set_altrepo(childpath, mdest)
            amap[sibling].remove(childpath)
            amap[mdest].add(childpath)
            args = ['remote', 'remove', virtref]
            grokmirror.run_git_command(sibling, args)
            logger.info('         : done')

    return mdest


def check_reclone_error(fullpath, config, errors):
    reclone = None
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    errlist = config['fsck'].get('reclone_on_errors', '').split('\n')
    for line in errors:
        for estring in errlist:
            if line.find(estring) != -1:
                # is this repo used for alternates?
                gitdir = '/' + os.path.relpath(fullpath, toplevel).lstrip('/')
                if grokmirror.is_alt_repo(toplevel, gitdir):
                    logger.critical('\tused for alternates, not requesting auto-reclone')
                    return
                else:
                    reclone = line
                    logger.critical('\trequested auto-reclone')
                break
            if reclone is not None:
                break
    if reclone is None:
        return

    set_repo_reclone(fullpath, reclone)


def get_repo_size(fullpath):
    oi = grokmirror.get_repo_obj_info(fullpath)
    kbsize = 0
    for field in ['size', 'size-pack', 'size-garbage']:
        try:
            kbsize += int(oi[field])
        except (KeyError, ValueError):
            pass
    logger.debug('%s size: %s kb', fullpath, kbsize)
    return kbsize


def get_human_size(kbsize):
    num = kbsize
    for unit in ['Ki', 'Mi', 'Gi']:
        if abs(num) < 1024.0:
            return "%3.2f %sB" % (num, unit)
        num /= 1024.0
    return "%.2f%s TiB" % num


def set_repo_reclone(fullpath, reason):
    rfile = os.path.join(fullpath, 'grokmirror.reclone')
    # Have we already requested a reclone?
    if os.path.exists(rfile):
        logger.debug('Already requested repo reclone for %s', fullpath)
        return

    with open(rfile, 'w') as rfh:
        rfh.write('Requested by grok-fsck due to error: %s' % reason)


def run_git_prune(fullpath, config):
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
    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    if error:
        warn = remove_ignored_errors(error, config)
        if warn:
            prune_ok = False
            log_errors(fullpath, args, warn)
            check_reclone_error(fullpath, config, warn)

    if isprecious:
        set_precious_objects(fullpath, True)

    return prune_ok


def is_safe_to_prune(fullpath, config):
    if config['fsck'].get('prune', 'yes') != 'yes':
        logger.debug('Pruning disabled in config file')
        return False
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    obstdir = os.path.realpath(config['core'].get('objstore'))
    gitdir = '/' + os.path.relpath(fullpath, toplevel).lstrip('/')
    if grokmirror.is_obstrepo(fullpath, obstdir):
        # We only prune if all repos pointing to us are public
        urls = set(grokmirror.list_repo_remotes(fullpath, withurl=True))
        mine = set([x[1] for x in urls])
        amap = grokmirror.get_altrepo_map(toplevel)
        if mine != amap[fullpath]:
            logger.debug('Cannot prune %s because it is used by non-public repos', gitdir)
            return False
    elif grokmirror.is_alt_repo(toplevel, gitdir):
        logger.debug('Cannot prune %s because it is used as alternates by other repos', gitdir)
        return False

    logger.debug('%s should be safe to prune', gitdir)
    return True


def remove_ignored_errors(output, config):
    ierrors = set([x.strip() for x in config['fsck'].get('ignore_errors', '').split('\n')])
    debug = list()
    warn = list()
    for line in output.split('\n'):
        ignored = False
        for estring in ierrors:
            if line.find(estring) != -1:
                ignored = True
                debug.append(line)
                break
        if not ignored:
            warn.append(line)
    if debug:
        logger.debug('Stderr: %s', '\n'.join(debug))

    return warn


def run_git_repack(fullpath, config, level=1, prune=True):
    # Returns false if we hit any errors on the way
    repack_ok = True
    obstdir = os.path.realpath(config['core'].get('objstore'))
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    gitdir = '/' + os.path.relpath(fullpath, toplevel).lstrip('/')

    if prune:
        # Make sure it's safe to do so
        prune = is_safe_to_prune(fullpath, config)

    if config['fsck'].get('precious', '') == 'always':
        always_precious = True
        set_precious_objects(fullpath, enabled=True)
    else:
        always_precious = False
        set_precious_objects(fullpath, enabled=False)

    set_precious_after = False
    gen_commitgraph = True

    # Figure out what our repack flags should be.
    repack_flags = list()
    rregular = config['fsck'].get('extra_repack_flags', '').split()
    if len(rregular):
        repack_flags += rregular

    full_repack_flags = ['-f', '--pack-kept-objects']
    rfull = config['fsck'].get('extra_repack_flags_full', '').split()
    if len(rfull):
        full_repack_flags += rfull

    if grokmirror.is_obstrepo(fullpath, obstdir):
        set_precious_after = True
        repack_flags.append('-a')
        if not prune and not always_precious:
            repack_flags.append('-k')

    elif grokmirror.is_alt_repo(toplevel, gitdir):
        set_precious_after = True
        if grokmirror.get_altrepo(fullpath):
            gen_commitgraph = False
            logger.warning(' warning : has alternates and is used by others for alternates')
            logger.warning('         : this can cause grandchild corruption')
            repack_flags.append('-A')
            repack_flags.append('-l')
        else:
            repack_flags.append('-a')
            repack_flags.append('-b')
            if not always_precious:
                repack_flags.append('-k')

    elif grokmirror.get_altrepo(fullpath):
        # we are a "child repo"
        gen_commitgraph = False
        repack_flags.append('-l')
        repack_flags.append('-A')
        if prune:
            repack_flags.append('--unpack-unreachable=yesterday')

    else:
        # we have no relationships with other repos
        repack_flags.append('-a')
        repack_flags.append('-b')
        if prune:
            repack_flags.append('--unpack-unreachable=yesterday')

    if level > 1:
        logger.info('   repack: performing a full repack for optimal deltas')
        repack_flags += full_repack_flags

    if not always_precious:
        repack_flags.append('-d')

    # If we have a logs dir, then run reflog expire
    if os.path.isdir(os.path.join(fullpath, 'logs')):
        args = ['reflog', 'expire', '--all', '--stale-fix']
        logger.info('   reflog: expiring reflogs')
        grokmirror.run_git_command(fullpath, args)

    args = ['repack'] + repack_flags
    logger.info('   repack: repacking with "%s"', ' '.join(repack_flags))

    # We always tack on -q
    args.append('-q')

    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    # With newer versions of git, repack may return warnings that are safe to ignore
    # so use the same strategy to weed out things we aren't interested in seeing
    if error:
        warn = remove_ignored_errors(error, config)
        if warn:
            repack_ok = False
            log_errors(fullpath, args, warn)
            check_reclone_error(fullpath, config, warn)

    if not repack_ok:
        # No need to repack refs if repo is broken
        if set_precious_after:
            set_precious_objects(fullpath, enabled=True)
        return False

    if gen_commitgraph and config['fsck'].get('commitgraph', 'yes') == 'yes':
        grokmirror.set_git_config(fullpath, 'core.commitgraph', 'true')
        run_git_commit_graph(fullpath)

    # repacking refs requires a separate command, so run it now
    args = ['pack-refs']
    if level > 1:
        logger.info(' packrefs: repacking all refs')
        args.append('--all')
    else:
        logger.info(' packrefs: repacking refs')
    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    # pack-refs shouldn't return anything, but use the same ignore_errors block
    # to weed out any future potential benign warnings
    if error:
        warn = remove_ignored_errors(error, config)
        if warn:
            repack_ok = False
            log_errors(fullpath, args, warn)
            check_reclone_error(fullpath, config, warn)

    if prune:
        repack_ok = run_git_prune(fullpath, config)

    if set_precious_after:
        set_precious_objects(fullpath, enabled=True)

    return repack_ok


def run_git_fsck(fullpath, config, conn_only=False):
    args = ['fsck', '--no-progress', '--no-dangling', '--no-reflogs']
    obstdir = os.path.realpath(config['core'].get('objstore'))
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

    retcode, output, error = grokmirror.run_git_command(fullpath, args)
    output = output + '\n' + error

    if output:
        warn = remove_ignored_errors(output, config)
        if warn:
            log_errors(fullpath, args, warn)
            check_reclone_error(fullpath, config, warn)


def run_git_commit_graph(fullpath):
    # Does our version of git support commit-graph?
    if not grokmirror.git_newer_than('2.18.0'):
        logger.debug('Git version too old, not generating commit-graph')
    logger.info('    graph: generating commit-graph')
    args = ['commit-graph', 'write']
    retcode, output, error = grokmirror.run_git_command(fullpath, args)
    if retcode == 0:
        return True

    return False


def set_precious_objects(fullpath, enabled=True):
    # It's better to just set it blindly without checking first,
    # as this results in one fewer shell-out.
    logger.debug('Setting preciousObjects for %s', fullpath)
    if enabled:
        poval = 'true'
    else:
        poval = 'false'
    grokmirror.set_git_config(fullpath, 'extensions.preciousObjects', poval)


def check_precious_objects(fullpath):
    return grokmirror.is_precious(fullpath)


def fsck_mirror(config, force=False, repack_only=False, conn_only=False,
                repack_all_quick=False, repack_all_full=False):

    if repack_all_quick or repack_all_full:
        force = True

    statusfile = config['fsck'].get('statusfile')
    if not statusfile:
        logger.critical('Please define fsck.statusfile in the config')
        return 1

    st_dir = os.path.dirname(statusfile)
    if not os.path.isdir(os.path.dirname(statusfile)):
        logger.critical('Directory %s is absent', st_dir)
        return 1

    # Lock the tree to make sure we only run one instance
    lockfile = os.path.join(st_dir, '.%s.lock' % os.path.basename(statusfile))
    logger.debug('Attempting to obtain lock on %s', lockfile)
    flockh = open(lockfile, 'w')
    try:
        lockf(flockh, LOCK_EX | LOCK_NB)
    except IOError:
        logger.info('Could not obtain exclusive lock on %s', lockfile)
        logger.info('Assuming another process is running.')
        return 0

    manifile = config['core'].get('manifest')
    logger.info('Analyzing %s', manifile)
    grokmirror.manifest_lock(manifile)
    manifest = grokmirror.read_manifest(manifile)

    if os.path.exists(statusfile):
        logger.info('   status: reading %s', statusfile)
        stfh = open(statusfile, 'r')
        # noinspection PyBroadException
        try:
            # Format of the status file:
            #  {
            #    '/full/path/to/repository': {
            #      'lastcheck': 'YYYY-MM-DD' or 'never',
            #      'nextcheck': 'YYYY-MM-DD',
            #      'lastrepack': 'YYYY-MM-DD',
            #      'fingerprint': 'sha-1',
            #      's_elapsed': seconds,
            #      'quick_repack_count': times,
            #    },
            #    ...
            #  }

            status = json.loads(stfh.read())
        except:
            logger.critical('Failed to parse %s', statusfile)
            lockf(flockh, LOCK_UN)
            flockh.close()
            return 1
    else:
        status = dict()

    frequency = config['fsck'].getint('frequency', 30)

    today = datetime.datetime.today()
    todayiso = today.strftime('%F')

    if force:
        # Use randomization for next check, again
        checkdelay = random.randint(1, frequency)
    else:
        checkdelay = frequency

    commitgraph = config['fsck'].getboolean('commitgraph', True)

    # Is our git version new enough to support it?
    if commitgraph and not grokmirror.git_newer_than('2.18.0'):
        logger.info('Git version too old to support commit graphs, disabling')
        config['fsck']['commitgraph'] = 'no'

    # Go through the manifest and compare with status
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    changed = False
    for gitdir in list(manifest):
        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
        # Does it exist?
        if not os.path.isdir(fullpath):
            # Remove it from manifest and status
            manifest.pop(gitdir)
            status.pop(fullpath)
            changed = True
            continue

        if fullpath not in status.keys():
            # Newly added repository
            if not force:
                # Randomize next check between now and frequency
                delay = random.randint(0, frequency)
                nextdate = today + datetime.timedelta(days=delay)
                nextcheck = nextdate.strftime('%F')
            else:
                nextcheck = todayiso

            status[fullpath] = {
                'lastcheck': 'never',
                'nextcheck': nextcheck,
                'fingerprint': grokmirror.get_repo_fingerprint(toplevel, gitdir),
            }
            logger.info('%s:', fullpath)
            logger.info('    added: next check on %s', nextcheck)

    if 'manifest' in config:
        pretty = config['manifest'].getboolean('pretty', False)
    else:
        pretty = False

    if changed:
        grokmirror.write_manifest(manifile, manifest, pretty=pretty)

    grokmirror.manifest_unlock(manifile)

    # record newly found repos in the status file
    logger.debug('Updating status file in %s', statusfile)
    with open(statusfile, 'w') as stfh:
        stfh.write(json.dumps(status, indent=2))

    # Go through status and find all repos that need work done on them.
    to_process = set()

    total_checked = 0
    total_elapsed = 0
    space_saved = 0

    cfg_repack = config['fsck'].getboolean('repack', True)
    # Can be "always", which is why we don't getboolean
    cfg_precious = config['fsck'].get('precious', 'yes')

    obstdir = os.path.realpath(config['core'].get('objstore'))
    logger.info('   search: getting parent commit info from all repos, may take a while')
    top_roots, obst_roots = grokmirror.get_rootsets(toplevel, obstdir)
    amap = grokmirror.get_altrepo_map(toplevel)

    fetched_obstrepos = set()
    obst_changes = False
    analyzed = 0
    queued = 0
    logger.info('Analyzing %s (%s repos)', toplevel, len(status))
    stattime = time.time()
    baselines = [x.strip() for x in config['fsck'].get('baselines', '').split('\n')]
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
        gitdir = fullpath.replace(toplevel, '', 1)
        gitdir = '/' + gitdir.lstrip('/')

        if gitdir not in manifest:
            status.pop(fullpath)
            logger.debug('%s is gone, no longer in manifest', gitdir)
            continue

        # Make sure FETCH_HEAD is pointing to /dev/null
        fetch_headf = os.path.join(fullpath, 'FETCH_HEAD')
        if not os.path.islink(fetch_headf):
            logger.debug('  replacing FETCH_HEAD with symlink to /dev/null')
            try:
                os.unlink(fetch_headf)
            except FileNotFoundError:
                pass
            os.symlink('/dev/null', fetch_headf)

        # Objstore migration routines
        # Are we using objstore?
        altdir = grokmirror.get_altrepo(fullpath)
        is_private = grokmirror.is_private_repo(config, gitdir)
        if grokmirror.is_alt_repo(toplevel, gitdir):
            # Don't prune any repos that are parents -- until migration is fully complete
            m_prune = False
        else:
            m_prune = True

        if not altdir and not os.path.exists(os.path.join(fullpath, 'grokmirror.do-not-objstore')):
            # Do we match any obstdir repos?
            obstrepo = grokmirror.find_best_obstrepo(fullpath, obst_roots, toplevel, baselines)
            if obstrepo:
                obst_changes = True
                # Yes, set ourselves up to be using that obstdir
                logger.info('%s: can use %s', gitdir, os.path.basename(obstrepo))
                grokmirror.set_altrepo(fullpath, obstrepo)
                if not is_private:
                    grokmirror.add_repo_to_objstore(obstrepo, fullpath)
                    # Fetch into the obstrepo
                    logger.info('    fetch: fetching %s', gitdir)
                    grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                    obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)
                run_git_repack(fullpath, config, level=1, prune=m_prune)
                space_saved += start_size - get_repo_size(fullpath)
            else:
                # Do we have any toplevel siblings?
                obstrepo = None
                my_roots = grokmirror.get_repo_roots(fullpath)
                top_siblings = grokmirror.find_siblings(fullpath, my_roots, top_roots)
                if len(top_siblings):
                    # Am I a private repo?
                    if is_private:
                        # Are there any non-private siblings?
                        for top_sibling in top_siblings:
                            # Are you a private repo?
                            if grokmirror.is_private_repo(config, top_sibling):
                                continue
                            # Great, make an objstore repo out of this sibling
                            obstrepo = grokmirror.setup_objstore_repo(obstdir)
                            logger.info('%s: can use %s', gitdir, os.path.basename(obstrepo))
                            logger.info('     init: new objstore repo %s', os.path.basename(obstrepo))
                            grokmirror.add_repo_to_objstore(obstrepo, top_sibling)
                            # Fetch into the obstrepo
                            logger.info('    fetch: fetching %s', top_sibling)
                            grokmirror.fetch_objstore_repo(obstrepo, top_sibling)
                            obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)
                            # It doesn't matter if this fails, because repacking is still safe
                            # Other siblings will match in their own due course
                            break
                    else:
                        # Make an objstore repo out of myself
                        obstrepo = grokmirror.setup_objstore_repo(obstdir)
                        logger.info('%s: can use %s', gitdir, os.path.basename(obstrepo))
                        logger.info('     init: new objstore repo %s', os.path.basename(obstrepo))
                        grokmirror.add_repo_to_objstore(obstrepo, fullpath)

                if obstrepo:
                    obst_changes = True
                    # Set alternates to the obstrepo
                    grokmirror.set_altrepo(fullpath, obstrepo)
                    if not is_private:
                        # Fetch into the obstrepo
                        logger.info('    fetch: fetching %s', gitdir)
                        grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                    run_git_repack(fullpath, config, level=1, prune=m_prune)
                    space_saved += start_size - get_repo_size(fullpath)
                    obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)

        elif not os.path.isdir(altdir):
            logger.critical('  reclone: %s (alternates repo gone)', gitdir)
            set_repo_reclone(fullpath, 'Alternates repository gone')
            continue

        elif altdir.find(obstdir) != 0:
            # We have an alternates repo, but it's not an objstore repo
            # Probably left over from grokmirror-1.x
            # Do we have any matching obstrepos?
            obstrepo = grokmirror.find_best_obstrepo(fullpath, obst_roots, toplevel, baselines)
            if obstrepo:
                logger.info('%s: migrating to %s', gitdir, os.path.basename(obstrepo))
                if altdir not in fetched_obstrepos:
                    # We're already sharing objects with altdir, so no need to check if it's private
                    grokmirror.add_repo_to_objstore(obstrepo, altdir)
                    logger.info('    fetch: fetching %s (previous parent)', os.path.relpath(altdir, toplevel))
                    success = grokmirror.fetch_objstore_repo(obstrepo, altdir)
                    fetched_obstrepos.add(altdir)
                    if success:
                        set_precious_objects(altdir, enabled=False)
                        pre_size = get_repo_size(altdir)
                        run_git_repack(altdir, config, level=1, prune=False)
                        space_saved += pre_size - get_repo_size(altdir)
                    else:
                        logger.critical('Unsuccessful fetching %s into %s', altdir, os.path.basename(obstrepo))
                        obstrepo = None
            else:
                # Make a new obstrepo out of mommy
                obstrepo = grokmirror.setup_objstore_repo(obstdir)
                logger.info('%s: migrating to %s', gitdir, os.path.basename(obstrepo))
                logger.info('     init: new objstore repo %s', os.path.basename(obstrepo))
                grokmirror.add_repo_to_objstore(obstrepo, altdir)
                logger.info('    fetch: fetching %s (previous parent)', os.path.relpath(altdir, toplevel))
                success = grokmirror.fetch_objstore_repo(obstrepo, altdir)
                fetched_obstrepos.add(altdir)
                if success:
                    grokmirror.set_altrepo(altdir, obstrepo)
                    # mommy is no longer precious
                    set_precious_objects(altdir, enabled=False)
                    # Don't prune, because there may be objects others are still borrowing
                    # It can only be pruned once the full migration is completed
                    pre_size = get_repo_size(altdir)
                    run_git_repack(altdir, config, level=1, prune=False)
                    space_saved += pre_size - get_repo_size(altdir)
                else:
                    logger.critical('Unsuccessful fetching %s into %s', altdir, os.path.basename(obstrepo))
                    obstrepo = None

            if obstrepo:
                obst_changes = True
                if not is_private:
                    # Fetch into the obstrepo
                    grokmirror.add_repo_to_objstore(obstrepo, fullpath)
                    logger.info('    fetch: fetching %s', gitdir)
                    if grokmirror.fetch_objstore_repo(obstrepo, fullpath):
                        grokmirror.set_altrepo(fullpath, obstrepo)
                        set_precious_objects(fullpath, enabled=False)
                        run_git_repack(fullpath, config, level=1, prune=m_prune)
                        space_saved += start_size - get_repo_size(fullpath)
                else:
                    # Grab all the objects from the previous parent, since we can't simply
                    # fetch ourselves into the obstrepo (we're private).
                    args = ['repack', '-a']
                    logger.info('    fetch: restoring private repo %s', gitdir)
                    if grokmirror.run_git_command(fullpath, args):
                        grokmirror.set_altrepo(fullpath, obstrepo)
                        set_precious_objects(fullpath, enabled=False)
                        # Now repack ourselves to get rid of any public objects
                        run_git_repack(fullpath, config, level=1, prune=m_prune)

                obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)

        elif altdir.find(obstdir) == 0 and not is_private:
            # Make sure this repo is properly set up with obstrepo
            # (e.g. it could have been cloned/copied and obstrepo is not tracking it yet)
            obstrepo = altdir
            s_remotes = grokmirror.list_repo_remotes(obstrepo, withurl=True)
            found = False
            for virtref, childpath in s_remotes:
                if childpath == fullpath:
                    found = True
                    break
            if not found:
                # Set it up properly
                grokmirror.add_repo_to_objstore(obstrepo, fullpath)
                logger.info(' reconfig: %s to fetch into %s', gitdir, os.path.basename(obstrepo))

        obj_info = grokmirror.get_repo_obj_info(fullpath)
        try:
            packs = int(obj_info['packs'])
            count_loose = int(obj_info['count'])
        except KeyError:
            logger.warning('Unable to count objects in %s, skipping' % fullpath)
            continue

        schedcheck = datetime.datetime.strptime(status[fullpath]['nextcheck'], '%Y-%m-%d')
        nextcheck = today + datetime.timedelta(days=checkdelay)

        if not cfg_repack:
            # don't look at me if you turned off repack
            logger.debug('Not repacking because repack=no in config')
            repack_level = None
        elif repack_all_full and (count_loose > 0 or packs > 1):
            logger.debug('repack_level=2 due to repack_all_full')
            repack_level = 2
        elif repack_all_quick and count_loose > 0:
            logger.debug('repack_level=1 due to repack_all_quick')
            repack_level = 1
        elif status[fullpath].get('fingerprint') != grokmirror.get_repo_fingerprint(toplevel, gitdir):
            logger.debug('Checking repack level of %s', fullpath)
            repack_level = grokmirror.get_repack_level(obj_info)
        else:
            repack_level = None

        # trigger a level-1 repack if it's regular check time and the fingerprint has changed
        if (not repack_level and schedcheck <= today
                and status[fullpath].get('fingerprint') != grokmirror.get_repo_fingerprint(toplevel, gitdir)):
            status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
            logger.info('     aged: %s (forcing repack)', fullpath)
            repack_level = 1

        # If we're not already repacking the repo, run a prune if we find garbage in it
        if obj_info['garbage'] != '0' and not repack_level and is_safe_to_prune(fullpath, config):
            logger.info('  garbage: %s (%s files, %s KiB)', gitdir, obj_info['garbage'], obj_info['size-garbage'])
            try:
                grokmirror.lock_repo(fullpath, nonblocking=True)
                run_git_prune(fullpath, config)
                grokmirror.unlock_repo(fullpath)
            except IOError:
                pass

        if repack_level and (cfg_precious == 'always' and check_precious_objects(fullpath)):
            # if we have preciousObjects, then we only repack based on the same
            # schedule as fsck.
            logger.debug('preciousObjects is set')
            # for repos with preciousObjects, we use the fsck schedule for repacking
            if schedcheck <= today:
                logger.debug('Time for a full periodic repack of a preciousObjects repo')
                status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
                repack_level = 2
            else:
                logger.debug('Not repacking preciousObjects repo outside of schedule')
                repack_level = None

        if repack_level:
            queued += 1
            to_process.add((fullpath, 'repack', repack_level))
            if repack_level > 1:
                logger.info('   queued: %s (full repack)', fullpath)
            else:
                logger.info('   queued: %s (repack)', fullpath)
        elif repack_only or repack_all_quick or repack_all_full:
            continue
        elif schedcheck <= today or force:
            queued += 1
            to_process.add((fullpath, 'fsck', None))
            logger.info('   queued: %s (fsck)', fullpath)

    logger.info('     done: %s analyzed, %s queued', analyzed, queued)

    if obst_changes:
        # Refresh the alt repo map cache
        amap = grokmirror.get_altrepo_map(toplevel, refresh=True)

    obstrepos = grokmirror.find_all_gitdirs(obstdir, normalize=True, exclude_objstore=False)

    analyzed = 0
    queued = 0
    logger.info('Analyzing %s (%s repos)', obstdir, len(obstrepos))
    objstore_uses_plumbing = config['core'].getboolean('objstore_uses_plumbing', False)
    islandcores = [x.strip() for x in config['fsck'].get('islandcores', '').split('\n')]
    stattime = time.time()
    for obstrepo in obstrepos:
        if time.time() - stattime >= 5:
            logger.info('      ---: %s/%s analyzed, %s queued', analyzed, len(obstrepos), queued)
            stattime = time.time()
        analyzed += 1
        logger.debug('Processing objstore repo: %s', os.path.basename(obstrepo))
        my_roots = grokmirror.get_repo_roots(obstrepo)
        if obstrepo in amap and len(amap[obstrepo]):
            # Is it redundant with any other objstore repos?
            strategy = config['fsck'].get('obstrepo_merge_strategy', 'exact')
            if strategy == 'blobs':
                siblings = find_siblings_by_blobs(obstrepo, obstdir, ratio=75)
            else:
                exact_merge = True
                if strategy == 'loose':
                    exact_merge = False
                siblings = grokmirror.find_siblings(obstrepo, my_roots, obst_roots, exact=exact_merge)
            if len(siblings):
                siblings.add(obstrepo)
                mdest = merge_siblings(siblings, amap)
                obst_changes = True
                if mdest in status:
                    # Force full repack of merged obstrepos
                    status[mdest]['nextcheck'] = todayiso

                # Recalculate my roots
                my_roots = grokmirror.get_repo_roots(obstrepo, force=True)
                obst_roots[obstrepo] = my_roots

        # Not an else, because the previous step may have migrated things
        if obstrepo not in amap or not len(amap[obstrepo]):
            obst_changes = True
            # XXX: Is there a possible race condition here if grok-pull cloned a new repo
            #      while we were migrating this one?
            logger.info('%s: deleting (no longer used by anything)', os.path.basename(obstrepo))
            if obstrepo in amap:
                amap.pop(obstrepo)
            shutil.rmtree(obstrepo)
            continue

        # Record the latest sibling info in the tracking file
        telltale = os.path.join(obstrepo, 'grokmirror.objstore')
        with open(telltale, 'w') as fh:
            fh.write(grokmirror.OBST_PREAMBULE)
            fh.write('\n'.join(sorted(list(amap[obstrepo]))) + '\n')

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
                logger.info('%s: removed remote %s (no longer used)', os.path.basename(obstrepo), childpath)
                continue
            valid_virtrefs.add(virtref)

            # Does it need fetching?
            fetch = True
            l_fpf = os.path.join(obstrepo, 'grokmirror.%s.fingerprint' % virtref)
            r_fpf = os.path.join(childpath, 'grokmirror.fingerprint')
            try:
                with open(l_fpf) as fh:
                    l_fp = fh.read().strip()
                with open(r_fpf) as fh:
                    r_fp = fh.read().strip()
                if l_fp == r_fp:
                    fetch = False
            except IOError:
                pass

            gitdir = '/' + os.path.relpath(childpath, toplevel)
            if fetch:
                grokmirror.lock_repo(obstrepo, nonblocking=False)
                logger.info('    fetch: %s -> %s', gitdir, os.path.basename(obstrepo))
                success = grokmirror.fetch_objstore_repo(obstrepo, childpath, use_plumbing=objstore_uses_plumbing)
                if not success and objstore_uses_plumbing:
                    # Try using git porcelain
                    grokmirror.fetch_objstore_repo(obstrepo, childpath)
                grokmirror.unlock_repo(obstrepo)

            if gitdir not in manifest:
                continue

            # Do we need to set any alternateRefsPrefixes?
            for baseline in baselines:
                # Does this repo match a baseline
                if fnmatch.fnmatch(gitdir, baseline):
                    baseline_refs.add('refs/virtual/%s/heads/' % virtref)
                    break

            # Do we need to set islandCore?
            if not set_islandcore:
                is_islandcore = False
                for islandcore in islandcores:
                    # Does this repo match a baseline
                    if fnmatch.fnmatch(gitdir, islandcore):
                        is_islandcore = True
                        break
                if is_islandcore:
                    set_islandcore = True
                    # is it already set to that?
                    entries = grokmirror.get_config_from_git(obstrepo, r'pack\.island*')
                    if entries.get('islandcore') != virtref:
                        new_islandcore = True
                        logger.info(' reconfig: %s (islandCore to %s)', os.path.basename(obstrepo), virtref)
                        grokmirror.set_git_config(obstrepo, 'pack.islandCore', virtref)

            if refrepo is None:
                # Legacy "reference=" setting in manifest
                refrepo = gitdir
                manifest[gitdir]['reference'] = None
            else:
                manifest[gitdir]['reference'] = refrepo

            manifest[gitdir]['forkgroup'] = os.path.basename(obstrepo[:-4])

        if len(baseline_refs):
            # sort the list, so we have deterministic value
            br = list(baseline_refs)
            br.sort()
            refpref = ' '.join(br)
            # Go through all remotes and set their alternateRefsPrefixes
            for s_virtref, s_childpath in my_remotes:
                # is it already set to that?
                entries = grokmirror.get_config_from_git(s_childpath, r'core\.alternate*')
                if entries.get('alternaterefsprefixes') != refpref:
                    s_gitdir = '/' + os.path.relpath(s_childpath, toplevel)
                    logger.info(' reconfig: %s (baseline)', s_gitdir)
                    grokmirror.set_git_config(s_childpath, 'core.alternateRefsPrefixes', refpref)
        repack_requested = False
        if os.path.exists(os.path.join(obstrepo, 'grokmirror.repack')):
            repack_requested = True

        # Go through all our refs and find all stale virtrefs
        args = ['for-each-ref', '--format=%(refname)', 'refs/virtual/']
        trimmed_virtrefs = set()
        ecode, out, err = grokmirror.run_git_command(obstrepo, args)
        if ecode == 0 and out:
            for line in out.split('\n'):
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
            repack_level = 2
        else:
            obj_info = grokmirror.get_repo_obj_info(obstrepo)
            repack_level = grokmirror.get_repack_level(obj_info)

        nextcheck = datetime.datetime.strptime(status[obstrepo]['nextcheck'], '%Y-%m-%d')
        if repack_level > 1 and nextcheck > today:
            # Don't do full repacks outside of schedule
            repack_level = 1

        if repack_level:
            queued += 1
            to_process.add((obstrepo, 'repack', repack_level))
            if repack_level > 1:
                logger.info('   queued: %s (full repack)', os.path.basename(obstrepo))
            else:
                logger.info('   queued: %s (repack)', os.path.basename(obstrepo))
        elif repack_only or repack_all_quick or repack_all_full:
            continue
        elif (nextcheck <= today or force) and not repack_only:
            queued += 1
            status[obstrepo]['nextcheck'] = nextcheck.strftime('%F')
            to_process.add((obstrepo, 'fsck', None))
            logger.info('   queued: %s (fsck)', os.path.basename(obstrepo))

    logger.info('     done: %s analyzed, %s queued', analyzed, queued)

    if obst_changes:
        # We keep the same mtime, because the repos themselves haven't changed
        grokmirror.manifest_lock(manifile)
        # Re-read manifest, so we can update reference and forkgroup data
        disk_manifest = grokmirror.read_manifest(manifile)
        # Go through my manifest and update and changes in forkgroup data
        for gitdir in manifest:
            if gitdir not in disk_manifest:
                # What happened here?
                continue
            if 'reference' in manifest[gitdir]:
                disk_manifest[gitdir]['reference'] = manifest[gitdir]['reference']
            if 'forkgroup' in manifest[gitdir]:
                disk_manifest[gitdir]['forkgroup'] = manifest[gitdir]['forkgroup']

        grokmirror.write_manifest(manifile, disk_manifest, pretty=pretty)
        grokmirror.manifest_unlock(manifile)

    if not len(to_process):
        logger.info('No repos need attention.')
        return

    # Delete some vars that are huge for large repo sets -- we no longer need them and the
    # next step will likely eat lots of ram.
    del obst_roots
    del top_roots
    gc.collect()

    logger.info('Processing %s repositories', len(to_process))

    for fullpath, action, repack_level in to_process:
        logger.info('%s:', fullpath)
        start_size = get_repo_size(fullpath)
        checkdelay = frequency if not force else random.randint(1, frequency)
        nextcheck = today + datetime.timedelta(days=checkdelay)

        # Calculate elapsed seconds
        startt = time.time()

        # Wait till the repo is available and lock it for the duration of checks,
        # otherwise there may be false-positives if a mirrored repo is updated
        # in the middle of fsck or repack.
        grokmirror.lock_repo(fullpath, nonblocking=False)
        if action == 'repack':
            if run_git_repack(fullpath, config, repack_level):
                status[fullpath]['lastrepack'] = todayiso
                if repack_level > 1:
                    try:
                        os.unlink(os.path.join(fullpath, 'grokmirror.repack'))
                    except FileNotFoundError:
                        pass

                    status[fullpath]['lastfullrepack'] = todayiso
                    status[fullpath]['lastcheck'] = todayiso
                    status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
                    # Do we need to generate a preload bundle?
                    if config['fsck'].get('preload_bundle_outdir') and grokmirror.is_obstrepo(fullpath, obstdir):
                        gen_preload_bundle(fullpath, config)
                    logger.info('     next: %s', status[fullpath]['nextcheck'])
            else:
                logger.warning('Repacking %s was unsuccessful', fullpath)
                grokmirror.unlock_repo(fullpath)
                continue

        elif action == 'fsck':
            run_git_fsck(fullpath, config, conn_only)
            status[fullpath]['lastcheck'] = todayiso
            status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
            logger.info('     next: %s', status[fullpath]['nextcheck'])

        gitdir = '/' + os.path.relpath(fullpath, toplevel)
        status[fullpath]['fingerprint'] = grokmirror.get_repo_fingerprint(toplevel, gitdir)

        # noinspection PyTypeChecker
        elapsed = int(time.time()-startt)
        status[fullpath]['s_elapsed'] = elapsed

        # We're done with the repo now
        grokmirror.unlock_repo(fullpath)
        total_checked += 1
        total_elapsed += elapsed
        saved = start_size - get_repo_size(fullpath)
        space_saved += saved
        if saved > 0:
            logger.info('     done: %ss, %s saved', elapsed, get_human_size(saved))
        else:
            logger.info('     done: %ss', elapsed)
        if space_saved > 0:
            logger.info('      ---: %s done, %s queued, %s saved', total_checked,
                        len(to_process)-total_checked, get_human_size(space_saved))
        else:
            logger.info('      ---: %s done, %s queued', total_checked, len(to_process)-total_checked)

        # Write status file after each check, so if the process dies, we won't
        # have to recheck all the repos we've already checked
        logger.debug('Updating status file in %s', statusfile)
        with open(statusfile, 'w') as stfh:
            stfh.write(json.dumps(status, indent=2))

    logger.info('Processed %s repos in %0.2fs', total_checked, total_elapsed)

    with open(statusfile, 'w') as stfh:
        stfh.write(json.dumps(status, indent=2))

    lockf(flockh, LOCK_UN)
    flockh.close()


def parse_args():
    import argparse
    # noinspection PyTypeChecker
    op = argparse.ArgumentParser(prog='grok-fsck',
                                 description='Optimize and check mirrored repositories',
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    op.add_argument('-v', '--verbose', dest='verbose', action='store_true',
                    default=False,
                    help='Be verbose and tell us what you are doing')
    op.add_argument('-f', '--force', dest='force',
                    action='store_true', default=False,
                    help='Force immediate run on all repositories')
    op.add_argument('-c', '--config', dest='config',
                    required=True,
                    help='Location of the configuration file')
    op.add_argument('--repack-only', dest='repack_only',
                    action='store_true', default=False,
                    help='Only find and repack repositories that need optimizing')
    op.add_argument('--connectivity-only', dest='conn_only',
                    action='store_true', default=False,
                    help='Only check connectivity when running fsck checks')
    op.add_argument('--repack-all-quick', dest='repack_all_quick',
                    action='store_true', default=False,
                    help='(Assumes --force): Do a quick repack of all repos')
    op.add_argument('--repack-all-full', dest='repack_all_full',
                    action='store_true', default=False,
                    help='(Assumes --force): Do a full repack of all repos')
    op.add_argument('--version', action='version', version=grokmirror.VERSION)

    opts = op.parse_args()

    if opts.repack_all_quick and opts.repack_all_full:
        op.error('Pick either --repack-all-full or --repack-all-quick')

    return opts


def grok_fsck(cfgfile, verbose=False, force=False, repack_only=False, conn_only=False,
              repack_all_quick=False, repack_all_full=False):
    global logger

    config = grokmirror.load_config_file(cfgfile)

    obstdir = config['core'].get('objstore', None)
    if obstdir is None:
        obstdir = os.path.join(config['core'].get('toplevel'), 'objstore')
        config['core']['objstore'] = obstdir

    logfile = config['core'].get('log', None)
    if config['core'].get('loglevel', 'info') == 'debug':
        loglevel = logging.DEBUG
    else:
        loglevel = logging.INFO

    logger = grokmirror.init_logger('fsck', logfile, loglevel, verbose)

    rh = io.StringIO()
    ch = logging.StreamHandler(stream=rh)
    formatter = logging.Formatter('%(message)s')
    ch.setFormatter(formatter)
    ch.setLevel(logging.CRITICAL)
    logger.addHandler(ch)

    fsck_mirror(config, force, repack_only, conn_only, repack_all_quick, repack_all_full)

    report = rh.getvalue()
    if len(report):
        msg = EmailMessage()
        msg.set_content(report)
        subject = config['fsck'].get('report_subject')
        if not subject:
            import platform
            subject = 'grok-fsck errors on {} ({})'.format(platform.node(), cfgfile)
        msg['Subject'] = subject
        from_addr = config['fsck'].get('report_from', 'root')
        msg['From'] = from_addr
        report_to = config['fsck'].get('report_to', 'root')
        msg['To'] = report_to
        mailhost = config['fsck'].get('report_mailhost', 'localhost')
        s = smtplib.SMTP(mailhost)
        s.send_message(msg)
        s.quit()


def command():
    opts = parse_args()

    return grok_fsck(opts.config, opts.verbose, opts.force, opts.repack_only, opts.conn_only,
                     opts.repack_all_quick, opts.repack_all_full)


if __name__ == '__main__':
    command()
