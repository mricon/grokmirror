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

from email.message import EmailMessage
from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

# default basic logger. We override it later.
logger = logging.getLogger(__name__)


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
        # Put things we recognize as fairly benign into debug
        debug = list()
        warn = list()
        ierrors = set([x.strip() for x in config['fsck'].get('ignore_errors', '').split('\n')])
        for line in error.split('\n'):
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
        if warn:
            logger.critical('Pruning %s returned critical errors:', fullpath)
            prune_ok = False
            for entry in warn:
                logger.critical("\t%s", entry)
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


def run_git_repack(fullpath, config, level=1, prune=True):
    # Returns false if we hit any errors on the way
    repack_ok = True
    obstdir = os.path.realpath(config['core'].get('objstore'))
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    gitdir = '/' + os.path.relpath(fullpath, toplevel).lstrip('/')
    ierrors = set([x.strip() for x in config['fsck'].get('ignore_errors', '').split('\n')])

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
    extra_repack_flags = config['fsck'].get('extra_repack_flags', '')
    if extra_repack_flags:
        repack_flags += extra_repack_flags.split('\n')

    full_repack_flags = ['-f', '--pack-kept-objects']
    rfull = config['fsck'].get('extra_repack_flags_full', '')
    if len(rfull):
        full_repack_flags += rfull.split()

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
        # Put things we recognize as fairly benign into debug
        debug = list()
        warn = list()
        for line in error.split('\n'):
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
        if warn:
            logger.critical('Repacking %s returned critical errors:', fullpath)
            repack_ok = False
            for entry in warn:
                logger.critical("\t%s", entry)
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
        # Put things we recognize as fairly benign into debug
        debug = list()
        warn = list()
        for line in error.split('\n'):
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
        if warn:
            logger.critical('Repacking refs %s returned critical errors:', fullpath)
            repack_ok = False
            for entry in warn:
                logger.critical("\t%s", entry)

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

    if output or error:
        # Put things we recognize as fairly benign into debug
        debug = list()
        warn = list()
        ierrors = set([x.strip() for x in config['fsck'].get('ignore_errors', '').split('\n')])
        for line in output.split('\n') + error.split('\n'):
            if not len(line.strip()):
                continue
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
        if warn:
            logger.critical('%s has critical errors:', fullpath)
            for entry in warn:
                logger.critical("\t%s", entry)
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


def get_repack_level(obj_info, max_loose_objects=1200, max_packs=20, pc_loose_objects=10, pc_loose_size=10):
    # for now, hardcode the maximum loose objects and packs
    # XXX: we can probably set this in git config values?
    #      I don't think this makes sense as a global setting, because
    #      optimal values will depend on the size of the repo as a whole
    packs = int(obj_info['packs'])
    count_loose = int(obj_info['count'])

    needs_repack = 0

    # first, compare against max values:
    if packs >= max_packs:
        logger.debug('Triggering full repack because packs > %s', max_packs)
        needs_repack = 2
    elif count_loose >= max_loose_objects:
        logger.debug('Triggering quick repack because loose objects > %s', max_loose_objects)
        needs_repack = 1
    else:
        # is the number of loose objects or their size more than 10% of
        # the overall total?
        in_pack = int(obj_info['in-pack'])
        size_loose = int(obj_info['size'])
        size_pack = int(obj_info['size-pack'])
        total_obj = count_loose + in_pack
        total_size = size_loose + size_pack
        # set some arbitrary "worth bothering" limits so we don't
        # continuously repack tiny repos.
        if total_obj > 500 and count_loose / total_obj * 100 >= pc_loose_objects:
            logger.debug('Triggering repack because loose objects > %s%% of total', pc_loose_objects)
            needs_repack = 1
        elif total_size > 1024 and size_loose / total_size * 100 >= pc_loose_size:
            logger.debug('Triggering repack because loose size > %s%% of total', pc_loose_size)
            needs_repack = 1

    return needs_repack


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
    logger.info('Analyzing %s (%s repos)', toplevel, len(status))
    for fullpath in list(status):
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

        if not altdir:
            # Do we match any obstdir repos?
            obstrepo = grokmirror.find_best_obstrepo(fullpath, obst_roots)
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
                        for top_sibling in grokmirror.find_siblings(fullpath, my_roots, top_roots):
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
                    logger.info('      ---: %s analyzed, %s queued, %s total', analyzed, len(to_process), len(status))
                    obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)

        elif not os.path.isdir(altdir):
            logger.critical('  reclone: %s (alternates repo gone)', gitdir)
            set_repo_reclone(fullpath, 'Alternates repository gone')
            continue

        elif altdir.find(obstdir) != 0:
            # We have an alternates repo, but it's not an objstore repo
            # Probably left over from grokmirror-1.x
            # Do we have any matching obstrepos?
            obstrepo = grokmirror.find_best_obstrepo(fullpath, obst_roots)
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
                        logger.info('      ---: %s analyzed, %s queued, %s total', analyzed, len(to_process),
                                    len(status))
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
                    logger.info('      ---: %s analyzed, %s queued, %s total', analyzed, len(to_process), len(status))
                else:
                    logger.critical('Unsuccessful fetching %s into %s', altdir, os.path.basename(obstrepo))
                    obstrepo = None

            if obstrepo:
                obst_changes = True
                # It should be safe now to repoint the alternates without doing a repack first
                grokmirror.set_altrepo(fullpath, obstrepo)
                if not is_private:
                    # Fetch into the obstrepo
                    grokmirror.add_repo_to_objstore(obstrepo, fullpath)
                    logger.info('    fetch: fetching %s', gitdir)
                    grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                    set_precious_objects(fullpath, enabled=False)
                    run_git_repack(fullpath, config, level=1, prune=m_prune)
                    space_saved += start_size - get_repo_size(fullpath)
                    logger.info('      ---: %s analyzed, %s queued, %s total', analyzed, len(to_process), len(status))
                else:
                    logger.info('    fetch: not fetching %s (private)', gitdir)

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
            repack_level = get_repack_level(obj_info)
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
            to_process.add((fullpath, 'repack', repack_level))
            if repack_level > 1:
                logger.info('   queued: %s (full repack)', fullpath)
            else:
                logger.info('   queued: %s (repack)', fullpath)
            logger.info('      ---: %s analyzed, %s queued, %s total', analyzed, len(to_process), len(status))
        elif repack_only or repack_all_quick or repack_all_full:
            continue
        elif schedcheck <= today or force:
            to_process.add((fullpath, 'fsck', None))
            logger.info('   queued: %s (fsck)', fullpath)
            logger.info('      ---: %s analyzed, %s queued, %s total', analyzed, len(to_process), len(status))

    if obst_changes:
        # Refresh the alt repo map cache
        amap = grokmirror.get_altrepo_map(toplevel, refresh=True)
        # Lock and re-read manifest, so we can update reference and forkgroup data
        grokmirror.manifest_lock(manifile)
        manifest = grokmirror.read_manifest(manifile)

    obstrepos = grokmirror.find_all_gitdirs(obstdir, normalize=True, exclude_objstore=False, flat=True)

    analyzed = 0
    logger.info('Analyzing %s (%s repos)', obstdir, len(obstrepos))
    baselines = [x.strip() for x in config['fsck'].get('baselines', '').split('\n')]
    islandcores = [x.strip() for x in config['fsck'].get('islandcores', '').split('\n')]
    for obstrepo in obstrepos:
        analyzed += 1
        logger.debug('Processing objstore repo: %s', os.path.basename(obstrepo))
        my_roots = grokmirror.get_repo_roots(obstrepo)
        if obstrepo in amap and len(amap[obstrepo]):
            # Is it redundant with any other objstore repos?
            siblings = grokmirror.find_siblings(obstrepo, my_roots, obst_roots, exact=True)
            if len(siblings):
                siblings.add(obstrepo)
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
                        obst_changes = True
                        if mdest in status:
                            # Force full repack of merged obstrepos
                            status[mdest]['nextcheck'] = todayiso

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
        set_baseline = False
        set_islandcore = False
        new_islandcore = False
        for virtref, childpath in my_remotes:
            # Is it still relevant?
            if childpath not in amap[obstrepo]:
                # Remove it and let prune take care of it
                grokmirror.remove_from_objstore(obstrepo, childpath)
                logger.info('%s: removed remote %s (no longer used)', os.path.basename(obstrepo), childpath)
                continue

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
                logger.info('    fetch: %s -> %s', gitdir, os.path.basename(obstrepo))
                grokmirror.fetch_objstore_repo(obstrepo, childpath)

            if gitdir not in manifest:
                continue

            # Do we need to set any alternateRefsPrefixes?
            if not set_baseline:
                is_baseline = False
                for baseline in baselines:
                    # Does this repo match a baseline
                    if fnmatch.fnmatch(gitdir, baseline):
                        is_baseline = True
                        break
                if is_baseline:
                    set_baseline = True
                    refpref = 'refs/virtual/%s/heads/' % virtref
                    # Go through all remotes and set their alternateRefsPrefixes
                    for s_virtref, s_childpath in my_remotes:
                        # is it already set to that?
                        entries = grokmirror.get_config_from_git(s_childpath, r'core\.alternate*')
                        if entries.get('alternaterefsprefixes') != refpref:
                            s_gitdir = '/' + os.path.relpath(s_childpath, toplevel)
                            logger.info(' reconfig: %s (baseline to %s)', s_gitdir, virtref)
                            grokmirror.set_git_config(s_childpath, 'core.alternateRefsPrefixes', refpref)

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

        if obstrepo not in status or new_islandcore:
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
            repack_level = get_repack_level(obj_info)

        nextcheck = datetime.datetime.strptime(status[obstrepo]['nextcheck'], '%Y-%m-%d')
        if repack_level > 1 and nextcheck > today:
            # Don't do full repacks outside of schedule
            repack_level = 1

        if repack_level:
            to_process.add((obstrepo, 'repack', repack_level))
            if repack_level > 1:
                logger.info('   queued: %s (full repack)', os.path.basename(obstrepo))
            else:
                logger.info('   queued: %s (repack)', os.path.basename(obstrepo))
            logger.info('      ---: %s analyzed, %s queued, %s total', analyzed, len(to_process), len(status))
        elif repack_only or repack_all_quick or repack_all_full:
            continue
        elif (nextcheck <= today or force) and not repack_only:
            status[obstrepo]['nextcheck'] = nextcheck.strftime('%F')
            to_process.add((obstrepo, 'fsck', None))
            logger.info('   queued: %s (fsck)', os.path.basename(obstrepo))
            logger.info('      ---: %s analyzed, %s queued, %s total', analyzed, len(to_process), len(status))

    if obst_changes:
        # We keep the same mtime, because the repos themselves haven't changed
        grokmirror.write_manifest(manifile, manifest, pretty=pretty)
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
                    status[fullpath]['lastfullrepack'] = todayiso
                    status[fullpath]['lastcheck'] = todayiso
                    status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
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
        logger.info('      ---: %s done, %s queued', total_checked, len(to_process)-total_checked)

        # Write status file after each check, so if the process dies, we won't
        # have to recheck all the repos we've already checked
        logger.debug('Updating status file in %s', statusfile)
        with open(statusfile, 'w') as stfh:
            stfh.write(json.dumps(status, indent=2))

    logger.info('Processed %s repos in %0.2fs, %s saved', total_checked, total_elapsed, get_human_size(space_saved))

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
