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

import grokmirror
import logging

import time
import json
import random
import datetime
import shutil

import enlighten

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

    rfile = os.path.join(fullpath, 'grokmirror.reclone')
    # Have we already requested a reclone?
    if os.path.exists(rfile):
        logger.debug('Already requested repo reclone for %s', fullpath)
        return

    with open(rfile, 'w') as rfh:
        rfh.write('Requested by grok-fsck due to error: %s' % reclone)


def run_git_prune(fullpath, config):
    prune_ok = True
    do_prune = config['fsck'].getboolean('prune', True)
    if not do_prune:
        return prune_ok

    # We set expire to yesterday in order to avoid race conditions
    # in repositories that are actively being accessed at the time of
    # running the prune job.
    args = ['prune', '--expire=yesterday']
    logger.info('  prune : pruning')
    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    if error:
        # Put things we recognize as fairly benign into debug
        debug = []
        warn = []
        ierrors = config['fsck'].get('ignore_errors', '').split('\n')
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

    return prune_ok


def run_git_repack(fullpath, config, level=1, prune=True):
    # Returns false if we hit any errors on the way
    repack_ok = True
    obstdir = os.path.realpath(config['core'].get('objstore'))
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    gitdir = '/' + os.path.relpath(fullpath, toplevel).lstrip('/')
    ierrors = config['fsck'].get('ignore_errors', '').split('\n')

    if config['fsck'].get('prune', 'yes') != 'yes':
        logger.debug('Pruning disabled in config file')
        prune = False

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

        # We only prune if all repos pointing to us are public
        urls = set(grokmirror.list_repo_remotes(fullpath, withurl=True))
        mine = set([x[1] for x in urls])
        amap = grokmirror.get_altrepo_map(toplevel)
        if mine != amap[fullpath]:
            logger.debug('Cannot prune %s because it is used by non-public repos', fullpath)
            prune = False
            if not always_precious:
                repack_flags.append('-k')

    elif grokmirror.is_alt_repo(toplevel, gitdir):
        prune = False
        set_precious_after = True
        if grokmirror.get_altrepo(fullpath):
            gen_commitgraph = False
            logger.warning('warning : has alternates and is used by others for alternates')
            logger.warning('        : this can cause grandchild corruption')
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
        logger.info(' repack : performing a full repack for optimal deltas')
        repack_flags += full_repack_flags

    if not always_precious:
        repack_flags.append('-d')

    args = ['repack'] + repack_flags
    logger.info(' repack : repacking with "%s"', ' '.join(repack_flags))

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
        run_git_commit_graph(fullpath)

    # only repack refs on full repacks
    if level > 1:
        # repacking refs requires a separate command, so run it now
        args = ['pack-refs', '--all']
        logger.info(' repack : repacking refs')
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
        # run prune now
        repack_ok = run_git_prune(fullpath, config)

    if set_precious_after:
        set_precious_objects(fullpath, enabled=True)

    return repack_ok


def run_git_fsck(fullpath, config, conn_only=False):
    args = ['fsck', '--no-progress', '--no-dangling', '--no-reflogs']
    if conn_only:
        args.append('--connectivity-only')
        logger.info('   fsck : running with --connectivity-only')
    else:
        logger.info('   fsck : running full checks')

    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    if output or error:
        # Put things we recognize as fairly benign into debug
        debug = []
        warn = []
        ierrors = config['fsck'].get('ignore_errors', '').split('\n')
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
    logger.info('  graph : generating commit-graph --reachable')
    args = ['commit-graph', 'write', '--reachable']
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
    args = ['config', '--get', 'extensions.preciousObjects']
    retcode, output, error = grokmirror.run_git_command(fullpath, args)
    if output.strip().lower() == 'true':
        return True
    return False


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


def fsck_mirror(config, verbose=False, force=False, repack_only=False,
                conn_only=False, repack_all_quick=False, repack_all_full=False):
    global logger
    logger = logging.getLogger('fsck')
    logger.setLevel(logging.DEBUG)

    # noinspection PyTypeChecker
    em = enlighten.get_manager(series=' -=#')

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

    if conn_only or repack_all_quick or repack_all_full:
        force = True

    logger.info('Running grok-fsck for [%s]', config['core'].get('toplevel'))

    # Lock the tree to make sure we only run one instance
    lockfile = config['core'].get('lock')
    logger.debug('Attempting to obtain lock on %s', lockfile)
    flockh = open(lockfile, 'w')
    try:
        lockf(flockh, LOCK_EX | LOCK_NB)
    except IOError:
        logger.info('Could not obtain exclusive lock on %s', lockfile)
        logger.info('Assuming another process is running.')
        return 0

    manifile = config['core'].get('manifest')
    manifest = grokmirror.read_manifest(manifile)

    statusfile = config['fsck'].get('statusfile')
    if os.path.exists(statusfile):
        logger.info('Reading status from %s', statusfile)
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
    # noinspection PyTypeChecker
    e_find = em.counter(total=len(manifest), desc='Discovering', unit='repos', leave=False)
    toplevel = os.path.realpath(config['core'].get('toplevel'))
    for gitdir in list(manifest):
        e_find.update()
        fullpath = os.path.join(toplevel, gitdir.lstrip('/'))
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
            logger.info('  added : next check on %s', nextcheck)

    e_find.close()

    # record newly found repos in the status file
    logger.debug('Updating status file in %s', statusfile)
    with open(statusfile, 'w') as stfh:
        stfh.write(json.dumps(status, indent=2))

    # Go through status and find all repos that need work done on them.
    to_process = set()

    total_checked = 0
    total_elapsed = 0

    cfg_repack = config['fsck'].getboolean('repack', True)
    # Can be "always", which is why we don't getboolean
    cfg_precious = config['fsck'].get('precious', 'yes')

    obstdir = os.path.realpath(config['core'].get('objstore'))
    logger.info('Getting root commit info from all repos, may take a while')
    top_roots, obst_roots = grokmirror.get_rootsets(toplevel, obstdir, em=em)
    amap = grokmirror.get_altrepo_map(toplevel)

    # noinspection PyTypeChecker
    e_cmp = em.counter(total=len(status), desc='Analyzing (toplevel)', unit='repos', leave=False)
    fetched_obstrepos = set()
    obst_changes = False
    for fullpath in list(status):
        e_cmp.update()
        e_cmp.refresh()
        # We do obstrepos separately below, as logic is different
        if grokmirror.is_obstrepo(fullpath, obstdir):
            logger.debug('Skipping %s (obstrepo)')
            continue

        # Check to make sure it's still in the manifest
        gitdir = fullpath.replace(toplevel, '', 1)
        gitdir = '/' + gitdir.lstrip('/')

        if gitdir not in manifest.keys():
            status.pop(fullpath)
            logger.debug('%s is gone, no longer in manifest', gitdir)
            continue

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
                    logger.info('  fetch : fetching %s', gitdir)
                    grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                    obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)
                run_git_repack(fullpath, config, level=1, prune=m_prune)
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
                            logger.info('   init : new objstore repo %s', os.path.basename(obstrepo))
                            grokmirror.add_repo_to_objstore(obstrepo, top_sibling)
                            # Fetch into the obstrepo
                            logger.info('  fetch : fetching %s', top_sibling)
                            grokmirror.fetch_objstore_repo(obstrepo, top_sibling)
                            obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)
                            # It doesn't matter if this fails, because repacking is still safe
                            # Other siblings will match in their own due course
                            break
                    else:
                        # Make an objstore repo out of myself
                        obstrepo = grokmirror.setup_objstore_repo(obstdir)
                        logger.info('%s: can use %s', gitdir, os.path.basename(obstrepo))
                        logger.info('   init : new objstore repo %s', os.path.basename(obstrepo))
                        grokmirror.add_repo_to_objstore(obstrepo, fullpath)

                if obstrepo:
                    obst_changes = True
                    # Set alternates to the obstrepo
                    grokmirror.set_altrepo(fullpath, obstrepo)
                    if not is_private:
                        # Fetch into the obstrepo
                        logger.info('  fetch : fetching %s', gitdir)
                        grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                    run_git_repack(fullpath, config, level=1, prune=m_prune)
                    obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)

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
                    logger.info('  fetch : fetching %s (previous parent)', os.path.relpath(altdir, toplevel))
                    success = grokmirror.fetch_objstore_repo(obstrepo, altdir)
                    fetched_obstrepos.add(altdir)
                    if success:
                        set_precious_objects(altdir, enabled=False)
                        run_git_repack(altdir, config, level=1, prune=False)
                    else:
                        logger.critical('Unsuccessful fetching %s into %s', altdir, os.path.basename(obstrepo))
                        obstrepo = None
            else:
                # Make a new obstrepo out of mommy
                obstrepo = grokmirror.setup_objstore_repo(obstdir)
                logger.info('%s: migrating to %s', gitdir, os.path.basename(obstrepo))
                logger.info('   init : new objstore repo %s', os.path.basename(obstrepo))
                grokmirror.add_repo_to_objstore(obstrepo, altdir)
                logger.info('  fetch : fetching %s (previous parent)', os.path.relpath(altdir, toplevel))
                success = grokmirror.fetch_objstore_repo(obstrepo, altdir)
                fetched_obstrepos.add(altdir)
                if success:
                    grokmirror.set_altrepo(altdir, obstrepo)
                    # mommy is no longer precious
                    set_precious_objects(altdir, enabled=False)
                    # Don't prune, because there may be objects others are still borrowing
                    # It can only be pruned once the full migration is completed
                    run_git_repack(altdir, config, level=1, prune=False)
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
                    logger.info('  fetch : fetching %s', gitdir)
                    grokmirror.fetch_objstore_repo(obstrepo, fullpath)
                    set_precious_objects(fullpath, enabled=False)
                    run_git_repack(fullpath, config, level=1, prune=m_prune)
                else:
                    logger.info('  fetch : not fetching %s (private)', gitdir)

                obst_roots[obstrepo] = grokmirror.get_repo_roots(obstrepo, force=True)

        obj_info = grokmirror.get_repo_obj_info(fullpath)
        try:
            packs = int(obj_info['packs'])
            count_loose = int(obj_info['count'])
        except KeyError:
            logger.warning('Unable to count objects in %s, skipping' % fullpath)
            continue

        # emit a warning if we find garbage in a repo
        # we do it here so we don't spam people nightly on every cron run,
        # but only do it when a repo needs actual work done on it
        if obj_info['garbage'] != '0':
            logger.warning('%s:\n\tcontains %s garbage files (garbage-size: %s KiB)',
                           fullpath, obj_info['garbage'], obj_info['size-garbage'])

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
        elif conn_only:
            # don't do any repacks if we're running forced connectivity checks, unless
            # you specifically passed --repack-all-foo
            logger.debug('repack_level=None due to --conn-only')
            repack_level = None
        else:
            logger.debug('Checking repack level of %s', fullpath)
            repack_level = get_repack_level(obj_info)

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
        elif repack_only or repack_all_quick or repack_all_full:
            continue
        elif schedcheck <= today or force:
            to_process.add((fullpath, 'fsck', None))

    e_cmp.close()

    if obst_changes:
        # Refresh the alt repo map cache
        amap = grokmirror.get_altrepo_map(toplevel, refresh=True)
        # Lock and re-read manifest, so we can update reference and forkgroup data
        grokmirror.manifest_lock(manifile)
        manifest = grokmirror.read_manifest(manifile)

    obstrepos = grokmirror.find_all_gitdirs(obstdir, normalize=True, exclude_objstore=False)
    # noinspection PyTypeChecker
    e_obst = em.counter(total=len(obstrepos), desc='Analyzing (objstore)', unit='repos', leave=False)

    for obstrepo in obstrepos:
        e_obst.update()
        e_obst.refresh()
        logger.debug('Processing objstore repo: %s', os.path.basename(obstrepo))
        my_roots = grokmirror.get_repo_roots(obstrepo)
        if obstrepo in amap and len(amap[obstrepo]):
            # Is it redundant with any other objstore repos?
            siblings = grokmirror.find_siblings(obstrepo, my_roots, obst_roots)
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
                        logger.info(' moving: %s', childpath)

                        success = grokmirror.add_repo_to_objstore(mdest, childpath)
                        if not success:
                            logger.critical('Could not add %s to %s', childpath, mdest)
                            continue

                        logger.info('       : fetching into %s', os.path.basename(mdest))
                        success = grokmirror.fetch_objstore_repo(mdest, childpath)
                        if not success:
                            logger.critical('Failed to fetch %s from %s to %s', childpath, os.path.basename(sibling),
                                            os.path.basename(mdest))
                            continue
                        logger.info('       : repointing alternates')
                        grokmirror.set_altrepo(childpath, mdest)
                        amap[sibling].remove(childpath)
                        amap[mdest].add(childpath)
                        args = ['remote', 'remove', virtref]
                        grokmirror.run_git_command(sibling, args)
                        logger.info('       : done')
                        obst_changes = True
                        if mdest in status:
                            # Force full repack of merged obstrepos
                            status[mdest]['nextcheck'] = todayiso

        # Not an else, because the previous step may have migrated things
        if obstrepo not in amap or not len(amap[obstrepo]):
            obst_changes = True
            # XXX: Theoretically, nothing should have cloned a new repo while we were migrating, because
            # they should have found a better candidate as well.
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
        for virtref, childpath in my_remotes:
            # Is it still relevant?
            if childpath not in amap[obstrepo]:
                # Remove it and let prune take care of it
                args = ['remote', 'remove', virtref]
                logger.info('%s: removed remote %s (no longer used)', os.path.basename(obstrepo), childpath)
                grokmirror.run_git_command(obstrepo, args)
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
                logger.info('Fetching %s into %s', gitdir, os.path.basename(obstrepo))
                grokmirror.fetch_objstore_repo(obstrepo, childpath)

            if refrepo is None:
                # Legacy "reference=" setting in manifest
                refrepo = gitdir
                manifest[gitdir]['reference'] = None
            else:
                manifest[gitdir]['reference'] = refrepo

            manifest[gitdir]['forkgroup'] = os.path.basename(obstrepo[:-4])

        if obstrepo not in status:
            # We don't use obstrepo fingerprints, so we set it to None
            status[obstrepo] = {
                'lastcheck': 'never',
                'nextcheck': todayiso,
                'fingerprint': None,
            }

        nextcheck = datetime.datetime.strptime(status[obstrepo]['nextcheck'], '%Y-%m-%d')
        obj_info = grokmirror.get_repo_obj_info(obstrepo)
        repack_level = get_repack_level(obj_info)
        if repack_level > 1 and nextcheck > today:
            # Don't do full repacks outside of schedule
            repack_level = 1

        if repack_level:
            to_process.add((obstrepo, 'repack', repack_level))
        elif repack_only or repack_all_quick or repack_all_full:
            continue
        elif (nextcheck <= today or force) and not repack_only:
            status[obstrepo]['nextcheck'] = nextcheck.strftime('%F')
            to_process.add((obstrepo, 'fsck', None))

    e_obst.close()
    if obst_changes:
        # We keep the same mtime, because the repos themselves haven't changed
        grokmirror.write_manifest(manifile, manifest, mtime=os.stat(manifile)[8])
        grokmirror.manifest_unlock(manifile)

    if not len(to_process):
        logger.info('No repos need attention.')
        em.stop()
        return

    logger.info('Processing %s repositories', len(to_process))

    # noinspection PyTypeChecker
    run = em.counter(total=len(to_process), desc='Processing', unit='repos', leave=False)
    for fullpath, action, repack_level in to_process:
        logger.info('%s:', fullpath)
        checkdelay = frequency if not force else random.randint(1, frequency)
        nextcheck = today + datetime.timedelta(days=checkdelay)

        # Calculate elapsed seconds
        run.refresh()
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
                    logger.info('   next : %s', status[fullpath]['nextcheck'])
            else:
                logger.warning('Repacking %s was unsuccessful', fullpath)
                grokmirror.unlock_repo(fullpath)
                run.update()
                continue

        elif action == 'fsck':
            run_git_fsck(fullpath, config, conn_only)
            status[fullpath]['lastcheck'] = todayiso
            status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
            logger.info('   next : %s', status[fullpath]['nextcheck'])

        # noinspection PyTypeChecker
        elapsed = int(time.time()-startt)
        status[fullpath]['s_elapsed'] = elapsed

        logger.info('   done : %ss', elapsed)
        run.update()

        # We're done with the repo now
        grokmirror.unlock_repo(fullpath)
        total_checked += 1
        total_elapsed += elapsed

        # Write status file after each check, so if the process dies, we won't
        # have to recheck all the repos we've already checked
        logger.debug('Updating status file in %s', statusfile)
        with open(statusfile, 'w') as stfh:
            stfh.write(json.dumps(status, indent=2))

    run.close()
    em.stop()
    logger.info('Processed %s repos in %0.2fs', total_checked, total_elapsed)

    with open(statusfile, 'w') as stfh:
        stfh.write(json.dumps(status, indent=2))

    lockf(flockh, LOCK_UN)
    flockh.close()


def parse_args():
    from optparse import OptionParser

    usage = '''usage: %prog -c fsck.conf
    Run a git-fsck check on grokmirror-managed repositories.
    '''

    op = OptionParser(usage=usage, version=grokmirror.VERSION)
    op.add_option('-v', '--verbose', dest='verbose', action='store_true',
                  default=False,
                  help='Be verbose and tell us what you are doing')
    op.add_option('-f', '--force', dest='force',
                  action='store_true', default=False,
                  help='Force immediate run on all repositories.')
    op.add_option('-c', '--config', dest='config',
                  help='Location of the configuration file')
    op.add_option('--repack-only', dest='repack_only',
                  action='store_true', default=False,
                  help='Only find and repack repositories that need optimizing')
    op.add_option('--connectivity', dest='conn_only',
                  action='store_true', default=False,
                  help='(Assumes --force): Run git fsck on all repos, but only check connectivity')
    op.add_option('--repack-all-quick', dest='repack_all_quick',
                  action='store_true', default=False,
                  help='(Assumes --force): Do a quick repack of all repos')
    op.add_option('--repack-all-full', dest='repack_all_full',
                  action='store_true', default=False,
                  help='(Assumes --force): Do a full repack of all repos')

    opts, args = op.parse_args()

    if opts.repack_all_quick and opts.repack_all_full:
        op.error('Pick either --repack-all-full or --repack-all-quick')

    if not opts.config:
        op.error('You must provide the path to the config file')

    return opts, args


def grok_fsck(cfgfile, verbose=False, force=False, repack_only=False, conn_only=False,
              repack_all_quick=False, repack_all_full=False):

    config = grokmirror.load_config_file(cfgfile)

    obstdir = config['core'].get('objstore', None)
    if obstdir is None:
        obstdir = os.path.join(config['core'].get('toplevel'), '_alternates')
        config['core']['objstore'] = obstdir

    fsck_mirror(config, verbose, force, repack_only, conn_only,
                repack_all_quick, repack_all_full)


def command():
    opts, args = parse_args()

    return grok_fsck(opts.config, opts.verbose, opts.force, opts.repack_only, opts.conn_only,
                     opts.repack_all_quick, opts.repack_all_full)


if __name__ == '__main__':
    command()
