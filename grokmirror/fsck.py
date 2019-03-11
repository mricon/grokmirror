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

import enlighten

from fcntl import lockf, LOCK_EX, LOCK_UN, LOCK_NB

# default basic logger. We override it later.
logger = logging.getLogger(__name__)


def check_reclone_error(fullpath, config, errors):
    reclone = None
    for line in errors:
        for estring in config['reclone_on_errors']:
            if line.find(estring) != -1:
                # is this repo used for alternates?
                gitdir = '/' + os.path.relpath(fullpath, config['toplevel']).lstrip('/')
                if grokmirror.is_alt_repo(config['toplevel'], gitdir):
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
    if 'prune' not in config or config['prune'] != 'yes':
        return prune_ok

    # Are any other repos using us in their objects/info/alternates?
    gitdir = '/' + os.path.relpath(fullpath, config['toplevel']).lstrip('/')
    if grokmirror.is_alt_repo(config['toplevel'], gitdir):
        logger.info('  prune : skipped, is alternate to other repos')
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
        for line in error.split('\n'):
            ignored = False
            for estring in config['ignore_errors']:
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


def run_git_repack(fullpath, config, level=1):
    # Returns false if we hit any errors on the way
    repack_ok = True

    if 'precious' not in config:
        config['precious'] = 'yes'

    is_precious = False
    set_precious = False

    # Figure out what our repack flags should be.
    repack_flags = list()
    if 'extra_repack_flags' in config and len(config['extra_repack_flags']):
        repack_flags += config['extra_repack_flags'].split()

    full_repack_flags = ['-f', '-b', '--pack-kept-objects']
    if 'extra_repack_flags_full' in config and len(config['extra_repack_flags_full']):
        full_repack_flags += config['extra_repack_flags_full'].split()

    # Are any other repos using us in their objects/info/alternates?
    gitdir = '/' + os.path.relpath(fullpath, config['toplevel']).lstrip('/')
    if grokmirror.is_alt_repo(config['toplevel'], gitdir):
        # we are a "mother repo"
        # Force preciousObjects if precious is "always"
        if config['precious'] == 'always':
            is_precious = True
            set_precious_objects(fullpath, enabled=True)
        else:
            # Turn precious off during repacks
            set_precious_objects(fullpath, enabled=False)
            # Turn it back on after we're done
            set_precious = True

        # are we using alternates ourselves? Multiple levels of alternates are
        # a bad idea in general due high possibility of corruption.
        if os.path.exists(os.path.join(fullpath, 'objects', 'info', 'alternates')):
            logger.warning('warning : has alternates and is used by others for alternates')
            logger.warning('        : this can cause grandchild corruption')
            repack_flags.append('-A')
            repack_flags.append('-l')
        else:
            repack_flags.append('-a')
            if not is_precious:
                repack_flags.append('-k')

            if level > 1:
                logger.info(' repack : performing a full repack for optimal deltas')
                repack_flags += full_repack_flags

    elif os.path.exists(os.path.join(fullpath, 'objects', 'info', 'alternates')):
        # we are a "child repo"
        repack_flags.append('-l')
        if level > 1:
            repack_flags.append('-A')

    else:
        # we have no relationships with other repos
        if level > 1:
            logger.info(' repack : performing a full repack for optimal deltas')
            repack_flags.append('-a')
            if not is_precious:
                repack_flags.append('-k')
            repack_flags += full_repack_flags

    if not is_precious:
        repack_flags.append('-d')

    args = ['repack'] + repack_flags
    logger.info(' repack : repacking with "%s"', ' '.join(repack_flags))

    # We always tack on -q
    repack_flags.append('-q')

    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    if set_precious:
        set_precious_objects(fullpath, enabled=True)

    # With newer versions of git, repack may return warnings that are safe to ignore
    # so use the same strategy to weed out things we aren't interested in seeing
    if error:
        # Put things we recognize as fairly benign into debug
        debug = []
        warn = []
        for line in error.split('\n'):
            ignored = False
            for estring in config['ignore_errors']:
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
        return False

    # only repack refs on full repacks
    if level < 2:
        # do we still have loose objects after repacking?
        obj_info = get_repo_obj_info(fullpath)
        if obj_info['count'] != '0':
            return run_git_prune(fullpath, config)
        return repack_ok

    # repacking refs requires a separate command, so run it now
    args = ['pack-refs', '--all']
    logger.info(' repack : repacking refs')
    retcode, output, error = grokmirror.run_git_command(fullpath, args)

    # pack-refs shouldn't return anything, but use the same ignore_errors block
    # to weed out any future potential benign warnings
    if error:
        # Put things we recognize as fairly benign into debug
        debug = []
        warn = []
        for line in error.split('\n'):
            ignored = False
            for estring in config['ignore_errors']:
                if line.find(estring) != -1:
                    ignored = True
                    debug.append(line)
                    break
            if not ignored:
                warn.append(line)

        if debug:
            logger.debug('Stderr: %s', '\n'.join(debug))
        if warn:
            logger.critical('Repacking refs %s returned critical errors:',
                            fullpath)
            repack_ok = False
            for entry in warn:
                logger.critical("\t%s", entry)

            check_reclone_error(fullpath, config, warn)

    if repack_ok and 'prune' in config and config['prune'] == 'yes':
        # run prune now
        return run_git_prune(fullpath, config)

    return repack_ok


def run_git_fsck(fullpath, config, conn_only=False):
    args = ['fsck', '--no-dangling', '--no-reflogs']
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
        for line in output.split('\n') + error.split('\n'):
            if not len(line.strip()):
                continue
            ignored = False
            for estring in config['ignore_errors']:
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


def get_repo_obj_info(fullpath):
    args = ['count-objects', '-v']
    retcode, output, error = grokmirror.run_git_command(fullpath, args)
    obj_info = {}

    if output:
        for line in output.split('\n'):
            key, value = line.split(':')
            obj_info[key] = value.strip()

    return obj_info


def set_precious_objects(fullpath, enabled=True):
    # It's better to just set it blindly without checking first,
    # as this results in one fewer shell-out.
    logger.debug('Setting preciousObjects for %s', fullpath)
    if enabled:
        poval = 'true'
    else:
        poval = 'false'
    args = ['config', 'extensions.preciousObjects', poval]
    grokmirror.run_git_command(fullpath, args)


def check_precious_objects(fullpath):
    args = ['config', '--get', 'extensions.preciousObjects']
    retcode, output, error = grokmirror.run_git_command(fullpath, args)
    if output.strip().lower() == 'true':
        return True
    return False


def fsck_mirror(name, config, verbose=False, force=False, repack_only=False,
                conn_only=False, repack_all_quick=False, repack_all_full=False):
    global logger
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # noinspection PyTypeChecker
    em = enlighten.get_manager(series=' -=#')

    if 'log' in config:
        ch = logging.FileHandler(config['log'])
        formatter = logging.Formatter(
            "[%(process)d] %(asctime)s - %(levelname)s - %(message)s")
        ch.setFormatter(formatter)
        loglevel = logging.INFO

        if 'loglevel' in config:
            if config['loglevel'] == 'debug':
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

    logger.info('Running grok-fsck for [%s]', name)

    # Lock the tree to make sure we only run one instance
    logger.debug('Attempting to obtain lock on %s', config['lock'])
    flockh = open(config['lock'], 'w')
    try:
        lockf(flockh, LOCK_EX | LOCK_NB)
    except IOError:
        logger.info('Could not obtain exclusive lock on %s', config['lock'])
        logger.info('Assuming another process is running.')
        return 0

    manifest = grokmirror.read_manifest(config['manifest'])

    if os.path.exists(config['statusfile']):
        logger.info('Reading status from %s', config['statusfile'])
        stfh = open(config['statusfile'], 'rb')
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

            status = json.loads(stfh.read().decode('utf-8'))
        except:
            # Huai le!
            logger.critical('Failed to parse %s', config['statusfile'])
            lockf(flockh, LOCK_UN)
            flockh.close()
            return 1
    else:
        status = {}

    if 'frequency' in config:
        frequency = int(config['frequency'])
    else:
        frequency = 30

    today = datetime.datetime.today()
    todayiso = today.strftime('%F')

    if force:
        # Use randomization for next check, again
        checkdelay = random.randint(1, frequency)
    else:
        checkdelay = frequency

    # Go through the manifest and compare with status
    # noinspection PyTypeChecker
    e_find = em.counter(total=len(manifest), desc='Discovering:', unit='repos', leave=False)
    for gitdir in list(manifest):
        e_find.update()
        fullpath = os.path.join(config['toplevel'], gitdir.lstrip('/'))
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
            }
            logger.info('%s:', fullpath)
            logger.info('  added : next check on %s', nextcheck)

    e_find.close()

    # record newly found repos in the status file
    logger.debug('Updating status file in %s', config['statusfile'])
    with open(config['statusfile'], 'wb') as stfh:
        stfh.write(json.dumps(status, indent=2).encode('utf-8'))

    # Go through status and find all repos that need work done on them.
    # This is a dictionary that contains:
    # full_path_to_repo:
    #   repack: 0, 1, 2 (0-no, 1-needs quick repack, 2-needs full repack)
    #   fsck: 0/1

    to_process = {}

    total_checked = 0
    total_elapsed = 0

    # noinspection PyTypeChecker
    e_cmp = em.counter(total=len(status), desc='Analyzing:', unit='repos', leave=False)
    for fullpath in list(status):
        e_cmp.update()

        # Check to make sure it's still in the manifest
        gitdir = fullpath.replace(config['toplevel'], '', 1)
        gitdir = '/' + gitdir.lstrip('/')

        if gitdir not in manifest.keys():
            del status[fullpath]
            logger.debug('%s is gone, no longer in manifest', gitdir)
            continue

        needs_repack = needs_prune = needs_fsck = 0

        obj_info = get_repo_obj_info(fullpath)
        try:
            packs = int(obj_info['packs'])
            count_loose = int(obj_info['count'])
        except KeyError:
            logger.warning('Unable to count objects in %s, skipping' % fullpath)
            continue

        schedcheck = datetime.datetime.strptime(status[fullpath]['nextcheck'], '%Y-%m-%d')
        nextcheck = today + datetime.timedelta(days=checkdelay)

        if 'precious' not in config:
            config['precious'] = 'yes'

        if 'repack' not in config or config['repack'] != 'yes':
            # don't look at me if you turned off repack
            logger.debug('Not repacking because repack=no in config')
            needs_repack = 0
        elif repack_all_full and (count_loose > 0 or packs > 1):
            logger.debug('needs_repack=2 due to repack_all_full')
            needs_repack = 2
        elif repack_all_quick and count_loose > 0:
            logger.debug('needs_repack=1 due to repack_all_quick')
            needs_repack = 1
        elif conn_only:
            # don't do any repacks if we're running forced connectivity checks, unless
            # you specifically passed --repack-all-foo
            logger.debug('needs_repack=0 due to --conn-only')
            needs_repack = 0
        else:
            # for now, hardcode the maximum loose objects and packs
            # TODO: we can probably set this in git config values?
            #       I don't think this makes sense as a global setting, because
            #       optimal values will depend on the size of the repo as a whole
            max_loose_objects = 1200
            max_packs = 20
            pc_loose_objects = 10
            pc_loose_size = 10

            # first, compare against max values:
            if packs >= max_packs:
                logger.debug('Triggering full repack of %s because packs > %s', fullpath, max_packs)
                needs_repack = 2
            elif count_loose >= max_loose_objects:
                logger.debug('Triggering quick repack of %s because loose objects > %s', fullpath, max_loose_objects)
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
                if total_obj > 500 and count_loose/total_obj*100 >= pc_loose_objects:
                    logger.debug('Triggering repack of %s because loose objects > %s%% of total',
                                 fullpath, pc_loose_objects)
                    needs_repack = 1
                elif total_size > 1024 and size_loose/total_size*100 >= pc_loose_size:
                    logger.debug('Triggering repack of %s because loose size > %s%% of total',
                                 fullpath, pc_loose_size)
                    needs_repack = 1

        if needs_repack > 0 and (config['precious'] == 'always' and check_precious_objects(fullpath)):
            # if we have preciousObjects, then we only repack based on the same
            # schedule as fsck.
            logger.debug('preciousObjects is set')
            # for repos with preciousObjects, we use the fsck schedule for repacking
            if schedcheck <= today:
                logger.debug('Time for a full periodic repack of a preciousObjects repo')
                status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
                needs_repack = 2
            else:
                logger.debug('Not repacking preciousObjects repo outside of schedule')
                needs_repack = 0

        # Do we need to fsck it?
        if not (repack_all_quick or repack_all_full or repack_only):
            if schedcheck <= today or force:
                status[fullpath]['nextcheck'] = nextcheck.strftime('%F')
                needs_fsck = 1

        if needs_repack or needs_fsck or needs_prune:
            # emit a warning if we find garbage in a repo
            # we do it here so we don't spam people nightly on every cron run,
            # but only do it when a repo needs actual work done on it
            if obj_info['garbage'] != '0':
                logger.warning('%s:\n\tcontains %s garbage files (garbage-size: %s KiB)',
                               fullpath, obj_info['garbage'], obj_info['size-garbage'])

            to_process[fullpath] = {
                'repack': needs_repack,
                'prune': needs_prune,
                'fsck': needs_fsck,
            }

    e_cmp.close()

    if not len(to_process):
        logger.info('No repos need attention.')
        em.stop()
        return

    logger.info('Processing %s repositories', len(to_process))

    # noinspection PyTypeChecker
    run = em.counter(total=len(to_process), desc='Processing:', unit='repos', leave=False)
    for fullpath, needs in to_process.items():
        logger.info('%s:', fullpath)
        # Calculate elapsed seconds
        run.refresh()
        startt = time.time()

        # Wait till the repo is available and lock it for the duration of checks,
        # otherwise there may be false-positives if a mirrored repo is updated
        # in the middle of fsck or repack.
        grokmirror.lock_repo(fullpath, nonblocking=False)
        if needs['repack']:
            if run_git_repack(fullpath, config, needs['repack']):
                status[fullpath]['lastrepack'] = todayiso
                if needs['repack'] > 1:
                    status[fullpath]['lastfullrepack'] = todayiso
            else:
                logger.warning('Repacking %s was unsuccessful, '
                               'not running fsck.', fullpath)
                grokmirror.unlock_repo(fullpath)
                continue

        if needs['prune']:
            run_git_prune(fullpath, config)

        if needs['fsck']:
            run_git_fsck(fullpath, config, conn_only)
            endt = time.time()
            status[fullpath]['lastcheck'] = todayiso
            status[fullpath]['s_elapsed'] = int(endt-startt)

            logger.info('   done : %ss, next check on %s',
                        status[fullpath]['s_elapsed'],
                        status[fullpath]['nextcheck'])

        run.update()

        # We're done with the repo now
        grokmirror.unlock_repo(fullpath)
        total_checked += 1
        total_elapsed += time.time()-startt

        # Write status file after each check, so if the process dies, we won't
        # have to recheck all the repos we've already checked
        logger.debug('Updating status file in %s', config['statusfile'])
        with open(config['statusfile'], 'wb') as stfh:
            stfh.write(json.dumps(status, indent=2).encode('utf-8'))

    run.close()
    em.stop()
    logger.info('Processed %s repos in %0.2fs', total_checked, total_elapsed)

    with open(config['statusfile'], 'wb') as stfh:
        stfh.write(json.dumps(status, indent=2).encode('utf-8'))

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
                  help='Location of fsck.conf')
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


def grok_fsck(config, verbose=False, force=False, repack_only=False, conn_only=False,
              repack_all_quick=False, repack_all_full=False):
    try:
        from configparser import ConfigParser
    except ImportError:
        from ConfigParser import ConfigParser

    ini = ConfigParser()
    ini.read(config)

    for section in ini.sections():
        config = {}
        for (option, value) in ini.items(section):
            config[option] = value

        if 'ignore_errors' not in config:
            config['ignore_errors'] = [
                'notice: HEAD points to an unborn branch',
                'notice: No default references',
                'contains zero-padded file modes',
                'warning: disabling bitmap writing, as some objects are not being packed',
                'ignoring extra bitmap file'
            ]
        else:
            ignore_errors = []
            for estring in config['ignore_errors'].split('\n'):
                estring = estring.strip()
                if len(estring):
                    ignore_errors.append(estring)
            config['ignore_errors'] = ignore_errors

        if 'reclone_on_errors' not in config:
            # We don't do any defaults for this one
            config['reclone_on_errors'] = []
        else:
            reclone_on_errors = []
            for estring in config['reclone_on_errors'].split('\n'):
                estring = estring.strip()
                if len(estring):
                    reclone_on_errors.append(estring)
            config['reclone_on_errors'] = reclone_on_errors

        fsck_mirror(section, config, verbose, force, repack_only, conn_only,
                    repack_all_quick, repack_all_full)


def command():
    opts, args = parse_args()

    return grok_fsck(opts.config, opts.verbose, opts.force, opts.repack_only, opts.conn_only,
                     opts.repack_all_quick, opts.repack_all_full)


if __name__ == '__main__':
    command()
