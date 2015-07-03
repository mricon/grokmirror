#!/usr/bin/env python2 -tt
# -*- coding: utf-8 -*-
##
# Copyright (C) 2015 by Konstantin Ryabitsev and contributors
#
# This program is free software; you can redistribute it and/or
# modify it under the terms of the GNU General Public License
# as published by the Free Software Foundation; either version 3
# of the License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA
# 02111-1307, USA.

from __future__ import (absolute_import,
                        division,
                        print_function)

__author__ = 'Konstantin Ryabitsev <konstantin@linuxfoundation.org>'

import unittest

import pygit2
import logging

import sys
import os
import shutil

from io import open

import grokmirror
from grokmirror import manifest

logger = logging.getLogger('grokmirror')
logger.setLevel(logging.DEBUG)

ch = logging.FileHandler('test.log')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s][%(levelname)s:%(funcName)s:"
                              "%(lineno)s] %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

# ----------------- test constants that might change in the future
MASTER1FPR = '5ac4b08d8b42b4edfe9d39eaafbda93fc35cae9d'
MASTER1MODIFIED = 1429729359


def fullpathify(path):
    path = path.lstrip('/')
    fullpath = os.path.join(os.path.realpath(os.path.curdir), path)
    return fullpath


def clone_bundle_into(bundle, dest):
    grokmirror.run_git_command(
        args=['clone', '--bare', bundle, dest], env=None
    )


class GATest(unittest.TestCase):
    def setUp(self):
        self.tearDown()
        # we pretty much always need master1.git
        logger.debug('Setting up test/sources/master1.git')
        os.mkdir('test/sources')
        os.mkdir('test/targets')
        clone_bundle_into('test/master1.bundle', 'test/sources/master1.git')

    def tearDown(self):
        logger.debug('Cleaning up')
        try:
            shutil.rmtree('test/sources')
            shutil.rmtree('test/targets')
        except OSError:
            pass

    def testCheckRepoSanity(self):
        logger.info('Testing if master1 is a valid bare repo')
        self.assertTrue(grokmirror.is_bare_git_repo('test/sources/master1.git'))
        self.assertFalse(grokmirror.is_bare_git_repo('test/sources/bupkes.git'))
        os.mkdir('test/sources/bupkes.git')
        self.assertFalse(grokmirror.is_bare_git_repo('test/sources/bupkes.git'))

    def testLockRepo(self):
        logger.info('Locking test/sources/master1 non-blocking')
        toplevel = fullpathify('test/sources')
        grepo = grokmirror.Repository(toplevel, 'master1.git')

        # Just test to make sure nothing tracebacks
        grepo.lock(nonblocking=True)
        grepo.unlock()

    def testRepoTimestamp(self):
        logger.info('Testing repo timestamps')
        # Currently, no timestamp exists as we just cloned it from bundle
        toplevel = fullpathify('test/sources')
        grepo = grokmirror.Repository(toplevel, 'master1.git')

        self.assertEqual(grepo.timestamp, 0)

        grepo.timestamp = 5551212
        tsfile = os.path.join(grepo.fullpath, 'grokmirror.timestamp')
        fh = open(tsfile, encoding='ascii')
        ts = fh.read()
        fh.close()

        self.assertEqual(int(ts), 5551212)

        del grepo.timestamp
        self.assertFalse(os.path.exists(tsfile))

        logger.info('Writing bogus values into the timestamp file')
        fh = open(tsfile, 'w', encoding='ascii')
        fh.write(u'Bupkes')
        fh.close()
        del grepo
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        self.assertEqual(grepo.timestamp, 0)

    def testRepoFingerprint(self):
        logger.info('Testing repo fingerprints')
        # Currently, no fingerprint exists, as we just cloned from bundle
        toplevel = fullpathify('test/sources')
        grepo = grokmirror.Repository(toplevel, 'master1.git')

        self.assertEqual(grepo.fingerprint, MASTER1FPR)

        logger.info('Setting fingerprint to bogus value')
        grepo.fingerprint = u'BUPKES'

        fpfile = os.path.join(grepo.fullpath, 'grokmirror.fingerprint')
        fh = open(fpfile, 'r', encoding='ascii')
        contents = fh.read()
        fh.close()

        self.assertEqual(contents, u'BUPKES')

        logger.info('Deleting the fingerprint should remove file')
        del grepo.fingerprint
        self.assertFalse(os.path.exists(fpfile))

        self.assertEqual(grepo.fingerprint, MASTER1FPR)

        # check that it got saved in the file again
        fh = open(fpfile, 'r', encoding='ascii')
        contents = fh.read()
        fh.close()

        self.assertEqual(contents, MASTER1FPR)

    def testFindGitdirs(self):
        logger.info('Test finding all gitdirs')
        logger.info('Creating some extra gitdirs for test')
        clone_bundle_into('test/master1.bundle', 'test/sources/findme.git')
        clone_bundle_into('test/master1.bundle', 'test/sources/missme.git')
        toplevel = fullpathify('test/sources')
        shouldfind = [
            os.path.join(toplevel, 'findme.git'),
            os.path.join(toplevel, 'master1.git'),
            os.path.join(toplevel, 'missme.git'),
        ]
        (found, symlinks) = grokmirror.find_all_gitdirs(toplevel)
        found.sort()
        self.assertListEqual(found, shouldfind)
        logger.info('Retrying with ignores')
        shouldfind = [
            os.path.join(toplevel, 'findme.git'),
            os.path.join(toplevel, 'master1.git'),
        ]
        (found, symlinks) = grokmirror.find_all_gitdirs(toplevel, ignore=['*/miss*'])
        found.sort()
        self.assertListEqual(found, shouldfind)

    def testRepoDescription(self):
        logger.info('Testing repo description setting/getting')
        toplevel = fullpathify('test/sources')
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        descfile = os.path.join(grepo.fullpath, 'description')
        desc_to_set = u'Le répo de Bupkes'
        grepo.description = desc_to_set

        fh = open(descfile, 'r')
        description = fh.read()
        fh.close()
        self.assertEqual(description, desc_to_set)

        del grepo
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        self.assertEqual(grepo.description, desc_to_set)

    def testRepoOwner(self):
        logger.info('Testing repo owner setting/getting')
        toplevel = fullpathify('test/sources')
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        cfgfile = os.path.join(grepo.fullpath, 'config')

        owner_to_set = u'Mélanie Gruyère'
        grepo.owner = owner_to_set

        # look into the config file and find our entries
        fh = open(cfgfile, 'r', encoding='utf-8')
        contents = fh.read()
        fh.close()
        self.assertIn(u'owner = %s' % owner_to_set, contents)

        del grepo
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        self.assertEqual(grepo.owner, owner_to_set)

    def testRepoLastModifiedGuess(self):
        logger.info('Testing repo last modification guessing')
        toplevel = fullpathify('test/sources')
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        self.assertEqual(grepo.modified, MASTER1MODIFIED)

    def testRepoAlternates(self):
        logger.info('Testing repo alternates')
        toplevel = fullpathify('test/sources')
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        self.assertEqual(grepo.alternates, [])

        # This should ignore an attempt to set alternates to a non-existing repo
        grepo.alternates = ['bupkes.git']
        self.assertEqual(grepo.alternates, [])

        clone_bundle_into('test/master1.bundle', 'test/sources/altmaster.git')
        agrepo = grokmirror.Repository(toplevel, 'altmaster.git')
        agrepo.alternates = ['master1.git']

        altfile = os.path.join(agrepo.fullpath, 'objects/info/alternates')
        fh = open(altfile, 'r', encoding='ascii')
        contents = fh.read()
        fh.close()

        destpath = os.path.join(toplevel, 'master1.git', 'objects')
        self.assertEqual(contents.strip(), destpath)

        # This should ignore an attempt to remove an alternate
        agrepo.aternates = []
        self.assertEqual(agrepo.alternates, ['master1.git'])

        clone_bundle_into('test/master1.bundle', 'test/sources/foomaster.git')
        agrepo.alternates = ['master1.git', 'foomaster.git']
        fh = open(altfile, 'r', encoding='ascii')
        contents = fh.read()
        fh.close()
        expected_contents = u'%s\n%s' % (
            destpath, os.path.join(toplevel, 'foomaster.git', 'objects'))
        self.assertEqual(contents.strip(), expected_contents)

        # Attempt to remove alternates. This should be ignored.
        agrepo.alternates = ['master1.git']
        self.assertEqual(agrepo.alternates, ['master1.git', 'foomaster.git'])

    def testBasicManifest(self):
        logger.info('Testing basic manifest creation')
        logger.info('Testing uncompressed manifest')
        toplevel = fullpathify('test/sources')
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        desc_to_set = u'Le répo de Bupkes'
        grepo.description = desc_to_set

        testdict = {
            'master1.git': {
                'description': u"Le répo de Bupkes",
                'reference': None,
                'modified': MASTER1MODIFIED,
                'fingerprint': MASTER1FPR,
                'owner': u'Grokmirror',
                'alternates': []}}

        for manifmt in ('manifest.js', 'manifest.js.gz'):
            manifile = fullpathify(os.path.join('test', 'sources', manifmt))
            mani = grokmirror.Manifest(manifile)

            #  Nothing there, so should return an empty dict
            self.assertEqual(mani.repos, {})
            mani.populate(toplevel)
            self.assertEqual(mani.repos, testdict)
            # Attempt simple save
            mani.save()

            # Now load it again
            mani = grokmirror.Manifest(manifile)
            self.assertEqual(mani.repos, testdict)

        # Check setting of mtime
        mtime = 1000000000
        mani.save(mtime=mtime)
        statinfo = os.stat(manifile)
        self.assertEqual(mtime, statinfo[8])

        # Check saving of alternates into manifest
        clone_bundle_into('test/master1.bundle', 'test/sources/altmaster.git')
        clone_bundle_into('test/master1.bundle', 'test/sources/foomaster.git')
        grepo = grokmirror.Repository(toplevel, 'master1.git')
        grepo.alternates = ['altmaster.git', 'foomaster.git']

        mani.populate(toplevel)
        mani.save()
        mani = grokmirror.Manifest(manifile)

        self.assertEqual(mani.repos['master1.git'], {
            'description': u"Le répo de Bupkes",
            'reference': 'altmaster.git',
            'modified': MASTER1MODIFIED,
            'fingerprint': MASTER1FPR,
            'owner': u'Grokmirror',
            'alternates': ['altmaster.git', 'foomaster.git']})

        # Mark one of them export-ok and make sure manifest matches
        grepo.export_ok = True
        mani.populate(toplevel, only_export_ok=True)
        self.assertEqual(len(mani.repos), 1)
        agrepo = grokmirror.Repository(toplevel, 'foomaster.git')
        agrepo.export_ok = True
        mani.populate(toplevel, only_export_ok=True)
        self.assertEqual(len(mani.repos), 2)
        grepo.export_ok = False
        mani.populate(toplevel, only_export_ok=True)
        self.assertEqual(len(mani.repos), 1)

        # Ignore repos starting with "foomaster.git"
        mani.populate(toplevel, ignore=['foomaster.git'])
        self.assertEqual(len(mani.repos), 2)

        # Test to make sure symlinks are recorded properly
        os.symlink(os.path.join(toplevel, 'master1.git'),
                   os.path.join(toplevel, 'symlinked1.git'))
        os.symlink(os.path.join(toplevel, 'master1.git'),
                   os.path.join(toplevel, 'symlinked2.git'))
        mani.populate(toplevel)
        mani.save()

        mani = grokmirror.Manifest(manifile)
        checklinks = mani.repos['master1.git']['symlinks']
        checklinks.sort()

        self.assertEqual(checklinks, ['symlinked1.git', 'symlinked2.git'])


    def testManifestCommand(self):
        logger.info('Testing manifest command')
        clone_bundle_into('test/master1.bundle', 'test/sources/repo2.git')
        clone_bundle_into('test/master1.bundle', 'test/sources/repo3.git')

        # Create a new manifest
        toplevel = fullpathify('test/sources')
        manifile = fullpathify(os.path.join(toplevel, 'manifest.js'))

        #manifest.grok_manifest(manifile, toplevel)



if __name__ == '__main__':
    logger.info('----------Starting test run')
    unittest.main()
    logger.info('----------Ending test run')
