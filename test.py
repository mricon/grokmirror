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


class GATest(unittest.TestCase):
    def setUp(self):
        self.tearDown()
        # we pretty much always need master1.git
        logger.debug('Setting up test/sources/master1.git')
        os.mkdir('test/sources')
        os.mkdir('test/targets')
        grokmirror.run_git_command(args=['clone', '--bare',
                                         'test/master1.bundle',
                                         'test/sources/master1.git'],
                                   env=None)

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
        grokmirror.run_git_command(args=['clone', '--bare',
                                         'test/master1.bundle',
                                         'test/sources/findme.git'],
                                   env=None)
        grokmirror.run_git_command(args=['clone', '--bare',
                                         'test/master1.bundle',
                                         'test/sources/missme.git'],
                                   env=None)
        toplevel = fullpathify('test/sources')
        shouldfind = [
            os.path.join(toplevel, 'findme.git'),
            os.path.join(toplevel, 'master1.git'),
            os.path.join(toplevel, 'missme.git'),
        ]
        found = grokmirror.find_all_gitdirs(toplevel)
        found.sort()
        self.assertListEqual(found, shouldfind)
        logger.info('Retrying with ignores')
        shouldfind = [
            os.path.join(toplevel, 'findme.git'),
            os.path.join(toplevel, 'master1.git'),
        ]
        found = grokmirror.find_all_gitdirs(toplevel, ignore=['*/miss*'])
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

    def testBasicManifest(self):
        return
        logger.info('Testing basic manifest creation')
        logger.info('Testing uncompressed manifest')
        toplevel = fullpathify('test/sources')
        manifest.grok_manifest('test/sources/manifest.js', toplevel)


if __name__ == '__main__':
    logger.info('----------Starting test run')
    unittest.main()
    logger.info('----------Ending test run')
