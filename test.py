#/usr/bin/env python2 -tt
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

import grokmirror

logger = logging.getLogger('grokmirror')
logger.setLevel(logging.DEBUG)

ch = logging.FileHandler('test.log')
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter("[%(asctime)s][%(levelname)s:%(funcName)s:"
                              "%(lineno)s] %(message)s")
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.info('Starting test run')


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
        fullpath = fullpathify('test/sources/master1.git')

        # Just test to make sure nothing tracebacks
        grokmirror.lock_repo(fullpath, nonblocking=True)
        grokmirror.unlock_repo(fullpath)

    def testRepoTimestamp(self):
        logger.info('Testing repo timestamps')
        # Currently, no timestamp exists as we just cloned it from bundle
        self.assertEqual(grokmirror.get_repo_timestamp(fullpathify('test/sources'), 'master1.git'), 0)
        # Set timestamp to 5551212
        grokmirror.set_repo_timestamp(fullpathify('test/sources'), 'master1.git', 5551212)
        self.assertEqual(grokmirror.get_repo_timestamp(fullpathify('test/sources'), 'master1.git'), 5551212)


if __name__ == '__main__':

    unittest.main()
