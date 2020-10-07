#!/usr/bin/env python
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
import re
from setuptools import setup


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


def find_version(source):
    version_file = read(source)
    version_match = re.search(r"^VERSION = ['\"]([^'\"]*)['\"]", version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string.")


NAME = 'grokmirror'
VERSION = find_version('grokmirror/__init__.py')


setup(
    version=VERSION,
    url='https://git.kernel.org/pub/scm/utils/grokmirror/grokmirror.git',
    download_url='https://www.kernel.org/pub/software/network/grokmirror/%s-%s.tar.xz' % (NAME, VERSION),
    name=NAME,
    description='Smartly mirror git repositories that use grokmirror',
    author='Konstantin Ryabitsev',
    author_email='konstantin@linuxfoundation.org',
    packages=[NAME],
    license='GPLv3+',
    long_description=read('README.rst'),
    long_description_content_type='text/x-rst',
    keywords=['git', 'mirroring', 'repositories'],
    project_urls={
        'Source': 'https://git.kernel.org/pub/scm/utils/grokmirror/grokmirror.git',
        'Tracker': 'https://github.com/mricon/grokmirror/issues',
    },
    install_requires=[
        'requests',
    ],
    python_requires='>=3.6',
    entry_points={
        'console_scripts': [
            "grok-dumb-pull=grokmirror.dumb_pull:command",
            "grok-pull=grokmirror.pull:command",
            "grok-fsck=grokmirror.fsck:command",
            "grok-manifest=grokmirror.manifest:command",
            "grok-bundle=grokmirror.bundle:command",
            "grok-pi-piper=grokmirror.pi_piper:command",
        ]
    }
)
