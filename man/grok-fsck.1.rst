GROK-FSCK
=========
------------------------------------------
Check mirrored repositories for corruption
------------------------------------------

:Author:    mricon@kernel.org
:Date:      2019-02-14
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   1.2.0
:Manual section: 1

SYNOPSIS
--------
    grok-fsck -c /path/to/fsck.conf

DESCRIPTION
-----------
Git repositories can get corrupted whether they are frequently updated
or not, which is why it is useful to routinely check them using "git
fsck". Grokmirror ships with a "grok-fsck" utility that will run "git
fsck" on all mirrored git repositories. It is supposed to be run
nightly from cron, and will do its best to randomly stagger the checks
so only a subset of repositories is checked each night. Any errors will
be sent to the user set in MAILTO.

OPTIONS
-------
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -v, --verbose         Be verbose and tell us what you are doing
  -f, --force           Force immediate run on all repositories.
  -c CONFIG, --config=CONFIG
                        Location of fsck.conf
  --repack-only         Only find and repack repositories that need
                        optimizing (nightly run mode)
  --connectivity        (Assumes --force): Run git fsck on all repos,
                        but only check connectivity
  --repack-all-quick    (Assumes --force): Do a quick repack of all repos
  --repack-all-full     (Assumes --force): Do a full repack of all repos

EXAMPLES
--------
Locate fsck.conf and modify it to reflect your needs. The default
configuration file is heavily commented.

Set up a cron job to run nightly for quick repacks, and weekly for fsck
checks::

    # Make sure MAILTO is set, for error reports
    MAILTO=root
    # Run nightly repacks to optimize the repos
    0 2 1-6 * * mirror /usr/bin/grok-fsck -c /etc/grokmirror/fsck.conf --repack-only
    # Run weekly fsck checks on Sunday
    0 2 0 * * mirror /usr/bin/grok-fsck -c /etc/grokmirror/fsck.conf

You can force a full run using the ``-f`` flag, but unless you only have
a few smallish git repositories, it's not recommended, as it may take
several hours to complete, as it will do a full repack, prune and fsck
of all repositories. To make this process faster, you can use:

* ``--connectivity``: when doing fsck, only check object connectivity
* ``--repack-all-quick``: do a quick repack of all repositories
* ``--repack-all-full``: if you have ``extra_repack_flags_full`` defined
  in the configuration file, trigger a full repack of every repository.
  This can be handy if you need to bring up a newly cloned mirror and
  want to make sure it's repacked and all bitmaps are built before
  serving content.

SEE ALSO
--------
* grok-manifest(1)
* grok-pull(1)
* git(1)

SUPPORT
-------
Please open an issue on Github::

    https://github.com/mricon/grokmirror/issues
