GROK-FSCK
=========
------------------------------------------
Check mirrored repositories for corruption
------------------------------------------

:Author:    mricon@kernel.org
:Date:      2013-04-26
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   0.3
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

EXAMPLES
--------
Locate fsck.conf and modify it to reflect your needs. The default
configuration file is heavily commented.

Set up a cron job to run nightly and to email any discovered errors to
root::

    # Make sure MAILTO is set, for error reports
    MAILTO=root
    # Run nightly, at 2AM
    00 02 * * * mirror /usr/bin/grok-fsck -c /etc/grokmirror/fsck.conf

You can force a full run using the ``-f`` flag, but unless you only have
a few smallish git repositories, it's not recommended, as it may take
several hours to complete.

SEE ALSO
--------
  * grok-manifest(1)
  * grok-pull(1)
  * git(1)

SUPPORT
-------
Please send support requests to the mailing list::

    http://lists.kernel.org/mailman/listinfo/grokmirror
