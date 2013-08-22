GROK-PULL
=========
--------------------------------------
Clone or update local git repositories
--------------------------------------

:Author:    mricon@kernel.org
:Date:      2013-08-22
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   0.4
:Manual section: 1

SYNOPSIS
--------
    grok-pull -c /path/to/repos.conf

DESCRIPTION
-----------
This utility runs from a cronjob and downloads the latest manifest from
the grokmirror master. If there are new repositories or changes in the
existing repositories, grok-pull will perform the necessary git commands
to clone or fetch the required data from the master.

At the end of its run, grok-pull will generate its own manifest file,
which can then be used for further mirroring.

OPTIONS
-------
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -v, --verbose         Be verbose and tell us what you are doing
  -n, --no-mtime-check  Run without checking manifest mtime.
  -f, --force           Force full git update regardless of last-modified
                        times. Also useful when repos.conf has changed.
  -p, --purge           Remove any git trees that are no longer in manifest.
  -y, --pretty          Pretty-print the generated manifest (sort repos
                        and add indentation). This is much slower, so
                        should be used with caution on large
                        collections.
  -r, --no-reuse-existing-repos
                        If any existing repositories are found on disk,
                        do NOT set new remote origin and reuse, just
                        skip them entirely
  -c CONFIG, --config=CONFIG
                        Location of repos.conf

EXAMPLES
--------
Locate repos.conf and modify it to reflect your needs. The default
configuration file is heavily commented.

Add a cronjob to run as frequently as you like. For example, add the
following to ``/etc/cron.d/grokmirror.cron``::

    # Run grok-pull every minute as user "mirror"
    * * * * * mirror /usr/bin/grok-pull -p -c /etc/grokmirror/repos.conf

Make sure the user "mirror" (or whichever user you specified) is able to
write to the toplevel, log and lock locations specified in repos.conf.

SEE ALSO
--------
  * grok-manifest(1)
  * grok-fsck(1)
  * git(1)

SUPPORT
-------
Please send support requests to the mailing list::

    http://lists.kernel.org/mailman/listinfo/grokmirror
