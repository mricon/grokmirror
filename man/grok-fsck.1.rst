GROK-FSCK
=========
-------------------------------------------------------
Optimize mirrored repositories and check for corruption
-------------------------------------------------------

:Author:    mricon@kernel.org
:Date:      2020-08-14
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   2.0.0
:Manual section: 1

SYNOPSIS
--------
    grok-fsck -c /path/to/grokmirror.conf

DESCRIPTION
-----------
Git repositories should be routinely repacked and checked for
corruption. This utility will perform the necessary optimizations and
report any problems to the email defined via fsck.report_to ('root' by
default). It should run weekly from cron or from the systemd timer (see
contrib).

Please examine the example grokmirror.conf file for various things you
can tweak.

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

SEE ALSO
--------
* grok-manifest(1)
* grok-pull(1)
* git(1)

SUPPORT
-------
Email tools@linux.kernel.org.
