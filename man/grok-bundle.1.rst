GROK-BUNDLE
===========
-------------------------------------------------
Create clone.bundle files for use with "repo"
-------------------------------------------------

:Author:    mricon@kernel.org
:Date:      2020-09-04
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   2.0.0
:Manual section: 1

SYNOPSIS
--------
    grok-bundle [options] -c grokmirror.conf -o path

DESCRIPTION
-----------
Android's "repo" tool will check for the presence of clone.bundle files
before performing a fresh git clone. This is done in order to offload
most of the git traffic to a CDN and reduce the load on git servers
themselves.

This command will generate clone.bundle files in a hierarchy expected by
repo. You can then sync the output directory to a CDN service.

OPTIONS
-------

  -h, --help            show this help message and exit
  -v, --verbose         Be verbose and tell us what you are doing (default: False)
  -c CONFIG, --config CONFIG
                        Location of the configuration file
  -o OUTDIR, --outdir OUTDIR
                        Location where to store bundle files
  -g GITARGS, --gitargs GITARGS
                        extra args to pass to git (default: -c core.compression=9)
  -r REVLISTARGS, --revlistargs REVLISTARGS
                        Rev-list args to use (default: --branches HEAD)
  -s MAXSIZE, --maxsize MAXSIZE
                        Maximum size of git repositories to bundle (in GiB) (default: 2)
  -i, --include INCLUDE
                        List repositories to bundle (accepts shell globbing) (default: \*)

EXAMPLES
--------

    grok-bundle -c grokmirror.conf -o /var/www/bundles -i /pub/scm/linux/kernel/git/torvalds/linux.git /pub/scm/linux/kernel/git/stable/linux.git /pub/scm/linux/kernel/git/next/linux-next.git

SEE ALSO
--------
* grok-pull(1)
* grok-manifest(1)
* grok-fsck(1)
* git(1)

SUPPORT
-------
Email tools@linux.kernel.org.
