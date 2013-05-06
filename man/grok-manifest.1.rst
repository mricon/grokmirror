GROK-MANIFEST
=============
---------------------------------------
Create manifest for use with grokmirror
---------------------------------------

:Author:    mricon@kernel.org
:Date:      2013-04-26
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   0.2

SYNOPSIS
--------
    grok-manifest [opts] -m manifest.js[.gz] -t /path [/path/to/bare.git]

DESCRIPTION
-----------
Call grok-manifest from a git post-update or post-receive hook to create
the latest repository manifest. This manifest file is downloaded by
mirror slaves (if newer than what they already have) and used to only
clone/pull the repositories that have changed since the mirror's last run.

OPTIONS
-------
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -m MANIFILE, --manifest=MANIFILE
                        Location of manifest.js or manifest.js.gz
  -t TOPLEVEL, --toplevel=TOPLEVEL
                        Top dir where all repositories reside
  -c, --check-export-ok
                        Honor the git-daemon-export-ok magic file and 
                        do not export repositories not marked as such
  -n, --use-now         Use current timestamp instead of parsing commits
  -p, --purge           Purge deleted git repositories from manifest
  -x, --remove          Remove repositories passed as arguments from
                        the manifest file
  -i IGNORE, --ignore-paths=IGNORE
                        When finding git dirs, ignore these paths (can be used
                        multiple times, accepts shell-style globbing)
  -v, --verbose         Be verbose and tell us what you are doing

EXAMPLES
--------
The examples assume that the repositories are located in /repos. If your
repositories are in ``/var/lib/git``, adjust both ``-m`` and ``-t``
flags accordingly.

Initial manifest generation::

    /usr/bin/grok-manifest -m /repos/manifest.js.gz -t /repos

Inside the git hook::

    /usr/bin/grok-manifest -m /repos/manifest.js.gz -t /repos -n `pwd`

To purge deleted repositories, use the ``-p`` flag when running from
cron::

    /usr/bin/grok-manifest -m /repos/manifest.js.gz -t /repos -p

You can also add it to the gitolite's "rm" ADC using the ``-x`` flag::

    /usr/bin/grok-manifest -m /repos/manifest.js.gz -t /repos -x $repo.git

SEE ALSO
--------
  * grok-pull(1)
  * git(1)

SUPPORT
-------
Please send support requests to the mailing list::

    http://lists.kernel.org/mailman/listinfo/grokmirror
