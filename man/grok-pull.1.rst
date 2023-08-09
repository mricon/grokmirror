GROK-PULL
=========
--------------------------------------
Clone or update local git repositories
--------------------------------------

:Author:    mricon@kernel.org
:Date:      2020-08-14
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   2.0.0
:Manual section: 1

SYNOPSIS
--------
  grok-pull -c /path/to/grokmirror.conf

DESCRIPTION
-----------
Grok-pull is the main tool for replicating repository updates from the
grokmirror primary server to the mirrors.

Grok-pull has two modes of operation -- onetime and continuous
(daemonized). In one-time operation mode, it downloads the latest
manifest and applies any outstanding updates. If there are new
repositories or changes in the existing repositories, grok-pull will
perform the necessary git commands to clone or fetch the required data
from the master. Once all updates are applied, it will write its own
manifest and exit. In this mode, grok-pull can be run manually or from
cron.

In continuous operation mode (when run with -o), grok-pull will continue
running after all updates have been applied and will periodically
re-download the manifest from the server to check for new updates. For
this to work, you must set pull.refresh in grokmirror.conf to the amount
of seconds you would like it to wait between refreshes.

If pull.socket is specified, grok-pull will also listen on a socket for
any push updates (relative repository path as present in the manifest
file, terminated with newlines). This can be used for pubsub
subscriptions (see contrib).

OPTIONS
-------
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -v, --verbose         Be verbose and tell us what you are doing
  -n, --no-mtime-check  Run without checking manifest mtime.
  -o, --continuous      Run continuously (no effect if refresh is not set)
  -c CONFIG, --config=CONFIG
                        Location of the configuration file
  -p, --purge           Remove any git trees that are no longer in manifest.
  --force-purge         Force purge operation despite significant repo deletions

EXAMPLES
--------
Use grokmirror.conf and modify it to reflect your needs. The example
configuration file is heavily commented. To invoke, run::

  grok-pull -v -c /path/to/grokmirror.conf

SEE ALSO
--------
* grok-manifest(1)
* grok-fsck(1)
* git(1)

SUPPORT
-------
Please email tools@linux.kernel.org.
