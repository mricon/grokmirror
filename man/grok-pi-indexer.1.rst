GROK-PI-INDEXER
===============
----------------------------------------------------
Hook script for indexing mirrored public-inbox repos
----------------------------------------------------

:Author:    mricon@kernel.org
:Date:      2021-07-27
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   2.1.0
:Manual section: 1

SYNOPSIS
--------
    grok-pi-indexer [-h] [-v] -c PICONFIG [-l LOGFILE] {init,update,extindex}

DESCRIPTION
-----------
This is a helper hook for correctly initializing and indexing
public-inbox repositories. NOTE: a working public-inbox 1.6+ install is
required, and public-inbox commands must be in the PATH.

The command should be invoked via grokmirror hooks, for example, use
the following grokmirror configuration file to mirror lore.kernel.org::

    [core]
    toplevel = /ver/lib/git/lore.kernel.org
    manifest = ${toplevel}/manifest.js.gz
    log = /var/log/grokmirror/lore.kernel.org.log
    loglevel = info

    [remote]
    site = https://lore.kernel.org
    manifest = ${site}/manifest.js.gz

    [pull]
    default_owner = PublicInbox
    pull_threads = 2
    # Adjust as you see fit, or simply set to * to mirror everything
    include = /git/*
              /tools/*
    refresh = 60
    purge = no
    post_clone_complete_hook = /usr/bin/grok-pi-indexer -c /etc/public-inbox/config init
    post_update_hook = /usr/bin/grok-pi-indexer -c /etc/public-inbox/config update
    # Uncomment if you've defined any [extindex] sections
    #post_work_complete_hook = /usr/bin/grok-pi-indexer -c /etc/public-inbox/config extindex

    [fsck]
    frequency = 30
    report_to = root
    statusfile = ${core:toplevel}/fsck.status.js
    repack = yes
    commitgraph = yes
    prune = yes


OPTIONS
-------
  -h, --help            show this help message and exit
  -v, --verbose         Be verbose and tell us what you are doing (default: False)
  -c PICONFIG, --pi-config PICONFIG
                        Location of the public-inbox configuration file (default: None)
  -l LOGFILE, --logfile LOGFILE
                        Log activity in this log file (default: None)

SEE ALSO
--------
* grok-pull(1)
* git(1)

SUPPORT
-------
Email tools@linux.kernel.org.
