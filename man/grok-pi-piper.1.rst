GROK-PI-PIPER
=============
-----------------------------------------------------------
Hook script for piping new messages from public-inbox repos
-----------------------------------------------------------

:Author:    mricon@kernel.org
:Date:      2020-10-07
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   2.0.2
:Manual section: 1

SYNOPSIS
--------
    grok-pi-piper [-h] [-v] [-d] -c CONFIG [-l PIPELAST] [--version] repo

DESCRIPTION
-----------
This is a ready-made hook script that can be called from
pull.post_update_hook when mirroring public-inbox repositories. It will
pipe all newly received messages to arbitrary commands defined in the
config file. The simplest configuration for lore.kernel.org is::

    ~/.config/pi-piper.conf
    -----------------------
    [DEFAULT]
    pipe = /usr/bin/procmail
    shallow = yes

    ~/.procmailrc
    -------------
    DEFAULT=$HOME/Maildir/

    ~/.config/lore.conf
    -------------------
    [core]
    toplevel = ~/.local/share/grokmirror/lore
    log = ${toplevel}/grokmirror.log

    [remote]
    site = https://lore.kernel.org
    manifest = https://lore.kernel.org/manifest.js.gz

    [pull]
    post_update_hook = ~/.local/bin/grok-pi-piper -c ~/.config/pi-piper.conf
    include = /list-you-want/*
              /another-list/*

It assumes that grokmirror was installed from pip. If you installed it
via some other means, please check the path for the grok-pi-piper
script.

Note, that initial clone may take a long time, even if you set
shallow=yes.

See pi-piper.conf for other config options.


OPTIONS
-------
  -h, --help            show this help message and exit
  -v, --verbose         Be verbose and tell us what you are doing (default: False)
  -d, --dry-run         Do a dry-run and just show what would be done (default: False)
  -c CONFIG, --config CONFIG
                        Location of the configuration file (default: None)
  -l PIPELAST, --pipe-last PIPELAST
                        Force pipe last NN messages in the list, regardless of tracking (default: None)
  --version             show program's version number and exit


SEE ALSO
--------
* grok-pull(1)
* git(1)

SUPPORT
-------
Email tools@linux.kernel.org.
