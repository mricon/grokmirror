GROKMIRROR
==========
--------------------------------------------
Framework to smartly mirror git repositories
--------------------------------------------

:Author:    konstantin@linuxfoundation.org
:Date:      2020-09-18
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   2.0.0

DESCRIPTION
-----------
Grokmirror was written to make replicating large git repository
collections more efficient. Grokmirror uses the manifest file published
by the origin server in order to figure out which repositories to clone,
and to track which repositories require updating. The process is
lightweight and efficient both for the primary and for the replicas.

CONCEPTS
--------
The origin server publishes a json-formatted manifest file containing
information about all git repositories that it carries. The format of
the manifest file is as follows::

    {
      "/path/to/bare/repository.git": {
        "description": "Repository description",
        "head":        "ref: refs/heads/branchname",
        "reference":   "/path/to/reference/repository.git",
        "forkgroup":   "forkgroup-guid",
        "modified":    timestamp,
        "fingerprint": sha1sum(git show-ref),
        "symlinks": [
            "/location/to/symlink",
            ...
        ],
       }
       ...
    }

The manifest file is usually gzip-compressed to preserve bandwidth.

Each time a commit is made to one of the git repositories, it
automatically updates the manifest file using an appropriate git hook,
so the manifest.js file should always contain the most up-to-date
information about the state of all repositories.

The mirroring clients will poll the manifest.js file and download the
updated manifest if it is newer than the locally stored copy (using
``Last-Modified`` and ``If-Modified-Since`` http headers). After
downloading the updated manifest.js file, the mirrors will parse it to
find out which repositories have been updated and which new repositories
have been added.

Object Storage Repositories
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Grokmirror 2.0 introduces the concept of "object storage repositories",
which aims to optimize how repository forks are stored on disk and
served to the cloning clients.

When grok-fsck runs, it will automatically recognize related
repositories by analyzing their root commits. If it finds two or more
related repositories, it will set up a unified "object storage" repo and
fetch all refs from each related repository into it.

For example, you can have two forks of linux.git:
  torvalds/linux.git:
    refs/heads/master
    refs/tags/v5.0-rc3
    ...

and its fork:

  maintainer/linux.git:
    refs/heads/master
    refs/heads/devbranch
    refs/tags/v5.0-rc3
    ...

Grok-fsck will set up an object storage repository and fetch all refs from
both repositories:

  objstore/[random-guid-name].git
     refs/virtual/[sha1-of-torvalds/linux.git:12]/heads/master
     refs/virtual/[sha1-of-torvalds/linux.git:12]/tags/v5.0-rc3
     ...
     refs/virtual/[sha1-of-maintainer/linux.git:12]/heads/master
     refs/virtual/[sha1-of-maintainer/linux.git:12]/heads/devbranch
     refs/virtual/[sha1-of-maintainer/linux.git:12]/tags/v5.0-rc3
     ...

Then both torvalds/linux.git and maintainer/linux.git with be configured
to use objstore/[random-guid-name].git via objects/info/alternates
and repacked to just contain metadata and no objects.

The alternates repository will be repacked with "delta islands" enabled,
which should help optimize clone operations for each "sibling"
repository.

Please see the example grokmirror.conf for more details about
configuring objstore repositories.


ORIGIN SETUP
------------
Install grokmirror on the origin server using your preferred way.

**IMPORTANT: Only bare git repositories are supported.**

You will need to add a hook to each one of your repositories that would
update the manifest upon repository modification. This can either be a
post-receive hook, or a post-update hook. The hook must call the
following command::

    /usr/bin/grok-manifest -m /var/www/html/manifest.js.gz \
        -t /var/lib/gitolite3/repositories -n `pwd`

The **-m** flag is the path to the manifest.js file. The git process
must be able to write to it and to the directory the file is in (it
creates a manifest.js.randomstring file first, and then moves it in
place of the old one for atomicity).

The **-t** flag is to help grokmirror trim the irrelevant toplevel disk
path, so it is trimmed from the top.

The **-n** flag tells grokmirror to use the current timestamp instead of
the exact timestamp of the commit (much faster this way).

Before enabling the hook, you will need to generate the manifest.js of
all your git repositories. In order to do that, run the same command,
but omit the -n and the \`pwd\` argument. E.g.::

    /usr/bin/grok-manifest -m /var/www/html/manifest.js.gz \
        -t /var/lib/gitolite3/repositories

The last component you need to set up is to automatically purge deleted
repositories from the manifest. As this can't be added to a git hook,
you can either run the ``--purge`` command from cron::

    /usr/bin/grok-manifest -m /var/www/html/manifest.js.gz \
        -t /var/lib/gitolite3/repositories -p

Or add it to your gitolite's ``D`` command using the ``--remove`` flag::

    /usr/bin/grok-manifest -m /var/www/html/manifest.js.gz \
        -t /var/lib/gitolite3/repositories -x $repo.git

If you would like grok-manifest to honor the ``git-daemon-export-ok``
magic file and only add to the manifest those repositories specifically
marked as exportable, pass the ``--check-export-ok`` flag. See
``git-daemon(1)`` for more info on ``git-daemon-export-ok`` file.

You will need to have some kind of httpd server to serve the manifest
file.

REPLICA SETUP
-------------
Install grokmirror on the replica using your preferred way.

Locate grokmirror.conf and modify it to reflect your needs. The default
configuration file is heavily commented to explain what each option
does.

Make sure the user "mirror" (or whichever user you specified) is able to
write to the toplevel and log locations specified in grokmirror.conf.

You can either run grok-pull manually, from cron, or as a
systemd-managed daemon (see contrib). If you do it more frequently than
once every few hours, you should definitely run it as a daemon in order
to improve performance.

GROK-FSCK
---------
Git repositories should be routinely repacked and checked for
corruption. This utility will perform the necessary optimizations and
report any problems to the email defined via fsck.report_to ('root' by
default). It should run weekly from cron or from the systemd timer (see
contrib).

Please examine the example grokmirror.conf file for various things you
can tweak.

FAQ
---
Why is it called "grok mirror"?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Because it's developed at kernel.org and "grok" is a mirror of "korg".
Also, because it groks git mirroring.

Why not just use rsync?
~~~~~~~~~~~~~~~~~~~~~~~
Rsync is extremely inefficient for the purpose of mirroring git trees
that mostly consist of a lot of small files that very rarely change.
Since rsync must calculate checksums on each file during each run, it
mostly results in a lot of disk thrashing.

Additionally, if several repositories share objects between each-other,
unless the disk paths are exactly the same on both the remote and local
mirror, this will result in broken git repositories.

It is also a bit silly, considering git provides its own extremely
efficient mechanism for specifying what changed between revision X and
revision Y.
