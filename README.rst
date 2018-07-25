GROKMIRROR
==========
--------------------------------------------
Framework to smartly mirror git repositories
--------------------------------------------

:Author:    konstantin@linuxfoundation.org
:Date:      2018-04-24
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   1.1.1

DESCRIPTION
-----------
Grokmirror was written to make mirroring large git repository
collections more efficient. Grokmirror uses the manifest file published
by the master mirror in order to figure out which repositories to
clone, and to track which repositories require updating. The process is
extremely lightweight and efficient both for the master and for the
mirrors.

CONCEPTS
--------
Grokmirror master publishes a json-formatted manifest file containing
information about all git repositories that it carries. The format of
the manifest file is as follows::

    {
      "/path/to/bare/repository.git": {
        "description": "Repository description",
        "reference":   "/path/to/reference/repository.git",
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
so the manifest.js file always contains the most up-to-date information
about the repositories provided by the git server and their
last-modified date.

The mirroring clients will constantly poll the manifest.js file and
download the updated manifest if it is newer than the locally stored
copy (using ``Last-Modified`` and ``If-Modified-Since`` http headers).
After downloading the updated manifest.js file, the mirrors will parse
it to find out which repositories have been updated and which new
repositories have been added.

For all newly-added repositories, the clients will do::

    git clone --mirror git://server/path/to/repository.git \
        /local/path/to/repository.git

For all updated repositories, the clients will do::

    GIT_DIR=/local/path/to/repository.git git remote update

When run with ``--purge``, the clients will also purge any repositories
no longer present in the manifest file received from the server.

Shared repositories
~~~~~~~~~~~~~~~~~~~
Grokmirror will automatically recognize when repositories share objects
via alternates. E.g. if repositoryB is a shared clone of repositoryA
(that is, it's been cloned using ``git clone -s repositoryA``), the
manifest will mention the referencing repository, so grokmirror will
mirror repositoryA first, and then mirror repositoryB with a
``--reference`` flag. This greatly reduces the bandwidth and disk use
for large repositories.

See man git-clone_ for more info.

.. _git-clone: https://www.kernel.org/pub/software/scm/git/docs/git-clone.html

SERVER SETUP
------------
Install grokmirror on the server using your preferred way.

**IMPORTANT: Currently, only bare git repositories are supported.**

You will need to add a hook to each one of your repositories that would
update the manifest upon repository modification. This can either be a
post-receive hook, or a post-update hook. The hook must call the
following command::

    /usr/bin/grok-manifest -m /repos/manifest.js.gz -t /repos -n `pwd`

The **-m** flag is the path to the manifest.js file. The git process must be
able to write to it and to the directory the file is in (it creates a
manifest.js.randomstring file first, and then moves it in place of the
old one for atomicity).

The **-t** flag is to help grokmirror trim the irrelevant toplevel disk
path. E.g. if your repository is in /var/lib/git/repository.git, but it
is exported as git://server/repository.git, then you specify ``-t
/var/lib/git``.

The **-n** flag tells grokmirror to use the current timestamp instead of the
exact timestamp of the commit (much faster this way).

Before enabling the hook, you will need to generate the manifest.js of
all your git repositories. In order to do that, run the same command,
but omit the -n and the \`pwd\` argument. E.g.::

    /usr/bin/grok-manifest -m /repos/manifest.js.gz -t /repos

The last component you need to set up is to automatically purge deleted
repositories from the manifest. As this can't be added to a git hook,
you can either run the ``--purge`` command from cron::

    /usr/bin/grok-manifest -m /repos/manifest.js.gz -t /repos -p

Or add it to your gitolite's ``D`` command using the ``--remove`` flag::

    /usr/bin/grok-manifest -m /repos/manifest.js.gz -t /repos -x $repo.git

If you would like grok-manifest to honor the ``git-daemon-export-ok``
magic file and only add to the manifest those repositories specifically
marked as exportable, pass the ``--check-export-ok`` flag. See
``git-daemon(1)`` for more info on ``git-daemon-export-ok`` file.

MIRROR SETUP
------------
Install grokmirror on the mirror using your preferred way.

Locate repos.conf and modify it to reflect your needs. The default
configuration file is heavily commented.

Add a cronjob to run as frequently as you like. For example, add the
following to ``/etc/cron.d/grokmirror.cron``::

    # Run grok-pull every minute as user "mirror"
    * * * * * mirror /usr/bin/grok-pull -p -c /etc/grokmirror/repos.conf

Make sure the user "mirror" (or whichever user you specified) is able to
write to the toplevel and log locations specified in repos.conf.

If you already have a bunch of repositories in the hierarchy that
matches the upstream mirror and you'd like to reuse them instead of
re-downloading everything from the master, you can pass the ``-r`` flag
to tell grok-pull that it's okay to reuse existing repos. This will
delete any existing remotes defined in the repository and set the new
origin to match what is configured in the repos.conf.

GROK-FSCK
---------
Git repositories can get corrupted whether they are frequently updated
or not, which is why it is useful to routinely check them using "git
fsck". Grokmirror ships with a "grok-fsck" utility that will run "git
fsck" on all mirrored git repositories. It is supposed to be run
nightly from cron, and will do its best to randomly stagger the checks
so only a subset of repositories is checked each night. Any errors will
be sent to the user set in MAILTO.

To enable grok-fsck, first locate the fsck.conf file and edit it to
match your setup -- e.g., it must know where you keep your local
manifest. Then, add the following to ``/etc/cron.d/grok-fsck.cron``::

    # Make sure MAILTO is set, for error reports
    MAILTO=root
    # Run nightly, at 2AM
    00 02 * * * mirror /usr/bin/grok-fsck -c /etc/grokmirror/fsck.conf

You can force a full run using the ``-f`` flag, but unless you only have
a few smallish git repositories, it's not recommended, as it may take
several hours to complete.

Before it runs, grok-fsck will put an advisory lock for the git-directory
being checked (.repository.git.lock). Grok-pull will recognize the lock
and will postpone any incoming updates to that repository until the lock
is freed.

You can also tell grok-fsck to repack repository after checking it for
errors. To do this, set "repack" value in fsck.conf to "yes". If you
have repositories using alternates, the safer value for repack flags is
"-Adlq".

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

Why not just run "git pull" from cron every minute?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
This is not a complete mirroring strategy, as this won't notify you when
the remote mirror adds new repositories. It is also not very nice to the
remote server, especially the one that carries hundreds of repositories.

Additionally, this will not automatically take care of shared
repositories for you. See "Shared repositories" under "CONCEPTS".
