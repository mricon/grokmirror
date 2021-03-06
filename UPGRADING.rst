Upgrading from Grokmirror 1.x to 2.x
------------------------------------
Grokmirror-2.0 introduced major changes to how repositories are
organized, so it deliberately breaks the upgrade path in order to force
admins to make proper decisions. Installing the newer version on top of
the old one will break replication, as it will refuse to work with old
configuration files.

Manifest compatibility
----------------------
Manifest files generated by grokmirror-1.x will continue to work on
grokmirror-2.x replicas. Similarly, manifest files generated by
grokmirror-2.x origin servers will work on grokmirror-1.x replicas.

In other words, upgrading the origin servers and replicas does not need
to happen at the same time. While grokmirror-2.x adds more entries to
the manifest file (e.g. "forkgroup" and "head" records), they will be
ignored by grokmirror-1.x replicas.

Upgrading the origin server
---------------------------
Breaking changes affecting the origin server are related to grok-fsck
runs. Existing grok-manifest hooks should continue to work without any
changes required.

Grok-fsck will now automatically recognize related repositories by
comparing the output of ``git rev-list --max-parents=0 --all``. When two
or more repositories are recognized as forks of each-other, a new
"object storage" repository will be set up that will contain refs from
all siblings. After that, individual repositories will be repacked to
only contain repository metadata (and loose objects in need of pruning).

Existing repositories that already use alternates will be automatically
migrated to objstore repositories during the first grok-fsck run. If you
have a small collection of repositories, or if the vast majority of them
aren't forks of each-other, then the upgrade can be done live with
little impact.

If the opposite is true and most of your repositories are forks, then
the initial grok-fsck run will take a lot of time and resources to
complete, as repositories will be automatically repacked to take
advantage of the new object storage layout. Doing so without preparation
can significantly impact the availability of your server, so you should
plan the upgrade appropriately.

Recommended scenario for large collections with lots of forks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
1. Set up a temporary system with fast disk IO and plenty of CPUs
   and RAM. Repacking will go a lot faster on fast systems with plenty
   of IO cycles.
2. Install grokmirror-2 and configure it to replicate from the origin
   **INTO THE SAME PATH AS ON THE ORIGIN SERVER**. If your origin server
   is hosting repos out of /var/lib/gitolite3/repositories, then your
   migration replica should be configured with toplevel in
   /var/lib/gitolite3/repositories. This is important, because when the
   "alternates" file is created, it specifies a full path to the
   location of the object storage directory and moving repositories into
   different locations post-migration will result in breakage. *Avoid
   using symlinks for this purpose*, as grokmirror-2 will realpath them
   before using internally.
3. Perform initial grok-pull replication from the current origin server
   to the migration replica. This should set up all repositories
   currently using alternates as objstore repositories.
4. Once the initial replication is complete, run grok-fsck on the new
   hierarchy. This should properly repack all new object storage
   repositories to benefit from delta islands, plus automatically find
   all repositories that are forks of each-other but aren't already set
   up for alternates. The initial grok-fsck process may take a LONG time
   to run, depending on the size of your repository collection.
5. Schedule migration downtime.
6. Right before downtime, run grok-pull to get the latest updates.
7. At the start of downtime, block access to the origin server, so no
   pushes are allowed to go through. Run final grok-pull on the
   migration replica.
8. Back up your existing hierarchy, because you know you should, or move
   it out of the way if you have enough disk space for this.
9. Copy the new hierarchy from the migration replica (e.g. using rsync).
10. Run any necessary steps such as "gitolite setup" in order to set
    things up.
11. Rerun grok-manifest on the toplevel in order to generate the fresh
    manifest.js.gz file.
12. Create a new grokmirror.conf for fsck runs (grokmirror-1.x
    configuration files are purposefully not supported).
13. Enable the grok-fsck timer.

Upgrading the replicas
----------------------
The above procedure should also be considered for upgrading the
replicas, unless you have a small collection that doesn't use a lot of
forks and alternates. You can find out if that is the case by running
``find . -name alternates`` at the top of your mirrored tree. If the
number of returned hits is significant, then the first time grok-fsck
runs, it will spend a lot of time repacking the repositories to benefit
from the new layout. On the upside, you can expect significant storage
use reduction after this conversion is completed.

If your replica is providing continuous access for members of your
development team, then you may want to perform this conversion prior to
upgrading grokmirror on your production server, in order to reduce the
impact on server load. Just follow the instructions from the section
above.

Converting the configuration file
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Grokmirror-1.x used two different config files -- one for grok-pull and
another for grok-fsck. This separation only really made sense on the
origin server and was cumbersome for the replicas, since they ended up
duplicating a lot of configuration options between the two config files.

Grokmirror-1.x:
  - separate configuration files for grok-pull and grok-fsck
  - multiple origin servers can be listed in one file

Grokmirror-2.x:
  - one configuration file for all grokmirror tools
  - one origin server per configuration file

Grokmirror-2.x will refuse to run with configuration files created for
the previous version, so you will need to create a new configuration
file in order to continue using it after upgrading. Most configuration
options will be familiar to you from version 1.x, and the rest are
documented in the grokmirror.conf file provided with the distribution.

Converting from cron to daemon operation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Grokmirror-1.x expected grok-pull to run from cron, but this had a set
of important limitations. In contrast, grokmirror-2.x is written to run
grok-pull as a daemon. It is strongly recommended to switch away from
cron-based regular runs if you do them more frequently than once every
few hours, as this will result in more efficient operation. See the set
of systemd unit files included in the contrib directory for where to get
started.

Grok-fsck can continue to run from cron if you prefer, or you can run it
from a systemd timer as well.
