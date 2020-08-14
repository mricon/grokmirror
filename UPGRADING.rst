Upgrading from Grokmirror 1.x to 2.x
------------------------------------
Grokmirror-2.0 introduced major changes to how repositories are
organized, so it deliberately breaks the upgrade path in order to force
admins to make proper decisions.

Upgrading the origin server
---------------------------
Breaking changes affecting the origin server are related to grok-fsck
runs. Existing grok-manifest hooks should continue to work without any
changes required.

Grok-fsck will now automatically recognize related repositories by
comparing the output of ``git rev-list --max-parents=0 --all``. When two
or more repositories are recognized as forks of each-other, a new
"object storage" repository will be set up that will contain refs from
all siblings.  After that, individual repositories will be repacked to
only contain repository metadata (and loose objects in need of pruning).

Existing repositories that already use alternates will be automatically
migrated to objstore repositories during the first grok-fsck run,
however this process can take an extremely long time for large
repository collections, so performing this "live" on repositories that
are being continuously modified is NOT recommended.

This is the recommended upgrade scenario:

1. Set up a separate location for the new hierarchy. It can be on the
   same server or on a different system entirely.
2. Perform a grok-pull replication from the current hierarchy to the new
   location. This should set up all repositories currently using
   alternates as objstore repositories.
3. Once the initial replication is complete, run grok-fsck on the new
   hierarchy. This should properly repack all new object storage
   repositories to benefit from delta islands.
4. Run regular grok-pull to get the latest updates.
5. Schedule migration downtime.
6. Swap the new hierarchy with the old location, performing any
   necessary steps such as "gitolite setup".
7. Rerun grok-manifest to generate the fresh manifest.js.gz file.

Upgrading the replicas
----------------------
TBD.
