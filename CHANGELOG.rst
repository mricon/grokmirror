v2.0.9 (2021-07-13)
-------------------
- Add initial support for post_clone_complete_hook that fires only after
  all new clones have been completed.
- Fix grok-manifest traceback due to unicode errors in the repo
  description file.
- Minor code cleanups.

v2.0.8 (2021-03-11)
-------------------
- Fixes around symlink handling in manifest files. Adding and deleting
  symlinks should properly work again.
- Don't require [fsck] section in the config file (though you'd almost
  always want it there).

v2.0.7 (2021-01-19)
-------------------
- A slew of small fixes improving performance on very large repository
  collections (CAF internally is 32,500).

v2.0.6 (2021-01-07)
-------------------
- Use fsck.extra_repack_flags when doing quick post-clone repacks
- Store objects in objstore after grok-dumb-pull call on a repo that uses
  objstore repositories

v2.0.5 (2020-11-25)
-------------------
- Prioritize baseline repositories when finding related objstore repos.
- Minor fixes.

v2.0.4 (2020-11-06)
-------------------
- Add support to use git plumbing for objstore operations, via enabling
  core.objstore_uses_plumbing. This allows to significantly speed up
  fetching objects into objstore during pull operations. Fsck operations
  will continue to use porcelain "git fetch", since speed is less important
  in those cases and it's best to opt for maximum safety. As a benchmark,
  with remote.preload_bundle_url and core.objstore_uses_plumbing settings
  enabled, cloning a full replica of git.kernel.org takes less than an hour
  as opposed to over a day.

v2.0.3 (2020-11-04)
-------------------
- Refuse to delete ffonly repos
- Add new experimental bundle_preload feature for generating objstore
  repo bundles and using them to preload objstores on the mirrors

v2.0.2 (2020-10-06)
-------------------
- Provide pi-piper utility for piping new messages from public-inbox
  repositories. It can be specified as post_update_hook:
  post_update_hook = /usr/bin/grok-pi-piper -c ~/.config/pi-piper.conf
- Add -r option to grok-manifest to ignore specific refs when calculating
  repository fingerprint. This is mostly useful for mirroring from gerrit.

v2.0.1 (2020-09-30)
-------------------
- fix potential corruption when migrating repositories with existing
  alternates to new object storage format
- improve grok-fsck console output to be less misleading for large repo
  collections (was misreporting obstrepo/total repo numbers)
- use a faster repo search algorithm that doesn't needlessly recurse
  into git repos themselves, once found


v2.0.0 (2020-09-21)
-------------------
Major rewrite to improve shared object storage and replication for VERY
LARGE repository collections (codeaurora.org is ~30,000 repositories,
which are mostly various forks of Android).

See UPGRADING.rst for the upgrade strategy.

Below are some major highlights.

- Drop support for python < 3.6
- Introduce "object storage" repositories that benefit from git-pack
  delta islands and improve overall disk storage footprint (depending on
  the number of forks).
- Drop dependency on GitPython, use git calls directly for all operations
- Remove progress bars to slim down dependencies (drops enlighten)
- Make grok-pull operate in daemon mode (with -o) (see contrib for
  systemd unit files). This is more efficient than the cron mode when
  run very frequently.
- Provide a socket listener for pubsub push updates (see contrib for
  Google pubsubv1.py).
- Merge fsck.conf and repos.conf into a single config file. This
  requires creating a new configuration file after the upgrade. See
  UPGRADING.rst for details.
- Record and propagate HEAD position using the manifest file.
- Add grok-bundle command to create clone.bundle files for CDN-offloaded
  cloning (mostly used by Android's repo command).
- Add SELinux policy for EL7 (see contrib).


v1.2.2 (2019-10-23)
-------------------
- Small bugfixes
- Generate commit-graph file if the version of git is new
  enough to support it. This is done during grok-fsck any time we
  decide that the repository needs to be repacked. You can force
  this off by setting commitgraph=never in config.


v1.2.1 (2019-03-11)
-------------------
- Minor feature improvement changing how precious=yes works.
  Grokmirror will now turn preciousObjects off for the duration
  of the repack. We still protect shared repositories against
  inadvertent object pruning by outside processes, but this
  allows us to clean up loose objects and obsolete packs.
  To have the 1.2.0 behaviour back, set precious=always, but it
  is only really useful in very rare cases.


v1.2.0 (2019-02-14)
-------------------
- Make sure to set gc.auto=0 on repositories to avoid pruning repos
  that are acting as alternates to others. We run our own prune
  during fsck, so there is no need to auto-gc, ever (unless you
  didn't set up grok-fsck, in which case you're not doing it right).
- Rework the repack code to be more clever -- instead of repacking
  based purely on dates, we now track the number of loose objects
  and the number of generated packs. Many of the settings are
  hardcoded for the moment while testing, but will probably end up
  settable via global and per-repository config settings.
- The following fsck.conf settings have no further effect:
    - repack_flags (replaced with extra_repack_flags)
    - full_repack_flags (replaced with extra_repack_flags_full)
    - full_repack_every (we now figure it out ourselves)
- Move git command invocation routines into a central function to
  reduce the amount of code duplication. You can also set the path
  to the git binary using the GITBIN env variable or by simply
  adding it to your path.
- Add "reclone_on_errors" setting in fsck.conf. If fsck/repack/prune
  comes across a matching error, it will mark the repository for
  recloning and it will be cloned anew from the master the next time
  grok-pull runs. This is useful for auto-correcting corruption on the
  mirrors. You can also manually request a reclone by creating a
  "grokmirror.reclone" file in a repository.
- Set extensions.preciousObjects for repositories used with git
  alternates if precious=yes is set in fsck.conf. This helps further
  protect shared repos from erroneous pruning (e.g. done manually by
  an administrator).


v1.1.1 (2018-07-25)
-------------------
- Quickfix a bug that was causing repositories to never be repacked
  due to miscalculated fingerprints.


v1.1.0 (2018-04-24)
-------------------
- Make Python3 compatible (thanks to QuLogic for most of the work)
- Rework grok-fsck to improve functionality:

  - run repack and prune before fsck, for optimal safety
  - add --connectivity flag to run fsck with --connectivity-only
  - add --repack-all-quick to trigger a quick repack of all repos
  - add --repack-all-full to trigger a full repack of all repositories
    using the defined full_repack_flags from fsck.conf
  - always run fsck with --no-dangling, because mirror admins are not
    responsible for cleaning those up anyway
  - no longer locking repos when running repack/prune/fsck, because
    these operations are safe as long as they are done by git itself

- fix grok-pull so it no longer purges repos that are providing
  alternates to others
- fix grok-fsck so it's more paranoid when pruning repos providing
  alternates to others (checks all repos on disk, not just manifest)
- in verbose mode, most commands will draw progress bars (handy with
  very large connections of repositories)
