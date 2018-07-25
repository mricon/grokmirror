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
