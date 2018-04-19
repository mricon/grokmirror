v1.0.2 (2018-04-18)
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

- fix grok-pull so it no longer purges repos known to be providing
  alternates to others
