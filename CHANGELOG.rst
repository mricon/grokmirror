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
