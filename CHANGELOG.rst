v3.0 (TBD)
----------
- Require Python 3.9 or newer
- New ``grok-shallow-tar`` command, which publishes shallow single-branch
  repositories as tarballs so a CI swarm can pull them off a CDN instead
  of making the server build an identical ``--depth=1`` pack for every
  job. Each ``--branches REPOGLOB:BRANCHGLOB`` switch names repositories
  and the branches of them to publish; nothing is published unless a
  pattern asks for it. The tarball goes out under a dated name carrying
  the abbreviated branch tip, beside a JSON sidecar with the size and
  SHA-256 and a ``latest`` symlink, and the previous tarball is kept for
  one cycle so a node that read ``latest`` from one frontend can still
  fetch it from another. The clone inside is single-branch and tagless,
  and its origin is the public URL built from ``--clone-url-base``, so
  the node's first fetch asks about exactly one ref. Each run also
  sweeps up the scratch directories a killed run left behind. An
  unpacked tarball says what it is, in a ``.git/description`` recording
  where it came from and a ``.git/shallow-tar.readme`` covering what to
  run before trusting a tree that arrived over the network, and how to
  catch it up with ``git fetch --depth=1`` rather than ``git remote
  update``, which on a merge-heavy branch transfers most of the
  repository. ``--strip-prefix`` takes a shared leading directory off
  the published layout, so a set of repositories under
  ``/pub/scm/linux/kernel/git`` can go out as ``stable/``, ``torvalds/``
  and ``next/`` instead of five levels of path nobody reads. See grok-shallow-tar(1)
- New ``--config-check`` flag for grok-pull, grok-fsck and grok-manifest.
  It reads the config file, reports every problem it can find and exits
  without doing anything else: a misspelled option name, a value that is
  not the shape the option wants, an option left with nothing after the
  equals sign, a directory that is not writable by the user running the
  check, a hook that does not exist or is not executable, a glob list
  that matches nothing in the local manifest, and -- for grok-pull -- a
  manifest URL the remote site will not answer for. Each command answers
  only for the sections it reads, errors exit 1 and warnings exit 0,
  ``--no-network`` skips the checks that contact the remote site, and
  ``--json`` writes the same report as a JSON object
- grok-fsck now fingerprints every repository on every run and compares
  the result with the manifest, reporting the ones that disagree. A
  repository git cannot fingerprint at all -- a damaged ref file makes
  "git show-ref" fail for the whole repository -- is one grok-manifest
  has been skipping, leaving its manifest entry frozen and the
  repository silently unreplicated. One that fingerprints fine but does
  not match means grok-manifest or grok-pull is not getting through. A
  mismatch has to survive two runs before it is reported, since a
  replica is legitimately behind for a while after a push
- New ``[manifest]ignore_refs`` setting, the config-file equivalent of
  grok-manifest's ``--ignore-refs``, honoured by every command that
  fingerprints. Only grok-manifest used to know about ignored refs, so a
  replica of an origin that ignores any could never agree with the
  manifest: grok-pull refetched those repositories on every run, for
  ever, without anything having changed. Set it to the same value the
  origin uses
- grok-fsck now explains what "<ref>: invalid sha1 pointer 000...0" is
  about. Git reports every ref it cannot resolve that way, and the usual
  cause is a symbolic ref whose target has been deleted -- which git
  itself considers legal, so no version of git says anything more useful.
  The report now names the symref and the target it cannot find. Refs
  that are not symrefs keep their original error, since
  those really are broken ref files
- grok-fsck now repairs a stale commit-graph itself. A graph still
  listing commits that have since been pruned makes git fsck complain
  about every one of them, which used to fill the report with hundreds
  of lines (and, before the fix above, hundreds of reclone requests).
  The graph is now thrown away and rewritten, and only the errors that
  are not about the commit-graph are reported
- grok-fsck now writes every commit-graph with ``--reachable``. A
  repository that both borrows from and lends to others used to get a
  graph built from its packs, which with reflogs present went on listing
  the old side of a force-push. cgit's object reachability check trusts
  a repository's own graph to list only commits its refs reach
- New ``[fsck] changedpaths`` option, off by default, which makes
  grok-fsck write changed-path Bloom filters into commit-graphs. They
  make path-limited logs, such as cgit's ``log/<path>`` pages, several
  times faster on large repositories, at the cost of a much slower first
  graph write and a graph about a fifth larger. ``max_new_filters``
  spreads that first-write cost over several runs. Objstore repos never
  get filters, since forks never read their graphs. With git 2.46 or
  newer the filters are version 2
- grok-manifest now reads the log file location from ``[core]log``, the
  same setting every other command uses. It used to look for
  ``[core]logfile``, which nothing writes and the sample config has
  never documented, so running grok-manifest from a config file logged
  nowhere at all
- ``[fsck]commitgraph`` and ``[fsck]prune`` now accept every boolean
  spelling ConfigParser does (yes/no, true/false, on/off, 1/0). Both
  used to be compared against the literal string "yes" in some places
  and read as booleans in others, so "commitgraph = true" meant enabled
  in one place and disabled in two more: no graph was written, and the
  code that checks the graph then treated its absence as correct. A
  value that is neither true nor false is now reported by name instead
  of being silently taken as false
- Every boolean and whole-number option in the config file is now
  checked when the file is loaded, so a value that is neither true nor
  false, or that is not a number, is reported by name -- with its
  section and the value itself -- before the command does any work.
  These used to raise ValueError wherever the option happened to be
  read, which for several of them is inside a worker thread: a
  traceback in a cron mailbox rather than an answer. The whole file is
  checked whichever command is running, since all of them share it.
  grok-pi-piper checks its own ``shallow`` the same way
- The sample config now documents ``[fsck]obstrepo_merge_strategy``,
  which grok-fsck has always read but nothing has ever mentioned. It
  chooses how eagerly objstore repos holding the same history are merged
  into one: "exact" (the default), "loose" or "blobs"
- The reclone decision is now logged once per repository instead of once
  per matching error line
- grok-fsck no longer requests auto-reclones for repositories that have
  no mirror remote to reclone from. On an origin server there is no
  grok-pull to act on the request, so the marker file was never picked
  up and every report repeated the same "requested auto-reclone" line.
  Such repositories are now reported as needing manual attention
- grok-fsck now requires git 2.20 or newer and says so up front, instead
  of failing midway through the tree when an objstore repository needs
  delta islands the running git does not have. Git 2.41 or newer is
  recommended (geometric repacks and cruft packs); older versions log a
  note that the all-into-one repack strategy is in use
- Repacks of repositories that use alternates now pass
  --no-write-bitmap-index explicitly, since bitmaps cannot cover objects
  borrowed from an alternate. Previously git emitted "warning: disabling
  bitmap writing" on every such repack, and only the matching
  ignore_errors entry in the sample config kept it from being counted as
  a repack failure; that entry is no longer needed and has been removed
  from the sample config (existing configs keep working)
- Full repacks of standalone and child repositories now collect
  unreachable objects into a cruft pack (git repack --cruft) instead of
  exploding them into loose objects with -A. The grace period stays the
  same (--cruft-expiration=yesterday when pruning); with pruning off,
  the cruft pack keeps unreachable objects indefinitely, where the old
  standalone flags silently dropped packed unreachable objects.
  Repositories that provide alternates to others still never expire
  anything. Requires git 2.41 or newer; older git keeps the old flags
- Quick repacks are now geometric (git repack --geometric=2) when git is
  2.41 or newer. An all-into-one repack rewrote every pack on every
  nightly run, which on a large server means rewriting terabytes to move
  a few megabytes of new objects; a geometric repack only rolls up the
  small packs and never deletes anything. Bitmaps move into a
  multi-pack-index where more than one pack remains. Full repacks still
  consolidate everything into a single pack, and unreachable objects now
  only get expired by those, so expect somewhat higher disk usage between
  full repack cycles. With git older than 2.41 (the first version where
  geometric repacking of repositories borrowing from alternates has no
  known corner-case bugs) the old behavior is kept
- Full repacks no longer pass -f to git repack. Recomputing every delta
  from scratch is very expensive on large repositories and buys nothing
  for correctness -- delta islands are respected even for reused deltas.
  Add -f to extra_repack_flags_full if you want the old behavior back
- Stop forcing pack.compression=9 on objstore repositories. The
  strongest zlib level costs a lot of CPU on every repack of the
  largest repositories on a server and only wins a percent or two of
  pack size over the default. New objstore repositories are created
  without the setting, and grok-fsck removes the old forced value when
  it next repacks an existing one; a different compression level set on
  purpose is left alone
- Remove the grokmirror-1.x object storage migration from grok-fsck.
  Repositories whose alternates point at another regular repository are
  no longer converted into objstore repositories; they are left alone
  (and still repacked and fscked as usual) with a warning in the log.
  Upgrade via grokmirror-2.x if you still need that migration, and see
  UPGRADING.rst
- Switch to a pyproject.toml-based build (setup.py is gone; downstream
  packaging should use the PEP 517 interface)
- Add new hook post_work_complete_hook that fires after all work is
  complete and grokmirror goes idle
- Add new command grok-pi-indexer for indexing public-inbox mirrored
  repositories
- Read and write all text files (configs, logs, descriptions, manifests,
  state files) as UTF-8 regardless of the system locale, so behavior no
  longer changes under e.g. LC_ALL=C cron environments
- Report expected failures (bad config, unreachable manifest) as clean
  one-line errors raised through a new GrokError exception hierarchy
  instead of exiting from deep inside library code; a failed manifest
  fetch no longer kills grok-pull running in daemon mode, and Ctrl-C is
  no longer swallowed by bare except clauses
- Always release repository locks on failure paths: a failed bare-init
  during grok-pull could previously leave the repository locked for the
  life of the daemon, and a repository with no remotes left grok-dumb-pull
  holding its lock
- grok-pull now runs its workers as a thread pool instead of forked
  processes, and worker completion is reported through futures instead
  of polling queues; an unexpected error in a worker is now logged with
  its full traceback and counted as a failure, where it previously
  killed the worker silently and left the repository blocked from all
  further updates until the daemon was restarted
- A failed post-pull treatment (objstore fetch, repack) in grok-pull no
  longer kills the treatment worker; the error is logged and later
  treatments still run
- git commands that talk to a remote (grok-pull and grok-dumb-pull
  fetches, and the remote manifest_command) now run under a generous
  six-hour ceiling, so a worker stuck on a dead connection eventually
  recovers instead of hanging forever; hooks and indexing commands are
  deliberately not limited, since those can legitimately run for days
- Fix grok-fsck keeping the manifest locked for the rest of the run when
  the status file could not be parsed
- Fix grok-fsck treating a blank line in reclone_on_errors as "reclone on
  any error at all", and a blank line in ignore_errors as "ignore every
  error"; blank patterns are now dropped instead of matching everything
- Path containment checks (is a repo inside the objstore? does a symlink
  point outside the toplevel?) now compare paths instead of string
  prefixes, so e.g. /srv/objstore-private is no longer considered to be
  inside /srv/objstore, and a symlink target that merely mentions the
  toplevel somewhere in its path no longer passes as internal
- Only treat a manifest file or URL ending in .gz as gzipped, instead of
  one containing ".gz" anywhere in its path (a plain-text manifest under
  a directory like /srv/my.gz-mirrors/ used to crash the reader)
- Writing the manifest and projects.list no longer zeroes the process-wide
  umask while it works, which could give any file created by another
  thread at that moment mode 0666; the mode is also now computed
  correctly, where a umask denying execute (0077, 0027) previously
  produced a world-executable manifest
- The manifest and projects.list are now swapped into place with
  os.replace() rather than shutil.move(), which silently degrades to
  copy-then-delete and defeats the atomicity the temporary file exists for
- Manifest entries missing a key grokmirror normally writes no longer
  traceback: grok-manifest's symlink and purge handling, grok-fsck's
  forkgroup update and grok-pull's queueing all read them defensively now.
  grok-pull also no longer dies outright when an objstore migration reaches
  it without a forkgroup; it logs the repository as failed and carries on
- Config options that take shell globs (include, exclude, private, nopurge,
  ffonly, baselines, islandcores, ignore) are now compiled into a single
  regular expression instead of being matched pattern-by-pattern against
  every repository, which cuts a large amount of work out of grok-pull and
  grok-fsck runs over big manifests
- Fix grok-fsck dying with a ValueError when objstore_uses_plumbing is on
  and a sibling repository has lost every one of its refs; its stale refs
  in the objstore repo are now cleaned up as they should be
- Fix grok-fsck reading a "baselines" setting that is not configured as a
  single blank baseline, which made it run an extra "git remote -v" over
  every objstore candidate for no reason
- Fix grok-bundle mangling repository names that contain ".git" somewhere
  other than at the end: a repository named foo.github.io.git used to be
  bundled into a directory called "foohub.io"
- Locking a repository or the manifest a second time from the same process
  is now reported as an error instead of silently succeeding; fcntl locks
  cannot protect a process from itself, and re-locking the manifest used
  to quietly drop the lock the process believed it still held
- grok-pi-piper now asks git for NUL-terminated records when listing new
  messages, so a subject line containing an exotic line break (a vertical
  tab, a form feed, U+0085) can no longer be mistaken for the end of one
  record and the start of another
- Fix grok-pi-piper labeling its log entries as coming from grok-pull
- Fix grok-manifest traceback when purging repositories that no longer
  exist on disk (broken since v2.0.0)
- Fix grok-manifest not removing a repository from the manifest when it
  was replaced by a symlink; it remained listed as a real repository in
  addition to showing up in the target's symlinks
- Fix grok-manifest recording a "reference" harvested from the current
  working directory when a repository listed a forkgroup but no longer
  had alternates
- Fix grok-manifest traceback when the manifest path has no directory
  component (e.g. "-m manifest.js.gz")
- Fix grok-manifest failing on every repository with recent versions of
  git on Python older than 3.11: git renders a UTC committer date with a
  trailing "Z", which datetime.fromisoformat() did not accept until 3.11
- Fix grok-manifest reporting a nonsensical runtime if the system clock
  was adjusted mid-run
- Fix grok-fsck traceback when reporting repository sizes of 1 TiB and
  above
- Fix grok-fsck traceback on repositories excluded from object storage
  with the grokmirror.do-not-objstore setting
- Fix grok-fsck traceback when merging siblings and none of them had
  usable root commits
- Fix grok-fsck writing the literal string "None" as the fingerprint of a
  repository with no refs, which every reader then accepted as valid
- Fix grok-fsck dying with a traceback, and losing the error report
  entirely, when the mail host could not be reached; the report is now
  written to the log instead
- Fix grok-bundle traceback when passed an empty --gitargs or
  --revlistargs
- Fix grok-pull error message on remote manifest command failure, which
  itself raised an exception while reporting the error
- grok-pull now reports a clear error when the [remote] section does not
  define "site", or defines neither "manifest" nor "manifest_command",
  instead of failing with a traceback inside a worker
- Fix grok-pull traceback when the config file has no [pull] section; all
  of its settings are optional, so leaving it out now works
- Fix grok-pull burying remote manifest failures (missing manifest,
  failing or unparseable manifest_command) under a traceback instead of
  just reporting them and exiting non-zero
- Fix grok-pi-piper hanging instead of exiting when no pipe was
  configured
- Fix grok-pi-piper not actually checking that the configured pipe
  command is executable, reporting a confusing error when it was not
- Fix grok-pi-piper silently doing nothing and exiting 0 when the
  configuration file passed to -c does not exist; it now says so and
  exits non-zero
- Report a clear error instead of a traceback when the configuration file
  has no "toplevel" defined in the [core] section
- Interrupting any of the commands with Ctrl-C is now reliable; several
  code paths used bare except clauses that swallowed KeyboardInterrupt
- The contrib pubsub v1 listener works again, and now requires falcon
  3.0 or newer; it also reads the request body in a way that does not
  depend on WSGI server internals
- Development: the tree is checked with ruff, mypy, ty and pyright; run
  ./ci.sh before committing and ./ci-matrix.sh before releases

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
