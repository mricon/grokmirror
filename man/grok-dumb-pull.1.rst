GROK-DUMB-PULL
==============
-------------------------------------------------
Update git repositories not managed by grokmirror
-------------------------------------------------

:Author:    mricon@kernel.org
:Date:      2013-08-22
:Copyright: The Linux Foundation and contributors
:License:   GPLv3+
:Version:   0.4
:Manual section: 1

SYNOPSIS
--------
    grok-dumb-pull [options] /path/to/repos

DESCRIPTION
-----------
This is a satellite utility that updates repositories not exported via
grokmirror manifest. You will need to manually clone these repositories
using "git clone --mirror" and then define a cronjob to update them as
frequently as you require. Grok-dumb-pull will bluntly execute "git
remote update" in each of them.


OPTIONS
-------
  --version             show program's version number and exit
  -h, --help            show this help message and exit
  -v, --verbose         Be verbose and tell us what you are doing
  -s, --svn             The remotes for these repositories are Subversion
  -r REMOTES, --remote-names=REMOTES
                        Only fetch remotes matching this name (accepts globbing,
                        can be passed multiple times)
  -u POSTHOOK, --post-update-hook=POSTHOOK
                        Run this hook after each repository is updated. Passes
                        full path to the repository as the sole argument.
  -l LOGFILE, --logfile=LOGFILE
                        Put debug logs into this file

EXAMPLES
--------
The following will update all bare git repositories found in
/path/to/repos hourly, and /path/to/special/repo.git daily, fetching
only the "github" remote::

    MAILTO=root
    # Update all repositories found in /path/to/repos hourly
    0 * * * * mirror /usr/bin/grok-dumb-pull /path/to/repos
    # Update /path/to/special/repo.git daily, fetching "github" remote
    0 0 * * * mirror /usr/bin/grok-dumb-pull -r github /path/to/special/repo.git

Make sure the user "mirror" (or whichever user you specified) is able to
write to the repos specified.

SEE ALSO
--------
  * grok-pull(1)
  * grok-manifest(1)
  * grok-fsck(1)
  * git(1)

SUPPORT
-------
Please send support requests to the mailing list::

    http://lists.kernel.org/mailman/listinfo/grokmirror
