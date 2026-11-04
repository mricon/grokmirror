# SPDX-License-Identifier: GPL-3.0-or-later
"""grok-fsck repacks, fscks and repairs repositories, and runs objstore migration.

The objstore part is where the interesting failures are. Repositories that share
a root commit are treated as forks of each other and get migrated into a single
objstore repository, which they then use through git alternates. Anything that
opts out of that -- a private repository, one carrying the
grokmirror.do-not-objstore marker, one that was pulled back out by hand -- takes
a path that has historically been much less travelled.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import grokmirror
import grokmirror.fsck
from grokmirror.fsck import refresh_obst_roots

from support import GrokTree, bloom_filters, git

pytestmark = pytest.mark.slow


def test_do_not_objstore_repo_is_left_alone(tree: GrokTree) -> None:
    # A repository with the marker and no alternates fell through into branches
    # that all assume altdir is set, reaching os.path.isdir(None) -> TypeError.
    # The whole fsck run died, so no repository got checked that night.
    tree.add_repo('test/one.git')
    tree.add_repo('test/fork.git')
    excluded = tree.add_repo('test/excluded.git')
    (excluded / 'grokmirror.do-not-objstore').touch()
    tree.run_manifest()
    tree.write_config()

    tree.run_fsck('-f')

    assert tree.alternates('test/excluded.git') is None
    # The other two are unaffected and still share an objstore repository.
    assert len(tree.objstore_repos()) == 1
    assert tree.alternates('test/one.git') == tree.alternates('test/fork.git')


def test_do_not_objstore_repo_with_a_single_sibling(tree: GrokTree) -> None:
    # The marked repository is the *only* other repo sharing these roots, so it
    # is also the only sibling candidate. Nothing should get migrated at all.
    tree.add_repo('test/one.git')
    excluded = tree.add_repo('test/excluded.git')
    (excluded / 'grokmirror.do-not-objstore').touch()
    tree.run_manifest()
    tree.write_config()

    tree.run_fsck('-f')

    assert tree.alternates('test/excluded.git') is None


def test_do_not_objstore_repo_with_dangling_alternates(tree: GrokTree) -> None:
    # The marker only short-circuits when there are no alternates. If the
    # alternates file points at something that is gone, fsck has to cope with
    # that too, rather than crashing on a path that does not exist.
    tree.add_repo('test/one.git')
    tree.add_repo('test/fork.git')
    excluded = tree.add_repo('test/excluded.git')
    git('remote', 'add', '--mirror=fetch', '_grokmirror', 'https://example.invalid/excluded.git', cwd=excluded)
    (excluded / 'grokmirror.do-not-objstore').touch()
    altfile = excluded / 'objects' / 'info' / 'alternates'
    altfile.write_text(str(tree.objstore / 'nosuchrepo.git' / 'objects') + '\n')
    tree.run_manifest()
    tree.write_config()

    res = tree.run_fsck('-f')

    # The repository is scheduled for a reclone, which is the honest answer: its
    # objects are gone and only the origin can supply them again.
    assert 'reclone: /test/excluded.git' in res.stdout + res.stderr
    assert (excluded / 'grokmirror.reclone').exists()
    # The other two are unaffected.
    assert len(tree.objstore_repos()) == 1


def test_dangling_alternates_on_an_origin_are_not_recloned(tree: GrokTree) -> None:
    # Same breakage, but on a server that has no grok-pull: the repository has
    # no mirror remote, so there is nothing to reclone from. Say so plainly
    # instead of leaving a marker file nobody will ever act on.
    tree.add_repo('test/one.git')
    tree.add_repo('test/fork.git')
    excluded = tree.add_repo('test/excluded.git')
    (excluded / 'grokmirror.do-not-objstore').touch()
    altfile = excluded / 'objects' / 'info' / 'alternates'
    altfile.write_text(str(tree.objstore / 'nosuchrepo.git' / 'objects') + '\n')
    tree.run_manifest()
    tree.write_config()

    res = tree.run_fsck('-f')

    out = res.stdout + res.stderr
    assert 'BROKEN: /test/excluded.git' in out
    assert 'needs manual attention' in out
    assert 'reclone' not in out
    assert not (excluded / 'grokmirror.reclone').exists()


def stale_commit_graph(repo: Path) -> str:
    """Leave `repo` with a commit-graph listing a commit that no longer exists.

    This is the ordinary end of an unreachable commit: it was in the graph when
    the graph was written, then it lost its ref and got pruned. Returns the id
    of the commit that went away.
    """
    head = git('rev-parse', 'HEAD', cwd=repo).strip()
    treeish = git('rev-parse', 'HEAD^{tree}', cwd=repo).strip()
    doomed = git('commit-tree', treeish, '-p', head, '-m', 'doomed', cwd=repo).strip()
    git('update-ref', 'refs/heads/doomed', doomed, cwd=repo)
    git('commit-graph', 'write', '--reachable', cwd=repo)
    git('update-ref', '-d', 'refs/heads/doomed', cwd=repo)
    git('prune', '--expire=now', cwd=repo)
    git('config', 'core.commitGraph', 'true', cwd=repo)
    return doomed


@pytest.mark.parametrize('changedpaths', ['no', 'yes'])
def test_stale_commit_graph_is_rebuilt_instead_of_reported(tree: GrokTree, changedpaths: str) -> None:
    # git fsck verifies the commit-graph, and a graph that has fallen behind the
    # object database makes it shout about every commit that is gone. Nothing is
    # damaged and nobody needs to look at it: rebuild the graph and move on.
    # The rebuild starts from scratch, so it has to ask for filters itself
    # rather than inherit them from the graph it replaces.
    repo = tree.add_repo('test/one.git')
    doomed = stale_commit_graph(repo)
    tree.run_manifest()
    tree.write_config({'fsck': {'changedpaths': changedpaths}})

    res = tree.run_fsck('-f')

    out = res.stdout + res.stderr
    assert 'commit-graph is out of date, rebuilding' in tree.log_text()
    assert 'reports errors' not in out
    assert doomed not in out
    assert not (repo / 'grokmirror.fsck.err').exists()
    # The graph is back, and now agrees with the object database.
    assert (repo / 'objects' / 'info' / 'commit-graph').exists()
    assert git('fsck', '--no-progress', '--no-dangling', '--no-reflogs', cwd=repo, check=False) == ''
    assert (bloom_filters(repo) is not None) == (changedpaths == 'yes')


def test_stale_commit_graph_is_removed_when_graphs_are_disabled(tree: GrokTree) -> None:
    # With commitgraph = no the correct end state is no graph at all, rather
    # than a stale one nobody will ever rewrite.
    repo = tree.add_repo('test/one.git')
    stale_commit_graph(repo)
    tree.run_manifest()
    tree.write_config({'fsck': {'commitgraph': 'no'}})

    res = tree.run_fsck('-f')

    assert 'reports errors' not in res.stdout + res.stderr
    assert not (repo / 'objects' / 'info' / 'commit-graph').exists()


def test_dangling_symref_is_explained_in_the_report(tree: GrokTree) -> None:
    # A symbolic ref whose target was deleted is legal as far as git is
    # concerned (an unborn branch looks the same), but git fsck still lists it
    # with the null object id and calls it an invalid sha1 pointer. Say what it
    # really is, since only the repository owner can clean it up.
    repo = tree.add_repo('test/one.git')
    git('symbolic-ref', 'refs/heads/for-next', 'refs/heads/pending', cwd=repo)
    tree.run_manifest()
    tree.write_config()

    res = tree.run_fsck('-f')

    out = res.stdout + res.stderr
    assert 'refs/heads/for-next: symbolic ref pointing at refs/heads/pending, which does not exist' in out
    assert 'invalid sha1 pointer' not in out
    # We only explain it -- removing somebody's ref is not ours to do.
    assert (repo / 'refs' / 'heads' / 'for-next').exists()


def test_null_oid_in_a_ref_file_keeps_its_error(tree: GrokTree) -> None:
    # The same fsck line with no symref behind it means the ref file itself is
    # broken, which is real local damage and has to stay in the report.
    #
    # run_git_fsck() is called directly here because `git show-ref` refuses to
    # list refs at all in this state, so the repository has no fingerprint and
    # never makes it into the manifest to be scheduled in the first place.
    repo = tree.add_repo('test/one.git')
    (repo / 'refs' / 'heads' / 'broken').write_text('0' * 40 + '\n')
    config = grokmirror.load_config_file(tree.write_config())

    grokmirror.fsck.run_git_fsck(grokmirror.GrokSession(), str(repo), config)

    reported = (repo / 'grokmirror.fsck.err').read_text()
    assert 'refs/heads/broken: invalid sha1 pointer' in reported
    assert 'symbolic ref pointing at' not in reported


def test_private_repo_contributes_no_objects(tree: GrokTree) -> None:
    # A private repository may still *use* the shared objstore -- that only lets
    # it avoid storing objects it can get from its siblings -- but none of its own
    # objects may end up there, so it is never added as a remote.
    tree.add_repo('test/one.git')
    tree.add_repo('test/fork.git')
    tree.add_repo('test/secret.git')
    tree.run_manifest()
    tree.write_config({'core': {'private': '/test/secret.git'}})

    tree.run_fsck('-f')

    assert tree.alternates('test/one.git') == tree.alternates('test/fork.git')
    (obstrepo,) = tree.objstore_repos()
    remotes = git('remote', cwd=obstrepo).split()
    assert len(remotes) == 2
    for remote in remotes:
        assert 'secret' not in git('remote', 'get-url', remote, cwd=obstrepo)


def test_no_fingerprint_is_recorded_for_a_repo_with_no_refs(tree: GrokTree) -> None:
    # get_repo_fingerprint() returns None for a repository with no refs, and that
    # None used to be written out as the literal string "None". Every reader then
    # accepted it as a valid fingerprint, because a non-empty string is truthy,
    # so the repository looked up to date forever.
    empty = tree.add_empty_repo('test/empty.git')

    assert grokmirror.set_repo_fingerprint(str(tree.toplevel), '/test/empty.git') is None
    assert not (empty / 'grokmirror.fingerprint').exists()


def test_fingerprint_is_recorded_once_there_are_refs(tree: GrokTree) -> None:
    repo = tree.add_repo('test/one.git')
    fpr = grokmirror.set_repo_fingerprint(str(tree.toplevel), '/test/one.git')

    assert fpr
    assert (repo / 'grokmirror.fingerprint').read_text() == fpr


def test_status_file_records_every_repo(tree: GrokTree) -> None:
    tree.add_repo('test/one.git')
    tree.add_repo('test/two.git', source='beta')
    tree.run_manifest()
    tree.write_config()
    tree.run_fsck('-f')

    # The status file is keyed by full path, not by the manifest's gitdir.
    status = json.loads(tree.statusfile.read_text())
    for gitdir in ('test/one.git', 'test/two.git'):
        entry = status[str(tree.path(gitdir))]
        assert entry['lastcheck']
        assert entry['nextcheck']
        assert entry['fingerprint'] == tree.read_manifest()[f'/{gitdir}']['fingerprint']


def test_second_run_checks_nothing_without_force(tree: GrokTree) -> None:
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()
    tree.run_fsck('-f')

    res = tree.run_fsck('-v')

    # Everything was just checked, so the next check is in the future.
    assert 'No repos need attention' in res.stdout + res.stderr


def test_emptied_objstore_repo_is_not_offered_as_a_sibling(tree: GrokTree) -> None:
    # refresh_obst_roots() drops an objstore repo from the roots map when it has
    # no roots left, instead of caching an empty entry. Every consumer skips
    # rootless repos anyway, so a cached empty entry meant an objstore repo whose
    # contents had just been migrated elsewhere could still be picked as a
    # sibling target.
    obst_roots: dict[str, set[str]] = {}
    obstrepo = str(tree.add_empty_repo('unused.git'))
    obst_roots[obstrepo] = {'0' * 40}

    assert not refresh_obst_roots(obst_roots, obstrepo)
    assert obstrepo not in obst_roots


def test_objstore_repo_roots_are_cached_when_present(tree: GrokTree) -> None:
    repo = str(tree.add_repo('test/one.git'))
    obst_roots: dict[str, set[str]] = {}
    roots = refresh_obst_roots(obst_roots, repo)

    assert roots
    assert obst_roots[repo] == roots


def test_unreachable_mailhost_does_not_lose_the_report(tree: GrokTree) -> None:
    # Anything logged at CRITICAL level gets mailed as a report, and the mail was
    # sent without a net: on a host with no local MTA, grok-fsck died with a
    # traceback after doing all the work, and the report went nowhere.
    repo = tree.add_repo('test/one.git')
    altfile = repo / 'objects' / 'info' / 'alternates'
    altfile.write_text(str(tree.objstore / 'nosuchrepo.git' / 'objects') + '\n')
    tree.run_manifest()
    tree.write_config()

    res = tree.run_fsck('-f')

    output = res.stdout + res.stderr
    assert 'Could not send the report' in output
    assert 'Report follows' in output


def test_plumbing_fetch_survives_a_sibling_that_lost_every_ref(tree: GrokTree) -> None:
    # With objstore_uses_plumbing, the refs on both sides are compared as sets of
    # "<objectname> <refname>" lines. A sibling whose refs have all gone away
    # gives git nothing to print, and split('\n') turned that empty output into a
    # set holding one empty string -- which then failed to unpack into obj/ref and
    # took down the whole fsck run with a ValueError.
    repo = tree.add_repo('test/one.git')
    obstrepo = grokmirror.setup_objstore_repo(str(tree.objstore))
    virtref = grokmirror.objstore_virtref(str(repo))
    grokmirror.add_repo_to_objstore(obstrepo, str(repo))

    assert grokmirror.fetch_objstore_repo(obstrepo, str(repo), use_plumbing=True)
    assert git('for-each-ref', '--format=%(refname)', f'refs/virtual/{virtref}', cwd=obstrepo)

    for refname in git('for-each-ref', '--format=%(refname)', cwd=repo).splitlines():
        git('update-ref', '-d', refname, cwd=repo)

    assert grokmirror.fetch_objstore_repo(obstrepo, str(repo), use_plumbing=True)

    # The stale virtual refs are gone, not left behind pointing at nothing.
    assert not git('for-each-ref', '--format=%(refname)', f'refs/virtual/{virtref}', cwd=obstrepo)


def test_pre_objstore_alternates_are_left_alone(tree: GrokTree) -> None:
    # Grokmirror-1.x had a fork borrow objects straight from another toplevel
    # repository, and grok-fsck used to convert that arrangement into a real
    # objstore repo on its first run. Grokmirror-3 dropped the migration, so
    # the arrangement now has to survive untouched -- a half-converted repo is
    # much worse than an old-fashioned one -- and say so out loud.
    mommy = tree.add_repo('test/mommy.git')
    child = tree.add_repo('test/child.git')
    borrowed = str(mommy / 'objects')
    (child / 'objects' / 'info' / 'alternates').write_text(f'{borrowed}\n')
    tree.run_manifest()
    tree.write_config()

    head = git('rev-parse', 'HEAD', cwd=child).strip()

    tree.run_fsck('-f')

    assert tree.alternates('test/child.git') == borrowed
    assert 'not an objstore repo' in tree.log_text()
    # Mommy is still lending objects to child, so nothing may prune her.
    assert git('cat-file', '-e', head, cwd=mommy) == ''


# -- the repack/fsck scheduling decision --------------------------------------
#
# Everything below characterizes the densest logic in fsck_mirror(): the chain
# that turns config, command-line flags, the object count, the fingerprint and
# the check schedule into "repack at level N", "fsck" or "leave it alone". It
# is all observable through the queued: lines and the status file, and it was
# reachable only as a side effect of other tests before.


def queued(tree: GrokTree, *args: str) -> list[str]:
    """Run grok-fsck verbosely and return the reason from each decision it logged.

    Just the reason in the trailing parentheses, not the whole line: the line
    also carries the repository's full path, and pytest names its tmpdir after
    the running test, so "repack" appears in the path of every test named after
    a repack and matching whole lines silently never fails.
    """
    res = tree.run_fsck('-v', *args)
    out = res.stdout + res.stderr
    return [line.rsplit('(', 1)[-1].rstrip(')') for line in out.splitlines() if 'queued:' in line or 'aged:' in line]


def patch_status(tree: GrokTree, gitdir: str, **fields: str) -> None:
    """Rewrite fields of a repository's status entry, to place it in time."""
    status = json.loads(tree.statusfile.read_text())
    status[str(tree.path(gitdir))].update(fields)
    tree.statusfile.write_text(json.dumps(status))


def test_repack_no_queues_an_fsck_but_never_a_repack(tree: GrokTree) -> None:
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': {'repack': 'no'}})

    decisions = queued(tree, '-f')

    assert decisions == ['fsck']


def test_repack_only_never_queues_an_fsck(tree: GrokTree) -> None:
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()

    decisions = queued(tree, '-f', '--repack-only')

    assert 'fsck' not in decisions


def test_repack_all_full_queues_a_full_repack(tree: GrokTree) -> None:
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()

    decisions = queued(tree, '--repack-all-full')

    assert decisions == ['full repack']


def test_repack_all_quick_queues_a_plain_repack(tree: GrokTree) -> None:
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()

    decisions = queued(tree, '--repack-all-quick')

    assert decisions == ['repack']


def test_unchanged_fingerprint_queues_no_repack(tree: GrokTree) -> None:
    # --force still queues the fsck, but the repo has not moved since the last
    # run, so there is nothing to repack.
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()
    tree.run_fsck('-f')

    decisions = queued(tree, '-f')

    assert decisions == ['fsck']


def test_aged_repo_with_a_changed_fingerprint_is_repacked(tree: GrokTree) -> None:
    # Due for its periodic check and no longer matching its recorded
    # fingerprint: that combination forces a level-1 repack even though the
    # object counts alone would not have asked for one, and it moves the
    # repository's next check out into the future.
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()
    tree.run_fsck('-f')

    patch_status(tree, 'test/one.git', nextcheck='2000-01-01', fingerprint='0' * 40)

    decisions = queued(tree)

    assert 'forcing repack' in decisions
    after = json.loads(tree.statusfile.read_text())[str(tree.path('test/one.git'))]
    assert after['nextcheck'] > '2000-01-01'


def test_precious_repo_is_not_repacked_outside_its_schedule(tree: GrokTree) -> None:
    # With precious=always, a preciousObjects repository is repacked on the fsck
    # schedule rather than whenever its object counts drift, because repacking
    # one is the risky operation the setting exists to hold back.
    repo = tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': {'precious': 'always'}})
    tree.run_fsck('-f')

    # Fresh loose objects, which would normally be worth a quick repack...
    src = tree.source()
    src.commit()
    src.push(repo)
    grokmirror.set_git_config(str(repo), 'extensions.preciousObjects', 'true')
    patch_status(tree, 'test/one.git', nextcheck='2099-01-01')

    # ...but this repo is not due, so it is left entirely alone.
    assert queued(tree, '--repack-all-quick') == []


def test_precious_repo_gets_a_full_repack_when_due(tree: GrokTree) -> None:
    # Once its fsck check comes around, the same repository is repacked -- and
    # at level 2, not the level 1 that was asked for, since this is its one
    # scheduled opportunity. The check is then pushed back into the future.
    repo = tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': {'precious': 'always'}})
    tree.run_fsck('-f')

    src = tree.source()
    src.commit()
    src.push(repo)
    grokmirror.set_git_config(str(repo), 'extensions.preciousObjects', 'true')
    patch_status(tree, 'test/one.git', nextcheck='2000-01-01')

    assert queued(tree, '--repack-all-quick') == ['full repack']
    after = json.loads(tree.statusfile.read_text())[str(tree.path('test/one.git'))]
    assert after['nextcheck'] > '2000-01-01'


def test_repository_that_cannot_be_fingerprinted_is_reported(tree: GrokTree) -> None:
    # git will not list any refs when it cannot parse one of them, so
    # grok-manifest leaves the entry frozen at whatever it last said and
    # replicas never hear about this repository again. Nothing says so today.
    repo = tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()
    tree.run_fsck('-f')
    Path(repo, 'refs', 'heads', 'broken').write_text('0' * 40 + '\n')

    res = tree.run_fsck('-f')

    out = res.stdout + res.stderr
    assert 'Repositories that git cannot fingerprint' in out
    assert 'refs/heads/broken' in out
    assert 'frozen' in out


def test_stale_manifest_entry_is_reported_on_the_second_run(tree: GrokTree) -> None:
    # A replica is behind the manifest for a moment after every push, so one
    # run seeing a mismatch is not worth an email. Two in a row is.
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()
    tree.run_fsck('-f')

    manifest = tree.read_manifest()
    manifest['/test/one.git']['fingerprint'] = 'f' * 40
    tree.write_manifest(manifest)

    res = tree.run_fsck('-f')
    assert 'do not match the manifest' not in res.stdout + res.stderr
    status = json.loads(tree.statusfile.read_text())
    assert status[str(tree.toplevel / 'test/one.git')]['fp_mismatch'] == 'f' * 40

    res = tree.run_fsck('-f')

    out = res.stdout + res.stderr
    assert 'Repositories that do not match the manifest' in out
    assert 'f' * 40 in out


def test_matching_repository_is_not_reported(tree: GrokTree) -> None:
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config()

    tree.run_fsck('-f')
    res = tree.run_fsck('-f')

    out = res.stdout + res.stderr
    assert 'manifest' not in out.lower()


def test_ignored_refs_are_left_out_of_the_manifest_comparison(tree: GrokTree) -> None:
    # The origin fingerprints without refs/meta/*; a replica that counts them
    # in would report every repository having one as out of date, forever.
    repo = tree.add_repo('test/one.git')
    git('update-ref', 'refs/meta/config', 'refs/heads/master', cwd=repo)
    tree.write_config(sections={'manifest': {'ignore_refs': 'refs/meta/*'}})
    tree.run_manifest('-c', str(tree.cfgfile))

    tree.run_fsck('-f')
    res = tree.run_fsck('-f')

    assert 'do not match the manifest' not in res.stdout + res.stderr
