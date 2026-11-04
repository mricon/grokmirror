# SPDX-License-Identifier: GPL-3.0-or-later
"""Unit tests for the parts of grokmirror that need no repository.

These are all cheap, so they cover the awkward inputs too: the sizes nobody has
on disk yet, the relative paths nobody passes on purpose, the empty strings that
turn up when a config option is present but blank. Several of the crashes fixed
in this tree were exactly that kind of input.
"""

from __future__ import annotations

import errno
import fnmatch
import gzip
import json
import os
import pathlib
import pickle
import stat
import subprocess
import threading
from configparser import ExtendedInterpolation
from pathlib import Path
from typing import IO, ClassVar

import pytest
from packaging.version import Version

import grokmirror
import grokmirror.fsck
import grokmirror.pull

from support import GrokTree

KIB = 1
MIB = 1024
GIB = 1024 * 1024
TIB = 1024 * 1024 * 1024


class TestHumanSize:
    """get_human_size() takes a size in KiB and formats it for the report."""

    @pytest.mark.parametrize(
        ('kbsize', 'expected'),
        [
            (0, '0.00 KiB'),
            (1, '1.00 KiB'),
            (1023, '1023.00 KiB'),
            (MIB, '1.00 MiB'),
            (MIB * 1023, '1023.00 MiB'),
            (GIB, '1.00 GiB'),
            (GIB * 1023, '1023.00 GiB'),
            # 1 TiB and up used to raise IndexError: the unit list ran out.
            # Thankfully few git repositories are this big, which is why nobody
            # hit it -- at git.kernel.org the whole collection is around 50GB,
            # thanks to object sharing.
            (TIB, '1.00 TiB'),
            (TIB * 5, '5.00 TiB'),
            (TIB * 1024, '1024.00 TiB'),
            (TIB * 1024 * 1024, '1048576.00 TiB'),
        ],
    )
    def test_boundaries(self, kbsize: int, expected: str) -> None:
        assert grokmirror.fsck.get_human_size(kbsize) == expected

    def test_no_size_raises(self) -> None:
        # Every size the report can be handed must format, without exception.
        for exponent in range(64):
            assert grokmirror.fsck.get_human_size(2**exponent)


class TestLockName:
    """Lock files live next to the file they protect."""

    def test_absolute_path(self, tmp_path: Path) -> None:
        target = tmp_path / 'sub' / 'manifest.js.gz'
        assert grokmirror._lockname(str(target)) == tmp_path / 'sub' / '.manifest.js.gz.lock'
        # The containing directory gets created, since the manifest may not
        # exist yet on a mirror's first run.
        assert (tmp_path / 'sub').is_dir()

    def test_bare_filename(self) -> None:
        # os.path.dirname('manifest.js.gz') is '', and makedirs('') raises
        # FileNotFoundError instead of being a no-op, so `grok-manifest -m
        # manifest.js.gz` used to die before doing any work. A cron job running
        # from the manifest's own directory is a perfectly plausible setup.
        assert grokmirror._lockname('manifest.js.gz') == Path('.manifest.js.gz.lock')

    def test_manifest_lock_round_trip_with_a_relative_name(self, tmp_path: Path) -> None:
        os.chdir(tmp_path)
        grokmirror.manifest_lock('manifest.js.gz')
        try:
            assert (tmp_path / '.manifest.js.gz.lock').exists()
        finally:
            grokmirror.manifest_unlock('manifest.js.gz')
        assert grokmirror.MANIFEST_LOCKH is None

    def test_repo_lock_round_trip(self, tmp_path: Path) -> None:
        target = str(tmp_path / 'repo.git')
        grokmirror.lock_repo(target)
        try:
            assert target in grokmirror.REPO_LOCKH
        finally:
            grokmirror.unlock_repo(target)
        assert target not in grokmirror.REPO_LOCKH

    def test_repo_lock_is_exclusive(self, tmp_path: Path) -> None:
        target = str(tmp_path / 'repo.git')
        grokmirror.lock_repo(target)
        try:
            # A second holder must not be able to take it. This is what keeps two
            # grok-fsck runs out of the same repository.
            code = (
                'import sys, grokmirror\n'
                f'try:\n    grokmirror.lock_repo({target!r}, nonblocking=True)\n'
                'except grokmirror.GrokLockError:\n    sys.exit(7)\n'
                'sys.exit(0)\n'
            )
            res = subprocess.run(['python3', '-c', code], capture_output=True, text=True, check=False)
            assert res.returncode == 7, res.stderr
        finally:
            grokmirror.unlock_repo(target)


class TestLockedRepo:
    """locked_repo() must release the lock however the block exits."""

    def test_releases_on_success(self, tmp_path: Path) -> None:
        target = str(tmp_path / 'repo.git')
        with grokmirror.locked_repo(target):
            assert target in grokmirror.REPO_LOCKH
        assert target not in grokmirror.REPO_LOCKH

    def test_releases_on_exception(self, tmp_path: Path) -> None:
        # This is how sys.exit() deep inside library code used to kill a
        # process while it still held repository locks: an exception raised
        # under a manual lock/unlock pair skipped the unlock.
        target = str(tmp_path / 'repo.git')
        with pytest.raises(grokmirror.GrokError, match='boom'), grokmirror.locked_repo(target):
            raise grokmirror.GrokError('boom')
        assert target not in grokmirror.REPO_LOCKH

    def test_failed_lock_does_not_leak_the_handle(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        # The nonblocking path fails routinely (it means "someone else is
        # working on this repo"), so it must close the lockfile handle it
        # just opened rather than leak it.
        target = str(tmp_path / 'repo.git')
        seen: list[IO[str]] = []

        def deny(fh: IO[str], _flags: int) -> None:
            seen.append(fh)
            raise BlockingIOError(errno.EAGAIN, 'locked elsewhere')

        monkeypatch.setattr(grokmirror, 'lockf', deny)
        with pytest.raises(grokmirror.GrokLockError):
            grokmirror.lock_repo(target, nonblocking=True)
        assert target not in grokmirror.REPO_LOCKH
        assert len(seen) == 1
        assert seen[0].closed


class TestDoubleLocking:
    """Locking what this process already holds is a bug, and now says so.

    fcntl locks cannot protect a process from itself: taking the same lock a
    second time succeeds instantly, and closing either handle silently drops
    both locks. Worse, manifest_lock() used to overwrite the stored handle,
    so the "held" lock quietly evaporated as soon as the replaced handle was
    garbage-collected.
    """

    def test_repo_double_lock_raises(self, tmp_path: Path) -> None:
        target = str(tmp_path / 'repo.git')
        with grokmirror.locked_repo(target), pytest.raises(grokmirror.GrokLockError, match='already locked'):
            grokmirror.lock_repo(target)
        # The refusal must not have clobbered the original lock's bookkeeping
        assert target not in grokmirror.REPO_LOCKH

    def test_manifest_double_lock_raises(self, tmp_path: Path) -> None:
        manifile = str(tmp_path / 'manifest.js.gz')
        with grokmirror.locked_manifest(manifile), pytest.raises(grokmirror.GrokLockError, match='already locked'):
            grokmirror.manifest_lock(manifile)
        assert grokmirror.MANIFEST_LOCKH is None

    def test_threaded_lock_race_has_a_single_winner(self, tmp_path: Path) -> None:
        # grok-pull's workers are threads now, and fcntl cannot arbitrate
        # between the threads of one process: every thread's lockf() call on
        # the same file succeeds. The lock registry is the real gate, so
        # checking for an entry and claiming it must be one atomic step --
        # with a plain check-then-set, two threads starting together could
        # both pass the check and both believe they hold the repository.
        target = str(tmp_path / 'repo.git')
        nthreads = 8
        barrier = threading.Barrier(nthreads)
        outcomes: list[bool] = []

        def contend() -> None:
            barrier.wait()
            try:
                grokmirror.lock_repo(target, nonblocking=True)
                outcomes.append(True)
            except grokmirror.GrokLockError:
                outcomes.append(False)

        threads = [threading.Thread(target=contend) for _ in range(nthreads)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

        assert outcomes.count(True) == 1
        grokmirror.unlock_repo(target)
        assert target not in grokmirror.REPO_LOCKH


class TestLockedManifest:
    """locked_manifest() must release the lock however the block exits."""

    def test_releases_on_success(self, tmp_path: Path) -> None:
        manifile = str(tmp_path / 'manifest.js.gz')
        with grokmirror.locked_manifest(manifile):
            assert grokmirror.MANIFEST_LOCKH is not None
        assert grokmirror.MANIFEST_LOCKH is None

    def test_releases_on_exception(self, tmp_path: Path) -> None:
        # grok-fsck used to keep the manifest locked when it bailed out on an
        # unparseable status file: the early return skipped the manual
        # manifest_unlock() call.
        manifile = str(tmp_path / 'manifest.js.gz')
        with pytest.raises(grokmirror.GrokError, match='boom'), grokmirror.locked_manifest(manifile):
            raise grokmirror.GrokError('boom')
        assert grokmirror.MANIFEST_LOCKH is None


def fake_bare_repo(path: Path, altrepo: Path | None = None) -> Path:
    """The bare minimum that is_bare_git_repo() accepts, no git needed."""
    (path / 'objects' / 'info').mkdir(parents=True)
    (path / 'refs').mkdir()
    (path / 'HEAD').write_text('ref: refs/heads/master\n', encoding='utf-8')
    if altrepo is not None:
        (path / 'objects' / 'info' / 'alternates').write_text(f'{altrepo}/objects\n', encoding='utf-8')
    return path


class TestGrokSession:
    """GrokSession carries the per-run state that used to be module globals."""

    def test_requests_session_is_memoized(self) -> None:
        ses = grokmirror.GrokSession()
        first = ses.get_requests_session()
        assert first.headers['User-Agent'] == f'grokmirror/{grokmirror.VERSION}'
        assert ses.get_requests_session() is first

    def test_close_forgets_the_session(self) -> None:
        # grok-pull closes the HTTP session after fetching the remote
        # manifest. With the old module global the *closed* session stayed
        # memoized, so any later caller would get a dead object.
        ses = grokmirror.GrokSession()
        first = ses.get_requests_session()
        ses.close_requests_session()
        assert ses.get_requests_session() is not first

    def test_close_without_open_is_fine(self) -> None:
        grokmirror.GrokSession().close_requests_session()

    def test_altrepo_map_and_is_alt_repo(self, tmp_path: Path) -> None:
        tmp_path = tmp_path.resolve()
        shared = fake_bare_repo(tmp_path / 'objstore' / 'shared.git')
        child = fake_bare_repo(tmp_path / 'toplevel' / 'child.git', altrepo=shared)
        fake_bare_repo(tmp_path / 'toplevel' / 'loner.git')

        ses = grokmirror.GrokSession()
        assert ses.get_altrepo_map(str(tmp_path)) == {str(shared): {str(child)}}
        assert ses.is_alt_repo(str(tmp_path), 'objstore/shared.git')
        assert not ses.is_alt_repo(str(tmp_path), 'toplevel/loner.git')

    def test_altrepo_map_is_cached_per_toplevel(self, tmp_path: Path) -> None:
        # The old module-level cache ignored which toplevel it was built
        # from: whoever asked first won, and every later caller got that
        # same answer regardless of the path they passed.
        tmp_path = tmp_path.resolve()
        shared = fake_bare_repo(tmp_path / 'shared.git')
        fake_bare_repo(tmp_path / 'a' / 'child.git', altrepo=shared)
        (tmp_path / 'b').mkdir()

        ses = grokmirror.GrokSession()
        assert ses.get_altrepo_map(str(tmp_path / 'a'))
        assert not ses.get_altrepo_map(str(tmp_path / 'b'))

    def test_refresh_rescans(self, tmp_path: Path) -> None:
        tmp_path = tmp_path.resolve()
        shared = fake_bare_repo(tmp_path / 'shared.git')
        toplevel = tmp_path / 'toplevel'
        toplevel.mkdir()

        ses = grokmirror.GrokSession()
        assert not ses.get_altrepo_map(str(toplevel))
        child = fake_bare_repo(toplevel / 'child.git', altrepo=shared)
        # The cached answer stays stale until a refresh is asked for
        assert not ses.get_altrepo_map(str(toplevel))
        assert ses.get_altrepo_map(str(toplevel), refresh=True) == {str(shared): {str(child)}}

    def test_find_all_gitdirs_primes_the_altrepo_map(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        tmp_path = tmp_path.resolve()
        shared = fake_bare_repo(tmp_path / 'shared.git')
        child = fake_bare_repo(tmp_path / 'toplevel' / 'child.git', altrepo=shared)

        ses = grokmirror.GrokSession()
        assert ses.find_all_gitdirs(str(tmp_path / 'toplevel')) == {str(child)}

        # The walk doubles as the alternates scan, so asking for the map now
        # must not kick off a second sweep of the tree.
        def no_glob(_self: pathlib.Path, _pattern: str) -> None:
            raise AssertionError('the cache was primed, nothing should be rescanning')

        monkeypatch.setattr(pathlib.Path, 'glob', no_glob)
        assert ses.get_altrepo_map(str(tmp_path / 'toplevel')) == {str(shared): {str(child)}}

    def test_survives_pickling(self, tmp_path: Path) -> None:
        # Workers receive the session through mp.Process args, which pickles
        # it under the forkserver and spawn start methods (forkserver is the
        # Linux default since Python 3.14). The live HTTP session must be
        # dropped; the alternates caches must come along.
        tmp_path = tmp_path.resolve()
        shared = fake_bare_repo(tmp_path / 'shared.git')
        child = fake_bare_repo(tmp_path / 'toplevel' / 'child.git', altrepo=shared)

        ses = grokmirror.GrokSession()
        ses.get_requests_session()
        ses.get_altrepo_map(str(tmp_path / 'toplevel'))

        clone = pickle.loads(pickle.dumps(ses))
        assert clone._requests is None
        assert clone.get_altrepo_map(str(tmp_path / 'toplevel')) == {str(shared): {str(child)}}


class TestMergeSiblings:
    """merge_siblings() folds objstore repos into whichever has most children."""

    def test_all_siblings_orphaned(self) -> None:
        # mdest stayed None and siblings.remove(None) raised KeyError.
        siblings = {'/objstore/one.git', '/objstore/two.git'}
        amap: dict[str, set[str]] = {'/objstore/one.git': set(), '/objstore/two.git': set()}
        assert grokmirror.fsck.merge_siblings(siblings, amap) is None

    def test_siblings_missing_from_the_map(self) -> None:
        siblings = {'/objstore/one.git'}
        assert grokmirror.fsck.merge_siblings(siblings, {}) is None

    def test_no_siblings_at_all(self) -> None:
        assert grokmirror.fsck.merge_siblings(set(), {}) is None


class TestRepackLevel:
    """get_repack_level() decides between no repack (0), quick (1) and full (2)."""

    @staticmethod
    def obj_info(**overrides: int) -> dict[str, str]:
        """Build a `git count-objects -v` result, as run_git_command reports it.

        Keys are spelled as git spells them, so they are passed as
        obj_info(**{'in-pack': 600}) rather than as identifiers.
        """
        info = {'count': 0, 'size': 0, 'in-pack': 0, 'packs': 0, 'size-pack': 0}
        unknown = set(overrides) - set(info)
        assert not unknown, f'not a count-objects key: {sorted(unknown)}'
        info.update(overrides)
        return {key: str(value) for key, value in info.items()}

    def test_tidy_repo_needs_nothing(self) -> None:
        info = self.obj_info(**{'in-pack': 1000, 'size-pack': 4096, 'packs': 1})
        assert grokmirror.get_repack_level(info) == 0

    def test_too_many_packs_means_full_repack(self) -> None:
        assert grokmirror.get_repack_level(self.obj_info(packs=20)) == 2

    def test_too_many_loose_objects_means_quick_repack(self) -> None:
        assert grokmirror.get_repack_level(self.obj_info(count=1200, packs=1)) == 1

    def test_thresholds_are_configurable(self) -> None:
        info = self.obj_info(count=50, packs=3)
        assert grokmirror.get_repack_level(info) == 0
        assert grokmirror.get_repack_level(info, max_loose_objects=50) == 1
        assert grokmirror.get_repack_level(info, max_packs=3) == 2

    def test_loose_objects_as_a_share_of_the_total(self) -> None:
        # Over 500 objects in total and more than 10% of them loose.
        assert grokmirror.get_repack_level(self.obj_info(count=100, packs=1, **{'in-pack': 600})) == 1
        assert grokmirror.get_repack_level(self.obj_info(count=10, packs=1, **{'in-pack': 600})) == 0

    def test_loose_size_as_a_share_of_the_total(self) -> None:
        # Over 1KiB in total and more than 10% of it loose.
        assert grokmirror.get_repack_level(self.obj_info(size=200, packs=1, **{'size-pack': 1000})) == 1
        assert grokmirror.get_repack_level(self.obj_info(size=10, packs=1, **{'size-pack': 1000})) == 0

    def test_tiny_repos_are_left_alone(self) -> None:
        # All loose, but too small to be worth repacking repeatedly.
        assert grokmirror.get_repack_level(self.obj_info(count=100)) == 0


class TestIsObstrepo:
    """is_obstrepo(path, obstdir) asks whether a path is inside the objstore."""

    def test_repo_inside_obstdir(self, tmp_path: Path) -> None:
        obstdir = str(tmp_path / 'objstore')
        assert grokmirror.is_obstrepo(str(tmp_path / 'objstore' / 'x.git'), obstdir)

    def test_sibling_sharing_a_name_prefix_is_not_inside(self, tmp_path: Path) -> None:
        # A string-prefix check said /srv/objstore-private/x.git was inside
        # /srv/objstore. Directory containment is not string containment.
        obstdir = str(tmp_path / 'objstore')
        assert not grokmirror.is_obstrepo(str(tmp_path / 'objstore-private' / 'x.git'), obstdir)


class TestFsckErrorClassification:
    """Blank patterns must not classify anything.

    Splitting an empty config value on newlines yields [''], and every
    string "contains" the empty string, so an unset ignore_errors used to
    swallow every error into the debug log, and an unset reclone_on_errors
    used to request a reclone for any error at all. The two masked each
    other just well enough to go unnoticed.
    """

    @staticmethod
    def _config(toplevel: Path) -> grokmirror.GrokConfigParser:
        config = grokmirror.GrokConfigParser(interpolation=ExtendedInterpolation())
        config.read_dict({'core': {'toplevel': str(toplevel)}, 'fsck': {}})
        return config

    def test_empty_ignore_errors_ignores_nothing(self, tmp_path: Path) -> None:
        config = self._config(tmp_path)
        warn = grokmirror.fsck.remove_ignored_errors('error: it went badly', config)
        assert warn == ['error: it went badly']

    def test_configured_ignore_pattern_still_matches_substring(self, tmp_path: Path) -> None:
        config = self._config(tmp_path)
        config['fsck']['ignore_errors'] = 'went badly\ndangling commit'
        warn = grokmirror.fsck.remove_ignored_errors('error: it went badly\nerror: novel problem', config)
        assert warn == ['error: novel problem']

    def test_empty_reclone_on_errors_reclones_nothing(self, tmp_path: Path) -> None:
        repo = tmp_path / 'mirror' / 'test.git'
        repo.mkdir(parents=True)
        config = self._config(tmp_path / 'mirror')
        ses = grokmirror.GrokSession()
        grokmirror.fsck.check_reclone_error(ses, str(repo), config, ['fatal: any error at all'])
        assert not (repo / 'grokmirror.reclone').exists()

    @staticmethod
    def _replica_repo(tmp_path: Path, remotename: str = '_grokmirror') -> Path:
        """A repository that grok-pull maintains, i.e. one we can reclone."""
        repo = tmp_path / 'mirror' / 'test.git'
        subprocess.run(['git', 'init', '-q', '--bare', str(repo)], check=True)
        subprocess.run(
            ['git', 'remote', 'add', '--mirror=fetch', remotename, 'https://example.invalid/test.git'],
            cwd=repo,
            check=True,
        )
        return repo

    def test_configured_reclone_pattern_triggers_reclone(self, tmp_path: Path) -> None:
        repo = self._replica_repo(tmp_path)
        config = self._config(tmp_path / 'mirror')
        config['fsck']['reclone_on_errors'] = 'fatal: bad tree object'
        ses = grokmirror.GrokSession()
        grokmirror.fsck.check_reclone_error(ses, str(repo), config, ['fatal: bad tree object deadbeef'])
        assert (repo / 'grokmirror.reclone').exists()

    def test_custom_remotename_is_recognized(self, tmp_path: Path) -> None:
        # Whether a reclone is possible is decided by the remote grok-pull would
        # fetch from, so the name configured in [pull] is the one to look for.
        repo = self._replica_repo(tmp_path, remotename='upstream')
        config = self._config(tmp_path / 'mirror')
        config['fsck']['reclone_on_errors'] = 'fatal: bad tree object'
        config.read_dict({'pull': {'remotename': 'upstream'}})
        ses = grokmirror.GrokSession()
        grokmirror.fsck.check_reclone_error(ses, str(repo), config, ['fatal: bad tree object deadbeef'])
        assert (repo / 'grokmirror.reclone').exists()

    def test_many_matching_errors_are_reported_once(self, tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
        # A repository with a few hundred bad objects used to produce a few
        # hundred identical "requested auto-reclone" lines in the report, since
        # the loop kept going after it had made up its mind.
        repo = self._replica_repo(tmp_path)
        config = self._config(tmp_path / 'mirror')
        config['fsck']['reclone_on_errors'] = 'missing commit'
        ses = grokmirror.GrokSession()
        errors = [f'missing commit {n:040x}' for n in range(200)]
        with caplog.at_level('CRITICAL'):
            grokmirror.fsck.check_reclone_error(ses, str(repo), config, errors)
        assert caplog.text.count('requested auto-reclone') == 1

    def test_repo_without_a_mirror_remote_is_not_recloned(
        self, tmp_path: Path, caplog: pytest.LogCaptureFixture
    ) -> None:
        # On an origin server there is no grok-pull and no remote to refetch
        # from, so a reclone marker would sit there forever while every report
        # claimed a reclone had been requested.
        repo = tmp_path / 'mirror' / 'test.git'
        subprocess.run(['git', 'init', '-q', '--bare', str(repo)], check=True)
        config = self._config(tmp_path / 'mirror')
        config['fsck']['reclone_on_errors'] = 'fatal: bad tree object'
        ses = grokmirror.GrokSession()
        with caplog.at_level('CRITICAL'):
            grokmirror.fsck.check_reclone_error(ses, str(repo), config, ['fatal: bad tree object deadbeef'])
        assert not (repo / 'grokmirror.reclone').exists()
        assert 'needs manual attention' in caplog.text
        assert 'auto-reclone' not in caplog.text


class TestCommitGraphErrors:
    """A stale commit-graph is not damage, and grok-fsck sorts it out itself.

    git fsck verifies the commit-graph, so a graph still listing commits that
    have since been pruned produces two lines per commit -- and only one of them
    says what it is about.
    """

    STALE: ClassVar[list[str]] = [
        'error: Could not read 01fbb5471cca68fd07bc020e2da2b0bbb9a011dd',
        'failed to parse commit 01fbb5471cca68fd07bc020e2da2b0bbb9a011dd from object database for commit-graph',
    ]

    def test_stale_graph_pairs_are_split_off(self) -> None:
        graph, rest = grokmirror.fsck.split_commit_graph_errors(self.STALE)
        assert graph == self.STALE
        assert rest == []

    def test_real_errors_are_kept(self) -> None:
        real = ['broken link from tree 05bf1a1c5e3ee1d9a2b1e3ee0aae94cbbd0b7dd3']
        graph, rest = grokmirror.fsck.split_commit_graph_errors([*self.STALE, *real])
        assert graph == self.STALE
        assert rest == real

    def test_unrelated_unreadable_object_is_kept(self) -> None:
        # Only the object ids the graph complained about get paired up. Anything
        # else git could not read is a genuine problem.
        other = 'error: Could not read 5f2a0d97a51f3a0f8c0c1c0dbb6c9a1d38d6b60c'
        graph, rest = grokmirror.fsck.split_commit_graph_errors([*self.STALE, other])
        assert graph == self.STALE
        assert rest == [other]

    def test_errors_without_a_graph_complaint_pass_through(self) -> None:
        errors = ['error: Could not read 5f2a0d97a51f3a0f8c0c1c0dbb6c9a1d38d6b60c', 'missing blob deadbeef']
        graph, rest = grokmirror.fsck.split_commit_graph_errors(errors)
        assert graph == []
        assert rest == errors


class TestCommitGraphArgs:
    """commit_graph_args() asks git for only the filter features it has.

    The suite's own git has all of them, so the version gates can only be
    reached by pretending to be an older git.
    """

    BASE: ClassVar[list[str]] = ['commit-graph', 'write', '--reachable']

    @pytest.mark.parametrize(
        ('gitver', 'expected'),
        [
            ('2.27.0', [*BASE, '--changed-paths']),
            ('2.30.0', [*BASE, '--changed-paths', '--max-new-filters=-1']),
            # v2 has no command-line flag, only config.
            (
                '2.46.0',
                ['-c', 'commitGraph.changedPathsVersion=2', *BASE, '--changed-paths', '--max-new-filters=-1'],
            ),
        ],
    )
    def test_filter_flags_follow_the_git_version(
        self, tree: GrokTree, monkeypatch: pytest.MonkeyPatch, gitver: str, expected: list[str]
    ) -> None:
        monkeypatch.setattr(grokmirror, 'git_newer_than', lambda minver: Version(gitver) >= Version(minver))
        config = tree.load_config({'fsck': {'changedpaths': 'yes'}})
        repo = str(tree.toplevel / 'test' / 'one.git')

        assert grokmirror.fsck.commit_graph_args(repo, config) == expected

    def test_off_by_default(self, tree: GrokTree) -> None:
        config = tree.load_config()
        repo = str(tree.toplevel / 'test' / 'one.git')

        assert grokmirror.fsck.commit_graph_args(repo, config) == self.BASE

    def test_cap_is_passed_through(self, tree: GrokTree) -> None:
        config = tree.load_config({'fsck': {'changedpaths': 'yes', 'max_new_filters': '200000'}})
        repo = str(tree.toplevel / 'test' / 'one.git')

        assert '--max-new-filters=200000' in grokmirror.fsck.commit_graph_args(repo, config)

    def test_objstore_repos_get_no_filters(self, tree: GrokTree) -> None:
        # A fork reads its own graph and never the objstore repo's, so filters
        # there would cost the most CPU of any graph and never be used.
        config = tree.load_config({'fsck': {'changedpaths': 'yes'}})
        obstrepo = os.path.realpath(tree.objstore / 'abcdef.git')

        assert grokmirror.fsck.commit_graph_args(obstrepo, config) == self.BASE


class TestCompileGlobs:
    """compile_globs() replaces the per-pattern fnmatch loops.

    Every config option that takes globs (include, exclude, private, nopurge,
    ffonly, baselines, islandcores, ignore) is tested against every repository,
    so these lists are compiled into one regex. The semantics have to stay
    exactly what a "for pattern in patterns: fnmatch()" loop gave.
    """

    @pytest.mark.parametrize(
        ('patterns', 'name', 'matched'),
        [
            pytest.param(['/test/*'], '/test/one.git', True, id='simple'),
            pytest.param(['/test/*'], '/other/one.git', False, id='no-match'),
            pytest.param(['*'], '/anything.git', True, id='star'),
            pytest.param(['/a/*', '/b/*'], '/b/two.git', True, id='second-alternative'),
            # fnmatch anchors both ends: a prefix is not a match.
            pytest.param(['/test'], '/test/one.git', False, id='prefix-is-not-a-match'),
            pytest.param(['*/one.git'], '/test/one.git', True, id='suffix-glob'),
            # A blank pattern matched nothing under fnmatch, and still does.
            pytest.param([''], '/test/one.git', False, id='blank-pattern'),
            pytest.param(['', '/test/*'], '/test/one.git', True, id='blank-alongside-a-real-one'),
            pytest.param([], '/test/one.git', False, id='empty-list'),
            pytest.param([' /test/* '], '/test/one.git', True, id='surrounding-whitespace'),
        ],
    )
    def test_matching(self, patterns: list[str], name: str, matched: bool) -> None:
        assert bool(grokmirror.compile_globs(patterns).match(name)) is matched

    def test_agrees_with_fnmatch(self) -> None:
        # The property that matters: same answer as the loop it replaced.
        patterns = ['/test/*', '*/linux.git', '/pub/scm/*/torvalds/*', 'relative/*.git']
        names = [
            '/test/one.git',
            '/test/deep/two.git',
            '/other/linux.git',
            '/pub/scm/linux/kernel/git/torvalds/linux.git',
            'relative/thing.git',
            '/nothing/matches/this.git',
            '',
        ]
        matcher = grokmirror.compile_globs(patterns)
        for name in names:
            expected = any(fnmatch.fnmatch(name, x) for x in patterns)
            assert bool(matcher.match(name)) is expected, name

    def test_special_characters_are_not_regex(self) -> None:
        # A repo path is not a regular expression: the dot has to be literal.
        matcher = grokmirror.compile_globs(['/test/one.git'])
        assert matcher.match('/test/one.git')
        assert not matcher.match('/test/oneXgit')


class TestCullManifest:
    """cull_manifest() applies [pull]include/exclude to the remote manifest."""

    @staticmethod
    def _config(include: str | None = None, exclude: str | None = None) -> grokmirror.GrokConfigParser:
        config = grokmirror.GrokConfigParser(interpolation=ExtendedInterpolation())
        config.read_dict({'core': {}, 'pull': {}})
        if include is not None:
            config['pull']['include'] = include
        if exclude is not None:
            config['pull']['exclude'] = exclude
        return config

    MANIFEST: ClassVar[grokmirror.Manifest] = {
        '/test/one.git': {'fingerprint': 'aaa'},
        '/test/two.git': {'fingerprint': 'bbb'},
        '/other/three.git': {'fingerprint': 'ccc'},
    }

    def test_default_includes_everything(self) -> None:
        culled = grokmirror.pull.cull_manifest(dict(self.MANIFEST), self._config())
        assert set(culled) == set(self.MANIFEST)

    def test_include_and_exclude(self) -> None:
        config = self._config(include='/test/*', exclude='/test/two.git')
        culled = grokmirror.pull.cull_manifest(dict(self.MANIFEST), config)
        assert set(culled) == {'/test/one.git'}

    def test_multiple_patterns(self) -> None:
        config = self._config(include='/test/one.git\n/other/*')
        culled = grokmirror.pull.cull_manifest(dict(self.MANIFEST), config)
        assert set(culled) == {'/test/one.git', '/other/three.git'}

    def test_blank_exclude_excludes_nothing(self) -> None:
        # The reclone_on_errors shape: an option present but empty must not
        # turn into "matches everything".
        config = self._config(exclude='')
        culled = grokmirror.pull.cull_manifest(dict(self.MANIFEST), config)
        assert set(culled) == set(self.MANIFEST)

    def test_repo_without_a_fingerprint_is_skipped(self) -> None:
        manifest: grokmirror.Manifest = {**self.MANIFEST, '/test/broken.git': {'modified': 5}}
        culled = grokmirror.pull.cull_manifest(manifest, self._config())
        assert '/test/broken.git' not in culled


class TestPullUpdateManifest:
    """What grok-pull publishes back out after a round of work.

    A mirror's own manifest is what its downstreams read, so this is where a
    local-only key must be dropped and where the keys grokmirror-1.x clients
    still insist on have to be present.
    """

    @staticmethod
    def _config(tmp_path: Path) -> grokmirror.GrokConfigParser:
        config = grokmirror.GrokConfigParser(interpolation=ExtendedInterpolation())
        config.read_dict({'core': {'manifest': str(tmp_path / 'manifest.js')}, 'pull': {}})
        return config

    def _run(self, tmp_path: Path, entries: list[grokmirror.pull.DoneItem]) -> grokmirror.Manifest:
        config = self._config(tmp_path)
        grokmirror.pull.update_manifest(config, entries)
        return grokmirror.read_manifest(config['core']['manifest'])

    def test_private_is_never_published(self, tmp_path: Path) -> None:
        # 'private' is this mirror's own reading of its [core] private config,
        # not something the origin said. Publishing it would tell every
        # downstream which of our repositories we consider private.
        entry: grokmirror.RepoInfo = {'fingerprint': 'aaa', 'private': True}
        manifest = self._run(tmp_path, [('/test/one.git', entry, 'pull', True)])
        assert 'private' not in manifest['/test/one.git']
        assert manifest['/test/one.git'].get('fingerprint') == 'aaa'

    def test_null_head_and_forkgroup_are_dropped(self, tmp_path: Path) -> None:
        # grok-2.0 wrote these out as nulls; there is no reason to keep
        # publishing "we know nothing about this" as a key.
        entry: grokmirror.RepoInfo = {'fingerprint': 'aaa', 'head': None, 'forkgroup': None}
        manifest = self._run(tmp_path, [('/test/one.git', entry, 'pull', True)])
        assert 'head' not in manifest['/test/one.git']
        assert 'forkgroup' not in manifest['/test/one.git']

    def test_real_head_and_forkgroup_survive(self, tmp_path: Path) -> None:
        entry: grokmirror.RepoInfo = {'fingerprint': 'aaa', 'head': 'ref: refs/heads/main', 'forkgroup': 'fg1'}
        manifest = self._run(tmp_path, [('/test/one.git', entry, 'pull', True)])
        assert manifest['/test/one.git'].get('head') == 'ref: refs/heads/main'
        assert manifest['/test/one.git'].get('forkgroup') == 'fg1'

    def test_reference_is_always_written(self, tmp_path: Path) -> None:
        # grokmirror-1.x clients read this key unconditionally, so it goes out
        # even when there is nothing to point at.
        entry: grokmirror.RepoInfo = {'fingerprint': 'aaa'}
        manifest = self._run(tmp_path, [('/test/one.git', entry, 'pull', True)])
        # Present, and explicitly null -- .get() alone could not tell the two apart.
        assert 'reference' in manifest['/test/one.git']
        assert manifest['/test/one.git'].get('reference') is None

    def test_purge_removes_the_entry(self, tmp_path: Path) -> None:
        config = self._config(tmp_path)
        grokmirror.write_manifest(config['core']['manifest'], {'/test/one.git': {'fingerprint': 'aaa'}})
        # A purge carries no repoinfo: there is nothing left on disk to describe.
        grokmirror.pull.update_manifest(config, [('/test/one.git', {}, 'purge', True)])
        assert grokmirror.read_manifest(config['core']['manifest']) == {}

    def test_a_failed_action_changes_nothing(self, tmp_path: Path) -> None:
        entry: grokmirror.RepoInfo = {'fingerprint': 'aaa'}
        manifest = self._run(tmp_path, [('/test/one.git', entry, 'pull', False)])
        assert manifest == {}

    def test_the_entry_list_is_drained(self, tmp_path: Path) -> None:
        # The caller keeps handing the same list back, so anything left in it
        # would be published a second time on the next flush.
        entries: list[grokmirror.pull.DoneItem] = [('/test/one.git', {'fingerprint': 'aaa'}, 'pull', True)]
        grokmirror.pull.update_manifest(self._config(tmp_path), entries)
        assert entries == []


class TestWriteProjectsList:
    """projects.list is the flat repository list gitweb and cgit read."""

    @staticmethod
    def _run(tmp_path: Path, pullopts: dict[str, str], manifest: grokmirror.Manifest) -> list[str]:
        plfile = tmp_path / 'projects.list'
        config = grokmirror.GrokConfigParser(interpolation=ExtendedInterpolation())
        config.read_dict({'core': {}, 'pull': {'projectslist': str(plfile), **pullopts}})
        grokmirror.pull.write_projects_list(config, manifest)
        return plfile.read_text(encoding='utf-8').splitlines()

    def test_leading_slash_is_always_dropped(self, tmp_path: Path) -> None:
        # cgit breaks on an absolute path in projects.list.
        manifest: grokmirror.Manifest = {'/pub/scm/one.git': {}}
        assert self._run(tmp_path, {}, manifest) == ['pub/scm/one.git']

    def test_trimtop_is_removed_from_the_front(self, tmp_path: Path) -> None:
        manifest: grokmirror.Manifest = {'/pub/scm/one.git': {}}
        assert self._run(tmp_path, {'projectslist_trimtop': '/pub/scm'}, manifest) == ['one.git']

    def test_trimtop_only_matches_at_the_front(self, tmp_path: Path) -> None:
        # A repository the setting does not apply to is written out whole,
        # rather than having the prefix cut out of the middle of it.
        manifest: grokmirror.Manifest = {'/other/pub/scm/one.git': {}}
        assert self._run(tmp_path, {'projectslist_trimtop': '/pub/scm'}, manifest) == ['other/pub/scm/one.git']

    def test_symlinks_are_listed_when_asked_for(self, tmp_path: Path) -> None:
        manifest: grokmirror.Manifest = {'/pub/scm/one.git': {'symlinks': ['/pub/scm/alias.git']}}
        listed = self._run(tmp_path, {'projectslist_trimtop': '/pub/scm', 'projectslist_symlinks': 'yes'}, manifest)
        assert sorted(listed) == ['alias.git', 'one.git']

    def test_trimtop_only_matches_at_the_front_of_a_symlink(self, tmp_path: Path) -> None:
        # Symlinks get the same two steps as the repos above, so they need the
        # same guarantee: the prefix is only ever cut off the front, never out
        # of the middle of a name that merely contains it.
        manifest: grokmirror.Manifest = {'/pub/scm/one.git': {'symlinks': ['/other/pub/scm/alias.git']}}
        listed = self._run(tmp_path, {'projectslist_trimtop': '/pub/scm', 'projectslist_symlinks': 'yes'}, manifest)
        assert sorted(listed) == ['one.git', 'other/pub/scm/alias.git']


class TestRunShellCommand:
    """run_shell_command() is the choke point for every external command.

    Every git invocation and every hook goes through here, so the environment
    and stdin contracts below are load-bearing for the whole tree.
    """

    def test_inherits_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # Commands must inherit the caller's environment: the test harness
        # points git at its own config purely through environment variables,
        # and mirror operators expect proxy and ssh settings to apply. (A
        # 2021 refactor accidentally ran every command with an empty
        # environment instead; it never shipped in a release.)
        monkeypatch.setenv('GROK_TEST_CANARY', 'chirp')
        ecode, out, _err = grokmirror.run_shell_command(['sh', '-c', 'printf %s "${GROK_TEST_CANARY-unset}"'])
        assert ecode == 0
        assert out == 'chirp'

    def test_explicit_env_replaces_the_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv('GROK_TEST_CANARY', 'chirp')
        ecode, out, _err = grokmirror.run_shell_command(
            ['sh', '-c', 'printf %s "${GROK_TEST_CANARY-unset}:${PI_CONFIG-unset}"'], env={'PI_CONFIG': '/pi'}
        )
        assert ecode == 0
        assert out == 'unset:/pi'

    def test_timeout_kills_the_command(self) -> None:
        # 124 is what timeout(1) exits with, so it reads familiarly in logs.
        ecode, _out, _err = grokmirror.run_shell_command(['sleep', '30'], timeout=0.5)
        assert ecode == 124

    def test_no_stdin_means_eof_not_the_callers_terminal(self) -> None:
        # A hook that reads stdin must see EOF immediately, not hang waiting
        # on whatever stdin the daemon happens to have inherited.
        ecode, out, _err = grokmirror.run_shell_command(['cat'])
        assert ecode == 0
        assert out == ''

    def test_stdin_bytes_are_delivered(self) -> None:
        ecode, out, _err = grokmirror.run_shell_command(['cat'], stdin=b'hello there')
        assert ecode == 0
        assert out == 'hello there'


class TestReadManifest:
    """read_manifest() has to cope with gzipped, plain, missing and broken files."""

    def test_gzipped(self, tmp_path: Path) -> None:
        manifile = tmp_path / 'manifest.js.gz'
        with gzip.open(manifile, 'wb') as fh:
            fh.write(json.dumps({'/test/one.git': {'modified': 5}}).encode())
        assert grokmirror.read_manifest(str(manifile))['/test/one.git'].get('modified') == 5

    def test_plain(self, tmp_path: Path) -> None:
        manifile = tmp_path / 'manifest.js'
        manifile.write_text('{"/test/one.git": {"modified": 5}}')
        assert grokmirror.read_manifest(str(manifile))['/test/one.git'].get('modified') == 5

    def test_gz_in_the_directory_name_does_not_mean_gzip(self, tmp_path: Path) -> None:
        # The opener used to be picked by looking for '.gz' anywhere in the
        # path, so a plain-text manifest under /srv/my.gz-mirrors/ was fed
        # to gzip and blew up.
        mdir = tmp_path / 'my.gz-mirrors'
        mdir.mkdir()
        manifile = mdir / 'manifest.js'
        manifile.write_text('{"/test/one.git": {"modified": 5}}')
        assert grokmirror.read_manifest(str(manifile))['/test/one.git'].get('modified') == 5

    def test_missing_is_an_empty_manifest(self, tmp_path: Path) -> None:
        # A mirror's first run: no manifest yet, and that is not an error.
        assert grokmirror.read_manifest(str(tmp_path / 'nope.js.gz')) == {}

    def test_unparseable_is_an_empty_manifest(self, tmp_path: Path) -> None:
        # Better to regenerate than to refuse to run: a truncated manifest is
        # what a mirror finds after the disk filled up mid-write.
        manifile = tmp_path / 'manifest.js'
        manifile.write_text('{"/test/one.git": {"modif')
        assert grokmirror.read_manifest(str(manifile)) == {}

    def test_write_then_read_round_trip(self, tmp_path: Path) -> None:
        manifile = str(tmp_path / 'manifest.js.gz')
        manifest: grokmirror.Manifest = {
            '/test/one.git': {'modified': 5, 'fingerprint': 'abc', 'symlinks': ['/test/link.git']}
        }
        grokmirror.write_manifest(manifile, manifest)
        assert grokmirror.read_manifest(manifile) == manifest

    def test_write_with_an_explicit_mtime(self, tmp_path: Path) -> None:
        manifile = str(tmp_path / 'manifest.js.gz')
        grokmirror.write_manifest(manifile, {'/test/one.git': {'modified': 5}}, mtime=1600000000)
        assert int(Path(manifile).stat().st_mtime) == 1600000000

    @pytest.mark.parametrize(
        ('umask', 'expected'),
        [
            pytest.param(0o022, 0o644, id='022'),
            pytest.param(0o002, 0o664, id='002'),
            # These two are where "0o666 ^ umask" went wrong: XOR flipped the
            # execute bits on instead of clearing them, so a private mirror
            # wrote a world-executable manifest (0o641 and 0o611).
            pytest.param(0o027, 0o640, id='027'),
            pytest.param(0o077, 0o600, id='077'),
        ],
    )
    def test_written_mode_follows_the_umask(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch, umask: int, expected: int
    ) -> None:
        # mkstemp() creates 0600 regardless of the umask, so write_manifest()
        # has to put it back on by hand. grokmirror reads the umask once at
        # import time, so the test has to move both it and the real one.
        monkeypatch.setattr(grokmirror, 'UMASK', umask)
        old = os.umask(umask)
        try:
            manifile = str(tmp_path / 'manifest.js')
            grokmirror.write_manifest(manifile, {'/test/one.git': {'modified': 5}})
            assert stat.S_IMODE(Path(manifile).stat().st_mode) == expected
        finally:
            os.umask(old)

    def test_pretty_manifest_is_still_readable(self, tmp_path: Path) -> None:
        manifile = str(tmp_path / 'manifest.js')
        manifest: grokmirror.Manifest = {'/test/two.git': {'modified': 2}, '/test/one.git': {'modified': 1}}
        grokmirror.write_manifest(manifile, manifest, pretty=True)
        assert grokmirror.read_manifest(manifile) == manifest
        # Pretty means sorted and indented, which is the point of the option.
        text = Path(manifile).read_text(encoding='utf-8')
        assert text.index('/test/one.git') < text.index('/test/two.git')
        assert '\n ' in text


class TestFsckOptions:
    """The command-line knobs for a grok-fsck run, kept together as one object.

    The interesting part is the implication: --repack-all-quick and
    --repack-all-full both say "(Assumes --force)" in their help, and every
    place that reads force has to see it, not just the places that read the
    repack-all flags.
    """

    @pytest.mark.parametrize('flag', ['repack_all_quick', 'repack_all_full'])
    def test_repack_all_implies_force(self, flag: str) -> None:
        options = grokmirror.fsck.FsckOptions(**{flag: True})

        assert options.force
        assert options.repack_all

    def test_nothing_is_forced_by_default(self) -> None:
        options = grokmirror.fsck.FsckOptions()

        assert not options.force
        assert not options.repack_all

    def test_force_alone_is_not_a_repack_all(self) -> None:
        # --force means "check everything now", not "repack everything now".
        options = grokmirror.fsck.FsckOptions(force=True)

        assert not options.repack_all


class TestManifestFingerprintCheck:
    """Every fsck run asks whether the manifest still describes the repository.

    The fingerprint is all grokmirror has to decide whether something needs
    replicating, so a repository that disagrees with its manifest entry is one
    that has quietly stopped being mirrored.
    """

    @staticmethod
    def _repo(tmp_path: Path, refs: tuple[str, ...] = ('refs/heads/master',)) -> tuple[Path, Path]:
        top = tmp_path / 'top'
        repo = top / 'test.git'
        subprocess.run(['git', 'init', '-q', '--bare', str(repo)], check=True)

        def run(*args: str) -> str:
            res = subprocess.run(['git', *args], cwd=repo, capture_output=True, text=True, check=True)
            return res.stdout.strip()

        # An empty tree is enough of a commit for a fingerprint.
        commit = run('commit-tree', run('hash-object', '-t', 'tree', '-w', os.devnull), '-m', 'one')
        for ref in refs:
            run('update-ref', ref, commit)
        return top, repo

    def _fingerprint(self, top: Path, ignorerefs: list[str] | None = None) -> str:
        fingerprint = grokmirror.get_repo_fingerprint(top, '/test.git', force=True, ignorerefs=ignorerefs)
        assert fingerprint is not None
        return fingerprint

    def test_matching_fingerprint_is_not_reported(self, tmp_path: Path) -> None:
        top, _repo = self._repo(tmp_path)
        minfo: grokmirror.RepoInfo = {'fingerprint': self._fingerprint(top)}
        stentry: dict[str, object] = {'fp_mismatch': 'left over from a mismatch that has since been fixed'}

        assert grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', minfo, stentry, None) is None
        assert 'fp_mismatch' not in stentry

    def test_mismatch_is_reported_only_when_it_persists(self, tmp_path: Path) -> None:
        # A replica is legitimately behind the manifest for a while after a
        # push, so one run seeing a mismatch means nothing on its own.
        top, _repo = self._repo(tmp_path)
        minfo: grokmirror.RepoInfo = {'fingerprint': 'a' * 40}
        stentry: dict[str, object] = {}

        assert grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', minfo, stentry, None) is None
        assert stentry['fp_mismatch'] == 'a' * 40

        verdict = grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', minfo, stentry, None)

        assert verdict is not None
        kind, detail = verdict
        assert kind == 'stale'
        assert 'a' * 40 in detail

    def test_a_moving_target_is_not_reported(self, tmp_path: Path) -> None:
        # The manifest changed between the two runs, so the repository is being
        # updated -- we just keep catching it at a different moment.
        top, _repo = self._repo(tmp_path)
        stentry: dict[str, object] = {}

        first: grokmirror.RepoInfo = {'fingerprint': 'a' * 40}
        second: grokmirror.RepoInfo = {'fingerprint': 'b' * 40}
        assert grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', first, stentry, None) is None
        assert grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', second, stentry, None) is None
        assert stentry['fp_mismatch'] == 'b' * 40

    def test_unreadable_refs_are_reported_at_once(self, tmp_path: Path) -> None:
        # git refuses to list any refs when it cannot parse one of them, so
        # nothing can fingerprint this repository -- including grok-manifest,
        # which is why its manifest entry has been frozen ever since.
        top, repo = self._repo(tmp_path)
        minfo: grokmirror.RepoInfo = {'fingerprint': self._fingerprint(top)}
        Path(repo, 'refs', 'heads', 'broken').write_text('0' * 40 + '\n')
        stentry: dict[str, object] = {}

        verdict = grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', minfo, stentry, None)

        assert verdict is not None
        kind, detail = verdict
        assert kind == 'broken'
        assert 'refs/heads/broken' in detail
        assert 'fp_mismatch' not in stentry

    def test_repository_without_refs_is_not_reported(self, tmp_path: Path) -> None:
        # grok-manifest does not fingerprint an empty repository either.
        top = tmp_path / 'top'
        subprocess.run(['git', 'init', '-q', '--bare', str(top / 'test.git')], check=True)

        assert grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', {}, {}, None) is None

    def test_ignored_refs_are_left_out_of_the_comparison(self, tmp_path: Path) -> None:
        # The origin fingerprints without refs/meta/*, so a replica that counts
        # them would report every such repository as out of date forever.
        top, _repo = self._repo(tmp_path, refs=('refs/heads/master', 'refs/meta/config'))
        minfo: grokmirror.RepoInfo = {'fingerprint': self._fingerprint(top, ignorerefs=['refs/meta/*'])}
        stentry: dict[str, object] = {}

        # Counting refs/meta/config in makes it disagree, twice over.
        grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', minfo, stentry, None)
        assert grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', minfo, stentry, None) is not None

        stentry.clear()
        verdict = grokmirror.fsck.check_manifest_fingerprint(str(top), '/test.git', minfo, stentry, ['refs/meta/*'])
        assert verdict is None


class TestGetIgnorerefs:
    """[manifest]ignore_refs has to read the same on every command.

    A fingerprint only means anything next to another one, so a replica that
    calculates them differently from its origin disagrees about repositories
    that are perfectly up to date -- and refetches them for ever, because the
    two numbers can never converge.
    """

    @staticmethod
    def _config(sections: dict[str, dict[str, str]]) -> grokmirror.GrokConfigParser:
        config = grokmirror.GrokConfigParser(interpolation=ExtendedInterpolation())
        config.read_dict(sections)
        return config

    def test_no_manifest_section_is_not_an_error(self) -> None:
        # grok-bundle and grok-pull configs need not have one at all.
        assert grokmirror.get_ignorerefs(self._config({'core': {}})) == []

    def test_unset_means_nothing_is_ignored(self) -> None:
        assert grokmirror.get_ignorerefs(self._config({'manifest': {}})) == []

    def test_blank_value_does_not_become_an_empty_glob(self) -> None:
        # compile_globs() turns [''] into a pattern matching the empty string,
        # which is the kind of thing that silently changes every fingerprint.
        assert grokmirror.get_ignorerefs(self._config({'manifest': {'ignore_refs': '\n  \n'}})) == []

    def test_one_per_line_and_stripped(self) -> None:
        config = self._config({'manifest': {'ignore_refs': 'refs/meta/*\n   refs/changes/*  '}})

        assert grokmirror.get_ignorerefs(config) == ['refs/meta/*', 'refs/changes/*']
