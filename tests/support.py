# SPDX-License-Identifier: GPL-3.0-or-later
"""Machinery for building real git repositories to test grokmirror against.

Almost everything grokmirror does is I/O against real git repositories, so
mocking git out would test very little. Instead, every test here builds actual
repositories in a temporary directory and runs the real console scripts over
them.

The two things to know before writing a test:

* `GrokTree` is a throwaway grokmirror installation: a toplevel with bare
  repositories in it, an objstore, a manifest and a config file. Build one with
  the `tree` fixture, add repositories with `add_repo()`, then drive the
  commands with `run()`.
* `run()` fails the test on an unexpected exit code *and* on a traceback in the
  output. grokmirror is run unattended from cron, where a traceback means an
  admin finds out much later that mirroring stopped, so "it crashed" is always
  a test failure and not just noise.

See conftest.py for the environment isolation, which matters more than it looks
like: the tests run with the current directory inside a decoy repository, so
that any code path asking git about "no repository" gets caught instead of
quietly reporting on whatever repository the test runner happens to stand in.
"""

from __future__ import annotations

import contextlib
import functools
import gzip
import http.server
import json
import os
import struct
import subprocess
import threading
from collections.abc import Iterator
from configparser import ConfigParser
from pathlib import Path
from typing import Any, NamedTuple

import grokmirror
import grokmirror.pull

# Fixed point in time for commits, so fingerprints and timestamps are
# reproducible from run to run: 2020-09-13 12:26:40 UTC.
BASE_TIMESTAMP = 1600000000

# The decoy repository's remote. If this URL ever turns up in a manifest or in
# command output, some code path ran git without telling it which repository to
# operate on and got an answer from the current directory instead.
DECOY_URL = 'https://decoy.example.com/must-not-be-used.git'


def git(*args: str, cwd: Path | str | None = None, check: bool = True) -> str:
    """Run git and return its stdout. Raises on failure unless check=False."""
    res = subprocess.run(
        ['git', *args],
        cwd=str(cwd) if cwd is not None else None,
        capture_output=True,
        text=True,
        check=False,
    )
    if check and res.returncode != 0:
        raise AssertionError(f'git {" ".join(args)} failed with {res.returncode}:\n{res.stdout}\n{res.stderr}')
    return res.stdout


def commit_graph_chunks(repo: Path) -> dict[bytes, bytes]:
    """Return the chunks of repo's own commit-graph file, keyed by chunk id.

    Read from the file directly: git has no porcelain that shows what a graph
    holds, and the chunk format is stable on disk.
    """
    data = (repo / 'objects' / 'info' / 'commit-graph').read_bytes()
    assert data[:4] == b'CGPH'
    # The table of contents ends with a terminating entry, whose offset is
    # where the last real chunk ends.
    toc = [struct.unpack('>4sQ', data[8 + 12 * i : 20 + 12 * i]) for i in range(data[6] + 1)]
    return {toc[i][0]: data[toc[i][1] : toc[i + 1][1]] for i in range(len(toc) - 1)}


def bloom_filters(repo: Path) -> tuple[int, int] | None:
    """Return the version of repo's changed-path filters and the bytes they use.

    None means the graph has no filters at all. The data chunk opens with a
    12-byte header (version, hash count, bits per entry), so a size of 0 means
    the chunk is there but git computed no filters into it.
    """
    chunks = commit_graph_chunks(repo)
    if b'BDAT' not in chunks:
        return None
    bdat = chunks[b'BDAT']
    return struct.unpack('>I', bdat[:4])[0], len(bdat) - 12


def make_decoy_repo(path: Path) -> Path:
    """Create a repository that no test should ever be operating on.

    It has a remote, a commit and an unusual branch name, so that anything
    harvested from it is instantly recognizable in a failing assertion.
    """
    path.mkdir(parents=True, exist_ok=True)
    git('init', '-q', '-b', 'decoybranch', str(path))
    git('remote', 'add', 'origin', DECOY_URL, cwd=path)
    (path / 'decoy.txt').write_text('This file belongs to the decoy repository.\n')
    git('add', 'decoy.txt', cwd=path)
    git('commit', '-q', '-m', 'Decoy commit', cwd=path)
    return path


class Source:
    """A normal (non-bare) repository used as the source of pushed history."""

    def __init__(self, path: Path, branch: str = 'master') -> None:
        self.path = path
        self.branch = branch
        self.commits = 0
        path.mkdir(parents=True, exist_ok=True)
        git('init', '-q', '-b', branch, str(path))

    def commit(self, message: str | None = None, content: str | None = None, filename: str = 'file.txt') -> str:
        """Add one commit and return its full hash.

        The commit content is seeded with the source's name, so two sources with
        different names have unrelated histories (no shared root commit) while
        two sources with the same name are byte-for-byte reproducible.

        `filename` matters for public-inbox repositories, where each commit adds
        a single file called 'm' holding the message.
        """
        self.commits += 1
        if message is None:
            message = f'Commit {self.commits}'
        if content is None:
            content = f'{self.path.name}: contents at commit {self.commits}\n'
        (self.path / filename).write_text(content)
        # A distinct, fixed date per commit keeps the hashes stable across runs
        # while still ordering the history the obvious way.
        stamp = f'{BASE_TIMESTAMP + self.commits * 60} +0000'
        env = dict(os.environ, GIT_AUTHOR_DATE=stamp, GIT_COMMITTER_DATE=stamp)
        subprocess.run(
            ['git', 'add', filename],
            cwd=str(self.path),
            capture_output=True,
            text=True,
            check=True,
        )
        res = subprocess.run(
            ['git', 'commit', '-q', '-m', message],
            cwd=str(self.path),
            env=env,
            capture_output=True,
            text=True,
            check=False,
        )
        if res.returncode != 0:
            raise AssertionError(f'git commit failed:\n{res.stdout}\n{res.stderr}')
        return self.head()

    def head(self) -> str:
        return git('rev-parse', 'HEAD', cwd=self.path).strip()

    def tag(self, name: str) -> None:
        git('tag', name, cwd=self.path)

    def push(self, dest: Path, refspec: str | None = None) -> None:
        if refspec is None:
            refspec = f'HEAD:refs/heads/{self.branch}'
        git('push', '-q', str(dest), refspec, cwd=self.path)


@contextlib.contextmanager
def http_server(servedir: Path) -> Iterator[str]:
    """Serve `servedir` over HTTP on loopback and yield its base URL.

    grokmirror talks plain HTTP in two places -- fetching a remote manifest and
    preloading an objstore bundle -- and both are worth exercising against a
    real server rather than a mocked `requests`, since the retry adapter,
    streaming download and status handling are the interesting parts.
    """

    class QuietHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

    handler = functools.partial(QuietHandler, directory=str(servedir))
    server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f'http://127.0.0.1:{server.server_port}'
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=30)


def write_script(path: Path, body: str, executable: bool = True) -> Path:
    """Write a /bin/sh script at `path`; `body` follows the shebang line.

    Pass executable=False for the tests that check what happens to a hook or
    command someone forgot to chmod: saying so on the call line reads as the
    deliberate choice it is, rather than as a mode nobody got round to setting.
    """
    path.write_text(f'#!/bin/sh\n{body}', encoding='utf-8')
    path.chmod(0o755 if executable else 0o644)
    return path


def pi_message(subject: str, body: str = 'Nothing to see here.\n') -> str:
    """An RFC822-ish message body, as public-inbox stores it in the file 'm'."""
    return (
        'From: Tester <tests@grokmirror.invalid>\n'
        'To: mylist@grokmirror.invalid\n'
        f'Subject: {subject}\n'
        f'Message-Id: <{subject.replace(" ", "-")}@grokmirror.invalid>\n'
        '\n'
        f'{body}'
    )


class MirrorRepo(NamedTuple):
    """A bare mirror repository wired up to an origin, and the config behind it."""

    fullpath: str
    remotename: str
    config: grokmirror.GrokConfigParser


class GrokTree:
    """A throwaway grokmirror toplevel, with helpers to drive the commands."""

    def __init__(self, root: Path, decoy: Path) -> None:
        self.root = root
        self.decoy = decoy
        self.toplevel = root / 'toplevel'
        self.objstore = root / 'objstore'
        self.manifest = root / 'manifest.js.gz'
        self.logfile = root / 'grokmirror.log'
        self.statusfile = root / 'fsck.status.js'
        self.cfgfile = root / 'grokmirror.conf'
        self.toplevel.mkdir(parents=True, exist_ok=True)
        self.objstore.mkdir(parents=True, exist_ok=True)
        self._sources: dict[str, Source] = {}

    # -- building repositories ------------------------------------------------

    def source(self, name: str = 'source', commits: int = 1, branch: str = 'master') -> Source:
        """Get (creating on first use) a source repository with some history.

        Repositories pushed from the *same* source share root commits, which is
        what makes grok-fsck consider them forks and migrate them into a shared
        objstore repository.
        """
        if name not in self._sources:
            src = Source(self.root / 'sources' / name, branch=branch)
            for _ in range(commits):
                src.commit()
            self._sources[name] = src
        return self._sources[name]

    def add_repo(
        self,
        gitdir: str,
        source: str | Source = 'source',
        export_ok: bool = False,
        description: str | None = None,
        owner: str | None = None,
    ) -> Path:
        """Create a bare repository under toplevel and push history into it.

        `gitdir` is relative to toplevel, e.g. 'test/repo.git'.
        """
        fullpath = self.toplevel / gitdir.lstrip('/')
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        git('init', '-q', '--bare', str(fullpath))
        src = source if isinstance(source, Source) else self.source(source)
        src.push(fullpath)
        # Point HEAD at something that exists, the way a real mirror would.
        git('symbolic-ref', 'HEAD', f'refs/heads/{src.branch}', cwd=fullpath)
        if export_ok:
            (fullpath / 'git-daemon-export-ok').touch()
        if description is not None:
            (fullpath / 'description').write_text(description + '\n')
        if owner is not None:
            git('config', 'gitweb.owner', owner, cwd=fullpath)
        return fullpath

    def add_empty_repo(self, gitdir: str, export_ok: bool = False) -> Path:
        """Create a bare repository with no refs at all.

        Freshly created mirrors and repositories whose refs were all deleted
        look like this, and they have historically been a good source of
        crashes, since most of the code assumes there is at least one ref.
        """
        fullpath = self.toplevel / gitdir.lstrip('/')
        fullpath.parent.mkdir(parents=True, exist_ok=True)
        git('init', '-q', '--bare', str(fullpath))
        if export_ok:
            (fullpath / 'git-daemon-export-ok').touch()
        return fullpath

    def add_symlink(self, gitdir: str, target: str) -> Path:
        """Symlink one gitdir at another, both relative to toplevel."""
        linkpath = self.toplevel / gitdir.lstrip('/')
        linkpath.parent.mkdir(parents=True, exist_ok=True)
        if linkpath.is_symlink() or linkpath.exists():
            raise AssertionError(f'{linkpath} already exists')
        linkpath.symlink_to(self.toplevel / target.lstrip('/'))
        return linkpath

    def path(self, gitdir: str) -> Path:
        """Absolute path of a gitdir given relative to toplevel."""
        return self.toplevel / gitdir.lstrip('/')

    def alternates(self, gitdir: str) -> str | None:
        """Contents of a repository's objects/info/alternates, if any."""
        altfile = self.path(gitdir) / 'objects' / 'info' / 'alternates'
        if not altfile.exists():
            return None
        return altfile.read_text().strip()

    def objstore_repos(self) -> list[Path]:
        return sorted(p for p in self.objstore.glob('*.git') if p.is_dir())

    # -- configuration --------------------------------------------------------

    def write_config(self, sections: dict[str, dict[str, str]] | None = None, cfgfile: Path | None = None) -> Path:
        """Write a config file with sane defaults, merged with `sections`.

        Pass a value of '' to drop an inherited default, so tests can build the
        deliberately broken configs the error-handling paths need.
        """
        config = ConfigParser()
        config['core'] = {
            'toplevel': str(self.toplevel),
            'objstore': str(self.objstore),
            'manifest': str(self.manifest),
            'log': str(self.logfile),
            'loglevel': 'debug',
        }
        config['fsck'] = {
            'statusfile': str(self.statusfile),
            'frequency': '30',
            # grok-fsck mails a report whenever anything is logged at CRITICAL
            # level, so point it at a port nobody is listening on: no test may
            # ever hand real mail to a real MTA.
            'report_mailhost': '127.0.0.1:1',
        }
        for name, values in (sections or {}).items():
            if name not in config:
                config[name] = {}
            for key, value in values.items():
                if value == '':
                    config.remove_option(name, key)
                else:
                    config[name][key] = value
        # An explicitly empty section still has to exist, since the commands
        # read it, but ConfigParser drops the keys we removed above.
        target = cfgfile if cfgfile is not None else self.cfgfile
        with Path(target).open('w', encoding='utf-8') as fh:
            config.write(fh)
        return target

    def write_mirror_config(
        self,
        origin: GrokTree,
        sections: dict[str, dict[str, str]] | None = None,
    ) -> Path:
        """Write a config that makes this tree a mirror of `origin`.

        Both ends are local directories reached over file:// URLs, which is the
        same code path a real mirror uses for its remote manifest and clones.
        """
        merged: dict[str, dict[str, str]] = {
            'remote': {
                'site': f'file://{origin.toplevel}',
                'manifest': f'file://{origin.manifest}',
            },
            'pull': {
                'projectslist': str(self.root / 'projects.list'),
                'pull_threads': '2',
            },
        }
        for name, values in (sections or {}).items():
            merged.setdefault(name, {}).update(values)
        return self.write_config(merged)

    def load_config(self, sections: dict[str, dict[str, str]] | None = None) -> grokmirror.GrokConfigParser:
        """Write a config file and parse it back the way the commands do.

        Tests that call grokmirror's internals directly need a parsed config
        rather than a path, but it has to be one that came off disk: the
        commands only ever see what load_config_file() makes of the file.
        """
        return grokmirror.load_config_file(str(self.write_config(sections)))

    def load_remote_config(
        self,
        manifest: Path | str | None = None,
        manifest_command: str | None = None,
        extra: dict[str, dict[str, str]] | None = None,
    ) -> grokmirror.GrokConfigParser:
        """A parsed config with a [remote] whose site must never be contacted.

        For tests that call a pull internal directly and only care about the
        manifest: `site` points at a path that does not exist, so anything
        actually trying to reach the origin fails loudly instead of quietly
        succeeding against something real. `manifest` may be a URL or a Path,
        which is turned into the file:// URL a local origin would use.
        """
        remote: dict[str, str] = {'site': 'file:///nonexistent'}
        if manifest is not None:
            remote['manifest'] = manifest if isinstance(manifest, str) else f'file://{manifest}'
        if manifest_command is not None:
            remote['manifest_command'] = manifest_command
        sections: dict[str, dict[str, str]] = {'remote': remote, 'pull': {}}
        for name, values in (extra or {}).items():
            sections.setdefault(name, {}).update(values)
        return self.load_config(sections)

    def load_mirror_config(
        self,
        origin: GrokTree,
        sections: dict[str, dict[str, str]] | None = None,
    ) -> grokmirror.GrokConfigParser:
        """write_mirror_config(), parsed back the same way load_config() does."""
        return grokmirror.load_config_file(str(self.write_mirror_config(origin, sections)))

    def wire_mirror_repo(
        self,
        origin: GrokTree,
        gitdir: str,
        sections: dict[str, dict[str, str]] | None = None,
    ) -> MirrorRepo:
        """Bare-init `gitdir` here and point its remote at `origin`.

        This is what grok-pull's own init step leaves behind, minus the daemon
        around it, so a test can call a single pull internal against a repo that
        is already set up the way a real mirror run would have set it up.
        """
        origin.run_manifest()
        config = self.load_mirror_config(origin, sections)
        fullpath = str(self.path(gitdir))
        assert grokmirror.setup_bare_repo(fullpath)
        assert grokmirror.pull.fix_remotes(str(self.toplevel), gitdir, config['remote']['site'], config)
        return MirrorRepo(fullpath, config['pull'].get('remotename', '_grokmirror'), config)

    def run_pull(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """grok-pull with this tree's config file."""
        return self.run('grok-pull', '-c', str(self.cfgfile), *args, **kwargs)

    # -- manifest access ------------------------------------------------------

    def read_manifest(self, manifile: Path | None = None) -> dict[str, Any]:
        """Read the manifest without going through grokmirror's own reader."""
        target = manifile if manifile is not None else self.manifest
        if not target.exists():
            raise AssertionError(f'No manifest at {target}')
        if target.name.endswith('.gz'):
            with gzip.open(target, 'rb') as gfh:
                return json.loads(gfh.read().decode())
        return json.loads(target.read_text())

    def write_manifest(self, manifest: dict[str, Any], manifile: Path | None = None) -> Path:
        """Write a manifest by hand, for entries grok-manifest won't produce."""
        target = manifile if manifile is not None else self.manifest
        payload = json.dumps(manifest).encode()
        if target.name.endswith('.gz'):
            with gzip.open(target, 'wb') as gfh:
                gfh.write(payload)
        else:
            target.write_bytes(payload)
        return target

    # -- running the commands -------------------------------------------------

    def run(
        self,
        *argv: str,
        expect: int | None = 0,
        allow_traceback: bool = False,
        cwd: Path | None = None,
        stdin: str | None = None,
        timeout: int = 120,
    ) -> subprocess.CompletedProcess[str]:
        """Run a grokmirror console script and check how it went.

        Runs from inside the decoy repository unless told otherwise, fails the
        test if the exit code is not `expect` (pass None to accept any), and
        fails on a traceback even when the exit code was expected.

        A command that does not finish within `timeout` fails the test instead of
        blocking the suite: some of the bugs being guarded against here are
        hangs, not crashes, and a hang has to be just as loud.
        """
        try:
            res = subprocess.run(
                list(argv),
                cwd=str(cwd) if cwd is not None else str(self.decoy),
                input=stdin,
                capture_output=True,
                text=True,
                check=False,
                timeout=timeout,
            )
        except subprocess.TimeoutExpired as ex:
            raise AssertionError(f'{argv[0]} did not finish within {timeout}s: {" ".join(argv)}') from ex
        if expect is not None and res.returncode != expect:
            raise AssertionError(f'{argv[0]} exited with {res.returncode}, expected {expect}\n{self._report(res)}')
        if not allow_traceback and 'Traceback (most recent call last)' in res.stderr:
            raise AssertionError(f'{argv[0]} crashed\n{self._report(res)}')
        return res

    def run_manifest(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """grok-manifest against this tree's manifest and toplevel."""
        return self.run('grok-manifest', '-m', str(self.manifest), '-t', str(self.toplevel), *args, **kwargs)

    def run_bundle(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """grok-bundle with this tree's config file, writing into bundles/."""
        if not self.cfgfile.exists():
            self.write_config()
        outdir = self.root / 'bundles'
        return self.run('grok-bundle', '-c', str(self.cfgfile), '-o', str(outdir), *args, **kwargs)

    def run_shallow_tar(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """grok-shallow-tar with this tree's config file, writing into shallow/."""
        if not self.cfgfile.exists():
            self.write_config()
        outdir = self.root / 'shallow'
        return self.run('grok-shallow-tar', '-c', str(self.cfgfile), '-o', str(outdir), *args, **kwargs)

    def run_fsck(self, *args: str, **kwargs: Any) -> subprocess.CompletedProcess[str]:
        """grok-fsck with this tree's config file."""
        if not self.cfgfile.exists():
            self.write_config()
        return self.run('grok-fsck', '-c', str(self.cfgfile), *args, **kwargs)

    def _report(self, res: subprocess.CompletedProcess[str]) -> str:
        out = [f'--- command: {" ".join(res.args)}']
        for name, text in (('stdout', res.stdout), ('stderr', res.stderr)):
            if text.strip():
                out.append(f'--- {name}:\n{text.rstrip()}')
        if self.logfile.exists():
            log = self.logfile.read_text().strip()
            if log:
                out.append(f'--- logfile:\n{log}')
        return '\n'.join(out)

    def log_text(self) -> str:
        """Everything written to the tree's log file so far."""
        if not self.logfile.exists():
            return ''
        return self.logfile.read_text()
