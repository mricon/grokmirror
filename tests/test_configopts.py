# SPDX-License-Identifier: GPL-3.0-or-later
"""The option registry must stay level with the code and with the example config.

`grokmirror.configopts.KNOWN` is hand-written data, and hand-written data
about code rots the moment somebody adds an option and forgets it exists.
These tests read the two sources of truth it claims to describe -- the
config reads in `src/grokmirror/*.py`, and the options documented in
`grokmirror.conf` -- and fail when any of the three disagree.

So if one of these fails after you added an option, nothing is broken: add
it to `configopts.KNOWN` and document it in `grokmirror.conf`, and it goes
green. That is the whole point of it being a test.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Callable
from pathlib import Path

import pytest

from grokmirror import configopts

REPO_ROOT = Path(__file__).resolve().parent.parent
SOURCE_DIR = REPO_ROOT / 'src' / 'grokmirror'
EXAMPLE_CONF = REPO_ROOT / 'grokmirror.conf'

# grok-pi-piper reads its own config file (pi-piper.conf), which has a
# section per public-inbox list instead of grokmirror's fixed sections, so
# its reads have nothing to do with this registry.
SKIP_SOURCES = {'pi_piper.py'}

# Reads whose section or option name is only known at runtime. Each one is
# listed with the source it unparses to, so that a *new* dynamic read fails
# this test instead of quietly joining the exemption.
DYNAMIC_READS = {
    # validate_values() walking the registry itself.
    ('__init__.py', 'self.get_bool(section, option, fallback=False)'),
    ('__init__.py', 'self.get_int(section, option, fallback=0)'),
    # get_hookscripts(config, hookname), called with 'post_update_hook',
    # 'post_clone_complete_hook' and 'post_work_complete_hook'.
    ('pull.py', "config['pull'].get(hookname, '')"),
}

# How a read spells itself, and the kinds of option that may be read that
# way. A missing entry means the read tells us nothing about the kind:
# plain .get() and ['option'] return a string whatever the value means.
READER_KINDS = {
    'get_bool': {'bool'},
    'get_int': {'int'},
}

# Readers nothing should be using any more. getboolean() and getint() accept
# the same values as get_bool() and get_int(), but report a typo as a
# ValueError naming neither the section nor the option nor the file, which is
# no use from a cron job.
BANNED_READERS = {'getboolean', 'getint', 'getfloat'}

_STRING_READERS = {'get', 'getint', 'getboolean', 'getfloat'}

# The readers that take the section and the option as two arguments, rather
# than being called on a section proxy, and the kinds they are the right way
# to read.
TYPED_KINDS = {'bool': 'get_bool', 'int': 'get_int'}
TYPED_READERS = set(TYPED_KINDS.values())


def _literal_str(node: ast.expr | None) -> str | None:
    """Return the value of a string literal, or None for anything else.

    A section or option name that is not a literal string cannot be looked
    up in the registry, whether it is a variable or, in principle, a
    number -- both are reported as dynamic rather than guessed at.
    """
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    return None


class ConfigRead:
    """One place in the source where a config option is read."""

    def __init__(self, filename: str, lineno: int, section: str, option: str, reader: str) -> None:
        self.filename = filename
        self.lineno = lineno
        self.section = section
        self.option = option
        self.reader = reader

    def __str__(self) -> str:
        return f'{self.filename}:{self.lineno}: [{self.section}] {self.option} (via {self.reader})'


def _collect_reads() -> tuple[list[ConfigRead], list[tuple[str, str]]]:
    """Find every config read in the source tree, statically.

    Returns the reads whose section and option are literals, and the ones
    where at least one of them is computed.
    """
    reads: list[ConfigRead] = []
    dynamic: list[tuple[str, str]] = []

    for path in sorted(SOURCE_DIR.glob('*.py')):
        if path.name in SKIP_SOURCES:
            continue
        tree = ast.parse(path.read_text(encoding='utf-8'), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
                # config.get_bool('section', 'option', fallback), and get_int
                if node.func.attr in TYPED_READERS:
                    section = _literal_str(node.args[0]) if node.args else None
                    option = _literal_str(node.args[1]) if len(node.args) > 1 else None
                    if section is not None and option is not None:
                        reads.append(ConfigRead(path.name, node.lineno, section, option, node.func.attr))
                    else:
                        dynamic.append((path.name, ast.unparse(node)))
                    continue
                # config['section'].get('option') and friends
                if node.func.attr in _STRING_READERS:
                    target = node.func.value
                    if not (isinstance(target, ast.Subscript) and isinstance(target.value, ast.Name)):
                        continue
                    if target.value.id != 'config':
                        continue
                    section = _literal_str(target.slice)
                    option = _literal_str(node.args[0]) if node.args else None
                    if section is not None and option is not None:
                        reads.append(ConfigRead(path.name, node.lineno, section, option, node.func.attr))
                    else:
                        dynamic.append((path.name, ast.unparse(node)))
                    continue
            # config['section']['option'], reading it and not assigning to
            # it: grok-pull overrides [pull] purge from the command line,
            # and grok-fsck turns [fsck] commitgraph off when git is too
            # old to write one.
            if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Subscript):
                if not isinstance(node.ctx, ast.Load):
                    continue
                inner = node.value
                if not (isinstance(inner.value, ast.Name) and inner.value.id == 'config'):
                    continue
                section = _literal_str(inner.slice)
                option = _literal_str(node.slice)
                if section is not None and option is not None:
                    reads.append(ConfigRead(path.name, node.lineno, section, option, 'subscript'))
                else:
                    dynamic.append((path.name, ast.unparse(node)))

    return reads, dynamic


def _example_conf_options() -> set[tuple[str, str]]:
    """Every option assigned in grokmirror.conf, commented out or not.

    Options are documented by example there, so most of the file is
    commented-out assignments; those count. Continuation lines of a
    multi-line value are indented, so an anchored pattern skips them.
    """
    section_re = re.compile(r'^\[([a-z]+)\]\s*$')
    option_re = re.compile(r'^(?:#\s*)?([a-z_][a-z0-9_]*)\s*=')

    options: set[tuple[str, str]] = set()
    section = None
    for line in EXAMPLE_CONF.read_text(encoding='utf-8').splitlines():
        match = section_re.match(line)
        if match:
            section = match.group(1)
            continue
        match = option_re.match(line)
        if match and section is not None:
            options.add((section, match.group(1)))
    return options


REGISTERED = {(section, option) for section, options in configopts.KNOWN.items() for option in options}
READS, DYNAMIC = _collect_reads()


def test_the_example_config_documents_every_registered_option() -> None:
    # An option nobody can discover may as well not exist, and
    # grokmirror.conf is where they are discovered.
    missing = REGISTERED - _example_conf_options()
    assert not missing, 'registered but undocumented in grokmirror.conf: ' + ', '.join(
        f'[{section}] {option}' for section, option in sorted(missing)
    )


def test_the_registry_knows_every_option_in_the_example_config() -> None:
    # The other direction: an option documented in grokmirror.conf that no
    # longer appears in the registry is either a typo in the example or an
    # option the code stopped reading, and both mislead whoever copies it.
    unknown = _example_conf_options() - REGISTERED
    assert not unknown, 'documented in grokmirror.conf but not registered: ' + ', '.join(
        f'[{section}] {option}' for section, option in sorted(unknown)
    )


def test_every_config_read_in_the_source_is_registered() -> None:
    unregistered = sorted({(read.section, read.option) for read in READS} - REGISTERED)
    assert not unregistered, 'read by the code but not in configopts.KNOWN: ' + ', '.join(
        f'[{section}] {option}' for section, option in unregistered
    )


def test_no_new_config_read_hides_behind_a_variable() -> None:
    # A read whose option name is computed cannot be checked against the
    # registry, so each one has to be accounted for by hand.
    surprises = sorted(set(DYNAMIC) - DYNAMIC_READS)
    assert not surprises, (
        'config reads with a computed section or option name, which this test cannot check against the registry: '
        + '; '.join(f'{filename}: {source}' for filename, source in surprises)
        + '. Add it to DYNAMIC_READS once you have checked that every name it can be called with is registered.'
    )


# A read of an option the registry has never heard of says nothing about
# kinds; test_every_config_read_in_the_source_is_registered is the one that
# complains about those.
REGISTERED_READS = [read for read in READS if configopts.lookup(read.section, read.option) is not None]


@pytest.mark.parametrize('read', REGISTERED_READS, ids=str)
def test_each_read_matches_the_kind_the_registry_gives_the_option(read: ConfigRead) -> None:
    option = configopts.lookup(read.section, read.option)
    assert option is not None
    assert read.reader not in BANNED_READERS, (
        f'{read} -- use config.get_bool() or config.get_int(), which name the option when its value will not parse'
    )
    allowed = READER_KINDS.get(read.reader)
    if allowed is None:
        # A plain string read tells us nothing, except for the two kinds
        # that have a reader of their own: a boolean read as a string is the
        # bug that made 'commitgraph = true' mean enabled in one place and
        # disabled in two others, and an int read as a string is one int()
        # call away from the same traceback get_int() exists to prevent.
        assert option.kind not in TYPED_KINDS, (
            f'{read} -- [{read.section}] {read.option} is a{"n" if option.kind == "int" else ""} '
            f'{option.kind}, so read it with config.{TYPED_KINDS[option.kind]}()'
        )
        return
    assert option.kind in allowed, (
        f'{read} -- the registry calls [{read.section}] {read.option} a {option.kind}, '
        f'which is not something {read.reader}() should be reading'
    )


def test_every_registered_default_is_the_right_shape() -> None:
    # The defaults are written the way they would appear in the config
    # file, so the ones with a declared shape have to survive being read
    # back as that shape.
    for section, options in configopts.KNOWN.items():
        for option in options.values():
            if option.default is None:
                continue
            where = f'[{section}] {option.name}'
            if option.kind == 'int':
                # A leading minus is fine: git spells "no limit" as -1.
                assert option.default.lstrip('-').isdigit(), f'{where}: default {option.default!r} is not an integer'
            elif option.kind == 'bool':
                assert option.default.lower() in {'yes', 'no', 'true', 'false', 'on', 'off', '1', '0'}, (
                    f'{where}: default {option.default!r} is not a boolean'
                )


# The Option invariants run at import time, so a registry entry that
# contradicts itself is an ImportError rather than a confusing diagnostic
# much later. That only helps if the invariants themselves work, and
# nothing else in this file would notice if one stopped raising.
#
# Each case is a callable rather than a dict of keyword arguments, so that
# the type checkers see a real constructor call and nothing here needs a
# suppression to say what it means.
CONTRADICTIONS: list[tuple[str, Callable[[], configopts.Option], str]] = [
    ('enum without choices', lambda: configopts.Option('x', 'enum'), 'choices and kind="enum" go together'),
    ('choices without enum', lambda: configopts.Option('x', 'str', choices=('a',)), 'choices and kind="enum"'),
    (
        'default outside choices',
        lambda: configopts.Option('x', 'enum', default='b', choices=('a',)),
        'is not one of',
    ),
    ('writable non-path', lambda: configopts.Option('x', 'str', writable=True), 'writability only means something'),
    (
        'parent_writable non-path',
        lambda: configopts.Option('x', 'str', parent_writable=True),
        'writability only means something',
    ),
    (
        'both kinds of writable',
        lambda: configopts.Option('x', 'path', writable=True, parent_writable=True),
        'not both',
    ),
    (
        'must_exist non-path',
        lambda: configopts.Option('x', 'str', must_exist=True),
        'only a path can be required to exist',
    ),
    ('schemes on a non-url', lambda: configopts.Option('x', 'str', schemes=('http',)), 'schemes only means something'),
]


@pytest.mark.parametrize(
    ('build', 'complaint'),
    [(build, complaint) for _, build, complaint in CONTRADICTIONS],
    ids=[name for name, _, _ in CONTRADICTIONS],
)
def test_a_registry_entry_that_contradicts_itself_is_refused(
    build: Callable[[], configopts.Option], complaint: str
) -> None:
    with pytest.raises(ValueError, match=re.escape(complaint)):
        build()
