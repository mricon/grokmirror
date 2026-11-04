# SPDX-License-Identifier: GPL-3.0-or-later
"""Options set in the config file must actually take effect.

ConfigParser has no schema, so an option the code never reads -- or reads
under a different name, or reads two different ways in two different places --
fails completely silently: the mirror runs, and simply does not do what the
admin wrote down. Both cases below were exactly that, found by enumerating
every config read in the tree and comparing it against grokmirror.conf.
"""

from __future__ import annotations

from configparser import ConfigParser
from pathlib import Path

import pytest

from grokmirror import configopts

from support import GrokTree, bloom_filters


def test_manifest_honours_core_log(tree: GrokTree) -> None:
    # grok-manifest looked for [core] logfile, which nothing writes and
    # grokmirror.conf has never documented; every other command reads
    # [core] log. Configuring grok-manifest from a config file therefore
    # logged nowhere at all.
    tree.add_repo('test/one.git')
    tree.write_config()

    tree.run('grok-manifest', '--cfgfile', str(tree.cfgfile))

    assert tree.logfile.exists(), 'grok-manifest wrote no log file at all'
    assert 'test/one.git' in tree.log_text()


@pytest.mark.parametrize('value', ['yes', 'true', 'on', '1'])
def test_fsck_commitgraph_accepts_every_true_spelling(tree: GrokTree, value: str) -> None:
    # commitgraph was read with .getboolean() in one place and compared
    # against the literal 'yes' in two others, so "commitgraph = true" meant
    # enabled in the first and disabled in the rest: no graph was ever
    # written, and the code that verifies the graph then treated its absence
    # as the correct end state.
    repo = tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': {'commitgraph': value}})

    # A full repack is what reaches the graph-writing path; a small repo left
    # alone is never repacked and would never write a graph either way.
    tree.run_fsck('-f', '--repack-all-full')

    assert (repo / 'objects' / 'info' / 'commit-graph').exists()


@pytest.mark.parametrize('value', ['no', 'false', 'off', '0'])
def test_fsck_commitgraph_accepts_every_false_spelling(tree: GrokTree, value: str) -> None:
    repo = tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': {'commitgraph': value}})

    tree.run_fsck('-f', '--repack-all-full')

    assert not (repo / 'objects' / 'info' / 'commit-graph').exists()


@pytest.mark.parametrize(
    ('fsck', 'expected'),
    [
        # Opt-in: the first write is several times slower and the graph grows.
        ({}, None),
        # The test git is new enough for v2, and one commit gets one filter.
        ({'changedpaths': 'yes'}, (2, True)),
        # 0 is git's own "compute no new filters": the chunks are written but
        # stay empty, which shows the cap reached git and was not just accepted.
        ({'changedpaths': 'yes', 'max_new_filters': '0'}, (2, False)),
    ],
    ids=['default', 'changedpaths', 'capped'],
)
def test_fsck_changedpaths_writes_bloom_filters(
    tree: GrokTree, fsck: dict[str, str], expected: tuple[int, bool] | None
) -> None:
    repo = tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': fsck})

    tree.run_fsck('-f', '--repack-all-full')

    filters = bloom_filters(repo)
    assert (None if filters is None else (filters[0], filters[1] > 0)) == expected


@pytest.mark.parametrize('value', ['yes', 'true', 'on', '1'])
def test_fsck_prune_accepts_every_true_spelling(tree: GrokTree, value: str) -> None:
    # Same shape as commitgraph: prune was read as `!= 'yes'`, so every other
    # spelling of true quietly turned pruning off.
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': {'prune': value}})

    tree.run_fsck('-f', '--repack-all-full')

    assert 'Pruning disabled in config file' not in tree.log_text()


@pytest.mark.parametrize('value', ['no', 'false', 'off', '0'])
def test_fsck_prune_accepts_every_false_spelling(tree: GrokTree, value: str) -> None:
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': {'prune': value}})

    tree.run_fsck('-f', '--repack-all-full')

    assert 'Pruning disabled in config file' in tree.log_text()


def test_fsck_rejects_an_unparseable_boolean(tree: GrokTree) -> None:
    # An option that is neither true nor false is a typo, and the admin needs
    # to hear about it by name. This used to be silently false for prune, and
    # an uncaught ValueError deep inside the run for commitgraph.
    tree.add_repo('test/one.git')
    tree.run_manifest()
    tree.write_config({'fsck': {'prune': 'sometimes'}})

    res = tree.run_fsck('-f', '--repack-all-full', expect=1)

    out = res.stdout + res.stderr
    assert 'prune' in out
    assert 'sometimes' in out


def test_the_boolean_complaint_lists_every_spelling_that_would_have_worked(tree: GrokTree) -> None:
    # get_bool() takes all eight of ConfigParser's spellings, but its message
    # used to say "e.g. yes or no", which reads as if the other six were the
    # problem. Somebody with "enabled = True" in their config needs to be told
    # that True is fine and enabled is not.
    tree.add_repo('test/one.git')
    tree.write_config({'fsck': {'prune': 'sometimes'}})

    res = tree.run_fsck('-f', expect=1)

    out = res.stdout + res.stderr

    for spelling in ('yes/no', 'true/false', 'on/off', '1/0'):
        assert spelling in out


# Every option the registry gives a shape to, taken from the registry
# rather than listed again here, so that adding one covers it automatically.
# A typo in any of them used to surface as a ValueError -- from inside a
# worker thread, in several cases -- which is a traceback in a cron mailbox
# rather than an answer. "perhaps" is neither a boolean nor a number, so the
# one value spoils either kind.
TYPED_OPTIONS = [*configopts.options_of_kind('bool'), *configopts.options_of_kind('int')]


@pytest.mark.parametrize(('section', 'option'), TYPED_OPTIONS)
def test_a_value_that_will_not_parse_is_reported_by_name(tree: GrokTree, section: str, option: str) -> None:
    tree.add_repo('test/one.git')
    tree.write_config({section: {option: 'perhaps'}})

    res = tree.run_fsck('-f', expect=1)

    out = res.stdout + res.stderr
    assert option in out
    assert f'[{section}]' in out
    assert 'perhaps' in out


def blank_out(tree: GrokTree, section: str, option: str) -> None:
    """Set an option to nothing, in a config file that is otherwise fine.

    write_config() reads '' as "drop this option", which is the opposite of
    what these tests need: the line has to be there with nothing after the
    equals sign, since that is what a half-finished config looks like.
    """
    parser = ConfigParser(interpolation=None)
    parser.read(tree.cfgfile)
    if not parser.has_section(section):
        parser.add_section(section)
    parser.set(section, option, '')
    with tree.cfgfile.open('w', encoding='utf-8') as fh:
        parser.write(fh)


@pytest.mark.parametrize(('section', 'option'), TYPED_OPTIONS)
def test_an_option_set_to_nothing_is_refused_rather_than_guessed_at(tree: GrokTree, section: str, option: str) -> None:
    # "option =" is the empty string, not an absent option, and git would
    # read it as false. grokmirror does not guess: --config-check reports it
    # as an error, so the run has to agree and refuse to start, or the check
    # would be passing configs that do not work.
    tree.add_repo('test/one.git')
    tree.write_config()
    blank_out(tree, section, option)

    res = tree.run_fsck('-f', expect=1)

    out = res.stdout + res.stderr
    assert option in out
    assert f'[{section}]' in out


@pytest.mark.parametrize(('section', 'option'), TYPED_OPTIONS)
def test_a_value_that_will_not_parse_stops_a_pull_before_it_starts(
    origin: GrokTree, tree: GrokTree, section: str, option: str
) -> None:
    # The config file is shared by every command, so a value no command can
    # make sense of is refused on load -- before any repository is touched,
    # and whichever section it is in.
    origin.add_repo('test/one.git')
    origin.run_manifest()
    tree.write_mirror_config(origin, {section: {option: 'perhaps'}})

    res = tree.run_pull(expect=1)

    assert option in res.stdout + res.stderr
    assert not (tree.toplevel / 'test' / 'one.git').exists()


def test_pi_piper_reports_an_unparseable_shallow(tree: GrokTree, tmp_path: Path) -> None:
    # grok-pi-piper has its own config file, but the same reasoning applies:
    # it runs from a public-inbox hook, where a traceback goes nowhere useful.
    cfgfile = tmp_path / 'pi-piper.conf'
    cfgfile.write_text('[DEFAULT]\npipe = /bin/cat\nshallow = perhaps\n', encoding='utf-8')

    res = tree.run('grok-pi-piper', '-c', str(cfgfile), str(tmp_path), expect=1)

    assert 'shallow' in res.stdout + res.stderr
