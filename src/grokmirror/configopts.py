# Copyright (C) 2013-2020 by The Linux Foundation and contributors
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""Every configuration option grokmirror reads, and what shape it has.

ConfigParser has no schema: a section is a dict of strings, and a
misspelled option name is indistinguishable from one that was never set.
That is fine while a value is only ever read one way, but grokmirror reads
the same file from five commands and a worker thread, so "is this option
spelled right, and is its value the right kind of thing?" has no single
place to live -- until this one.

The registry below is data only. It answers what an option is called, what
kind of value it holds and what happens when it is absent; it does not read
a config, touch the filesystem or reach the network. `configcheck` uses it
to validate a config file, `GrokConfigParser.validate_values()` uses it to
find a mistyped yes/no or thread count before a command does any work, and
`tests/test_configopts.py` compares it against both the source tree and
`grokmirror.conf` so that an option added to one but not the others is a
test failure rather than a surprise in somebody's cron mailbox.

`pi-piper.conf` is deliberately absent: grok-pi-piper has its own config
file, with a section per public-inbox list rather than the fixed sections
grokmirror uses, so it has nothing to drift against.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

# What kind of value an option holds. These are about validation, not about
# the Python type a read produces -- 'path', 'url' and 'command' are all
# strings to ConfigParser, and the difference only matters to whoever is
# checking whether the value makes sense.
#
#   str       free-form text, anything goes
#   int       parsed with get_int()
#   bool      parsed with get_bool(): yes/no, true/false, on/off, 1/0
#   enum      one of `choices`
#   path      a filesystem path, subject to `writable`/`parent_writable`
#   url       a URL grokmirror fetches itself, subject to `schemes`
#   giturl    a repository URL we only ever hand to git, so git decides
#             what it means: any transport git has, or a helper on PATH
#   command   a shell command line; its first word must be an executable
#   globlist  one glob per line, compiled with compile_globs()
#   strlist   one plain string per line
#   args      whitespace-separated arguments passed to a git command
#   email     an email address, or something sendmail will accept as one
Kind = Literal['str', 'int', 'bool', 'enum', 'path', 'url', 'giturl', 'command', 'globlist', 'strlist', 'args', 'email']


@dataclass(frozen=True)
class Option:
    """One configuration option, as the code that reads it understands it."""

    name: str
    kind: Kind
    # The value used when the option is absent, written the way it would be
    # written in the config file. None means the code has no fallback and
    # either does without the option or fails when it is missing.
    default: str | None = None
    # For kind='enum', every accepted value.
    choices: tuple[str, ...] = ()
    # For kind='url': the schemes this particular option accepts, when they
    # are narrower than what grokmirror can fetch in general. Empty means
    # the general set.
    schemes: tuple[str, ...] = ()
    # For kind='path': the path itself must be a writable directory, or the
    # directory it lives in must be writable. A path option sets at most one
    # of these; neither means we only care that the value resolves.
    writable: bool = False
    parent_writable: bool = False
    # For kind='path': the path has to be there already. Most are created
    # on demand, so their absence is not a problem by itself.
    must_exist: bool = False

    def __post_init__(self) -> None:
        # Cheap invariants, checked at import time rather than left for a
        # checker to trip over with a confusing message much later.
        if (self.kind == 'enum') != bool(self.choices):
            raise ValueError(f'{self.name}: choices and kind="enum" go together')
        if self.choices and self.default is not None and self.default not in self.choices:
            raise ValueError(f'{self.name}: default {self.default!r} is not one of {self.choices}')
        if self.schemes and self.kind != 'url':
            raise ValueError(f'{self.name}: schemes only means something for kind="url"')
        if (self.writable or self.parent_writable) and self.kind != 'path':
            raise ValueError(f'{self.name}: writability only means something for kind="path"')
        if self.writable and self.parent_writable:
            raise ValueError(f'{self.name}: set writable or parent_writable, not both')
        if self.must_exist and self.kind != 'path':
            raise ValueError(f'{self.name}: only a path can be required to exist')


def _opts(*options: Option) -> dict[str, Option]:
    return {option.name: option for option in options}


# section -> option name -> Option. Every option grokmirror reads from a
# grokmirror.conf, whichever command reads it.
KNOWN: dict[str, dict[str, Option]] = {
    'core': _opts(
        # load_config_file() insists on this one and refuses to go on
        # without it, so it is the only option with no usable default, and
        # the only directory grokmirror will not create for itself.
        Option('toplevel', 'path', writable=True, must_exist=True),
        Option('manifest', 'path', default='${toplevel}/manifest.js.gz', parent_writable=True),
        Option('objstore', 'path', default='${toplevel}/objstore', writable=True),
        Option('log', 'path', parent_writable=True),
        Option('loglevel', 'enum', default='info', choices=('info', 'debug')),
        Option('objstore_uses_plumbing', 'bool', default='no'),
        Option('private', 'globlist', default=''),
    ),
    'manifest': _opts(
        Option('pretty', 'bool', default='no'),
        Option('ignore', 'globlist', default=''),
        Option('ignore_refs', 'strlist', default=''),
        Option('fetch_objstore', 'bool', default='no'),
        Option('check_export_ok', 'bool', default='no'),
    ),
    'remote': _opts(
        # Not a 'url': grok-pull never fetches this itself, it joins the
        # gitdir onto it and hands the result to "git remote add". So
        # ssh://, git:// and a local path are all perfectly good here,
        # even though none of them is something requests could fetch.
        Option('site', 'giturl'),
        # grok-pull needs "manifest" or "manifest_command", and checks for
        # that itself, which is why neither is required here on its own.
        Option('manifest', 'url'),
        Option('manifest_command', 'command'),
        # No file:// here, unlike the manifest: bundles are fetched with
        # a plain requests get(), and requests has no file:// adapter.
        Option('preload_bundle_url', 'url', schemes=('http', 'https')),
    ),
    'pull': _opts(
        Option('projectslist', 'path', default='', parent_writable=True),
        Option('projectslist_trimtop', 'str', default=''),
        Option('projectslist_symlinks', 'bool', default='no'),
        # Read through get_hookscripts(), which takes the option name as a
        # parameter, so grep will not find these three next to a literal.
        Option('post_update_hook', 'command', default=''),
        Option('post_clone_complete_hook', 'command', default=''),
        Option('post_work_complete_hook', 'command', default=''),
        Option('purge', 'bool', default='no'),
        Option('nopurge', 'globlist', default=''),
        Option('purgeprotect', 'int', default='5'),
        Option('default_owner', 'str', default='Grokmirror'),
        Option('remotename', 'str', default='_grokmirror'),
        Option('pull_threads', 'int', default='0'),
        Option('retries', 'int', default='3'),
        Option('include', 'globlist', default='*'),
        Option('exclude', 'globlist', default=''),
        Option('ffonly', 'globlist', default=''),
        Option('refresh', 'int', default='300'),
        Option('socket', 'path', parent_writable=True),
    ),
    'fsck': _opts(
        Option('frequency', 'int', default='30'),
        Option('statusfile', 'path', parent_writable=True),
        Option('ignore_errors', 'strlist', default=''),
        Option('reclone_on_errors', 'strlist', default=''),
        Option('repack', 'bool', default='yes'),
        Option('extra_repack_flags', 'args', default=''),
        Option('extra_repack_flags_full', 'args', default=''),
        Option('commitgraph', 'bool', default='yes'),
        Option('changedpaths', 'bool', default='no'),
        # -1 is git's own "no limit".
        Option('max_new_filters', 'int', default='-1'),
        Option('prune', 'bool', default='yes'),
        # Not a boolean: "always" means precious even when grokmirror would
        # rather repack, which is a third answer and not a louder "yes".
        Option('precious', 'enum', default='yes', choices=('always', 'yes', 'no')),
        Option('baselines', 'globlist', default=''),
        Option('islandcores', 'globlist', default=''),
        Option('obstrepo_merge_strategy', 'enum', default='exact', choices=('exact', 'loose', 'blobs')),
        Option('preload_bundle_outdir', 'path', writable=True),
        Option('report_to', 'email', default='root'),
        Option('report_from', 'email', default='root'),
        Option('report_subject', 'str'),
        Option('report_mailhost', 'str', default='localhost'),
    ),
}

# Every (section, option) pair of a given kind, in a stable order. Built once
# here rather than re-walked on every call: validate_values() asks for the
# booleans and the ints on every single config load.
_BY_KIND: dict[str, tuple[tuple[str, str], ...]] = {}
for _section, _options in KNOWN.items():
    for _option in _options.values():
        _BY_KIND.setdefault(_option.kind, ())
        _BY_KIND[_option.kind] += ((_section, _option.name),)


def options_of_kind(kind: Kind) -> tuple[tuple[str, str], ...]:
    """Return every (section, option) in KNOWN that holds this kind of value."""
    return _BY_KIND.get(kind, ())


def lookup(section: str, option: str) -> Option | None:
    """Return the registry entry for an option, or None if we don't know it."""
    return KNOWN.get(section, {}).get(option)
