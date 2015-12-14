#!/usr/bin/env python
"""
Get the current version number.

Loaded via git tags, package info or git-archive rewrite.

Public releases are expected to be tagged in the repository
with prefix 'datacube-' and a version number following PEP-440.

Eg. ::

    datacube-0.1.0
    datacube-0.2.0.dev1

See PEP440: https://www.python.org/dev/peps/pep-0440
Derived from https://github.com/Changaco/version.py
"""

import re
from os.path import dirname, isdir, join
from subprocess import CalledProcessError, check_output

PREFIX = 'datacube-'

GIT_TAG_PATTERN = re.compile(r'\btag: %s([0-9][^,]*)\b' % PREFIX)
VERSION_PATTERN = re.compile('^Version: (.+)$', re.M)
NUMERIC = re.compile('^\d+$')


def get_version():
    # Return the version if it has been injected into the file by git-archive
    version = GIT_TAG_PATTERN.search('$Format:%D$')
    if version:
        return version.group(1)

    d = dirname(__file__)

    if isdir(join(d, '.git')):
        cmd = 'git describe --tags --match %s[0-9]* --dirty' % PREFIX
        try:
            git_version = check_output(cmd.split()).decode().strip()[len(PREFIX):]
        except CalledProcessError:
            raise RuntimeError('Unable to get version number from git tags')
        components = git_version.split('-')
        version = components.pop(0)

        # Any other suffixes imply this is not a release: Append an internal build number 
        if components:
            # <commit count>.<git hash>.<whether the working tree is dirty>
            version += '+' + '.'.join(components)

    else:
        # Extract the version from the PKG-INFO file.
        with open(join(d, 'PKG-INFO')) as f:
            version = VERSION_PATTERN.search(f.read()).group(1)

    return version


if __name__ == '__main__':
    print(get_version())
