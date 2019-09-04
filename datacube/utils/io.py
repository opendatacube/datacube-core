import os
from pathlib import Path
from typing import Union


def check_write_path(fname: Union[Path, str], overwrite: bool) -> Path:
    """ Check is output file exists and either remove it first or raise IOError.

    :param fname: string or Path object
    :param overwrite: Whether to remove file when it exists

    exists   overwrite   Action
    ----------------------------------------------
    T            T       delete file, return Path
    T            F       raise IOError
    F            T       return Path
    F            F       return Path
    """
    if not isinstance(fname, Path):
        fname = Path(fname)

    if fname.exists():
        if overwrite:
            fname.unlink()
        else:
            raise IOError("File exists")
    return fname


def write_user_secret_file(text, fname, in_home_dir=False, mode='w'):
    """Write file only readable/writeable by the user"""

    if in_home_dir:
        fname = os.path.join(os.environ['HOME'], fname)

    open_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    access = 0o600  # Make sure file is readable by current user only
    with os.fdopen(os.open(fname, open_flags, access), mode) as handle:
        handle.write(text)
        handle.close()


def slurp(fname, in_home_dir=False, mode='r'):
    """
    Read an entire file into a string

    :param fname: file path
    :param in_home_dir: if True treat fname as a path relative to $HOME folder
    :return: Content of a file or None if file doesn't exist or can not be read for any other reason
    """
    if in_home_dir:
        fname = os.path.join(os.environ['HOME'], fname)
    try:
        with open(fname, mode) as handle:
            return handle.read()
    except IOError:
        return None
