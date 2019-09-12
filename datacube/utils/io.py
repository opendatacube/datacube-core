import os
from pathlib import Path
from typing import Union, Optional


def _norm_path(path: Union[str, Path], in_home_dir: bool = False) -> Path:
    if isinstance(path, str):
        path = Path(path)
    if in_home_dir:
        path = Path.home()/path
    return path


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


def write_user_secret_file(text: Union[str, bytes],
                           fname: Union[str, Path],
                           in_home_dir: bool = False,
                           mode: str = 'w'):
    """Write file only readable/writeable by the user"""

    fname = _norm_path(fname, in_home_dir)
    open_flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    access = 0o600  # Make sure file is readable by current user only
    with os.fdopen(os.open(str(fname), open_flags, access), mode) as handle:
        handle.write(text)
        handle.close()


def slurp(fname: Union[str, Path],
          in_home_dir: bool = False,
          mode: str = 'r') -> Optional[Union[bytes, str]]:
    """
    Read an entire file into a string

    :param fname: file path
    :param in_home_dir: if True treat fname as a path relative to $HOME folder
    :return: Content of a file or None if file doesn't exist or can not be read for any other reason
    """
    fname = _norm_path(fname, in_home_dir)
    try:
        with open(str(fname), mode) as handle:
            return handle.read()
    except IOError:
        return None
