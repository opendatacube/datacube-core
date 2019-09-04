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
