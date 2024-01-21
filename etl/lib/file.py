import os
from typing import Iterable


def dir_iter(*rel_paths: str) -> Iterable[str]:
    """
    Generator for iterating over a directory
    :param rel_paths: The ordered list of path components to the directory, absolute or relative
    :return: Absolute paths of files in the directory, sorted lexicographically
    """
    base_path = os.path.abspath(os.path.join(*rel_paths))
    for file_name in sorted(os.listdir(base_path)):
        yield os.path.join(base_path, file_name)
