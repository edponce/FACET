import os
from typing import List, Tuple
from QuickerUMLS.helpers import is_iterable


def unpack_dir(adir: str, hidden=False, recursive=False) -> List[str]:
    """Unpack directories into a list of filenames.

    Args:
        adir (str): Directory name.

        hidden (bool): If set, include hidden files.

        recursive (bool): If set, unpack files recursively.

    Returns (List): List of filenames.
    """
    files = []
    for file_or_dir in os.listdir(adir):
        fd = os.path.join(adir, file_or_dir)
        if os.path.isdir(fd) and recursive:
            files.extend(unpack_dir(fd))
        elif os.path.isfile(fd):
            if not hidden and os.path.basename(fd).startswith('.'):
                continue
            files.append(fd)
    return files


def corpus_generator(corpora: str,
                     phony=False, **kwargs) -> Tuple[str, str]:
    """Match concepts found in a vocabulary to terms in corpora.

    Args:
        corpora (str): Text data to process. Valid values are:
            * Directory
            * File
            * Raw text
            * An iterable of any combination of the above

        phony (bool): If set, attr:`corpora` items are not considered
            as file system objects when name collisions occur.
            Default is false.

        kwargs (Dict): Options passed directory to 'unpack_dir()'.

    Returns (Tuple): Corpus source and corpus content.
                     The source identifier for raw text is '_text'.
                     The source identifier for other is their file system name.
    """
    if not is_iterable(corpora):
        corpora = (corpora,)

    if not phony:
        _corpora = []
        for corpus in corpora:
            if os.path.isdir(corpus):
                _corpora.extend(unpack_dir(corpus, **kwargs))
            else:
                _corpora.append(corpus)
    else:
        _corpora = corpora

    for corpus in _corpora:
        # Assume corpus is raw text if it is not a file system object.
        if phony or not os.path.exists(corpus):
            yield '_text', corpus
        elif os.path.isfile(corpus):
            if os.path.basename(corpus).startswith('.'):
                continue
            with open(corpus) as fd:
                for line in fd:
                    yield corpus, line
