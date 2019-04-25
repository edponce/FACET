import os
import io
import re
import sys
import time
import math
import mmap
import queue
import pandas
import shutil
import argparse
import threading
import collections
import multiprocessing
from toolbox import CuiSemTypesDB, SimstringDBWriter
from constants import HEADERS_MRCONSO, HEADERS_MRSTY, LANGUAGES


try:
    from unidecode import unidecode
    UNIDECODE_AVAIL = True
except ImportError:
    UNIDECODE_AVAIL = False

# 0 = No status info
# 1 = General status info
# 2 = Detailed status info
PROFILE = 2


def get_file_content_mp(q,
                        map_file,
                        map_offsets,
                        mrsty_headers,
                        valid_cuis,
                        valid_semtypes,
                        field_delim=None,
                        batch_delim='\n',
                        batch_size=io.DEFAULT_BUFFER_SIZE):
    """
    Args:
        filename (str): File to open/read.

    Kwargs:
        field_delim (str, optional): Delimiter to split text.
            If a delimiter is provided, then a list of strings is returned.
            If None, then no text is not split and a sring is returned.

        max_split (int, optional): At most splits to occur.

        batch_delim (str, optional): See 'delim' in 'get_file_content_batch'.

        batch_size (str, optional): See 'batch_size' in 'get_file_content_batch'.
    """
    cuisty = []

    # Parse lines only until the fields we need, ignore remaining
    cui_idx = mrsty_headers.index('cui')
    sty_idx = mrsty_headers.index('sty')
    max_split = max(cui_idx, sty_idx) + 1

    batch_queue = 1000
    batch_queue_cnt = 0
    batch_iterator = get_mmap_content_batch_mp(map_file,
                                               map_offsets,
                                               delim=batch_delim,
                                               batch_size=batch_size)
    for batch in batch_iterator:
        for line in batch.split(batch_delim):
            if field_delim is None:
                content = line
            else:
                if max_split is None:
                    content = line.split(field_delim)
                else:
                    content = line.split(field_delim, max_split)

                # Try/except is a temporary hack because batches do not
                # necessarily end based on 'batch_delim' and trigger index
                # error when accessing 'content'.
                try:
                    cui = content[cui_idx]
                    if valid_cuis is not None and cui not in valid_cuis:
                        continue
                    sty = content[sty_idx]
                    if valid_semtypes is not None and sty not in valid_semtypes:
                        continue

                    cuisty.append((cui, sty))
                    batch_queue_cnt += 1

                    # Send batch data to main process
                    if batch_queue_cnt == batch_queue:
                        try:
                            q.put_nowait(cuisty)
                        except queue.Full as ex:
                            q.put(cuisty)
                        cuisty = []
                        batch_queue_cnt = 0
                except IndexError as ex:
                    print(f'IndexError: {content}')

    # Flush remaining data
    if len(cuisty) > 0:
        try:
            q.put_nowait(cuisty)
        except queue.Full as ex:
            q.put(cuisty)

    # Signal that we are done
    q.put(None)


def get_file_content1(file_name,
                     field_delim=None,
                     max_split=None,
                     batch_delim='\n',
                     batch_size=io.DEFAULT_BUFFER_SIZE):
    """
    Args:
        filename (str): File to open/read.

    Kwargs:
        field_delim (str, optional): Delimiter to split text.
            If a delimiter is provided, then a list of strings is returned.
            If None, then no text is not split and a string is returned.

        max_split (int, optional): At most splits to occur.

        batch_delim (str, optional): See 'delim' in 'get_file_content_batch'.

        batch_size (str, optional): See 'batch_size' in 'get_file_content_batch'.
    """
    with open(file_name, 'r+b') as fd:
        with mmap.mmap(fd.fileno(), 0, flags=mmap.MAP_PRIVATE, prot=mmap.PROT_READ) as mm:
            batch_iterator = get_mmap_content_batch(mm,
                                                    os.path.getsize(file_name),
                                                    delim=batch_delim,
                                                    batch_size=batch_size)
            for nb, batch in enumerate(batch_iterator):
                for nl, line in enumerate(batch.split(batch_delim)):
                    if field_delim is None:
                        yield line
                    else:
                        if max_split is None:
                            yield line.split(field_delim)
                        else:
                            yield line.split(field_delim, max_split)


def get_file_content0(file_name,
                     field_delim=None,
                     max_split=None,
                     batch_delim='\n',
                     batch_size=io.DEFAULT_BUFFER_SIZE):
    """
    Args:
        filename (str): File to open/read.

    Kwargs:
        field_delim (str, optional): Delimiter to split text.
            If a delimiter is provided, then a list of strings is returned.
            If None, then no text is not split and a string is returned.

        max_split (int, optional): At most splits to occur.

        batch_delim (str, optional): See 'delim' in 'get_file_content_batch'.

        batch_size (str, optional): See 'batch_size' in 'get_file_content_batch'.
    """
    with open(file_name, 'r') as fd:
        batch_iterator = get_file_content_batch(fd,
                                                delim=batch_delim,
                                                batch_size=batch_size)
        for nb, batch in enumerate(batch_iterator):
            for nl, line in enumerate(batch.split(batch_delim)):
                if field_delim is None:
                    yield line
                else:
                    if max_split is None:
                        yield line.split(field_delim)
                    else:
                        yield line.split(field_delim, max_split)


def get_file_content_batch(file_descriptor,
                           delim='\n',
                           batch_size=io.DEFAULT_BUFFER_SIZE):
    """
    Parse a file in batches containing up to N delimited elements.
    A batch always start after a delimiter character and ends either at
    a delimiter or EOF.

    Args:
        file_descriptor (file descriptor): File to read from.

    Kwargs:
        delim (str, optional): Delimiter for batch contents.

        batch_size (int, optional): Amount of bytes to read at a time.
    """
    batch = ''
    while True:
        # Read batch
        bytes_read = file_descriptor.read(batch_size)

        # EOF?
        if not bytes_read:
            # Last batch
            if batch:
                yield batch
            break

        batch += bytes_read

        # Find position of last delimiter
        delim_idx = batch.rfind(delim)

        # If delimiter is not found either:
        #   a) Last contents of file
        #   b) Batch does not contain delimiter, need more data
        if delim_idx >= 0:
            if delim_idx > 0:
                # Extract text (do not include delimiter)
                yield batch[:delim_idx]

            # Trim batch (skip delimiter)
            batch = batch[delim_idx+1:]


def get_mmap_content_batch_mp(map_file,
                              map_offsets,
                              delim='\n',
                              batch_size=mmap.ALLOCATIONGRANULARITY):
    """
    Parse a file in batches containing up to N delimited elements.
    A batch always start after a delimiter character and ends either at
    a delimiter or EOF.

    Args:
        file_descriptor (file descriptor): File to read from.

    Kwargs:
        delim (str, optional): Delimiter for batch contents.

        batch_size (int, optional): Amount of bytes to read at a time.
    """
    delim = delim.encode()
    end_idx = map_offsets[0]
    batch = b''
    while True:
        # EOF?
        start_idx = end_idx
        if start_idx >= map_offsets[1]:
            if batch:
                yield batch.decode()
            break

        end_idx = min(start_idx + batch_size, map_offsets[1])

        # Read batch
        batch += map_file[start_idx:end_idx]

        # Find position of last delimiter
        delim_idx = batch.rfind(delim)

        # If delimiter is not found either:
        #   a) Last contents of file
        #   b) Batch does not contain delimiter, need more data
        if delim_idx >= 0:
            if delim_idx > 0:
                # Extract text (do not include delimiter)
                yield batch[:delim_idx].decode()

            # Trim batch (skip delimiter)
            batch = batch[delim_idx+1:]


def get_mmap_content_batch(map_file,
                           map_size,
                           delim='\n',
                           batch_size=io.DEFAULT_BUFFER_SIZE):
    """
    Parse a file in batches containing up to N delimited elements.
    A batch always start after a delimiter character and ends either at
    a delimiter or EOF.

    Args:
        file_descriptor (file descriptor): File to read from.

    Kwargs:
        delim (str, optional): Delimiter for batch contents.

        batch_size (int, optional): Amount of bytes to read at a time.
    """
    delim = delim.encode()
    end_idx = 0
    batch = b''
    while True:
        # EOF?
        start_idx = end_idx
        if start_idx >= map_size:
            if batch:
                yield batch.decode()
            break

        end_idx = min(start_idx + batch_size, map_size)

        # Read batch
        batch += map_file[start_idx:end_idx]

        # Find position of last delimiter
        delim_idx = batch.rfind(delim)

        # If delimiter is not found either:
        #   a) Last contents of file
        #   b) Batch does not contain delimiter, need more data
        if delim_idx >= 0:
            if delim_idx > 0:
                # Extract text (do not include delimiter)
                yield batch[:delim_idx].decode()

            # Trim batch (skip delimiter)
            batch = batch[delim_idx+1:]


def reader_mp(fn, q, ms, dm):
    for content in get_file_content(fn, field_delim=dm, max_split=ms):
        try:
            q.put_nowait(content)
        except queue.Full as ex:
            q.put(content)
    q.put(None)


def extract_mrsty(mrsty_file, valid_cuis=None, valid_semtypes=None, mrsty_headers=HEADERS_MRSTY):
    """
    Args:
        mrsty_file (str): Path of UMLS MRSTY.RRF file

    Kwargs:
        valid_cuis (set, optional): Valid CUIs to include.
            If None, then all CUIs are included.

        valid_semtypes (set, optional): Valid semantic types to include.
            If None, then all semantic types are included.

    Note:
        * mrsty_headers should be automatically parsed from UMLS MRFILES.RRF.
    """
    cuisty = collections.defaultdict(set)

    method = 0
    chunk_size = 100000

    if method == 0:
        df = pandas.read_csv(mrsty_file, delimiter='|', names=mrsty_headers, usecols=['cui', 'sty'], index_col=False, memory_map=True, engine='c')

        for cui, sty in zip(df.cui, df.sty):
            if valid_cuis is not None and cui not in valid_cuis:
                continue
            if valid_semtypes is not None and sty not in valid_semtypes:
                continue
            cuisty[cui].add(sty)
    else:
        reader = pandas.read_csv(mrsty_file, delimiter='|', names=mrsty_headers, usecols=['cui', 'sty'], index_col=False, memory_map=True, engine='c', chunksize=chunk_size)

        for df in reader:
            for cui, sty in zip(df.cui, df.sty):
                if valid_cuis is not None and cui not in valid_cuis:
                    continue
                if valid_semtypes is not None and sty not in valid_semtypes:
                    continue
                cuisty[cui].add(sty)

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    return cuisty


def extract_mrsty6(mrsty_file, valid_cuis=None, valid_semtypes=None, mrsty_headers=HEADERS_MRSTY):
    """
    Args:
        mrsty_file (str): Path of UMLS MRSTY.RRF file

    Kwargs:
        valid_cuis (set, optional): Valid CUIs to include.
            If None, then all CUIs are included.

        valid_semtypes (set, optional): Valid semantic types to include.
            If None, then all semantic types are included.

    Note:
        * mrsty_headers should be automatically parsed from UMLS MRFILES.RRF.
    """
    cuisty = collections.defaultdict(set)

    # Multiprocessing partition
    file_size = os.path.getsize(mrsty_file)
    # num_procs = multiprocessing.cpu_count()
    num_procs = 4
    num_pages = math.ceil(file_size // mmap.ALLOCATIONGRANULARITY)
    pages_per_proc = num_pages // num_procs
    batch_size = pages_per_proc * mmap.ALLOCATIONGRANULARITY

    # Multiprocessing queue
    q = multiprocessing.Queue()

    workers = []
    with open(mrsty_file, 'r+b') as fd:
        with mmap.mmap(fd.fileno(), 0, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ) as mm:
            end_idx = 0
            for ip in range(num_procs):
                start_idx = end_idx
                if ip < num_procs - 1:
                    end_idx = mm[start_idx:start_idx + batch_size].rfind(b'\n')
                    if end_idx < 0:
                        print('ERROR: invalid partition for process')
                        end_idx = start_idx
                    else:
                        end_idx += 1
                else:
                    end_idx = file_size

                worker = multiprocessing.Process(target=get_file_content_mp,
                                                 args=(q,
                                                       mm,
                                                       (start_idx, end_idx),
                                                       mrsty_headers,
                                                       valid_cuis, valid_semtypes),
                                                 kwargs={'field_delim': '|'})
                workers.append(worker)
                worker.start()

            # Combine data from worker processes
            completed_procs = 0
            while True:
                data = q.get()
                if data is None:
                    completed_procs += 1
                    if completed_procs == num_procs:
                        break
                    continue
                for cui, sty in data:
                    cuisty[cui].add(sty)

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    return cuisty


def extract_mrsty5(mrsty_file, valid_cuis=None, valid_semtypes=None, mrsty_headers=HEADERS_MRSTY):
    """
    Args:
        mrsty_file (str): Path of UMLS MRSTY.RRF file

    Kwargs:
        valid_cuis (set, optional): Valid CUIs to include.
            If None, then all CUIs are included.

        valid_semtypes (set, optional): Valid semantic types to include.
            If None, then all semantic types are included.

    Note:
        * mrsty_headers should be automatically parsed from UMLS MRFILES.RRF.
    """
    cuisty = collections.defaultdict(set)

    # Multiprocessing partition
    file_size = os.path.getsize(mrsty_file)
    num_procs = multiprocessing.cpu_count()
    num_pages = math.ceil(file_size // mmap.ALLOCATIONGRANULARITY)
    pages_per_proc = num_pages // num_procs
    batch_size = pages_per_proc * mmap.ALLOCATIONGRANULARITY

    # Multiprocessing queue
    q = multiprocessing.Queue()

    workers = []
    with open(mrsty_file, 'r+b') as fd:
        with mmap.mmap(fd.fileno(), 0, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ) as mm:
            for ip in range(num_procs):
                start_idx = ip * batch_size
                if ip < num_procs - 1:
                    end_idx = start_idx + batch_size
                else:
                    end_idx = file_size

                worker = multiprocessing.Process(target=get_file_content_mp,
                                                 args=(q,
                                                       mm,
                                                       (start_idx, end_idx),
                                                       mrsty_headers,
                                                       valid_cuis, valid_semtypes),
                                                 kwargs={'field_delim': '|'})
                workers.append(worker)
                worker.start()

            # Combine data from worker processes
            completed_procs = 0
            while True:
                data = q.get()
                if data is None:
                    completed_procs += 1
                    if completed_procs == num_procs:
                        break
                    continue
                for cui, sty in data:
                    cuisty[cui].add(sty)

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    return cuisty


def extract_mrsty4(mrsty_file, valid_cuis=None, valid_semtypes=None, mrsty_headers=HEADERS_MRSTY):
    """
    Args:
        mrsty_file (str): Path of UMLS MRSTY.RRF file

    Kwargs:
        valid_cuis (set, optional): Valid CUIs to include.
            If None, then all CUIs are included.

        valid_semtypes (set, optional): Valid semantic types to include.
            If None, then all semantic types are included.

    Note:
        * mrsty_headers should be automatically parsed from UMLS MRFILES.RRF.
    """
    cuisty = collections.defaultdict(set)

    # Parse lines only until the fields we need, ignore remaining
    cui_idx = mrsty_headers.index('cui')
    sty_idx = mrsty_headers.index('sty')
    max_split = max(cui_idx, sty_idx) + 1

    # Multiprocessing queue
    q = multiprocessing.Queue()

    # P0: File reader
    reader = threading.Thread(target=reader_mp,
                              args=(mrsty_file, q, max_split, '|'))
    reader.start()

    # P1: Process/store contents
    while True:
        content = q.get()
        if content is None:
            break

        cui = content[cui_idx]
        if valid_cuis is not None and cui not in valid_cuis:
            continue
        sty = content[sty_idx]
        if valid_semtypes is not None and sty not in valid_semtypes:
            continue
        cuisty[cui].add(sty)

    reader.join()

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    return cuisty


def extract_mrsty3(mrsty_file, valid_cuis=None, valid_semtypes=None, mrsty_headers=HEADERS_MRSTY):
    """
    Args:
        mrsty_file (str): Path of UMLS MRSTY.RRF file

    Kwargs:
        valid_cuis (set, optional): Valid CUIs to include.
            If None, then all CUIs are included.

        valid_semtypes (set, optional): Valid semantic types to include.
            If None, then all semantic types are included.

    Note:
        * mrsty_headers should be automatically parsed from UMLS MRFILES.RRF.
    """
    cuisty = collections.defaultdict(set)

    # Parse lines only until the fields we need, ignore remaining
    cui_idx = mrsty_headers.index('cui')
    sty_idx = mrsty_headers.index('sty')
    max_split = max(cui_idx, sty_idx) + 1

    # Multiprocessing queue
    q = multiprocessing.Queue()

    # P0: File reader
    reader = multiprocessing.Process(target=reader_mp,
                                     args=(mrsty_file, q, max_split, '|'))
    reader.start()

    # P1: Process/store contents
    while True:
        content = q.get()
        if content is None:
            break

        cui = content[cui_idx]
        if valid_cuis is not None and cui not in valid_cuis:
            continue
        sty = content[sty_idx]
        if valid_semtypes is not None and sty not in valid_semtypes:
            continue
        cuisty[cui].add(sty)

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    return cuisty


def extract_mrsty2(mrsty_file, valid_cuis=None, valid_semtypes=None, mrsty_headers=HEADERS_MRSTY):
    """
    Args:
        mrsty_file (str): Path of UMLS MRSTY.RRF file

    Kwargs:
        valid_cuis (set, optional): Valid CUIs to include.
            If None, then all CUIs are included.

        valid_semtypes (set, optional): Valid semantic types to include.
            If None, then all semantic types are included.

    Note:
        * mrsty_headers should be automatically parsed from UMLS MRFILES.RRF.
    """
    cuisty = collections.defaultdict(set)

    # Parse lines only until the fields we need, ignore remaining
    cui_idx = mrsty_headers.index('cui')
    sty_idx = mrsty_headers.index('sty')
    max_split = max(cui_idx, sty_idx) + 1

    for content in get_file_content(mrsty_file,
                                    field_delim='|',
                                    max_split=max_split):
        cui = content[cui_idx]
        if valid_cuis is not None and cui not in valid_cuis:
            continue
        sty = content[sty_idx]
        if valid_semtypes is not None and sty not in valid_semtypes:
            continue
        cuisty[cui].add(sty)

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    return cuisty


def extract_mrsty1(mrsty_file, valid_cuis=None, valid_semtypes=None, mrsty_headers=HEADERS_MRSTY):
    """
    Args:
        mrsty_file (str): Path of UMLS MRSTY.RRF file

    Kwargs:
        valid_cuis (set, optional): Valid CUIs to include.
            If None, then all CUIs are included.

        valid_semtypes (set, optional): Valid semantic types to include.
            If None, then all semantic types are included.

    Note:
        * mrsty_headers should be automatically parsed from UMLS MRFILES.RRF.
    """
    cuisty = collections.defaultdict(set)
    with open(mrsty_file, 'r') as f:
        # Parse lines only until the fields we need, ignore remaining
        cui_idx = mrsty_headers.index('cui')
        sty_idx = mrsty_headers.index('sty')
        max_split = max(cui_idx, sty_idx) + 1

        for ln in f:
            content = ln.strip().split('|', max_split)
            cui = content[cui_idx]
            if valid_cuis is not None and cui not in valid_cuis:
                continue
            sty = content[sty_idx]
            if valid_semtypes is not None and sty not in valid_semtypes:
                continue
            cuisty[cui].add(sty)

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    return cuisty


def extract_mrsty0(mrsty_file, mrsty_headers=HEADERS_MRSTY):
    cuisty = collections.defaultdict(set)
    with open(mrsty_file, 'r') as f:
        # Parse lines only until the fields we need, ignore remaining
        cui_idx = mrsty_headers.index('cui')
        sty_idx = mrsty_headers.index('sty')
        max_split = max(cui_idx, sty_idx) + 1

        for ln in f:
            content = ln.strip().split('|', max_split)
            cui = content[cui_idx]
            sty = content[sty_idx]
            cuisty[cui].add(sty)

    # Profile
    if PROFILE > 1:
        print(f'Num unique CUIs: {len(cuisty)}')
        print(f'Num values in CUI-STY dictionary: {sum(len(v) for v in cuisty.values())}')
        print(f'Size of CUI-STY dictionary: {sys.getsizeof(cuisty)}')

    return cuisty


def extract_mrconso(mrconso_file, cuisty, mrconso_header=HEADERS_MRCONSO,
                    lowercase=False,
                    normalize_unicode=False,
                    language=['ENG']):
    with open(mrconso_file, 'r') as f:
        # Parse lines only until the fields we need, ignore remaining
        str_idx = mrconso_header.index('str')
        cui_idx = mrconso_header.index('cui')
        pref_idx = mrconso_header.index('ispref')
        lat_idx = mrconso_header.index('lat')
        max_split = max(str_idx, cui_idx, pref_idx, lat_idx) + 1

        processed = set()

        # Profile
        num_valid_lang = 0
        num_repeated_cuiterm = 0
        num_lines = 0

        for ln in f:
            # Profile
            num_lines += 1

            content = ln.strip().split('|', max_split)

            if content[lat_idx] not in language:
                continue

            # Profile
            num_valid_lang += 1

            cui = content[cui_idx]
            term = content[str_idx].strip()

            if (cui, term) in processed:
                # Profile
                num_repeated_cuiterm += 1
                continue

            processed.add((cui, term))

            if lowercase:
                term = term.lower()

            if normalize_unicode:
                term = unidecode(term)

            preferred = 1 if content[pref_idx] else 0

            yield (term, cui, cuisty[cui], preferred)

        # Profile
        if PROFILE > 1:
            print(f'Num lines to process: {num_lines}')
            print(f'Num valid language: {num_valid_lang}')
            print(f'Num repeated CUI-term: {num_repeated_cuiterm}')
            print(f'Num processed: {len(processed)}')
            print(f'Size of processed CUI-term: {sys.getsizeof(processed)}')


def dump_conso_sty(extracted_it, cuisty_dir,
                   bulk_size=1000, status_step=100000):
    # Profile
    prev_time = time.time()
    num_terms = 0

    cuisty_db = CuiSemTypesDB(cuisty_dir)
    terms = set()
    cui_bulk = []
    sty_bulk = []
    for i, (term, cui, stys, preferred) in enumerate(extracted_it, start=1):
        # Profile
        num_terms += 1

        terms.add(term)

        if len(cui_bulk) == bulk_size:
            cuisty_db.bulk_insert_cui(cui_bulk)
            cuisty_db.bulk_insert_sty(sty_bulk)
            cui_bulk = []
            sty_bulk = []
        else:
            cui_bulk.append((term, cui, preferred))
            sty_bulk.append((cui, stys))

        # Profile
        if PROFILE > 1 and i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')
            prev_time = curr_time

    # Flush remaining ones
    if len(cui_bulk) > 0:
        cuisty_db.bulk_insert_cui(cui_bulk)
        cuisty_db.bulk_insert_sty(sty_bulk)
        cui_bulk = []
        sty_bulk = []

        # Profile
        if PROFILE > 1:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')

    # Profile
    if PROFILE > 0:
        print(f'Num terms: {num_terms}')
        print(f'Num unique terms: {len(terms)}')
        print(f'Size of Simstring terms: {sys.getsizeof(terms)}')

    return terms


def dump_terms(simstring_dir, simstring_terms,
               status_step=100000):
    # Profile
    prev_time = time.time()

    ss_db = SimstringDBWriter(simstring_dir)
    for i, term in enumerate(simstring_terms, start=1):
        ss_db.insert(term)

        # Profile
        if PROFILE > 1 and i % status_step == 0:
            curr_time = time.time()
            print(f'{i}: {curr_time - prev_time} s, {(curr_time - prev_time) / status_step} s/term')
            prev_time = curr_time


def driver(opts):
    # UMLS files
    mrconso_file = os.path.join(opts.umls_dir, 'MRCONSO.RRF')
    mrsty_file = os.path.join(opts.umls_dir, 'MRSTY.RRF')

    # Create install directories for the two databases
    simstring_dir = os.path.join(opts.install_dir, 'umls-simstring.db')
    cuisty_dir = os.path.join(opts.install_dir, 'cui-semtypes.db')
    os.makedirs(simstring_dir)
    os.makedirs(cuisty_dir)

    print('Loading semantic types...')
    start = time.time()
    cuisty = extract_mrsty(mrsty_file)
    curr_time = time.time()
    print(f'Loading semantic types: {curr_time - start} s')

    sys.exit()

    print('Loading and parsing concepts...')
    start = time.time()
    conso_cuisty_iter = extract_mrconso(
                            mrconso_file, cuisty,
                            lowercase=opts.lowercase,
                            normalize_unicode=opts.normalize_unicode,
                            language=opts.language)
    terms = dump_conso_sty(conso_cuisty_iter, cuisty_dir)
    curr_time = time.time()
    print(f'Loading and parsing concepts: {curr_time - start} s')

    print('Writing Simstring database...')
    start = time.time()
    dump_terms(simstring_dir, terms)
    curr_time = time.time()
    print(f'Writing Simstring database: {curr_time - start} s')


if __name__ == '__main__':
    args = argparse.ArgumentParser()
    args.add_argument(
        '-u', '--umls_dir', required=True,
        help='Directory of UMLS RRF files'
    )
    args.add_argument(
        '-i', '--install_dir', required=True,
        help='Directory for installing QuickerUMLS files'
    )
    args.add_argument(
        '-l', '--lowercase', action='store_true',
        help='Consider only lowercase version of tokens'
    )
    args.add_argument(
        '-n', '--normalize-unicode', action='store_true',
        help='Normalize unicode strings to their closest ASCII representation'
    )
    args.add_argument(
        '-e', '--language', default=['ENG'], action='append', choices=LANGUAGES,
        help='Extract concepts of the specified language'
    )
    clargs = args.parse_args()

    if not os.path.exists(clargs.install_dir):
        print(f'Creating install directory: {clargs.install_dir}')
        os.makedirs(clargs.install_dir)
    elif len(os.listdir(clargs.install_dir)) > 0:
        print(f'Install directory ({clargs.install_dir}) is not empty...aborting')
        exit(1)

    if clargs.normalize_unicode:
        if not UNIDECODE_AVAIL:
            err = ("""'unidecode' is needed for unicode normalization
                   please install it via the 'pip install unidecode'
                   command.""")
            print(err, file=sys.stderr)
            exit(1)
        flag_fp = os.path.join(clargs.install_dir, 'normalize-unicode.flag')
        open(flag_fp, 'w').close()

    if clargs.lowercase:
        flag_fp = os.path.join(clargs.install_dir, 'lowercase.flag')
        open(flag_fp, 'w').close()

    flag_fp = os.path.join(clargs.install_dir, 'language.flag')
    with open(flag_fp, 'w') as f:
        f.write(os.linesep.join(clargs.language))

    start = time.time()
    driver(clargs)
    curr_time = time.time()
    print(f'Total runtime: {curr_time - start} s')
