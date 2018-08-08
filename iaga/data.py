# iaga.data
# Base IAGA data file reader.
#
# Author:  Benjamin Bengfort <benjamin@bengfort.com>
# Created: Wed Aug 08 09:27:52 2018 -0400
#
# ID: data.py [] benjamin@bengfort.com $

"""
Base IAGA data file reader.
"""

##########################################################################
## Imports
##########################################################################

import os
import re
import gzip
import numpy as np

from datetime import datetime
from .utils import memoized

# RE to parse a data file basename
DNAME = re.compile(r'([a-z]+)(\d+)vsec\.sec\.gz', re.I)


##########################################################################
## IAGA Data File
##########################################################################

class DataFile(object):
    """
    A DataFile object wraps a gzip compressed IAGA 2002 exchange format and
    allows multiple read access to the header and rows without decompressing
    the file on disk. The object also handles parsing the rows into numpy
    numeric objects for analytics processing.

    Note that because of the GZip compression, the entire file must be held in
    memory at one time, streaming access is not available. Therefore the
    object exposes a context manager for automatically opening and closing the
    data file when done.
    """

    def __init__(self, path):
        if not isinstance(path, DataPath):
            path = DataPath(path)

        self.path = path
        self._file = None

    @memoized
    def fields(self):
        """
        Searches for the field names as the last row in the header but
        modifies the field information according to our dtype modifications.
        """
        fields = list(self.header())[-1].rstrip("|").strip().split()

        # NOTE: we're manually modifying the dtype of DATE TIME and DOY.
        # This may not be general for all IAGA formats.
        return ["DATETIME", "DOY"] + fields[3:]

    @memoized
    def meta(self):
        """
        Searches for key/value meta data from the header.
        """
        meta = {}

        # Ignore last line as we presume that is the field names
        for line in list(self.header())[:-1]:
            line = line.rstrip("|").strip() # Remove extra characters
            if line.startswith("#"): continue # Ignore comments

            # Hopefully the keys and values are sep by more than 2 spaces ...
            line = line.split("  ")
            meta[line[0]] = " ".join(line[1:]).strip()

        return meta

    @property
    def name(self):
        """
        Return the basename of the file without the .gz extension
        """
        name, _ = os.path.splitext(os.path.basename(self.path))
        return name

    @memoized
    def nrecords(self):
        """
        Count the number of records in the data file (also returned by len)
        """
        self.ready() # check if we're ready
        return sum(
            1 for line in self._file
            if line.strip() and not line.endswith("|") and not line.startswith("#")
        )

    def header(self):
        """
        Return the header of the data file if any exists and store the field
        names if discovered on the last line of the header.
        """
        self.ready() # check if we're ready
        for line in self._file:
            if not line.strip(): continue
            if line.endswith("|"):
                yield line
            else:
                break

    def rows(self):
        """
        Returns the data rows parsed as a numpy array.
        """
        self.ready() # check if we're ready
        self._nrecords = 0

        for line in self._file:
            # Skip headers, comments, and empty lines
            if not line.strip() or line.endswith("|") or line.startswith("#"):
                continue

            # Split line and parse components into numpy dtype
            line = line.split()
            row  = [np.datetime64("T".join(line[:2])), np.int(line[2])]
            row += list(map(np.float64, line[3:]))

            # Count number of records and yield array
            self._nrecords += 1
            yield np.array(row)

    def open(self):
        """
        Open the data file and read in the lines.
        """
        with gzip.open(self.path, 'rb') as f:
            self._file = f.read().decode('utf8').split("\n")

    def close(self):
        """
        Close the data file and free any used memory.
        """
        # Free memory as best we can
        del self._file
        self._file = None

    def ready(self):
        if self._file is None:
            raise ValueError("data file is not ready, has it been opened?")
        return True

    def __iter__(self):
        for row in self.rows():
            yield row

    def __len__(self):
        return self.nrecords

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, *args, **kwargs):
        self.close()


##########################################################################
## Data Path Helper
##########################################################################

class DataPath(str):
    """
    A helper method that wraps a string to perform path checks.
    """

    def exists(self):
        return os.path.exists(self)

    @property
    def basename(self):
        return os.path.basename(self)

    @property
    def dirname(self):
        return os.path.dirname(self)

    def splitext(self):
        return os.path.splitext(self)

    @memoized
    def parse(self):
        match = DNAME.match(self.basename)
        if match is None:
            raise ValueError("could not parse '{}'".format(self.basename))
        return match.groups()

    @property
    def observatory(self):
        return self.parse[0].upper()

    @property
    def date(self):
        return datetime.strptime(self.parse[1], "%Y%m%d").date()


if __name__ == '__main__':
    import json
    path = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample", "HON", "OneSecond", "hon20140407vsec.sec.gz")
    with DataFile(path) as f:
        print(f.fields)
        print(json.dumps(f.meta, indent=2))
        print("{:,} records".format(len(f)))
        print("\n".join(f.header()))
