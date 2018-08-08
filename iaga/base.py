# iaga.base
# Implements a global data reader for IAGA 2002 data files
#
# Author:  Benjamin Bengfort <benjamin@bengfort.com>
# Created: Wed Aug 08 10:44:23 2018 -0400
#
# ID: base.py [] benjamin@bengfort.com $

"""
Implements a global data reader for IAGA 2002 data files
"""

##########################################################################
## Imports
##########################################################################

import os
import glob
import numpy as np

from itertools import groupby
from operator import attrgetter

from .utils import memoized
from .data import DataFile, DataPath

# Glob pattern for listing data files
VSEC = "**/OneSecond/*vsec.sec.gz"


##########################################################################
## Global Data Reader
##########################################################################

class GlobalData(object):
    """
    Allows reads of global observatory data stored in a directory, aligning
    the data and the rows so that they're read in a chronological order.
    """

    def __init__(self, root):
        self.root = root

    @property
    def files(self):
        """
        Returns a list of data files found in the specified directory, sorted
        """
        match = os.path.join(self.root, VSEC)
        paths = [DataPath(path) for path in glob.glob(match)]
        paths.sort(key=attrgetter('date', 'observatory'))
        return paths

    @memoized
    def observatories(self):
        """
        Returns the IAGA Code for the observatory that should be the directory
        names stored in the root of the global data directory.
        """
        return [
            name for name in os.listdir(self.root)
            if os.path.isdir(os.path.join(self.root, name))
        ]

    @memoized
    def fields(self):
        """
        Returns the fields from the first batch of data files.
        """
        for _, paths in groupby(self.files, attrgetter("date")):
            # Create data files for each path in the group
            data = [DataFile(path) for path in paths]

            # Open the data files
            for d in data: d.open()

            # Get the headers from each data file
            arrs = [d.fields for d in data]
            fields = np.concatenate([arrs[0][:2]] + [arr[2:] for arr in arrs])

            # Close and break
            for d in data: d.close()
            return fields

    def records(self):
        """
        Records opens a file per observatory, grouping them by date, then
        joins the data together, yielding a complete array. Note that
        currently no validation is being done.
        """
        # TODO: do better at validation, e.g. missing observatories or records
        # Ensure that the data is fully aligned and consistent
        # Create headers so that the data is named structurally
        n_observatories = len(self.observatories)

        # Group the paths by their date
        for date, paths in groupby(self.files, attrgetter("date")):
            # Create data files for each path in the group
            data = [DataFile(path) for path in paths]

            # Check to make sure the expected number of observatories exists.
            if len(data) != n_observatories:
                raise ValueError("observatory records are missing at {}".format(date))

            # Open the data files
            for d in data: d.open()

            # Loop through rows and concatenate
            for arrs in zip(*[d.rows() for d in data]):
                dt, doy = arrs[0][:2]

                # Ensure DATETIME and DOY are equal
                if not all(arr[0]==dt for arr in arrs):
                    raise ValueError("data is misaligned at {}".format(dt))

                if not all(arr[1]==doy for arr in arrs):
                    raise ValueError("data is misaligned at DOY {}".format(doy))

                yield np.concatenate([arrs[0][:2]] + [arr[2:] for arr in arrs])

            # Close the data files
            for d in data: d.close()

    def __iter__(self):
        return self.records()


if __name__ == '__main__':
    from .utils import Timer
    SAMPLE = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample")
    data = GlobalData(SAMPLE)
    print(data.fields)

    with Timer() as timer:
        records = np.array(list(data.records()))

    print("loaded data of shape {} in {}".format(records.shape, timer))
