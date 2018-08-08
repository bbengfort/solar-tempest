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

from operator import attrgetter
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

    @property
    def observatories(self):
        """
        Returns the IAGA Code for the observatory that should be the directory
        names stored in the root of the global data directory.
        """
        return [
            name for name in os.listdir(self.root)
            if os.path.isdir(os.path.join(self.root, name))
        ]


if __name__ == '__main__':
    SAMPLE = os.path.join(os.path.dirname(__file__), "..", "fixtures", "sample")
    data = GlobalData(SAMPLE)

    for f in data.files:
        print(f.basename)
