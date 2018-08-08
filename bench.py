#!/usr/bin/env python
# Run with mprof run python bench.py
# Then use mprof plot to show memory usage.

from iaga import GlobalData
from iaga.utils import Timer

if __name__ == '__main__':
    rows = 0
    data = GlobalData("fixtures/sample")

    with Timer() as timer:
        for record in data:
            rows += 1

    print("parsed {} rows from {} files in {}".format(rows, len(data.files), timer))
