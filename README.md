# Solar Tempest

**Predict solar weather by observing the earth's magnetosphere.**

This package contains sample USGS data in `fixtures/sample`, organized as compressed IAGA 2002 data files in `<OBSERVATORY>/OneSecond/` directories. This data structure can be read with the utilities in the `iaga` package, particularly the `DataFile` and `GlobalData` objects.

You can use the `DataFile` object to read a single compressed [IAGA 2002](https://www.ngdc.noaa.gov/IAGA/vdat/IAGA2002/iaga2002format.html) data file without decompressing the file to disk as follows:

```python
import json
from iaga import Datafile

# Use the context manager to ensure memory is cleaned up when done
with DataFile("fixtures/sample/HON/OneSecond/hon20140407vsec.sec.gz") as f:
    # Print meta data from header of data file
    print(json.dumps(f.meta))

    # Print the field names described in the file
    print(f.fields)

    # Loop over all the parsed records, which are numpy arrays
    for record in f:
        # Do something with record
```

The `GlobalData` utility manages the data directory as a whole, opening data files sorted by the date specified in their file name so that all data is read in chronological order.

```python
from iaga import GlobalData

data = GlobalData("fixtures/sample")

# Show the observatories (directories) being managed
print(data.observatories)

# Print the field names describing the record
print(data.fields)

# Loop over all joined records, which are numpy arrays
# Note that the records rows should be ordered by timestamp
# The record columns should be HDZF records for each observatory, ordered
# by the observatory name (alphabetically)
for record in data:
    # Do something with record
```

## Benchmark

This implementation is _memory efficient_, though unfortunately not time efficient. The memory performance for a global read of 14 observatories worth of data is as follows:

![Memory Performance](fixtures/memory_benchmark.png)

As you can see, the memory usage never really goes above 250MB and drops as each file is opened and closed.
