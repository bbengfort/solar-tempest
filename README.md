# Solar Tempest

**Predict solar weather by observing the earth's magnetosphere.**

This package contains sample USGS data in `fixtures/sample`, organized as compressed IAGA 2002 data files in `<OBSERVATORY>/OneSecond/` directories. This data structure can be read with the utilities in the `iaga` package, particularly the `DataFile` and `GlobalData` objects.

You can use the `DataFile` object to read a single compressed IAGA 2002 data file without decompressing the file to disk as follows:

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
print(data.observatories)
```
