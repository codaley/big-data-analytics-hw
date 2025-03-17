# REDUCER

#!/usr/bin/env python

import sys

(lastKey, maxValue) = (None, -1)  # initialize 'maxValue' to -1 to handle negative values

for line in sys.stdin:
    line = line.strip()
    (key, value) = line.split('\t')

    try:
        value = float(value)  # convert 'value' to float to handle decimal values
    except ValueError:
        # skip lines where 'value' cannot be converted to float
        continue

    if lastKey and lastKey != key:
        print('%s\t%s' % (lastKey, maxValue))
        (lastKey, maxValue) = (key, value)
    else:
        (lastKey, maxValue) = (key, max(maxValue, value))

if lastKey:
    print('%s\t%s' % (lastKey, maxValue))