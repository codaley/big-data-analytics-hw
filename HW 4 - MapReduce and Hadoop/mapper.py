# MAPPER

#!/usr/bin/env python

import sys

# skip header row
next(sys.stdin)

for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # split line into columns
    columns = line.split(",")

    # assign 'stock_symbol' and 'stock_price_high' to variables
    stock_symbol = columns[1]
    stock_price_high = columns[4]

    # print 'stock_symbol' and 'stock_price_high'
    print('%s\t%s' % (stock_symbol, stock_price_high))