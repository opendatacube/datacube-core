from collections import namedtuple

Range = namedtuple('Range', ('begin', 'end'))
Variable = namedtuple('Variable', ('dtype', 'nodata', 'dims', 'units'))
CellIndex = namedtuple('CellIndex', ('x', 'y'))
