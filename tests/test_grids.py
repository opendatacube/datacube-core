from collections import namedtuple

# Implementation code

TileDef = namedtuple('TileDef', 'xstart ystart xwidth ywidth')

def create_grid_from_extent(width, height, xtile, ytile):

    return [TileDef(xstart, ystart, xtile, ytile)
            for xstart in xrange(0, width, xtile)
            for ystart in xrange(0, height, ytile)]


def round_up_divide(a, b):
    """Divide a / b and round up. Integers"""
    return (a+(-a%b))//b

## Test Code
ImageDef = namedtuple('ImageDef', 'width height tilewidth tileheight')
def test_simple_grid():
    imagedef = ImageDef(1000, 1000, 100, 100)
    create_and_validate_grid(imagedef)

def test_trivial_grid():
    imagedef = ImageDef(100, 100, 100, 100)
    create_and_validate_grid(imagedef)

def test_slightly_larger_grid():
    imagedef = ImageDef(1001, 1000, 100, 100)
    create_and_validate_grid(imagedef)


def create_and_validate_grid(imagedef):
    tiles = create_grid_from_extent(*imagedef)

    num_expected_tiles = round_up_divide(imagedef.width, imagedef.tilewidth) * round_up_divide(imagedef.height, imagedef.tileheight)
    assert(len(tiles) == num_expected_tiles)
    total_width = 0
    for t in tiles:
        if t.ystart == 0:
            total_width += t.xwidth
        assert(type(t) == TileDef)
        assert(t.xwidth <= imagedef.tilewidth)
        assert(t.xstart < imagedef.width)
        assert(t.xstart + t.xwidth <= imagedef.width + imagedef.tilewidth)

    assert(total_width >= imagedef.width)


def test_expanded_extents():
    pass

def test_gridded_geo_box():
    pass