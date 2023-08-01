import sys

from time import monotonic
from datacube import Datacube
from datacube.model import Range
from datetime import timezone
from datetime import datetime
from odc.geo.geom import CRS, polygon


def benchmark(test, dc, label, n):
    total = 0.0
    total_first = 0.0
    last_count = None
    for i in range(n):
        start = monotonic()
        # count, first = test(dc)
        count = 0
        first = None
        for ds in test(dc):
            if not count:
                first = monotonic()
            count += 1
        if count == 0:
            first = start
        end = monotonic()
        if last_count and count != last_count:
            print(f"Count mismatch in {label}: {count} vs {last_count}")
        last_count = count
        print(f"Test {label}#{i+1}: {end-start}s   ({first-start}s to first returned dataset)")
        total += end - start
        total_first += first - start
    print(f"Test {label}-count: {count} rows")
    print(f"Test {label}-avg: {total/n}s  ({total/(n*count)})s/row")
    print(f"Test {label}-avg-to-first-return: {total_first/n}s  ({total/(n*count)})s/row")
    print()
    print("-----------------------------------------------------------------")


def test_less_than(dc):
    return dc.index.datasets.search(
        product='ga_ls8c_ard_3',
        cloud_cover=Range(None, 0.2)
    )


def test_geospatial_search(dc):
    return dc.index.datasets.search(
        product='ga_ls8c_ard_3',
        lat=Range(-30.0, -25.0),
        lon=Range(140.0, 145.0),
    )


def test_offset_geom(dc):
    if dc.index.supports_external_lineage:
        return dc.index.datasets.search(
            product='ga_ls8c_ard_3',
            geometry=polygon(
                [
                    [140.0, -25.0],
                    [142.0, -25.0],
                    [145.0, -30.0],
                    [145.0, -30.0],
                    [140.0, -25.0],
                ],
                crs=CRS("epsg:4326")
            )
        )
    else:
        return dc.index.datasets.search(
            product='ga_ls8c_ard_3',
            lat=Range(-30.0, -25.0),
            lon=Range(140.0, 145.0),
        )


def test_temporal_search(dc):
    return dc.index.datasets.search(
        product='ga_ls8c_ard_3',
        time=Range(datetime(2016, 1, 1, tzinfo=timezone.utc), datetime(2016, 4, 5, tzinfo=timezone.utc)),
    )


def main(args):
    if args:
        env = args.pop()
    else:
        env = "datacube_real"
    print("Testing on database ", env)
    dc = Datacube(env=env)
    benchmark(test_less_than, dc, "less_than", 20)
    benchmark(test_geospatial_search, dc, "geospatial", 20)
    benchmark(test_temporal_search, dc, "temporal", 20)
    benchmark(test_offset_geom, dc, "geom", 20)


if __name__ == "__main__":
    args = sys.argv[1:]
    main(args)

# For custom CRS search tests (TODO)
# BoundingBox(
#       left=762759.2567816022, bottom=-3326371.8490792206,
#       right=1295116.9248742603, top=-2727561.09954842, crs=CRS('EPSG:3577'))
