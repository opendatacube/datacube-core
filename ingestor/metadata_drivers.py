from netCDF4 import Dataset, num2date, date2num, date2index
import eodatasets
from eodatasets import type as ptype

class BomModisDriver(eodatasets.DatasetDriver):
    """
    Extend EODatasets to read metadata about alternative inputs for the datacube

    In this case, Bom-Modis data
    """

    def get_id(self):
        return 'bom_modis'

    def expected_source(self):
        """
        Expected source dataset (driver).
        :rtype: DatasetDriver
        """
        return None

    def get_ga_label(self, dataset):
        return dataset.ga_label

    def fill_metadata(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type path: Path
        :rtype: ptype.DatasetMetadata
        """

        if not dataset.extent:
            dataset.extent = ptype.ExtentMetadata()

        with Dataset(path) as nco:
            lats = nco.variables['latitude']
            lons = nco.variables['longitude']
            times = nco.variables['time']
            minlat, maxlat, minlon, maxlon = lats[0], lats[-1], lons[0], lons[-1]
            time = num2date(times[0], times.units, times.calendar)

        return dataset

    def to_band(self, dataset, path):
        """
        :type dataset: ptype.DatasetMetadata
        :type final_path: pathlib.Path
        :rtype: ptype.BandMetadata
        """
        pass
