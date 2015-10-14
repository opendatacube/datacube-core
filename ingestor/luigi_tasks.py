
import luigi

import logging

_log = logging.getLogger()

class ImportLandsatScene(luigi.Task):
    dataset_path = luigi.Parameter()

    def requires(self):
        yield Reproject(dataset_path)
        yield Tile(dataset_path)
        yield ImportToNetCDFs(dataset_path)
        yield RecordInDatabase(dataset_path)


if __name__ == '__main__':
    luigi.run()