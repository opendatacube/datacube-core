class ZerosReaderDriver(object):
    def __init__(self):
        self.name = 'ZerosReader'
        self.protocols = ['zero']
        self.formats = ['0']

    def supports(self, protocol, format):
        return protocol == 'zero'

    def new_datasource(self, dataset, band_name):
        return None  # TODO


def init_driver():
    return ZerosReaderDriver()
