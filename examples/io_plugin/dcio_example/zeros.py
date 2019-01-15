""" Sample plugin "reads" zeros each time every time

TODO: not implemented yet
"""


class ZerosReaderDriver(object):
    def __init__(self):
        self.name = 'ZerosReader'
        self.protocols = ['zero']
        self.formats = ['0']

    def supports(self, protocol, fmt):
        return protocol == 'zero'

    def new_datasource(self, band):
        return None  # TODO


def init_driver():
    return ZerosReaderDriver()
