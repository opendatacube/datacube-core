# TODO: all time stats

from .impl import Transformation


def year(time):
    return time.astype('datetime64[Y]')


def month(time):
    return time.astype('datetime64[M]')


def week(time):
    return time.astype('datetime64[W]')


def day(time):
    return time.astype('datetime64[D]')


class Mean(Transformation):
    """
    Take the mean of the measurements.
    """

    def __init__(self, dim='time'):
        self.dim = dim

    def measurements(self, input_measurements):
        return input_measurements

    def compute(self, data):
        return data.mean(dim=self.dim)
