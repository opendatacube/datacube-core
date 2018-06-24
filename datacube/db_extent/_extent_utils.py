from pandas import Timestamp
from datetime import date
import itertools


def peek_generator(iterable):
    """ See https://:stackoverflow.com/questions/661603/how-do-i-know-if-a-generator-is-empty-from-the-start """
    try:
        first = next(iterable)
    except StopIteration:
        return None
    return itertools.chain([first], iterable)


def parse_date(d):
    """
    Parses a time representation into a datetime object
    :param d: A time value
    :return datetime: datetime representation of given time value
    """
    if not isinstance(d, date):
        t = Timestamp(d)
        d = date(year=t.year, month=t.month, day=t.day)
    return d
