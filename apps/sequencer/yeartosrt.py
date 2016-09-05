#!/usr/bin/env python3
"""
Convert dates from filenames to SRT
"""
import sys
import re

from datetime import datetime, timedelta, time, date

DISPLAY = '%d %B %Y'
TIMEFMT = '%H:%M:%S,%f'
SRT ="""
{i}
{start} --> {end}
{txt}"""
PATTERN = '\d\d\d\d'


def run(filenames, timefmt=None, timeincr=1):
    if timeincr < 1.0:
        incr = timedelta(microseconds=timeincr*1000000)
    else:
        incr = timedelta(seconds=timeincr)
    st = time(0, 0, 0, 0)
    for i, fn in enumerate(filenames):
        end_time = (datetime.combine(date.today(), st) + incr).time()

        match = re.findall(PATTERN, fn)
        txt = next(iter(match))

        print(SRT.format(i=i, txt=txt,
                         start=st.strftime(TIMEFMT)[:-3],
                         end=end_time.strftime(TIMEFMT)[:-3]))
        st = end_time

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()

    parser.add_argument('-timeincr',
                        help='time increment in seconds',
                        type=float,
                        default=1,
                        required=False)

    args = parser.parse_args()
    kwargs = vars(args)
    filename = kwargs.pop('fn', None)

    if filename:
        filenames = [filename]
    else:
        filenames = [fn.strip() for fn in sys.stdin.readlines()]

    run(filenames, **kwargs)
