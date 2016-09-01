"""
Test date sequence generation functions as used by statistics apps


"""
from dateutil.parser import parse
from datacube.dates import date_sequence


def test_stats_dates():
    # Winter for 1990
    winter_1990 = list(date_sequence(start='1990-06-01', end='1990-08', step_size='3m', stats_duration='3m'))
    assert winter_1990 == [(parse('1990-06-01'), parse('1990-08-31'))]

    # Every winter from 1990 - 1992
    three_years_of_winter = list(date_sequence(start='1990-06-01', end='1992-08', step_size='1y', stats_duration='3m'))
    assert three_years_of_winter == [(parse('1990-06-01'), parse('1990-08-31')),
                                     (parse('1991-06-01'), parse('1991-08-31')),
                                     (parse('1992-06-01'), parse('1992-08-31'))]

    # Full years from 1990 - 1994
    five_full_years = list(date_sequence(start='1990-01-01', end='1994', step_size='1y', stats_duration='1y'))
    assert five_full_years == [(parse('1990-01-01'), parse('1990-12-31')),
                               (parse('1991-01-01'), parse('1991-12-31')),
                               (parse('1992-01-01'), parse('1992-12-31')),
                               (parse('1993-01-01'), parse('1993-12-31')),
                               (parse('1994-01-01'), parse('1994-12-31'))]

    # Every season (three months), starting in March, from 1990 until end 1992-02
    two_years_of_seasons = list(date_sequence(start='1990-03-01', end='1992-02', step_size='3m', stats_duration='3m'))
    assert len(two_years_of_seasons) == 8
    assert two_years_of_seasons == [(parse('1990-03-01'), parse('1990-05-31')),
                                    (parse('1990-06-01'), parse('1990-08-31')),
                                    (parse('1990-09-01'), parse('1990-11-30')),
                                    (parse('1990-12-01'), parse('1991-02-28')),
                                    (parse('1991-03-01'), parse('1991-05-31')),
                                    (parse('1991-06-01'), parse('1991-08-31')),
                                    (parse('1991-09-01'), parse('1991-11-30')),
                                    (parse('1991-12-01'), parse('1992-02-29'))]  # Leap year!

    # Every month from 1990-01 to 1990-06
    monthly = list(date_sequence(start='1990-01-01', end='1990-06', step_size='1m', stats_duration='1m'))
    assert len(monthly) == 6

    # Complex
    # I want the average over 5 years
