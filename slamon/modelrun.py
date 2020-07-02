#!/usr/bin/env python3
""" Classes to describe model runs, where to fetch metadata from thredds and
    which statuspage components they relate to.
"""
# pylint: disable=C0301, C0111

__author__ = "Christian Skarby"
__version__ = "0.2.0"
__license__ = "GPLv2+"


import datetime


class ModelRun:
    """ Abstract class for model run results
    """

    NAME = 'MODEL NAME'
    EXPECTED = datetime.timedelta(hours=2, minutes=20)
    WARNING_AFTER = datetime.timedelta(minutes=15)  # relative to expected
    ERROR_AFTER = datetime.timedelta(hours=1) # relative to expected
    PATTERN = 'filename_%Y%m%dT%HZ.nc'  # https://docs.python.org/3/library/datetime.html#strftime-strptime-behavior
    URL = 'http://thredds/catalog.xml'
    STATUSPAGE_ID = None
    INIT_HOURS = [0, 6, 12, 18]

    def __init__(self):
        self.bulletin = False

    @classmethod
    def required(cls, datetime_):
        """ Returns a ModelRun instance with bulletin date set to the most
            recent required modelrun at the given datetime_ that will not
            trigger warnings.

        >>> ModelRun.required(datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)).bulletin
        datetime.datetime(1969, 12, 30, 18, 0, tzinfo=datetime.timezone.utc)
        >>> ModelRun.required(datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + ModelRun.EXPECTED).bulletin
        datetime.datetime(1969, 12, 30, 18, 0, tzinfo=datetime.timezone.utc)
        >>> ModelRun.required(datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc) + ModelRun.EXPECTED + ModelRun.WARNING_AFTER).bulletin
        datetime.datetime(1970, 1, 1, 0, 0, tzinfo=datetime.timezone.utc)
        >>> ModelRun.required(datetime.datetime(1970, 1, 1, hour=18, minute=12, tzinfo=datetime.timezone.utc) + ModelRun.EXPECTED + ModelRun.WARNING_AFTER).bulletin
        datetime.datetime(1970, 1, 1, 18, 0, tzinfo=datetime.timezone.utc)
        """
        assert datetime_.tzinfo is not None
        utctime = datetime_.astimezone(datetime.timezone.utc) - cls.EXPECTED - cls.WARNING_AFTER # ModelRun is not required immedeately after init...
        init_hours = list(cls.INIT_HOURS)
        init_hours.sort(reverse=True)  # The following depends on reverse sorted init_hours, i.e. last first
        obj = cls()
        for prev_hour in init_hours:
            prev = datetime.datetime(datetime_.year, datetime_.month, datetime_.day, hour=prev_hour, tzinfo=datetime.timezone.utc)
            if utctime >= prev:
                obj.bulletin = prev
                break
        if not obj.bulletin:
            yesterday = utctime - datetime.timedelta(days=1)
            obj.bulletin = datetime.datetime(yesterday.year, yesterday.month, yesterday.day, hour=init_hours[0], tzinfo=datetime.timezone.utc)
        return obj

    def prev(self):
        """ Previous scheduled bulletin

        >>> dt = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        >>> mr = ModelRun()
        >>> mr.bulletin = dt
        >>> mr.prev().bulletin
        datetime.datetime(1969, 12, 30, 18, 0, tzinfo=datetime.timezone.utc)
        """
        assert self.bulletin is not False
        return self.required(self.bulletin)  # relying on that the result is not required immedeately

    def is_delayed(self, datetime_):
        """ Returns True if the bulletin was delayed at the time datetime_

        >>> mr = ModelRun()
        >>> now = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)
        >>> mr.bulletin = now
        >>> mr.is_delayed(now)
        False
        >>> mr.is_delayed(now + mr.EXPECTED + mr.ERROR_AFTER)
        True
        """
        assert self.bulletin is not False
        assert datetime_.tzinfo is not None
        return self.bulletin + self.EXPECTED + self.ERROR_AFTER <= datetime_


class MEPS(ModelRun):
    """ Abstract class for the MEPS family of model runs
    """
    URL = 'https://thredds.met.no/thredds/catalog/mepslatest/catalog.xml'
    INIT_HOURS = range(0, 24, 3)  # Every 3rd hour

class MEPSdet(MEPS):
    NAME = 'MEPS deterministic'
    EXPECTED = datetime.timedelta(hours=3, minutes=00)  # Statkraft SLA 03:30, Statnett SLA 03:00
    PATTERN = 'meps_det_2_5km_%Y%m%dT%HZ.ncml'
    STATUSPAGE_ID = 'bcsflmrp5rgk'


class MEPSdetpp(MEPS):
    NAME = 'MEPS deterministic post processed'
    EXPECTED = datetime.timedelta(hours=0, minutes=45)  # Statkraft SLA 02:25, Statnett SLA 00:45
    URL = 'https://thredds.met.no/thredds/catalog/metpplatest/catalog.xml'
    PATTERN = 'met_forecast_1_0km_nordic_%Y%m%dT%HZ.nc'
    STATUSPAGE_ID = 'hdt6qj6f7zv5'
    INIT_HOURS = range(0, 24)  # Every hour


class MEPSens(MEPS):
    NAME = 'MEPS ensemble'
    EXPECTED = datetime.timedelta(hours=5, minutes=10)  # Statkraft SLA, arkivref: 2017/1506-14
    PATTERN = 'meps_lagged_6_h_subset_2_5km_%Y%m%dT%HZ.ncml'
    STATUSPAGE_ID = '6bb2dq9t7vx9'


class AromeArctic(ModelRun):
    """ Abstract class for the AA family of model runs
    """
    URL = 'https://thredds.met.no/thredds/catalog/aromearcticlatest/catalog.xml'


class AAdet(AromeArctic):
    NAME = 'Arome Arctic deterministic'
    EXPECTED = datetime.timedelta(hours=3, minutes=30)  # eivinds@met.no
    PATTERN = 'arome_arctic_extracted_2_5km_%Y%m%dT%HZ.nc'
    STATUSPAGE_ID = '5w7hh6w1f3fr'


class AAdetpp(AromeArctic):
    NAME = 'Arome Arctic deterministic post processed'
    EXPECTED = datetime.timedelta(hours=3, minutes=30)  # eivinds@met.no
    PATTERN = 'arome_arctic_pp_2_5km_%Y%m%dT%HZ.nc'
    STATUSPAGE_ID = 'n1wsnn6gr8p9'


if __name__ == "__main__":
    import doctest
    doctest.testmod()
