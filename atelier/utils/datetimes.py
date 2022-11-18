#!/usr/bin/env python3
# -*- coding:utf-8 -*-
# ================================================================================================ #
# Project    : Atelier AI: Studio for AI Designers                                                 #
# Version    : 0.1.4                                                                               #
# Python     : 3.10.4                                                                              #
# Filename   : /datetimes.py                                                                       #
# ------------------------------------------------------------------------------------------------ #
# Author     : John James                                                                          #
# Email      : john.james.ai.studio@gmail.com                                                      #
# URL        : https://github.com/john-james-ai/atelier-ai                                         #
# ------------------------------------------------------------------------------------------------ #
# Created    : Sunday November 13th 2022 12:30:28 pm                                               #
# Modified   : Sunday November 13th 2022 03:17:02 pm                                               #
# ------------------------------------------------------------------------------------------------ #
# License    : MIT License                                                                         #
# Copyright  : (c) 2022 John James                                                                 #
# ================================================================================================ #
"""Module provides classes for manipulating and formatting datetime objects."""
from datetime import datetime
from dataclasses import dataclass

# ------------------------------------------------------------------------------------------------ #
SECONDS_PER_DAY = 86400
SECONDS_PER_HOUR = 3600
SECONDS_PER_MINUTE = 60
MINUTES_PER_HOUR = 60
HOURS_PER_DAY = 24

# ------------------------------------------------------------------------------------------------ #


@dataclass
class Duration:
    """Represents duration, given seconds"""

    seconds: int
    days: int = 0
    hours: int = 0
    minutes: int = 0

    def __post_init__(self) -> None:
        seconds = self.seconds
        self.seconds = seconds % SECONDS_PER_MINUTE
        self.minutes = int((seconds // SECONDS_PER_MINUTE) % MINUTES_PER_HOUR)
        self.hours = int((seconds // SECONDS_PER_HOUR) % HOURS_PER_DAY)
        self.days = seconds // SECONDS_PER_DAY

    def as_string(self, precision=3) -> str:
        """Returns the duration as a string."""

        dstring = "{} seconds.".format(round(self.seconds, precision))
        if self.minutes:
            dstring = "{} minutes, ".format(self.minutes) + dstring
        if self.hours:
            dstring = "{} hours, ".format(self.hours) + dstring
        if self.days:
            dstring = "{} days, ".format(self.days) + dstring
        return dstring


class Timer:
    """Timer object which tracks to the millisecond.

    Args:
        start (datetime): Start time as a datetime object.
        end (datetime): End time as a datetime object.

    """

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        self._started = None
        self._stopped = None
        self._seconds = None
        self._duration = None

    def start(self) -> None:
        self.reset()
        self._started = datetime.now()

    def stop(self) -> None:
        self._stopped = datetime.now()
        self._seconds = (self._stopped - self._started).total_seconds()
        self._duration = Duration(self._seconds)

    @property
    def started(self) -> datetime:
        """Returns start time as a datetime object or None if not started"""
        return self._started

    @property
    def stopped(self) -> datetime:
        """Returns stop time as a datetime object or None if not stopped"""
        return self._stopped

    @property
    def duration(self) -> Duration:
        return self._duration
